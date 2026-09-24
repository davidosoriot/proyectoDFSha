"""CatalogService: UNICO punto de mutacion del catalogo.

Cualquier cambio de estado del catalogo pasa por CatalogService.apply(...).
El metodo escribe el evento correspondiente en la tabla events y luego aplica
la mutacion SQL, todo en la misma transaccion. Hoy los eventos se quedan con
propagated=0. En el Hito 3, un propagador leera esos eventos y los enviara al
otro dominio o a los seguidores del ControlNode replicado.

Regla: si necesitas mutar el catalogo, hacelo por aca. No hay INSERT ni UPDATE
de tablas del catalogo por fuera.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from sqlalchemy.exc import IntegrityError

from ..config import settings
from ..db.models import (
    Block,
    DataNode,
    Directory,
    EventType,
    NodeStatus,
    ObjectRow,
    ObjectStatus,
    Replica,
    ReplicaStatus,
)
from ..db.repository import Repository
from ..errors import (
    ConflictError,
    InsufficientResourcesError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
)


# --- DTOs de entrada ---


@dataclass
class BlockInput:
    block_id: str
    index: int
    size: int
    checksum: str


@dataclass
class ObjectInitInput:
    owner_id: str
    object_key: str
    total_size: int
    block_size: int
    global_checksum: str
    blocks: List[BlockInput]


@dataclass
class PlacementDecision:
    """Resultado del PlacementService, consumido por init_object."""

    block_id: str
    replica_node_ids: List[str]  # orden importa: [0] primaria, [-1] cross-domain


# --- Locks compartidos por proceso ---
#
# Serializan la asignacion de version en commit para dos commits concurrentes
# de la misma (owner_id, object_key). Vive a nivel de proceso: correcto
# mientras haya un unico ControlNode. Cuando exista replicacion (Hito 3),
# esta responsabilidad se movera al lider (o al consenso).

_commit_locks: Dict[Tuple[str, str, str], asyncio.Lock] = defaultdict(asyncio.Lock)


def _commit_lock_for(owner_id: str, object_key: str, domain_id: str) -> asyncio.Lock:
    return _commit_locks[(owner_id, object_key, domain_id)]


# --- Servicio ---


class CatalogService:
    def __init__(self, repo: Repository):
        self.repo = repo

    # ---------- Objetos ----------

    async def init_object(
        self,
        inp: ObjectInitInput,
        placements: List[PlacementDecision],
    ) -> ObjectRow:
        """Crea el objeto en estado PENDING con sus bloques y replicas ASSIGNED.

        Todo en una sola transaccion. Emite evento OBJECT_INIT.
        """
        now = time.time()
        object_id = self._object_id_from_block_ids(inp.blocks)

        obj = ObjectRow(
            object_id=object_id,
            domain_id=settings.domain_id,
            owner_id=inp.owner_id,
            object_key=inp.object_key,
            version=0,  # se asigna en commit
            status=ObjectStatus.PENDING,
            total_size=inp.total_size,
            block_size=inp.block_size,
            global_checksum=inp.global_checksum,
            created_at=now,
            committed_at=None,
            deleted_at=None,
            expires_at=now + settings.pending_ttl_seconds,
        )
        await self.repo.create_object(obj)

        # Insertar bloques.
        blocks_orm = [
            Block(
                block_id=b.block_id,
                object_id=object_id,
                block_index=b.index,
                size=b.size,
                checksum=b.checksum,
            )
            for b in inp.blocks
        ]
        await self.repo.add_blocks(blocks_orm)

        # Insertar replicas ASSIGNED.
        replicas_orm: List[Replica] = []
        for placement in placements:
            for replica_index, node_id in enumerate(placement.replica_node_ids):
                replicas_orm.append(
                    Replica(
                        replica_id=str(uuid.uuid4()),
                        block_id=placement.block_id,
                        datanode_id=node_id,
                        replica_index=replica_index,
                        status=ReplicaStatus.ASSIGNED,
                        assigned_at=now,
                    )
                )
        await self.repo.add_replicas(replicas_orm)

        # Evento OBJECT_INIT.
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.OBJECT_INIT,
            entity_id=object_id,
            payload={
                "object_id": object_id,
                "owner_id": inp.owner_id,
                "object_key": inp.object_key,
                "total_size": inp.total_size,
                "block_size": inp.block_size,
                "n_blocks": len(inp.blocks),
            },
        )
        await self.repo.session.commit()
        return obj

    async def commit_object(self, object_id: str, requesting_user_id: str) -> ObjectRow:
        """PENDING -> COMMITTED si todas las replicas estan CONFIRMED.

        Asigna la version dentro de esta misma transaccion.
        """
        obj = await self.repo.get_object(object_id)
        if obj is None:
            raise NotFoundError(f"objeto {object_id} no existe")
        if obj.owner_id != requesting_user_id:
            raise PermissionDeniedError("el objeto no pertenece al usuario")
        if obj.status != ObjectStatus.PENDING:
            raise InvalidStateError(
                f"objeto en estado {obj.status}, no se puede commitear"
            )

        replicas = await self.repo.get_replicas_for_object(object_id)
        # Contar CONFIRMED por bloque.
        confirmed_by_block: dict[str, int] = {}
        for r in replicas:
            if r.status == ReplicaStatus.CONFIRMED:
                confirmed_by_block[r.block_id] = confirmed_by_block.get(r.block_id, 0) + 1

        blocks = await self.repo.get_blocks_for_object(object_id)
        missing = []
        for b in blocks:
            got = confirmed_by_block.get(b.block_id, 0)
            if got < settings.replication_factor:
                missing.append(
                    {
                        "block_id": b.block_id,
                        "confirmed": got,
                        "required": settings.replication_factor,
                    }
                )
        if missing:
            raise ConflictError(
                {"message": "replicas incompletas", "missing": missing}
            )

        # Capturar valores antes de tocar la sesion.
        owner_id = obj.owner_id
        object_key = obj.object_key
        domain_id = obj.domain_id

        # Serializar la asignacion de version por (owner, key). Sin esto, dos
        # commits concurrentes pueden leer el mismo MAX(version) y colisionar
        # en el indice unico parcial. El lock es intra-proceso (correcto
        # mientras el ControlNode no este replicado).
        now = time.time()
        async with _commit_lock_for(owner_id, object_key, domain_id):
            version = await self.repo.next_version_for_key(
                owner_id, object_key, domain_id
            )
            try:
                await self.repo.update_object_status(
                    object_id,
                    ObjectStatus.COMMITTED,
                    committed_at=now,
                    version=version,
                )
                await self.repo.append_event(
                    domain_id=settings.domain_id,
                    event_type=EventType.OBJECT_COMMITTED,
                    entity_id=object_id,
                    payload={
                        "object_id": object_id,
                        "owner_id": owner_id,
                        "object_key": object_key,
                        "version": version,
                    },
                )
                await self.repo.session.commit()
            except IntegrityError as exc:
                await self.repo.session.rollback()
                raise ConflictError(f"conflicto asignando version: {exc}") from exc

        # Recargamos con el estado actualizado.
        refreshed = await self.repo.get_object(object_id)
        assert refreshed is not None
        return refreshed

    async def delete_object(self, object_id: str, requesting_user_id: str) -> ObjectRow:
        obj = await self.repo.get_object(object_id)
        if obj is None:
            raise NotFoundError(f"objeto {object_id} no existe")
        if obj.owner_id != requesting_user_id:
            raise PermissionDeniedError("el objeto no pertenece al usuario")
        if obj.status == ObjectStatus.DELETED:
            raise InvalidStateError("el objeto ya fue borrado")
        if obj.status != ObjectStatus.COMMITTED:
            raise InvalidStateError(
                f"solo se puede borrar un objeto COMMITTED (estado actual: {obj.status})"
            )

        now = time.time()
        await self.repo.update_object_status(
            object_id, ObjectStatus.DELETED, deleted_at=now
        )
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.OBJECT_DELETED,
            entity_id=object_id,
            payload={"object_id": object_id, "object_key": obj.object_key},
        )
        await self.repo.session.commit()

        refreshed = await self.repo.get_object(object_id)
        assert refreshed is not None
        return refreshed

    async def expire_pending(self, object_id: str) -> None:
        """Elimina un objeto PENDING expirado y todos sus bloques/replicas.

        Los block_ids afectados quedan como huerfanos en los DataNodes; el
        proximo reporte los recibira en la respuesta como unknown_blocks.
        """
        obj = await self.repo.get_object(object_id)
        if obj is None:
            return
        if obj.status != ObjectStatus.PENDING:
            return

        block_ids = [b.block_id for b in await self.repo.get_blocks_for_object(object_id)]
        await self.repo.delete_object_cascade(object_id)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.OBJECT_PENDING_EXPIRED,
            entity_id=object_id,
            payload={"object_id": object_id, "block_ids": block_ids},
        )
        await self.repo.session.commit()

    # ---------- Replicas ----------

    async def confirm_replica(
        self, block_id: str, datanode_id: str
    ) -> Optional[Replica]:
        """Marca una replica como CONFIRMED. Idempotente.

        Devuelve None si (block_id, datanode_id) no existe (block desconocido
        para ese nodo). El llamador lo interpreta como 'orfano', a borrar.
        """
        replica = await self.repo.get_replica(block_id, datanode_id)
        if replica is None:
            return None

        now = time.time()
        await self.repo.update_replica_status(
            replica.replica_id,
            ReplicaStatus.CONFIRMED,
            confirmed_at=now,
        )
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.REPLICA_CONFIRMED,
            entity_id=replica.replica_id,
            payload={
                "replica_id": replica.replica_id,
                "block_id": block_id,
                "datanode_id": datanode_id,
            },
        )
        return replica

    async def mark_replica_lost(
        self, block_id: str, datanode_id: str, reason: str
    ) -> Optional[Replica]:
        replica = await self.repo.get_replica(block_id, datanode_id)
        if replica is None:
            raise NotFoundError(
                f"no existe replica de {block_id} en {datanode_id}"
            )
        await self.repo.update_replica_status(replica.replica_id, ReplicaStatus.LOST)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.REPLICA_LOST,
            entity_id=replica.replica_id,
            payload={
                "replica_id": replica.replica_id,
                "block_id": block_id,
                "datanode_id": datanode_id,
                "reason": reason,
            },
        )
        await self.repo.session.commit()
        return replica

    # ---------- DataNodes ----------

    async def register_datanode(
        self,
        node_id: str,
        domain_id: str,
        address: str,
        free_bytes: int,
        load_score: float,
    ) -> DataNode:
        existing = await self.repo.get_datanode(node_id)
        if existing is not None:
            raise ConflictError(f"DataNode {node_id} ya registrado")

        now = time.time()
        node = DataNode(
            node_id=node_id,
            domain_id=domain_id,
            address=address,
            free_bytes=free_bytes,
            load_score=load_score,
            status=NodeStatus.ALIVE,
            last_heartbeat_at=now,
            registered_at=now,
        )
        await self.repo.create_datanode(node)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.DATANODE_REGISTERED,
            entity_id=node_id,
            payload={
                "node_id": node_id,
                "domain_id": domain_id,
                "address": address,
            },
        )
        await self.repo.session.commit()
        return node

    async def receive_heartbeat(
        self, node_id: str, free_bytes: int, load_score: float
    ) -> DataNode:
        node = await self.repo.get_datanode(node_id)
        if node is None:
            raise NotFoundError(f"DataNode {node_id} no registrado")

        now = time.time()
        previous_status = node.status
        # Un heartbeat siempre lo vuelve ALIVE (incluso si estaba SUSPECTED o DEAD).
        await self.repo.update_datanode_status(
            node_id,
            NodeStatus.ALIVE,
            last_heartbeat_at=now,
            free_bytes=free_bytes,
            load_score=load_score,
        )

        if previous_status == NodeStatus.DEAD:
            await self.repo.append_event(
                domain_id=settings.domain_id,
                event_type=EventType.DATANODE_RECOVERED,
                entity_id=node_id,
                payload={"node_id": node_id, "previous_status": previous_status},
            )

        await self.repo.session.commit()
        refreshed = await self.repo.get_datanode(node_id)
        assert refreshed is not None
        return refreshed

    async def mark_datanode_suspected(self, node_id: str) -> None:
        node = await self.repo.get_datanode(node_id)
        if node is None or node.status != NodeStatus.ALIVE:
            return
        await self.repo.update_datanode_status(node_id, NodeStatus.SUSPECTED)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.DATANODE_SUSPECTED,
            entity_id=node_id,
            payload={"node_id": node_id},
        )
        await self.repo.session.commit()

    async def mark_datanode_dead(self, node_id: str) -> List[Replica]:
        node = await self.repo.get_datanode(node_id)
        if node is None or node.status == NodeStatus.DEAD:
            return []
        await self.repo.update_datanode_status(node_id, NodeStatus.DEAD)
        affected = await self.repo.mark_replicas_lost_for_node(node_id)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.DATANODE_DEAD,
            entity_id=node_id,
            payload={"node_id": node_id, "lost_replicas": len(affected)},
        )
        # Un evento REPLICA_LOST por cada replica afectada, para que el pipeline
        # de re-replicacion futuro los consuma uno a uno.
        for r in affected:
            await self.repo.append_event(
                domain_id=settings.domain_id,
                event_type=EventType.REPLICA_LOST,
                entity_id=r.replica_id,
                payload={
                    "replica_id": r.replica_id,
                    "block_id": r.block_id,
                    "datanode_id": node_id,
                    "reason": "DATANODE_DEAD",
                },
            )
        await self.repo.session.commit()
        return affected

    # ---------- Directorios ----------

    async def create_directory(self, owner_id: str, path: str) -> Directory:
        path = _normalize_path(path)
        if path == "/":
            raise ConflictError("no se puede crear el directorio raiz")

        parent, name = _split_path(path)

        # Verificar que el padre exista.
        parent_dir = await self.repo.get_directory(owner_id, parent, settings.domain_id)
        if parent_dir is None and parent != "/":
            raise NotFoundError(f"directorio padre {parent} no existe")
        if parent == "/":
            root = await self.repo.get_directory(owner_id, "/", settings.domain_id)
            if root is None:
                raise NotFoundError("directorio raiz no existe para este usuario")

        # Verificar que no exista ya.
        existing = await self.repo.get_directory(owner_id, path, settings.domain_id)
        if existing is not None:
            raise ConflictError(f"directorio {path} ya existe")

        now = time.time()
        d = Directory(
            dir_id=str(uuid.uuid4()),
            domain_id=settings.domain_id,
            owner_id=owner_id,
            full_path=path,
            parent_path=parent,
            created_at=now,
        )
        await self.repo.create_directory(d)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.NAMESPACE_MKDIR,
            entity_id=d.dir_id,
            payload={"owner_id": owner_id, "path": path},
        )
        await self.repo.session.commit()
        return d

    async def remove_directory(self, owner_id: str, path: str) -> None:
        path = _normalize_path(path)
        if path == "/":
            raise ConflictError("no se puede borrar el directorio raiz")

        d = await self.repo.get_directory(owner_id, path, settings.domain_id)
        if d is None:
            raise NotFoundError(f"directorio {path} no existe")

        has_children = await self.repo.directory_has_children(
            owner_id, path, settings.domain_id
        )
        has_objects = await self.repo.directory_has_committed_objects(
            owner_id, path, settings.domain_id
        )
        if has_children or has_objects:
            raise ConflictError(f"directorio {path} no esta vacio")

        await self.repo.delete_directory(d.dir_id)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.NAMESPACE_RMDIR,
            entity_id=d.dir_id,
            payload={"owner_id": owner_id, "path": path},
        )
        await self.repo.session.commit()

    async def ensure_root_directory(self, owner_id: str) -> Directory:
        existing = await self.repo.get_directory(owner_id, "/", settings.domain_id)
        if existing is not None:
            return existing
        d = Directory(
            dir_id=str(uuid.uuid4()),
            domain_id=settings.domain_id,
            owner_id=owner_id,
            full_path="/",
            parent_path=None,
            created_at=time.time(),
        )
        await self.repo.create_directory(d)
        # No emitimos evento para la raiz porque se crea implicitamente al registrarse.
        await self.repo.session.commit()
        return d

    # ---------- Helpers ----------

    @staticmethod
    def _object_id_from_block_ids(blocks: List[BlockInput]) -> str:
        """Deriva el object_id del prefijo comun de los block_ids.

        El chunker genera block_ids como '{object_id}_{index:06d}'; podemos
        recuperar el object_id partiendo por el separador '_'. Si el manifiesto
        viene vacio (no deberia, ya que init con 0 blocks no tiene sentido)
        se genera uno nuevo.
        """
        if not blocks:
            return str(uuid.uuid4())
        block_id = blocks[0].block_id
        sep = block_id.rfind("_")
        if sep == -1:
            return str(uuid.uuid4())
        return block_id[:sep]


# --- Utils de path ---


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    # Colapsar dobles slash y remover trailing.
    parts = [p for p in path.split("/") if p != ""]
    return "/" + "/".join(parts) if parts else "/"


def _split_path(path: str) -> tuple[str, str]:
    """Devuelve (parent_path, name)."""
    path = _normalize_path(path)
    if path == "/":
        return ("/", "")
    idx = path.rfind("/")
    if idx == 0:
        return ("/", path[1:])
    return (path[:idx], path[idx + 1:])
