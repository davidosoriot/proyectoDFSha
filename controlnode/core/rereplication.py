"""RereplicationService: restablece el factor de replicacion.

Barre periodicamente los bloques de objetos COMMITTED cuyo numero de
replicas CONFIRMED esta por debajo del replication_factor. Para cada uno:
    1. Elige un DataNode fuente: una replica CONFIRMED sobre un nodo ALIVE.
    2. Elige un DataNode destino: ALIVE, mismo dominio preferentemente, y
       que no sea ya portador de una replica del bloque.
    3. Crea una nueva Replica en estado ASSIGNED en el catalogo.
    4. Pide al DataNode fuente (via HTTP) que copie el bloque al destino.
    5. El bloque llegara al destino via PUT y, por la notificacion inmediata
       del DataNode, el ControlNode recibira un block report que la pasara
       a CONFIRMED.

Si no hay fuente ALIVE (todas las replicas en nodos DEAD) o no hay destino
disponible, se emite un log de alerta y se intenta en la proxima iteracion.

Este servicio es idempotente: si ya hay suficientes ASSIGNED+CONFIRMED en
una iteracion anterior, no hace nada.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from typing import Optional

import httpx

from ..config import settings
from ..db.connection import get_session_factory
from ..db.models import (
    Block,
    DataNode,
    EventType,
    NodeStatus,
    ObjectRow,
    ObjectStatus,
    Replica,
    ReplicaStatus,
)
from ..db.repository import Repository
from sqlalchemy import and_, select

log = logging.getLogger("controlnode.rereplication")


class RereplicationService:
    """Logica de deteccion y agendamiento. Separada del loop para poder testear."""

    def __init__(self, repo: Repository, http_client_factory=None) -> None:
        self.repo = repo
        self._client_factory = http_client_factory or (
            lambda: httpx.AsyncClient(timeout=10)
        )

    async def tick(self) -> dict:
        """Una pasada: devuelve estadisticas {scheduled, copied, failed, under_rf}."""
        stats = {"scheduled": 0, "copied": 0, "failed": 0, "under_rf": 0}
        under = await self._blocks_under_rf()
        stats["under_rf"] = len(under)

        for block, confirmed_nodes, assigned_nodes in under:
            needed = settings.replication_factor - (
                len(confirmed_nodes) + len(assigned_nodes)
            )
            if needed <= 0:
                continue

            # 1. Elegir fuente: una CONFIRMED en nodo ALIVE.
            source_node = await self._pick_source(block.block_id, confirmed_nodes)
            if source_node is None:
                log.warning(
                    "re-replicacion: sin fuente ALIVE para bloque %s", block.block_id
                )
                continue

            # 2. Elegir hasta `needed` destinos nuevos.
            targets = await self._pick_targets(
                exclude=set(confirmed_nodes) | set(assigned_nodes),
                count=needed,
            )
            if not targets:
                log.warning(
                    "re-replicacion: sin destinos disponibles para %s", block.block_id
                )
                continue

            for target in targets:
                replica = await self._assign_replica(block.block_id, target.node_id)
                stats["scheduled"] += 1
                try:
                    await self._trigger_copy(source_node, target, block.block_id)
                    stats["copied"] += 1
                except Exception as exc:
                    log.exception(
                        "re-replicacion: fallo al copiar %s de %s a %s: %s",
                        block.block_id,
                        source_node.node_id,
                        target.node_id,
                        exc,
                    )
                    stats["failed"] += 1
                    # Marcamos la replica reciente como LOST para intentar de nuevo.
                    await self.repo.update_replica_status(
                        replica.replica_id, ReplicaStatus.LOST
                    )
                    await self.repo.session.commit()

        return stats

    async def _blocks_under_rf(self) -> list[tuple[Block, list[str], list[str]]]:
        """Devuelve (bloque, nodos_confirmed, nodos_assigned) para bloques < RF.

        Solo mira bloques de objetos COMMITTED (no PENDING, no DELETED).
        """
        rows = await self.repo.session.execute(
            select(Block)
            .join(ObjectRow, Block.object_id == ObjectRow.object_id)
            .where(ObjectRow.status == ObjectStatus.COMMITTED)
        )
        candidates: list[tuple[Block, list[str], list[str]]] = []
        for block in rows.scalars().all():
            replicas = await self.repo.get_replicas_for_block(block.block_id)
            confirmed = [r.datanode_id for r in replicas if r.status == ReplicaStatus.CONFIRMED]
            assigned = [r.datanode_id for r in replicas if r.status == ReplicaStatus.ASSIGNED]
            total = len(confirmed) + len(assigned)
            if total < settings.replication_factor:
                candidates.append((block, confirmed, assigned))
        return candidates

    async def _pick_source(
        self, block_id: str, confirmed_nodes: list[str]
    ) -> Optional[DataNode]:
        if not confirmed_nodes:
            return None
        rows = await self.repo.session.execute(
            select(DataNode).where(
                and_(
                    DataNode.node_id.in_(confirmed_nodes),
                    DataNode.status == NodeStatus.ALIVE,
                )
            )
        )
        candidates = list(rows.scalars().all())
        return random.choice(candidates) if candidates else None

    async def _pick_targets(self, exclude: set[str], count: int) -> list[DataNode]:
        rows = await self.repo.session.execute(
            select(DataNode).where(DataNode.status == NodeStatus.ALIVE)
        )
        pool = [n for n in rows.scalars().all() if n.node_id not in exclude]
        # Ordenamos por mas espacio libre primero para repartir carga.
        pool.sort(key=lambda n: n.free_bytes, reverse=True)
        return pool[:count]

    async def _assign_replica(self, block_id: str, node_id: str) -> Replica:
        now = time.time()
        existing_replicas = await self.repo.get_replicas_for_block(block_id)
        # Si el destino ya tuvo una replica de este bloque (LOST por caida o
        # corrupcion), se reutiliza esa fila: (block_id, datanode_id) es UNIQUE.
        previous = next(
            (r for r in existing_replicas if r.datanode_id == node_id), None
        )
        if previous is not None:
            await self.repo.update_replica_status(
                previous.replica_id, ReplicaStatus.ASSIGNED
            )
            await self.repo.append_event(
                domain_id=settings.domain_id,
                event_type=EventType.REPLICA_ASSIGNED,
                entity_id=previous.replica_id,
                payload={
                    "replica_id": previous.replica_id,
                    "block_id": block_id,
                    "datanode_id": node_id,
                    "reason": "rereplication",
                },
            )
            await self.repo.session.commit()
            previous.status = ReplicaStatus.ASSIGNED
            return previous
        next_index = max((r.replica_index for r in existing_replicas), default=-1) + 1
        replica = Replica(
            replica_id=str(uuid.uuid4()),
            block_id=block_id,
            datanode_id=node_id,
            replica_index=next_index,
            status=ReplicaStatus.ASSIGNED,
            assigned_at=now,
        )
        await self.repo.add_replicas([replica])
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.REPLICA_ASSIGNED,
            entity_id=replica.replica_id,
            payload={
                "replica_id": replica.replica_id,
                "block_id": block_id,
                "datanode_id": node_id,
                "reason": "rereplication",
            },
        )
        await self.repo.session.commit()
        return replica

    async def _trigger_copy(
        self, source: DataNode, target: DataNode, block_id: str
    ) -> None:
        """Pide al nodo fuente que copie el bloque al destino.

        El endpoint esperado en el DataNode es POST /blocks/{id}/replicate
        con body {"target_address": "<url>"}. El destino registrara el bloque
        y lo confirmara via block report.
        """
        async with self._client_factory() as client:
            r = await client.post(
                f"{source.address}/blocks/{block_id}/replicate",
                json={"target_address": target.address},
            )
            if r.status_code >= 400:
                raise RuntimeError(
                    f"replicate failed: {r.status_code} {r.text}"
                )


class RereplicationTask:
    """Loop en background que ejecuta RereplicationService.tick periodicamente."""

    def __init__(self, interval_seconds: Optional[float] = None):
        self._interval = interval_seconds or settings.rereplication_interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _tick_once(self) -> dict:
        factory = get_session_factory()
        async with factory() as session:
            repo = Repository(session)
            svc = RereplicationService(repo)
            return await svc.tick()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                stats = await self._tick_once()
                if stats["under_rf"] > 0:
                    log.info("re-replicacion: %s", stats)
            except Exception:
                log.exception("RereplicationTask: fallo en tick")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="rereplication")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None
