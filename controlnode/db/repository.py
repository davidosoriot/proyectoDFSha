"""Capa de acceso a datos. Unica clase que ejecuta SQL.

Todos los servicios del core reciben un Repository (o una AsyncSession y
llaman a metodos estaticos aqui) y nunca escriben SQL directamente. Al
migrar de SQLite a PostgreSQL solo cambian los tipos de columna y las
particularidades del dialect; los servicios no se tocan.
"""

from __future__ import annotations

import json
import time
from typing import Iterable, List, Optional

from sqlalchemy import and_, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    Block,
    DataNode,
    Directory,
    Event,
    NodeStatus,
    ObjectRow,
    ObjectStatus,
    Replica,
    ReplicaStatus,
    User,
)


class Repository:
    """Wrapper de una AsyncSession con metodos tipados por entidad."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # ---------- Users ----------

    async def create_user(self, user: User) -> None:
        self.session.add(user)
        await self.session.flush()

    async def get_user_by_username(self, username: str) -> Optional[User]:
        r = await self.session.execute(select(User).where(User.username == username))
        return r.scalar_one_or_none()

    async def get_user(self, user_id: str) -> Optional[User]:
        return await self.session.get(User, user_id)

    # ---------- Directorios ----------

    async def create_directory(self, d: Directory) -> None:
        self.session.add(d)
        await self.session.flush()

    async def get_directory(
        self, owner_id: str, full_path: str, domain_id: str
    ) -> Optional[Directory]:
        r = await self.session.execute(
            select(Directory).where(
                and_(
                    Directory.owner_id == owner_id,
                    Directory.full_path == full_path,
                    Directory.domain_id == domain_id,
                )
            )
        )
        return r.scalar_one_or_none()

    async def list_directory_children(
        self, owner_id: str, parent_path: str, domain_id: str
    ) -> List[Directory]:
        r = await self.session.execute(
            select(Directory).where(
                and_(
                    Directory.owner_id == owner_id,
                    Directory.parent_path == parent_path,
                    Directory.domain_id == domain_id,
                )
            )
        )
        return list(r.scalars().all())

    async def directory_has_children(
        self, owner_id: str, full_path: str, domain_id: str
    ) -> bool:
        r = await self.session.execute(
            select(func.count()).select_from(Directory).where(
                and_(
                    Directory.owner_id == owner_id,
                    Directory.parent_path == full_path,
                    Directory.domain_id == domain_id,
                )
            )
        )
        return (r.scalar_one() or 0) > 0

    async def directory_has_committed_objects(
        self, owner_id: str, full_path: str, domain_id: str
    ) -> bool:
        # Objetos cuyo directorio inmediato es full_path.
        prefix = full_path if full_path.endswith("/") else full_path + "/"
        r = await self.session.execute(
            select(func.count()).select_from(ObjectRow).where(
                and_(
                    ObjectRow.owner_id == owner_id,
                    ObjectRow.domain_id == domain_id,
                    ObjectRow.status == ObjectStatus.COMMITTED,
                    ObjectRow.object_key.like(prefix + "%"),
                )
            )
        )
        return (r.scalar_one() or 0) > 0

    async def delete_directory(self, dir_id: str) -> None:
        await self.session.execute(delete(Directory).where(Directory.dir_id == dir_id))

    # ---------- Objetos ----------

    async def create_object(self, obj: ObjectRow) -> None:
        self.session.add(obj)
        await self.session.flush()

    async def add_blocks(self, blocks: Iterable[Block]) -> None:
        for b in blocks:
            self.session.add(b)
        await self.session.flush()

    async def add_replicas(self, replicas: Iterable[Replica]) -> None:
        for r in replicas:
            self.session.add(r)
        await self.session.flush()

    async def get_object(self, object_id: str) -> Optional[ObjectRow]:
        return await self.session.get(ObjectRow, object_id)

    async def get_latest_committed(
        self, owner_id: str, object_key: str, domain_id: str
    ) -> Optional[ObjectRow]:
        r = await self.session.execute(
            select(ObjectRow)
            .where(
                and_(
                    ObjectRow.owner_id == owner_id,
                    ObjectRow.object_key == object_key,
                    ObjectRow.domain_id == domain_id,
                    ObjectRow.status == ObjectStatus.COMMITTED,
                )
            )
            .order_by(ObjectRow.version.desc())
            .limit(1)
        )
        return r.scalar_one_or_none()

    async def get_all_versions(
        self, owner_id: str, object_key: str, domain_id: str
    ) -> List[ObjectRow]:
        r = await self.session.execute(
            select(ObjectRow)
            .where(
                and_(
                    ObjectRow.owner_id == owner_id,
                    ObjectRow.object_key == object_key,
                    ObjectRow.domain_id == domain_id,
                )
            )
            .order_by(ObjectRow.version.asc())
        )
        return list(r.scalars().all())

    async def next_version_for_key(
        self, owner_id: str, object_key: str, domain_id: str
    ) -> int:
        r = await self.session.execute(
            select(func.coalesce(func.max(ObjectRow.version), 0)).where(
                and_(
                    ObjectRow.owner_id == owner_id,
                    ObjectRow.object_key == object_key,
                    ObjectRow.domain_id == domain_id,
                )
            )
        )
        return (r.scalar_one() or 0) + 1

    async def get_blocks_for_object(self, object_id: str) -> List[Block]:
        r = await self.session.execute(
            select(Block)
            .where(Block.object_id == object_id)
            .order_by(Block.block_index.asc())
        )
        return list(r.scalars().all())

    async def get_replicas_for_object(self, object_id: str) -> List[Replica]:
        r = await self.session.execute(
            select(Replica)
            .join(Block, Replica.block_id == Block.block_id)
            .where(Block.object_id == object_id)
        )
        return list(r.scalars().all())

    async def get_replicas_for_block(self, block_id: str) -> List[Replica]:
        r = await self.session.execute(
            select(Replica).where(Replica.block_id == block_id)
        )
        return list(r.scalars().all())

    async def update_object_status(
        self,
        object_id: str,
        status: str,
        *,
        committed_at: Optional[float] = None,
        deleted_at: Optional[float] = None,
        version: Optional[int] = None,
    ) -> None:
        values = {"status": status}
        if committed_at is not None:
            values["committed_at"] = committed_at
        if deleted_at is not None:
            values["deleted_at"] = deleted_at
        if version is not None:
            values["version"] = version
        await self.session.execute(
            update(ObjectRow).where(ObjectRow.object_id == object_id).values(**values)
        )

    async def list_committed_objects_in_dir(
        self, owner_id: str, dir_path: str, domain_id: str
    ) -> List[ObjectRow]:
        """Objetos COMMITTED cuyo prefijo inmediato coincide con dir_path.

        Solo devuelve, por cada object_key distinta, la version mas alta.
        """
        prefix = dir_path if dir_path.endswith("/") else dir_path + "/"
        subq = (
            select(
                ObjectRow.object_key,
                func.max(ObjectRow.version).label("max_version"),
            )
            .where(
                and_(
                    ObjectRow.owner_id == owner_id,
                    ObjectRow.domain_id == domain_id,
                    ObjectRow.status == ObjectStatus.COMMITTED,
                    ObjectRow.object_key.like(prefix + "%"),
                )
            )
            .group_by(ObjectRow.object_key)
            .subquery()
        )
        r = await self.session.execute(
            select(ObjectRow).join(
                subq,
                and_(
                    ObjectRow.object_key == subq.c.object_key,
                    ObjectRow.version == subq.c.max_version,
                ),
            )
        )
        candidates = list(r.scalars().all())
        # Filtrar los que estan directamente en dir_path (no en subdirectorios mas profundos).
        result = []
        for obj in candidates:
            rest = obj.object_key[len(prefix):]
            if "/" not in rest and rest != "":
                result.append(obj)
        return result

    async def list_expired_pending(self, now: float) -> List[ObjectRow]:
        r = await self.session.execute(
            select(ObjectRow).where(
                and_(
                    ObjectRow.status == ObjectStatus.PENDING,
                    ObjectRow.expires_at < now,
                )
            )
        )
        return list(r.scalars().all())

    async def delete_object_cascade(self, object_id: str) -> None:
        """Borra el objeto y sus bloques/replicas por completo.

        Borramos replicas y bloques explicitamente porque `delete()` a nivel
        de statement no dispara los cascades ORM de relationship().
        """
        # Borrar replicas de todos los bloques del objeto.
        block_rows = await self.session.execute(
            select(Block.block_id).where(Block.object_id == object_id)
        )
        block_ids = [row[0] for row in block_rows.all()]
        if block_ids:
            await self.session.execute(
                delete(Replica).where(Replica.block_id.in_(block_ids))
            )
            await self.session.execute(
                delete(Block).where(Block.block_id.in_(block_ids))
            )
        await self.session.execute(
            delete(ObjectRow).where(ObjectRow.object_id == object_id)
        )

    async def update_replica_status(
        self,
        replica_id: str,
        status: str,
        *,
        confirmed_at: Optional[float] = None,
    ) -> None:
        values = {"status": status}
        if confirmed_at is not None:
            values["confirmed_at"] = confirmed_at
        await self.session.execute(
            update(Replica).where(Replica.replica_id == replica_id).values(**values)
        )

    async def get_replica(
        self, block_id: str, datanode_id: str
    ) -> Optional[Replica]:
        r = await self.session.execute(
            select(Replica).where(
                and_(
                    Replica.block_id == block_id,
                    Replica.datanode_id == datanode_id,
                )
            )
        )
        return r.scalar_one_or_none()

    # ---------- DataNodes ----------

    async def create_datanode(self, node: DataNode) -> None:
        self.session.add(node)
        await self.session.flush()

    async def get_datanode(self, node_id: str) -> Optional[DataNode]:
        return await self.session.get(DataNode, node_id)

    async def list_alive_datanodes(self, domain_id: Optional[str] = None) -> List[DataNode]:
        stmt = select(DataNode).where(DataNode.status == NodeStatus.ALIVE)
        if domain_id is not None:
            stmt = stmt.where(DataNode.domain_id == domain_id)
        r = await self.session.execute(stmt)
        return list(r.scalars().all())

    async def list_active_datanodes(self) -> List[DataNode]:
        """ALIVE + SUSPECTED. Los DEAD estan excluidos."""
        r = await self.session.execute(
            select(DataNode).where(
                DataNode.status.in_([NodeStatus.ALIVE, NodeStatus.SUSPECTED])
            )
        )
        return list(r.scalars().all())

    async def update_datanode_status(
        self,
        node_id: str,
        status: str,
        *,
        last_heartbeat_at: Optional[float] = None,
        free_bytes: Optional[int] = None,
        load_score: Optional[float] = None,
    ) -> None:
        values = {"status": status}
        if last_heartbeat_at is not None:
            values["last_heartbeat_at"] = last_heartbeat_at
        if free_bytes is not None:
            values["free_bytes"] = free_bytes
        if load_score is not None:
            values["load_score"] = load_score
        await self.session.execute(
            update(DataNode).where(DataNode.node_id == node_id).values(**values)
        )

    async def mark_replicas_lost_for_node(self, node_id: str) -> List[Replica]:
        """Marca todas las replicas ASSIGNED/CONFIRMED del nodo como LOST.

        Devuelve la lista de replicas afectadas para poder emitir eventos.
        """
        r = await self.session.execute(
            select(Replica).where(
                and_(
                    Replica.datanode_id == node_id,
                    Replica.status.in_(
                        [ReplicaStatus.ASSIGNED, ReplicaStatus.CONFIRMED]
                    ),
                )
            )
        )
        affected = list(r.scalars().all())
        if affected:
            await self.session.execute(
                update(Replica)
                .where(
                    and_(
                        Replica.datanode_id == node_id,
                        Replica.status.in_(
                            [ReplicaStatus.ASSIGNED, ReplicaStatus.CONFIRMED]
                        ),
                    )
                )
                .values(status=ReplicaStatus.LOST)
            )
        return affected

    # ---------- Events ----------

    async def append_event(
        self,
        domain_id: str,
        event_type: str,
        entity_id: str,
        payload: dict,
    ) -> None:
        event = Event(
            domain_id=domain_id,
            event_type=event_type,
            entity_id=entity_id,
            payload=json.dumps(payload),
            created_at=time.time(),
            propagated=0,
        )
        self.session.add(event)
        await self.session.flush()

    async def list_pending_events(self, limit: int = 100) -> List[Event]:
        r = await self.session.execute(
            select(Event)
            .where(Event.propagated == 0)
            .order_by(Event.event_id.asc())
            .limit(limit)
        )
        return list(r.scalars().all())
