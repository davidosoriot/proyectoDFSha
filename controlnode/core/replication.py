"""Replicacion leader-follower del ControlNode.

Modelo simple pero funcional para la entrega:

**Leader**:
    - Expone `/replication/snapshot` que devuelve un volcado completo del
      catalogo (users, dirs, objects, blocks, replicas, datanodes, acls,
      peer_domains, events) en JSON.
    - Expone `/replication/events?since=N` para pull incremental.
    - Autenticado con `X-Replication-Key`.

**Follower**:
    - Rechaza escrituras con 503 (`require_leader` middleware).
    - Un background task `FollowerSyncTask` llama al leader periodicamente,
      descarga el snapshot y repopula sus tablas locales. Simple y correcto
      para la demo; en produccion se usarian deltas incrementales + Raft.

Promocion: no se implementa eleccion automatica. Si el leader cae, un
operador cambia DFSHA_ROLE=LEADER en un follower y lo reinicia.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..db.connection import get_session_factory
from ..db.models import (
    ACL,
    Base,
    Block,
    DataNode,
    Directory,
    Event,
    ObjectRow,
    PeerDomain,
    Replica,
    User,
)
from ..db.repository import Repository

log = logging.getLogger("controlnode.replication")


# Tablas a replicar en orden (respetando FKs: users antes de directories, etc.).
_REPLICATED_TABLES = [
    (User, ["user_id", "username", "password_hash", "domain_id", "created_at"]),
    (DataNode, ["node_id", "domain_id", "address", "free_bytes", "load_score",
                "status", "last_heartbeat_at", "registered_at"]),
    (Directory, ["dir_id", "domain_id", "owner_id", "full_path", "parent_path",
                 "created_at"]),
    (ObjectRow, ["object_id", "domain_id", "owner_id", "object_key", "version",
                 "status", "total_size", "block_size", "global_checksum",
                 "created_at", "committed_at", "deleted_at", "expires_at"]),
    (Block, ["block_id", "object_id", "block_index", "size", "checksum"]),
    (Replica, ["replica_id", "block_id", "datanode_id", "replica_index",
               "status", "assigned_at", "confirmed_at"]),
    (ACL, ["acl_id", "owner_id", "grantee_id", "path_prefix", "permissions",
           "domain_id", "created_at"]),
    (PeerDomain, ["domain_id", "endpoint", "api_key", "status", "registered_at",
                  "last_seen_at"]),
]


async def export_snapshot(session: AsyncSession) -> dict:
    """Genera un snapshot JSON serializable del catalogo del leader."""
    out: dict[str, Any] = {}
    for cls, cols in _REPLICATED_TABLES:
        rows = await session.execute(select(cls))
        out[cls.__tablename__] = [
            {c: getattr(r, c) for c in cols} for r in rows.scalars().all()
        ]
    # Events: incluimos todos para dar continuidad al log del follower.
    rows = await session.execute(select(Event).order_by(Event.event_id.asc()))
    out["events"] = [
        {
            "event_id": e.event_id,
            "domain_id": e.domain_id,
            "event_type": e.event_type,
            "entity_id": e.entity_id,
            "payload": e.payload,
            "created_at": e.created_at,
            "propagated": e.propagated,
        }
        for e in rows.scalars().all()
    ]
    out["snapshot_at"] = time.time()
    return out


async def apply_snapshot(session: AsyncSession, snapshot: dict) -> dict:
    """Repopula las tablas locales desde el snapshot. Idempotente: borra y
    reinserta todo. Para catalogo pequeno-mediano esta bien; para catalogo
    grande se migraria a replicacion por deltas."""
    stats = {"tables": 0, "rows": 0}
    # Borrar en orden inverso para respetar FKs.
    for cls, _ in reversed(_REPLICATED_TABLES):
        await session.execute(delete(cls))
    await session.execute(delete(Event))

    for cls, cols in _REPLICATED_TABLES:
        rows = snapshot.get(cls.__tablename__, [])
        for row in rows:
            obj = cls(**{c: row.get(c) for c in cols})
            session.add(obj)
        stats["rows"] += len(rows)
        stats["tables"] += 1
    for e in snapshot.get("events", []):
        session.add(
            Event(
                event_id=e["event_id"],
                domain_id=e["domain_id"],
                event_type=e["event_type"],
                entity_id=e["entity_id"],
                payload=e["payload"],
                created_at=e["created_at"],
                propagated=e["propagated"],
            )
        )
    await session.commit()
    return stats


class FollowerSync:
    """Ejecuta la logica de pull-and-apply del follower.

    Si `session_factory` se provee, se usa en vez de get_session_factory();
    util en tests donde el factory activo puede ser cambiado por el transporte
    in-memory antes/durante la llamada HTTP.
    """

    def __init__(
        self,
        leader_url: str,
        api_key: str,
        client_factory=None,
        session_factory=None,
    ) -> None:
        self._leader_url = leader_url.rstrip("/")
        self._api_key = api_key
        self._factory = client_factory or (lambda: httpx.AsyncClient(timeout=30))
        self._session_factory = session_factory

    async def sync_once(self) -> dict:
        # Capturar la factory ANTES del HTTP call para que un transporte in-memory
        # que la mute durante la llamada al leader no nos haga escribir en la
        # DB equivocada.
        factory = self._session_factory or get_session_factory()
        async with self._factory() as c:
            r = await c.get(
                f"{self._leader_url}/replication/snapshot",
                headers={"X-Replication-Key": self._api_key},
            )
        if r.status_code >= 400:
            raise RuntimeError(f"snapshot GET {r.status_code}: {r.text}")
        snapshot = r.json()

        async with factory() as session:
            stats = await apply_snapshot(session, snapshot)
        return stats


class FollowerSyncTask:
    def __init__(self, interval_seconds: Optional[float] = None) -> None:
        self._interval = (
            interval_seconds or settings.replication_poll_interval_seconds
        )
        self._sync = FollowerSync(
            leader_url=settings.leader_url, api_key=settings.replication_api_key
        )
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._sync.sync_once()
            except Exception:
                log.exception("FollowerSyncTask: fallo en sync")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self) -> None:
        if settings.role != "FOLLOWER":
            log.info("role=LEADER; FollowerSyncTask no se inicia")
            return
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="follower-sync")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None
