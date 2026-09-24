"""HealthMonitorTask: detecta caida de DataNodes en segundo plano.

Corre cada `health_check_interval_seconds` (5 s por defecto).
- Nodos sin heartbeat por mas de `suspect_after_seconds` (20 s) -> SUSPECTED.
- Nodos sin heartbeat por mas de `dead_after_seconds` (30 s) -> DEAD.
    Cuando un nodo pasa a DEAD, todas sus replicas ASSIGNED/CONFIRMED pasan
    a LOST y se emite un evento REPLICA_LOST por cada una. El pipeline de
    re-replicacion (fuera del alcance del Hito 2) consumira esos eventos.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from ..config import settings
from ..db.connection import get_session_factory
from ..db.models import NodeStatus
from ..db.repository import Repository
from .catalog import CatalogService

logger = logging.getLogger("controlnode.health")


class HealthMonitorTask:
    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def _check_once(self) -> None:
        factory = get_session_factory()
        async with factory() as session:
            repo = Repository(session)
            catalog = CatalogService(repo)
            nodes = await repo.list_active_datanodes()
            now = time.time()
            for node in nodes:
                if node.last_heartbeat_at is None:
                    elapsed = now - node.registered_at
                else:
                    elapsed = now - node.last_heartbeat_at

                if elapsed > settings.dead_after_seconds and node.status != NodeStatus.DEAD:
                    logger.warning(
                        "DataNode %s no responde hace %.1fs -> DEAD", node.node_id, elapsed
                    )
                    await catalog.mark_datanode_dead(node.node_id)
                elif (
                    elapsed > settings.suspect_after_seconds
                    and node.status == NodeStatus.ALIVE
                ):
                    logger.info(
                        "DataNode %s no responde hace %.1fs -> SUSPECTED",
                        node.node_id,
                        elapsed,
                    )
                    await catalog.mark_datanode_suspected(node.node_id)

    async def _run(self) -> None:
        interval = settings.health_check_interval_seconds
        try:
            while not self._stop_event.is_set():
                try:
                    await self._check_once()
                except Exception:
                    logger.exception("HealthMonitorTask iteration failed")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="health-monitor")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None


class PendingCleanupTask:
    """Limpia periodicamente los objetos PENDING cuyo TTL expiro."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def _cleanup_once(self) -> None:
        factory = get_session_factory()
        async with factory() as session:
            repo = Repository(session)
            catalog = CatalogService(repo)
            expired = await repo.list_expired_pending(time.time())
            for obj in expired:
                logger.info("Objeto PENDING expirado: %s", obj.object_id)
                await catalog.expire_pending(obj.object_id)

    async def _run(self) -> None:
        interval = settings.pending_cleanup_interval_seconds
        try:
            while not self._stop_event.is_set():
                try:
                    await self._cleanup_once()
                except Exception:
                    logger.exception("PendingCleanupTask iteration failed")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="pending-cleanup")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
