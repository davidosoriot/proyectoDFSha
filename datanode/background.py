"""Tareas en background del DataNode.

- HeartbeatTask: envia heartbeat cada N segundos con espacio libre y carga.
- BlockReportTask: envia reporte completo de inventario periodicamente.
  Si el ControlNode responde con unknown_blocks, se borran localmente.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .controlnode_client import ControlNodeClient
from .storage import BlockStore

log = logging.getLogger("datanode.background")


class _PeriodicTask:
    def __init__(self, name: str, interval_seconds: float) -> None:
        self._name = name
        self._interval = interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name=self._name)

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception:
                log.exception("%s: fallo en tick", self._name)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:  # pragma: no cover - override
        raise NotImplementedError


class HeartbeatTask(_PeriodicTask):
    def __init__(
        self,
        client: ControlNodeClient,
        node_id: str,
        store: BlockStore,
        capacity_bytes: int,
        interval_seconds: float,
    ) -> None:
        super().__init__("heartbeat", interval_seconds)
        self._client = client
        self._node_id = node_id
        self._store = store
        self._capacity = capacity_bytes

    async def _tick(self) -> None:
        used = self._store.used_bytes()
        free = max(0, self._capacity - used)
        load = min(1.0, used / self._capacity) if self._capacity > 0 else 0.0
        await self._client.heartbeat(self._node_id, free, load)


class BlockReportTask(_PeriodicTask):
    def __init__(
        self,
        client: ControlNodeClient,
        node_id: str,
        store: BlockStore,
        interval_seconds: float,
    ) -> None:
        super().__init__("block_report", interval_seconds)
        self._client = client
        self._node_id = node_id
        self._store = store

    async def _tick(self) -> None:
        entries = [(e.block_id, e.checksum) for e in self._store.iter_inventory()]
        unknown = await self._client.block_report(self._node_id, entries)
        for block_id in unknown:
            log.info("ControlNode no conoce %s; borrando localmente", block_id)
            try:
                self._store.delete(block_id)
            except Exception:
                log.exception("fallo al borrar bloque huerfano %s", block_id)
