"""Punto de entrada del DataNode.

Levanta la app FastAPI que expone el plano de datos (PUT/GET/DELETE de bloques),
inicializa el storage, se registra contra el ControlNode y arranca los loops de
heartbeat y block report.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import build_router
from .background import BlockReportTask, HeartbeatTask
from .config import settings
from .controlnode_client import ControlNodeClient
from .crypto import BlockCipher
from .storage import BlockStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("datanode.main")


def _build_cipher() -> BlockCipher | None:
    if not settings.encryption_key_hex:
        return None
    return BlockCipher.from_hex(settings.encryption_key_hex)


def create_app() -> FastAPI:
    cipher = _build_cipher()
    store = BlockStore(settings.storage_path, cipher=cipher)
    client = ControlNodeClient(settings.controlnode_url, settings.controlnode_api_key)

    heartbeat = HeartbeatTask(
        client=client,
        node_id=settings.node_id,
        store=store,
        capacity_bytes=settings.capacity_bytes,
        interval_seconds=settings.heartbeat_interval_seconds,
    )
    block_report = BlockReportTask(
        client=client,
        node_id=settings.node_id,
        store=store,
        interval_seconds=settings.block_report_interval_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        used = store.used_bytes()
        free = max(0, settings.capacity_bytes - used)
        try:
            await client.register(
                node_id=settings.node_id,
                domain_id=settings.domain_id,
                address=settings.public_address,
                free_bytes=free,
                load_score=0.0,
            )
            log.info("registrado contra ControlNode como %s", settings.node_id)
        except Exception as exc:
            log.warning("no se pudo registrar al arrancar: %s (se reintentara en heartbeat)", exc)

        heartbeat.start()
        block_report.start()
        try:
            yield
        finally:
            await heartbeat.stop()
            await block_report.stop()

    app = FastAPI(
        title=f"DFSha DataNode ({settings.node_id})",
        version="0.1.0",
        description=(
            "Worker de DFSha. Almacena, sirve y verifica bloques. "
            "No conoce el namespace ni la logica de replicacion global; "
            "obedece al ControlNode."
        ),
        lifespan=lifespan,
    )
    async def _notify_block_stored(block_id: str, checksum: str) -> None:
        # Block report incremental: confirma la replica al ControlNode de
        # inmediato para que el commit del cliente no espere al tick periodico.
        await client.block_report(settings.node_id, [(block_id, checksum)])

    app.include_router(
        build_router(store, settings.node_id, on_block_stored=_notify_block_stored)
    )
    return app


app = create_app()
