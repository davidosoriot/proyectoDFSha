"""Punto de entrada del ControlNode.

Levanta la app FastAPI, crea las tablas si no existen y arranca los tasks
de fondo: HealthMonitorTask (deteccion de nodos caidos) y PendingCleanupTask
(limpieza de objetos PENDING expirados).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import acls, admin, auth, datanodes, federation, namespace, objects, replication
from .core.federation_svc import FederationPropagatorTask
from .core.health_monitor import HealthMonitorTask, PendingCleanupTask
from .core.replication import FollowerSyncTask
from .core.rereplication import RereplicationTask
from .db.connection import dispose_engine, get_engine
from .db.models import Base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

health_task = HealthMonitorTask()
cleanup_task = PendingCleanupTask()
rerep_task = RereplicationTask()
federation_task = FederationPropagatorTask()
follower_task = FollowerSyncTask()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Crear tablas al arrancar (idempotente).
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    health_task.start()
    cleanup_task.start()
    rerep_task.start()
    federation_task.start()
    follower_task.start()
    try:
        yield
    finally:
        await health_task.stop()
        await cleanup_task.stop()
        await rerep_task.stop()
        await federation_task.stop()
        await follower_task.stop()
        await dispose_engine()


def create_app() -> FastAPI:
    app = FastAPI(
        title="DFSha ControlNode",
        version="0.1.0",
        description=(
            "SuperPeer de DFSha. Mantiene el catalogo, el espacio de nombres, "
            "el registro de DataNodes y decide la colocacion de replicas. "
            "Nunca transporta contenido de bloques."
        ),
        lifespan=lifespan,
    )
    app.include_router(auth.router)
    app.include_router(namespace.router)
    app.include_router(objects.router)
    app.include_router(datanodes.router)
    app.include_router(admin.router)
    app.include_router(acls.router)
    app.include_router(federation.admin_router)
    app.include_router(federation.peer_router)
    app.include_router(replication.router)
    return app


app = create_app()
