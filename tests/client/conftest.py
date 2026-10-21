"""Fixtures del cliente: ControlNode + 3 DataNodes virtuales en memoria.

Montamos un ControlNode real con SQLite in-memory y tres DataNodes como
apps FastAPI independientes. El SDK habla con todos por ASGITransport.
"""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("DFSHA_DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("DFSHA_JWT_SECRET", "test-secret-32-bytes-minimum-long")
os.environ.setdefault("DFSHA_DATANODE_API_KEY", "test-datanode-key")
os.environ.setdefault("DFSHA_DOMAIN_ID", "domain-test")

from controlnode import config as _cnconfig  # noqa: E402
from controlnode.db import connection as _connection  # noqa: E402
from controlnode.db.models import Base  # noqa: E402
from controlnode.main import create_app as create_controlnode_app  # noqa: E402
from datanode.api import build_router as build_dn_router  # noqa: E402
from datanode.storage import BlockStore  # noqa: E402


@pytest_asyncio.fixture
async def cn_app():
    # Mantener la MISMA instancia de settings; sincronizar con env vars.
    _cnconfig.settings.replication_factor = 3

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import StaticPool

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _connection._engine = engine
    _connection._session_factory = factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    app = create_controlnode_app()

    @asynccontextmanager
    async def _noop(_):
        yield

    app.router.lifespan_context = _noop
    yield app
    await engine.dispose()
    _connection._engine = None
    _connection._session_factory = None


class _MultiNodeTransport(ASGITransport):
    """Transporte que enruta por el host de la URL a una app distinta.

    Si no hay coincidencia, falla con 503. Util para simular 3 DataNodes
    distintos con sus propios storages, sin levantar sockets.
    """

    def __init__(self, apps: dict[str, Any]) -> None:
        super().__init__(app=lambda scope, recv, send: None)  # app dummy
        self._apps = apps  # host -> app

    async def handle_async_request(self, request):
        host = request.url.host
        app = self._apps.get(host)
        if app is None:
            raise RuntimeError(f"no app registrada para host {host}")
        transport = ASGITransport(app=app)
        return await transport.handle_async_request(request)


@pytest_asyncio.fixture
async def datanodes(tmp_path, cn_app):
    """Monta 3 DataNodes como apps FastAPI y los registra todos en memoria.

    Cada DataNode al recibir un bloque notifica inmediatamente al ControlNode
    (simula un block report incremental), para que el commit no tenga que
    esperar al tick periodico. Ademas, el http_client_factory de cada DN
    enruta por el mismo transporte multiplexado -- esto permite probar la
    operacion /replicate (peer-to-peer entre DataNodes) sin red real.
    """
    from httpx import AsyncClient

    nodes: list[tuple[str, str, BlockStore, FastAPI]] = []
    apps_by_host: dict[str, FastAPI] = {}
    transport_holder = {}  # llenado mas abajo; usado por closures

    def _make_notifier(node_id: str):
        async def _notify(block_id: str, checksum: str) -> None:
            cn_transport = ASGITransport(app=cn_app)
            async with AsyncClient(transport=cn_transport, base_url="http://cn") as c:
                await c.post(
                    f"/datanodes/{node_id}/blocks",
                    headers={"X-DataNode-Key": "test-datanode-key"},
                    json={"blocks": [{"block_id": block_id, "checksum": checksum}]},
                )
        return _notify

    def _http_factory():
        # Devuelve un AsyncClient que usa el transporte multiplexado para
        # alcanzar cualquiera de los DN registrados.
        return AsyncClient(transport=transport_holder["t"], timeout=10)

    for i in (1, 2, 3):
        node_id = f"dn-{i}"
        host = f"dn-{i}"
        address = f"http://{host}"
        store = BlockStore(str(tmp_path / f"dn{i}_storage"))
        app = FastAPI()
        app.include_router(
            build_dn_router(
                store,
                node_id=node_id,
                on_block_stored=_make_notifier(node_id),
                http_client_factory=_http_factory,
            )
        )
        nodes.append((node_id, address, store, app))
        apps_by_host[host] = app

    transport = _MultiNodeTransport(apps_by_host)
    transport_holder["t"] = transport
    # Permitir que los tests expongan apps_by_host para agregar DN mas tarde.
    yield nodes, transport, apps_by_host


@pytest_asyncio.fixture
async def registered_env(cn_app, datanodes):
    """Registra los 3 DataNodes y crea un usuario autenticado.

    Devuelve dict con todo lo necesario para los tests.
    """
    from httpx import AsyncClient

    cn_transport = ASGITransport(app=cn_app)
    nodes, dn_transport, apps_by_host = datanodes

    headers_dn = {"X-DataNode-Key": "test-datanode-key"}
    async with AsyncClient(transport=cn_transport, base_url="http://cn") as cn:
        for node_id, address, _, _ in nodes:
            r = await cn.post(
                "/datanodes/register",
                headers=headers_dn,
                json={
                    "node_id": node_id,
                    "domain_id": "domain-test",
                    "address": address,
                    "free_bytes": 1_000_000_000,
                    "load_score": 0.0,
                },
            )
            assert r.status_code == 201, r.text

        await cn.post("/auth/register", json={"username": "u", "password": "p1234"})
        r = await cn.post("/auth/login", json={"username": "u", "password": "p1234"})
        token = r.json()["access_token"]

    return {
        "cn_app": cn_app,
        "cn_transport": cn_transport,
        "dn_transport": dn_transport,
        "nodes": nodes,
        "apps_by_host": apps_by_host,
        "token": token,
    }
