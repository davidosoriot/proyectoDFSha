"""Fixtures del ControlNode.

Cada test usa una DB SQLite en memoria propia. Los background tasks
(health monitor, pending cleanup) NO se arrancan en las pruebas para poder
controlar el tiempo determinsticamente.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Asegurar import path.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Forzar DB en memoria antes de importar el modulo (configuracion sensible al env).
os.environ["DFSHA_DB_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["DFSHA_JWT_SECRET"] = "test-secret"
os.environ["DFSHA_DATANODE_API_KEY"] = "test-datanode-key"
os.environ["DFSHA_DOMAIN_ID"] = "domain-test"

from controlnode import config as _config  # noqa: E402
from controlnode.db import connection as _connection  # noqa: E402
from controlnode.db.models import Base  # noqa: E402
from controlnode.main import create_app  # noqa: E402


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def app():
    """Crea una app fresca por test con DB en memoria y tablas recien creadas."""
    _config.reload_settings()

    # Cada test necesita un engine limpio (SQLite en memoria por conexion).
    # Usamos aiosqlite con StaticPool para compartir la MISMA conexion.
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import StaticPool

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)

    # Inyectar nuestro engine/factory reemplazando los del modulo.
    _connection._engine = engine
    _connection._session_factory = factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    app = create_app()
    # Anular el lifespan por completo: no queremos crear tablas ni arrancar tasks.
    app.router.lifespan_context = _noop_lifespan
    yield app

    await engine.dispose()
    _connection._engine = None
    _connection._session_factory = None


from contextlib import asynccontextmanager


@asynccontextmanager
async def _noop_lifespan(app):
    yield


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def user_token(client):
    """Registra un usuario y devuelve su JWT."""
    await client.post(
        "/auth/register",
        json={"username": "alice", "password": "s3cret"},
    )
    resp = await client.post(
        "/auth/login",
        json={"username": "alice", "password": "s3cret"},
    )
    return resp.json()["access_token"]


@pytest_asyncio.fixture
async def second_user_token(client):
    await client.post(
        "/auth/register",
        json={"username": "bob", "password": "s3cret"},
    )
    resp = await client.post(
        "/auth/login",
        json={"username": "bob", "password": "s3cret"},
    )
    return resp.json()["access_token"]


@pytest_asyncio.fixture
async def datanode_headers():
    return {"X-DataNode-Key": "test-datanode-key"}


@pytest_asyncio.fixture
async def registered_datanodes(client, datanode_headers):
    """Registra 3 DataNodes: 2 en domain-test y 1 en domain-other."""
    nodes = [
        ("dn-1", "domain-test"),
        ("dn-2", "domain-test"),
        ("dn-3", "domain-other"),
    ]
    for node_id, dom in nodes:
        await client.post(
            "/datanodes/register",
            headers=datanode_headers,
            json={
                "node_id": node_id,
                "domain_id": dom,
                "address": f"http://{node_id}:9000",
                "free_bytes": 1_000_000_000,
                "load_score": 0.0,
            },
        )
    return [n for n, _ in nodes]
