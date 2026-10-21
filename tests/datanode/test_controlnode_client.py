"""Pruebas de ControlNodeClient contra el ControlNode real en memoria.

Montamos la app del ControlNode con DB in-memory y le hacemos hablar al
cliente del DataNode usando ASGITransport. Esto valida el contrato real
entre los dos componentes sin levantar un servidor HTTP.
"""

from __future__ import annotations

import os
import sys

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Mismas env vars que usa el conftest del ControlNode.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ["DFSHA_DB_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["DFSHA_JWT_SECRET"] = "test-secret"
os.environ["DFSHA_DATANODE_API_KEY"] = "test-datanode-key"
os.environ["DFSHA_DOMAIN_ID"] = "domain-test"


from controlnode import config as _cnconfig  # noqa: E402
from controlnode.db import connection as _connection  # noqa: E402
from controlnode.db.models import Base  # noqa: E402
from controlnode.main import create_app  # noqa: E402
from datanode.controlnode_client import ControlNodeClient  # noqa: E402


class _ASGIControlNodeClient(ControlNodeClient):
    """Variante del cliente que llama via ASGITransport (sin red real)."""

    def __init__(self, app, api_key: str) -> None:
        super().__init__("http://cn", api_key)
        self._app = app

    async def _request(self, method: str, path: str, json=None):
        transport = ASGITransport(app=self._app)
        async with AsyncClient(transport=transport, base_url="http://cn") as c:
            resp = await c.request(method, path, json=json, headers=self._headers)
        return resp

    async def register(self, node_id, domain_id, address, free_bytes, load_score=0.0):
        resp = await self._request("POST", "/datanodes/register", json={
            "node_id": node_id,
            "domain_id": domain_id,
            "address": address,
            "free_bytes": free_bytes,
            "load_score": load_score,
        })
        if resp.status_code == 409:
            return {"registered": True, "already_present": True}
        resp.raise_for_status()
        return resp.json()

    async def heartbeat(self, node_id, free_bytes, load_score):
        resp = await self._request(
            "POST",
            f"/datanodes/{node_id}/heartbeat",
            json={"free_bytes": free_bytes, "load_score": load_score},
        )
        resp.raise_for_status()
        return resp.json()

    async def block_report(self, node_id, entries):
        resp = await self._request(
            "POST",
            f"/datanodes/{node_id}/blocks",
            json={"blocks": [{"block_id": b, "checksum": c} for b, c in entries]},
        )
        resp.raise_for_status()
        return resp.json().get("unknown_blocks", [])


@pytest_asyncio.fixture
async def cn_app():
    from contextlib import asynccontextmanager

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

    app = create_app()

    @asynccontextmanager
    async def _noop(_):
        yield

    app.router.lifespan_context = _noop
    yield app
    await engine.dispose()
    _connection._engine = None
    _connection._session_factory = None


@pytest.mark.asyncio
async def test_register_and_heartbeat(cn_app):
    client = _ASGIControlNodeClient(cn_app, api_key="test-datanode-key")
    r = await client.register("dn-a", "domain-test", "http://dn-a:9000", free_bytes=1_000)
    assert r["registered"] is True

    hb = await client.heartbeat("dn-a", free_bytes=900, load_score=0.1)
    assert hb["ok"] is True


@pytest.mark.asyncio
async def test_block_report_unknown_blocks_reported(cn_app):
    """Si el DataNode reporta bloques que el ControlNode no conoce, los lista."""
    client = _ASGIControlNodeClient(cn_app, api_key="test-datanode-key")
    await client.register("dn-a", "domain-test", "http://dn-a:9000", free_bytes=1_000)
    unknown = await client.block_report("dn-a", [("blockX", "aa" * 32)])
    assert "blockX" in unknown


@pytest.mark.asyncio
async def test_duplicate_register_is_handled(cn_app):
    client = _ASGIControlNodeClient(cn_app, api_key="test-datanode-key")
    await client.register("dn-a", "domain-test", "http://dn-a:9000", free_bytes=1_000)
    r = await client.register("dn-a", "domain-test", "http://dn-a:9000", free_bytes=1_000)
    assert r.get("already_present") is True
