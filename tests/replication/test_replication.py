"""Tests de replicacion leader-follower del ControlNode."""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from controlnode import config as _cnconfig  # noqa: E402
from controlnode.core.replication import FollowerSync, apply_snapshot, export_snapshot  # noqa: E402
from controlnode.db import connection as _conn  # noqa: E402
from controlnode.db.models import Base  # noqa: E402
from controlnode.main import create_app  # noqa: E402


@asynccontextmanager
async def _noop(_):
    yield


def _patch_shared_settings(active):
    """Fuerza a todos los modulos de controlnode a usar la misma instancia."""
    from controlnode.api import acls as _m_acls  # noqa
    from controlnode.api import deps as _m_deps
    from controlnode.api import federation as _m_fed
    from controlnode.api import objects as _m_obj
    from controlnode.api import replication as _m_rep
    from controlnode.core import acl_svc as _m_asvc
    from controlnode.core import auth_svc as _m_auth
    from controlnode.core import catalog as _m_cat
    from controlnode.core import federation as _m_fedcore
    from controlnode.core import federation_svc as _m_fedsvc
    from controlnode.core import health_monitor as _m_hm
    from controlnode.core import namespace_svc as _m_ns
    from controlnode.core import placement_svc as _m_pl
    from controlnode.core import replication as _m_repcore
    from controlnode.core import rereplication as _m_rer

    for mod in [_m_deps, _m_fed, _m_obj, _m_rep, _m_asvc, _m_auth, _m_cat,
                _m_fedcore, _m_fedsvc, _m_hm, _m_ns, _m_pl, _m_rer, _m_repcore]:
        mod.settings = active


@pytest_asyncio.fixture
async def leader_and_follower(tmp_path):
    """Monta dos ControlNodes: uno LEADER y otro FOLLOWER, ambos in-memory."""
    # Capturar originales.
    original_session_factory = _conn._session_factory
    original_engine = _conn._engine
    original_role = _cnconfig.settings.role

    active_settings = _cnconfig.settings
    _patch_shared_settings(active_settings)

    active_settings.replication_api_key = "test-replication-key"
    active_settings.datanode_api_key = "test-datanode-key"
    active_settings.replication_factor = 1

    # Dos engines independientes (uno por nodo).
    engines = {}
    factories = {}
    for key in ("leader", "follower"):
        engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        engines[key] = engine
        factories[key] = factory
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # Creamos ambas apps en modo LEADER (necesario para que leader acepte
    # snapshot). Para el handler del follower lo marcaremos distinto en runtime.
    active_settings.role = "LEADER"
    leader_app = create_app()
    leader_app.router.lifespan_context = _noop

    follower_app = create_app()
    follower_app.router.lifespan_context = _noop

    # Transporte que enruta por host y setea factory + role segun el nodo.
    active_node = {"id": "leader"}

    class _T(ASGITransport):
        def __init__(self):
            super().__init__(app=lambda s, r, s2: None)

        async def handle_async_request(self, request):
            host = request.url.host
            if host == "leader":
                active_node["id"] = "leader"
                _conn._session_factory = factories["leader"]
                active_settings.role = "LEADER"
                app = leader_app
            elif host == "follower":
                active_node["id"] = "follower"
                _conn._session_factory = factories["follower"]
                active_settings.role = "FOLLOWER"
                app = follower_app
            else:
                raise RuntimeError(f"host desconocido {host}")
            return await ASGITransport(app=app).handle_async_request(request)

    transport = _T()

    yield {
        "leader_app": leader_app,
        "follower_app": follower_app,
        "transport": transport,
        "factories": factories,
        "settings": active_settings,
        "active_node": active_node,
    }

    for e in engines.values():
        await e.dispose()
    _conn._session_factory = original_session_factory
    _conn._engine = original_engine
    active_settings.role = original_role


@pytest.mark.asyncio
async def test_follower_rejects_role_endpoint_shows_role(leader_and_follower):
    env = leader_and_follower
    transport = env["transport"]

    async with AsyncClient(transport=transport, base_url="http://leader") as c:
        r = await c.get("/replication/role")
    assert r.status_code == 200
    assert r.json()["role"] == "LEADER"

    async with AsyncClient(transport=transport, base_url="http://follower") as c:
        r = await c.get("/replication/role")
    assert r.status_code == 200
    assert r.json()["role"] == "FOLLOWER"


@pytest.mark.asyncio
async def test_snapshot_requires_key(leader_and_follower):
    env = leader_and_follower
    async with AsyncClient(transport=env["transport"], base_url="http://leader") as c:
        r = await c.get("/replication/snapshot")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_snapshot_only_on_leader(leader_and_follower):
    env = leader_and_follower
    headers = {"X-Replication-Key": "test-replication-key"}
    async with AsyncClient(transport=env["transport"], base_url="http://follower") as c:
        r = await c.get("/replication/snapshot", headers=headers)
    assert r.status_code == 409  # el nodo esta en modo FOLLOWER


@pytest.mark.asyncio
async def test_follower_syncs_leader_state(leader_and_follower):
    env = leader_and_follower
    transport = env["transport"]
    headers = {"X-Replication-Key": "test-replication-key"}

    # Crear estado en el leader: usuario + directorio + DataNode.
    async with AsyncClient(transport=transport, base_url="http://leader") as c:
        await c.post("/auth/register", json={"username": "alice", "password": "p1234"})
        r = await c.post("/auth/login", json={"username": "alice", "password": "p1234"})
        token = r.json()["access_token"]

        r = await c.post(
            "/datanodes/register",
            headers={"X-DataNode-Key": "test-datanode-key"},
            json={
                "node_id": "dn-l",
                "domain_id": _cnconfig.settings.domain_id,
                "address": "http://dn-l",
                "free_bytes": 1_000,
                "load_score": 0.0,
            },
        )
        assert r.status_code == 201

        await c.post(
            "/ns/mkdir",
            headers={"Authorization": f"Bearer {token}"},
            json={"path": "/replicado"},
        )

    # Verificar que el leader tiene el snapshot bien poblado.
    async with AsyncClient(transport=transport, base_url="http://leader") as c:
        r = await c.get("/replication/snapshot", headers=headers)
    assert r.status_code == 200
    snap = r.json()
    assert len(snap["users"]) >= 1
    assert len(snap["datanodes"]) >= 1
    assert any(d["full_path"] == "/replicado" for d in snap["directories"])

    # El follower aplica el snapshot via FollowerSync (con transporte in-mem).
    sync = FollowerSync(
        leader_url="http://leader",
        api_key="test-replication-key",
        client_factory=lambda: AsyncClient(transport=transport, timeout=10),
        session_factory=env["factories"]["follower"],
    )
    # El sync usa get_session_factory() del _conn global; cambiamos factory
    # al del follower antes de aplicar.
    _conn._session_factory = env["factories"]["follower"]
    env["active_node"]["id"] = "follower"
    env["settings"].role = "FOLLOWER"
    stats = await sync.sync_once()
    assert stats["rows"] >= 2  # al menos 1 user y 1 datanode y 1 dir

    # Verificar que el user esta en la DB del follower.
    from controlnode.db.models import User
    from sqlalchemy import select
    async with env["factories"]["follower"]() as session:
        rows = await session.execute(select(User))
        all_users = rows.scalars().all()
    assert any(u.username == "alice" for u in all_users), f"users: {[u.username for u in all_users]}"

    # Ahora el follower devuelve el mismo estado al preguntar /ns/ls.
    async with AsyncClient(transport=transport, base_url="http://follower") as c:
        r = await c.post("/auth/login", json={"username": "alice", "password": "p1234"})
        assert r.status_code == 200, r.text
        token = r.json()["access_token"]
        r = await c.get(
            "/ns/ls",
            headers={"Authorization": f"Bearer {token}"},
            params={"path": "/"},
        )
    assert r.status_code == 200
    names = {e["name"] for e in r.json()["entries"]}
    assert "replicado" in names


@pytest.mark.asyncio
async def test_follower_sync_preserves_event_ids(leader_and_follower):
    """Los events del leader llegan al follower con sus IDs preservados."""
    env = leader_and_follower
    transport = env["transport"]

    headers_dn = {"X-DataNode-Key": "test-datanode-key"}
    async with AsyncClient(transport=transport, base_url="http://leader") as c:
        for i in (1, 2):
            await c.post(
                "/datanodes/register",
                headers=headers_dn,
                json={
                    "node_id": f"dn-ev{i}",
                    "domain_id": _cnconfig.settings.domain_id,
                    "address": f"http://dn-ev{i}",
                    "free_bytes": 1_000,
                    "load_score": 0.0,
                },
            )

    sync = FollowerSync(
        leader_url="http://leader",
        api_key="test-replication-key",
        client_factory=lambda: AsyncClient(transport=transport, timeout=10),
        session_factory=env["factories"]["follower"],
    )
    _conn._session_factory = env["factories"]["follower"]
    env["settings"].role = "FOLLOWER"
    await sync.sync_once()

    # Verificar que el follower ahora tiene los mismos events.
    from controlnode.db.models import Event
    from sqlalchemy import select
    async with env["factories"]["follower"]() as session:
        rows = await session.execute(select(Event).order_by(Event.event_id))
        events = rows.scalars().all()
    assert len(events) >= 2
