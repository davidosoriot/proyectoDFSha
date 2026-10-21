"""Tests de federacion entre dos dominios DFSha.

Montamos **dos** ControlNodes separados (domain-A y domain-B), cada uno con
su propia DB SQLite in-memory. Luego:
    - Registramos cada uno como peer del otro.
    - Verificamos que la propagacion de eventos funciona entre peers.
    - Verificamos que un cliente de domain-A puede leer un objeto almacenado
      en domain-B via el endpoint /federation/objects.
"""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("DFSHA_JWT_SECRET", "test-secret-32-bytes-minimum-long")
os.environ.setdefault("DFSHA_DATANODE_API_KEY", "test-datanode-key")
os.environ.setdefault("DFSHA_FEDERATION_KEY", "test-federation-key")

from controlnode import config as _cnconfig  # noqa: E402
from controlnode.db.models import Base  # noqa: E402
from controlnode.main import create_app  # noqa: E402
from datanode.api import build_router as build_dn_router  # noqa: E402
from datanode.storage import BlockStore  # noqa: E402


@asynccontextmanager
async def _noop(_):
    yield


class _MultiTransport(ASGITransport):
    """Transporte que enruta por host a la app registrada en el dict."""

    def __init__(self, apps: dict[str, Any]) -> None:
        super().__init__(app=lambda s, r, s2: None)
        self._apps = apps

    async def handle_async_request(self, request):
        app = self._apps.get(request.url.host)
        if app is None:
            raise RuntimeError(f"no app for {request.url.host}")
        return await ASGITransport(app=app).handle_async_request(request)


@pytest_asyncio.fixture
async def two_domains(tmp_path):
    """Devuelve dict con dos ControlNodes y DataNodes propios cada uno."""
    from controlnode.db import connection as _conn

    # Capturar estado global antes de contaminarlo.
    original_get_factory = _conn.get_session_factory
    original_session_factory = _conn._session_factory
    original_engine = _conn._engine
    original_domain_id = _cnconfig.settings.domain_id

    # Patch de settings explicito. Problema: cada modulo de controlnode importo
    # settings por nombre en su momento, y algunos tests previos han rebindeado
    # ese atributo en modulos puntuales. Para evitar inconsistencias, forzamos
    # TODOS los modulos del controlnode a compartir la misma instancia.
    from controlnode.api import acls as _m_acls
    from controlnode.api import admin as _m_admin  # noqa
    from controlnode.api import deps as _m_deps
    from controlnode.api import federation as _m_fed
    from controlnode.api import objects as _m_obj
    from controlnode.core import acl_svc as _m_asvc
    from controlnode.core import auth_svc as _m_auth
    from controlnode.core import catalog as _m_cat
    from controlnode.core import federation as _m_fedcore
    from controlnode.core import federation_svc as _m_fedsvc
    from controlnode.core import health_monitor as _m_hm
    from controlnode.core import namespace_svc as _m_ns
    from controlnode.core import placement_svc as _m_pl
    from controlnode.core import rereplication as _m_rer

    active_settings = _cnconfig.settings
    for mod in [_m_deps, _m_fed, _m_obj, _m_asvc, _m_auth, _m_cat, _m_fedcore,
                _m_fedsvc, _m_hm, _m_ns, _m_pl, _m_rer]:
        mod.settings = active_settings
    active_settings.federation_api_key = "test-federation-key"
    active_settings.datanode_api_key = "test-datanode-key"
    active_settings.jwt_secret = "test-secret-32-bytes-minimum-long"
    active_settings.replication_factor = 1

    # Cada dominio tiene su propia engine. Mantenemos ambas engines; cambiamos
    # el engine global antes de cada request. Para evitar race conditions, los
    # dependers get_session_factory() usan _conn._session_factory global.
    engines = {}
    factories = {}
    apps = {}

    for domain_id in ("domain-A", "domain-B"):
        engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        engines[domain_id] = engine
        factories[domain_id] = factory
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # Modificamos la get_session_factory para que respete el "dominio activo".
    active_domain = {"id": "domain-A"}

    def _get_factory():
        return factories[active_domain["id"]]

    _conn.get_session_factory = _get_factory

    # Crear cada app pero envolviendo el lifespan en noop y la dependencia
    # get_repository para que use la factory del dominio asociado al host.
    for domain_id in ("domain-A", "domain-B"):
        active_settings.domain_id = domain_id
        active_settings.replication_factor = 1
        app = create_app()
        app.router.lifespan_context = _noop
        app.state.domain_id = domain_id
        apps[domain_id] = app

    # El problema de arriba es que settings.domain_id solo refleja el ultimo
    # dominio creado. Para que cada app opere con SU dominio, interceptamos
    # las requests y seteamos active_domain + settings.domain_id acordemente.
    hosts_to_domain = {"cn-a": "domain-A", "cn-b": "domain-B"}

    class _DomainAwareTransport(ASGITransport):
        def __init__(self, apps_dict):
            super().__init__(app=lambda s, r, s2: None)
            self._apps = apps_dict

        async def handle_async_request(self, request):
            host = request.url.host
            dom = hosts_to_domain.get(host)
            if dom:
                active_domain["id"] = dom
                active_settings.domain_id = dom
            target = self._apps.get(host)
            if target is None:
                raise RuntimeError(f"no app for host {host}")
            return await ASGITransport(app=target).handle_async_request(request)

    cn_apps_by_host = {"cn-a": apps["domain-A"], "cn-b": apps["domain-B"]}
    transport = _DomainAwareTransport(cn_apps_by_host)

    yield {
        "apps": apps,
        "engines": engines,
        "factories": factories,
        "active_domain": active_domain,
        "transport": transport,
        "hosts": hosts_to_domain,
        "tmp_path": tmp_path,
        "settings": active_settings,
    }

    for engine in engines.values():
        await engine.dispose()

    # Restaurar el estado global de connection.py para no contaminar otros tests.
    _conn.get_session_factory = original_get_factory
    _conn._session_factory = original_session_factory
    _conn._engine = original_engine
    active_settings.domain_id = original_domain_id


@pytest.mark.asyncio
async def test_peers_can_register_each_other(two_domains):
    transport = two_domains["transport"]
    headers = {"X-DataNode-Key": "test-datanode-key"}

    async with AsyncClient(transport=transport, base_url="http://cn-a") as a:
        r = await a.post(
            "/federation/peers",
            json={
                "domain_id": "domain-B",
                "endpoint": "http://cn-b",
                "api_key": "test-federation-key",
            },
            headers=headers,
        )
        assert r.status_code == 201

        r = await a.get("/federation/peers", headers=headers)
        assert r.status_code == 200
        peers = r.json()["peers"]
        assert len(peers) == 1
        assert peers[0]["domain_id"] == "domain-B"


@pytest.mark.asyncio
async def test_cannot_register_self_as_peer(two_domains):
    transport = two_domains["transport"]
    headers = {"X-DataNode-Key": "test-datanode-key"}
    async with AsyncClient(transport=transport, base_url="http://cn-a") as a:
        r = await a.post(
            "/federation/peers",
            json={
                "domain_id": "domain-A",
                "endpoint": "http://cn-a",
                "api_key": "k",
            },
            headers=headers,
        )
        assert r.status_code == 409


@pytest.mark.asyncio
async def test_lookup_remote_object(two_domains):
    """alice sube un archivo en domain-B. bob desde domain-A lo resuelve."""
    transport = two_domains["transport"]
    headers_dn = {"X-DataNode-Key": "test-datanode-key"}

    # 1. En domain-B: registrar un DataNode, crear usuario, subir archivo.
    dn_app_b = FastAPI()
    dn_store_b = BlockStore(str(two_domains["tmp_path"] / "dnb"))

    # Notifier que apunta a domain-B.
    async def _notify_b(block_id: str, checksum: str) -> None:
        async with AsyncClient(transport=transport, base_url="http://cn-b") as c:
            await c.post(
                "/datanodes/dn-b/blocks",
                headers=headers_dn,
                json={"blocks": [{"block_id": block_id, "checksum": checksum}]},
            )

    dn_app_b.include_router(
        build_dn_router(dn_store_b, node_id="dn-b", on_block_stored=_notify_b)
    )

    # Transporte extendido que incluye dn-b.
    extended_apps = {**two_domains["apps"], "dn-b": dn_app_b}
    hosts = {**two_domains["hosts"], "dn-b": "domain-B"}
    extended_apps_by_host = {"cn-a": two_domains["apps"]["domain-A"], "cn-b": two_domains["apps"]["domain-B"], "dn-b": dn_app_b}

    _settings_ref = two_domains["settings"]

    class _Transport(ASGITransport):
        def __init__(self):
            super().__init__(app=lambda s, r, s2: None)

        async def handle_async_request(self, request):
            dom = hosts.get(request.url.host)
            if dom:
                two_domains["active_domain"]["id"] = dom
                _settings_ref.domain_id = dom
            app = extended_apps_by_host.get(request.url.host)
            if app is None:
                raise RuntimeError(f"no app for host {request.url.host}")
            return await ASGITransport(app=app).handle_async_request(request)

    full_transport = _Transport()

    # Inyectar transporte en el FederationClient que usa el endpoint de proxy.
    from controlnode.core import federation_svc
    federation_svc.set_default_client_factory(
        lambda: AsyncClient(transport=full_transport, timeout=10)
    )

    async with AsyncClient(transport=full_transport, base_url="http://cn-b") as b:
        # Register datanode in B.
        r = await b.post(
            "/datanodes/register",
            headers=headers_dn,
            json={
                "node_id": "dn-b",
                "domain_id": "domain-B",
                "address": "http://dn-b",
                "free_bytes": 1_000_000,
                "load_score": 0.0,
            },
        )
        assert r.status_code == 201, r.text

        # Register user alice.
        await b.post("/auth/register", json={"username": "alice", "password": "p1234"})
        r = await b.post("/auth/login", json={"username": "alice", "password": "p1234"})
        token_alice = r.json()["access_token"]

        # alice uploads to /remote/x.bin.
        from client import DFShaClient
        alice = DFShaClient(
            controlnode_url="http://cn-b",
            token=token_alice,
            transport=full_transport,
            datanode_transport=full_transport,
        )
        await alice.mkdir("/remote")
        content = b"hola desde domain-B!" * 100
        await alice.put(content, "/remote/x.bin", block_size=1024)

    # 2. En domain-A: registrar domain-B como peer y luego hacer lookup.
    async with AsyncClient(transport=full_transport, base_url="http://cn-a") as a:
        r = await a.post(
            "/federation/peers",
            json={
                "domain_id": "domain-B",
                "endpoint": "http://cn-b",
                "api_key": "test-federation-key",
            },
            headers=headers_dn,
        )
        assert r.status_code == 201

        # Registrar bob en domain-A.
        await a.post("/auth/register", json={"username": "bob", "password": "p1234"})
        r = await a.post("/auth/login", json={"username": "bob", "password": "p1234"})
        token_bob = r.json()["access_token"]

        # bob lee x.bin con owner=alice y domain=domain-B.
        r = await a.get(
            "/objects",
            params={"key": "/remote/x.bin", "owner": "alice", "domain": "domain-B"},
            headers={"Authorization": f"Bearer {token_bob}"},
        )
        assert r.status_code == 200, r.text
        meta = r.json()
        assert meta["total_size"] == len(content)
        assert len(meta["blocks"]) > 0
        # Las ubicaciones apuntan a DataNodes de domain-B.
        for b in meta["blocks"]:
            for rep in b["replicas"]:
                assert rep["address"] == "http://dn-b"


@pytest.mark.asyncio
async def test_event_propagation_between_peers(two_domains):
    """Al registrar un peer, el propagador le envia los eventos pendientes."""
    transport = two_domains["transport"]
    headers = {"X-DataNode-Key": "test-datanode-key"}

    # Registrar B como peer de A.
    async with AsyncClient(transport=transport, base_url="http://cn-a") as a:
        r = await a.post(
            "/federation/peers",
            json={
                "domain_id": "domain-B",
                "endpoint": "http://cn-b",
                "api_key": "test-federation-key",
            },
            headers=headers,
        )
        assert r.status_code == 201

        # Crear algo de actividad en A (registrar usuario). Esto emite eventos.
        await a.post("/auth/register", json={"username": "x", "password": "p1234"})

        # Disparar propagacion desde A -- la propagacion abrira client http,
        # pero usa el httpx global (DNS). Inyectamos la fabrica custom.

    # Para que el propagador use el transporte in-memory, instanciamos el
    # servicio directamente con un client_factory.
    from controlnode.core.federation_svc import FederationPropagator
    from controlnode.db import connection as _conn

    # Asegurar que la active_domain apunte a domain-A antes del tick, y
    # setear _session_factory global para que el propagador lo use.
    two_domains["active_domain"]["id"] = "domain-A"
    two_domains["settings"].domain_id = "domain-A"
    _conn._session_factory = two_domains["factories"]["domain-A"]

    prop = FederationPropagator(
        client_factory=lambda: AsyncClient(transport=transport, timeout=10)
    )
    stats = await prop.tick()
    assert stats["peers"] == 1
    assert stats["pending"] > 0
    assert stats["sent"] > 0
    assert stats["failed"] == 0
