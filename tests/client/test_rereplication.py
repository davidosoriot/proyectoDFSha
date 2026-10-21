"""Re-replicacion end-to-end: muere un nodo, se restablece el RF en un nuevo nodo."""

from __future__ import annotations

import os
import sys

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from client import DFShaClient  # noqa: E402
from controlnode.core.catalog import CatalogService  # noqa: E402
from controlnode.core.rereplication import RereplicationService  # noqa: E402
from controlnode.db.connection import get_session_factory  # noqa: E402
from controlnode.db.repository import Repository  # noqa: E402
from datanode.api import build_router as build_dn_router  # noqa: E402
from datanode.storage import BlockStore  # noqa: E402


def _mk_client(env, dn_transport=None) -> DFShaClient:
    return DFShaClient(
        controlnode_url="http://cn",
        token=env["token"],
        transport=env["cn_transport"],
        datanode_transport=dn_transport or env["dn_transport"],
    )


@pytest.mark.asyncio
async def test_rereplication_restores_rf_after_node_death(registered_env, tmp_path):
    client = _mk_client(registered_env)
    content = b"abcdefghij" * 500  # 5 KB
    await client.mkdir("/d")
    await client.put(content, "/d/x.bin", block_size=1024)

    # 1. Matamos el primer DataNode a nivel de catalogo (sus replicas -> LOST).
    dead_node_id = registered_env["nodes"][0][0]
    factory = get_session_factory()
    async with factory() as session:
        catalog = CatalogService(Repository(session))
        affected = await catalog.mark_datanode_dead(dead_node_id)
    assert len(affected) > 0

    # 2. Agregar un cuarto DataNode y registrarlo. Para que su /replicate pueda
    # alcanzar a cualquier otro DN usamos el mismo transporte multiplexado.
    apps_by_host = registered_env["apps_by_host"]
    cn_app = registered_env["cn_app"]
    new_node_id = "dn-4"
    new_address = "http://dn-4"
    new_store = BlockStore(str(tmp_path / "dn4_storage"))

    async def _notify_dn4(block_id: str, checksum: str) -> None:
        cn_transport = ASGITransport(app=cn_app)
        async with AsyncClient(transport=cn_transport, base_url="http://cn") as c:
            await c.post(
                f"/datanodes/{new_node_id}/blocks",
                headers={"X-DataNode-Key": "test-datanode-key"},
                json={"blocks": [{"block_id": block_id, "checksum": checksum}]},
            )

    # Transporte que ya conocia dn-1..dn-3; agregamos dn-4.
    dn_transport = registered_env["dn_transport"]

    new_app = FastAPI()
    new_app.include_router(
        build_dn_router(
            new_store,
            node_id=new_node_id,
            on_block_stored=_notify_dn4,
            http_client_factory=lambda: AsyncClient(transport=dn_transport, timeout=10),
        )
    )
    apps_by_host["dn-4"] = new_app

    # Registrar dn-4 en el ControlNode.
    cn_transport = ASGITransport(app=cn_app)
    async with AsyncClient(transport=cn_transport, base_url="http://cn") as cn:
        r = await cn.post(
            "/datanodes/register",
            headers={"X-DataNode-Key": "test-datanode-key"},
            json={
                "node_id": new_node_id,
                "domain_id": "domain-test",
                "address": new_address,
                "free_bytes": 1_000_000_000,
                "load_score": 0.0,
            },
        )
        assert r.status_code == 201

    # 3. Disparar re-replicacion via el servicio directamente, inyectando el
    # transporte multiplexado como http_client_factory.
    factory = get_session_factory()
    async with factory() as session:
        svc = RereplicationService(
            Repository(session),
            http_client_factory=lambda: AsyncClient(transport=dn_transport, timeout=10),
        )
        stats = await svc.tick()

    assert stats["under_rf"] > 0
    assert stats["scheduled"] > 0
    assert stats["copied"] == stats["scheduled"]
    assert stats["failed"] == 0

    # 4. dn-4 tiene los bloques copiados.
    dn4_inv = list(new_store.iter_inventory())
    assert len(dn4_inv) > 0

    # 5. El cliente puede seguir leyendo (via cualquier replica, incluida dn-4).
    assert await client.get("/d/x.bin") == content


@pytest.mark.asyncio
async def test_rereplication_tick_is_noop_when_rf_satisfied(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/d")
    await client.put(b"content", "/d/x.bin")

    factory = get_session_factory()
    async with factory() as session:
        svc = RereplicationService(Repository(session))
        stats = await svc.tick()
    assert stats["under_rf"] == 0
    assert stats["scheduled"] == 0


@pytest.mark.asyncio
async def test_rereplication_admin_endpoint(registered_env):
    """El endpoint /admin/rereplicate dispara el servicio y devuelve stats."""
    cn_transport = ASGITransport(app=registered_env["cn_app"])
    async with AsyncClient(transport=cn_transport, base_url="http://cn") as cn:
        r = await cn.post(
            "/admin/rereplicate",
            headers={"X-DataNode-Key": "test-datanode-key"},
        )
    assert r.status_code == 200
    body = r.json()
    assert "under_rf" in body
    assert "scheduled" in body
