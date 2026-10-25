"""Pruebas de la API REST del DataNode con un BlockStore sobre tmpdir."""

from __future__ import annotations

import hashlib

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from datanode.api import build_router
from datanode.storage import BlockStore


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def app(tmp_path):
    store = BlockStore(str(tmp_path / "blocks"))
    app = FastAPI()
    app.include_router(build_router(store, node_id="dn-test"))
    app.state.store = store
    return app


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["node_id"] == "dn-test"


@pytest.mark.asyncio
async def test_put_and_get_block(client):
    data = b"hola mundo"
    bid = _digest(data)
    r = await client.put(f"/blocks/{bid}", content=data, params={"checksum": bid})
    assert r.status_code == 201
    assert r.json()["checksum"] == bid

    r = await client.get(f"/blocks/{bid}")
    assert r.status_code == 200
    assert r.content == data


@pytest.mark.asyncio
async def test_put_rejects_bad_checksum(client):
    data = b"xyz"
    r = await client.put(
        f"/blocks/{_digest(data)}",
        content=data,
        params={"checksum": "00" * 32},
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_get_missing_returns_404(client):
    r = await client.get(f"/blocks/{'ab' * 32}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_block(client):
    data = b"byebye"
    bid = _digest(data)
    await client.put(f"/blocks/{bid}", content=data)
    r = await client.delete(f"/blocks/{bid}")
    assert r.status_code == 204
    assert (await client.get(f"/blocks/{bid}")).status_code == 404


@pytest.mark.asyncio
async def test_put_empty_body_rejected(client):
    r = await client.put(f"/blocks/{'cd' * 32}", content=b"")
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_inventory(client):
    for i in range(3):
        data = f"b{i}".encode()
        await client.put(f"/blocks/{_digest(data)}", content=data)
    r = await client.get("/inventory")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 3


@pytest.mark.asyncio
async def test_get_corrupted_block_returns_410_and_quarantines(app, client, tmp_path):
    """Un bloque corrupto responde 410 y se elimina la copia local, para que el
    block report no la vuelva a confirmar ante el ControlNode."""
    data = b"z" * 2048
    bid = "blk_000000"
    r = await client.put(f"/blocks/{bid}", content=data, params={"checksum": _digest(data)})
    assert r.status_code == 201
    path = tmp_path / "blocks" / bid
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 0xFF
    path.write_bytes(bytes(raw))

    r = await client.get(f"/blocks/{bid}")
    assert r.status_code == 410
    assert not app.state.store.exists(bid)
    assert all(e.block_id != bid for e in app.state.store.iter_inventory())
