"""Integracion end-to-end: ControlNode + DataNode + chunker + assembler.

Simula el flujo real de un cliente:
    1. ControlNode arranca con DB vacia.
    2. DataNode arranca con storage vacio y se registra.
    3. Cliente parte un archivo en bloques con dfshacore.chunker.
    4. Cliente llama a /objects/init -> recibe placements.
    5. Cliente sube cada bloque a los DataNodes indicados (aqui todos al mismo).
    6. Cliente commitea.
    7. Cliente resuelve la clave, baja los bloques del DataNode y reensambla.
    8. Verifica que el contenido reensamblado == original.
"""

from __future__ import annotations

import hashlib
import io
import os
import sys

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ["DFSHA_DB_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["DFSHA_JWT_SECRET"] = "test-secret"
os.environ["DFSHA_DATANODE_API_KEY"] = "test-datanode-key"
os.environ["DFSHA_DOMAIN_ID"] = "domain-test"
os.environ["DFSHA_REPLICATION_FACTOR"] = "1"
os.environ["DFSHA_BLOCK_SIZE_DEFAULT"] = "4096"


from contextlib import asynccontextmanager

from fastapi import FastAPI

from controlnode import config as _cnconfig  # noqa: E402
from controlnode.db import connection as _connection  # noqa: E402
from controlnode.db.models import Base  # noqa: E402
from controlnode.main import create_app as create_controlnode_app  # noqa: E402
from datanode.api import build_router  # noqa: E402
from datanode.storage import BlockStore  # noqa: E402
from dfshacore import chunk_stream  # noqa: E402


class _BytesCollector(io.BytesIO):
    """BytesIO que no se cierra con close() para poder leerlo despues."""

    def __init__(self, buf: dict, key: str) -> None:
        super().__init__()
        self._buf = buf
        self._key = key

    def close(self) -> None:  # type: ignore[override]
        self._buf[self._key] = self.getvalue()
        super().close()


@pytest_asyncio.fixture
async def controlnode():
    # Los defaults del dataclass se cristalizan al importar; sobreescribimos
    # directamente la instancia que ya consumen catalog y placement. Si otro
    # conftest hizo reload_settings, forzamos nuestra copia de nuevo.
    _cnconfig.settings.replication_factor = 1
    from controlnode.core import placement_svc as _psvc
    from controlnode.core import catalog as _cat
    _psvc.settings = _cnconfig.settings
    _cat.settings = _cnconfig.settings
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


@pytest_asyncio.fixture
async def datanode(tmp_path):
    store = BlockStore(str(tmp_path / "dn_storage"))
    app = FastAPI()
    app.include_router(build_router(store, node_id="dn-1"))
    app.state.store = store
    return app


@pytest.mark.asyncio
async def test_full_roundtrip_put_get(controlnode, datanode):
    cn_transport = ASGITransport(app=controlnode)
    dn_transport = ASGITransport(app=datanode)

    headers_dn = {"X-DataNode-Key": "test-datanode-key"}

    async with AsyncClient(transport=cn_transport, base_url="http://cn") as cn:
        # 1. Registrar DataNode en el ControlNode.
        r = await cn.post(
            "/datanodes/register",
            headers=headers_dn,
            json={
                "node_id": "dn-1",
                "domain_id": "domain-test",
                "address": "http://dn-1:9000",
                "free_bytes": 1_000_000,
                "load_score": 0.0,
            },
        )
        assert r.status_code == 201

        # 2. Registrar usuario y loguearse.
        await cn.post("/auth/register", json={"username": "u", "password": "p1234"})
        r = await cn.post("/auth/login", json={"username": "u", "password": "p1234"})
        token = r.json()["access_token"]
        auth = {"Authorization": f"Bearer {token}"}

        # 3. Crear directorio y partir contenido.
        await cn.post("/ns/mkdir", headers=auth, json={"path": "/data"})
        content = b"DFSha end-to-end roundtrip!" * 500  # ~13 KB, 4 bloques de 4 KiB

        raw_blocks: dict[str, bytes] = {}

        def _writer(block_id: str, index: int):
            return _BytesCollector(raw_blocks, block_id)

        manifest = chunk_stream(
            source=io.BytesIO(content),
            object_key="/data/file.bin",
            block_writer=_writer,
            block_size=4096,
        )

        # 4. Init object.
        r = await cn.post(
            "/objects/init",
            headers=auth,
            json={
                "object_key": "/data/file.bin",
                "total_size": manifest.total_size,
                "block_size": manifest.block_size,
                "global_checksum": manifest.global_checksum,
                "blocks": [
                    {
                        "block_id": b.block_id,
                        "index": b.index,
                        "size": b.size,
                        "checksum": b.checksum,
                    }
                    for b in manifest.blocks
                ],
            },
        )
        assert r.status_code == 201, r.text
        init = r.json()
        object_id = init["object_id"]

        # 5. Subir cada bloque al DataNode.
        async with AsyncClient(transport=dn_transport, base_url="http://dn") as dn:
            for placement in init["placements"]:
                data = raw_blocks[placement["block_id"]]
                for node in placement["replica_nodes"]:
                    assert node["node_id"] == "dn-1"
                    r = await dn.put(
                        f"/blocks/{placement['block_id']}",
                        content=data,
                    )
                    assert r.status_code == 201, r.text

        # 6. Reportar bloques al ControlNode (confirma replicas).
        report_entries = [
            {"block_id": b.block_id, "checksum": b.checksum}
            for b in manifest.blocks
        ]
        r = await cn.post(
            "/datanodes/dn-1/blocks",
            headers=headers_dn,
            json={"blocks": report_entries},
        )
        assert r.status_code == 200
        assert r.json()["unknown_blocks"] == []

        # 7. Commit.
        r = await cn.post(f"/objects/{object_id}/commit", headers=auth)
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "COMMITTED"

        # 8. GET: resolver clave -> bajar bloques -> reensamblar.
        r = await cn.get("/objects", headers=auth, params={"key": "/data/file.bin"})
        assert r.status_code == 200
        resolved = r.json()
        assert resolved["total_size"] == len(content)

        async with AsyncClient(transport=dn_transport, base_url="http://dn") as dn:
            downloaded: dict[int, bytes] = {}
            for block in resolved["blocks"]:
                replica = block["replicas"][0]
                assert replica["node_id"] == "dn-1"
                r = await dn.get(f"/blocks/{block['block_id']}")
                assert r.status_code == 200
                downloaded[block["index"]] = r.content

        rebuilt = b"".join(downloaded[i] for i in sorted(downloaded))
        assert hashlib.sha256(rebuilt).hexdigest() == manifest.global_checksum
        assert rebuilt == content


@pytest.mark.asyncio
async def test_orphan_blocks_are_identified(controlnode, datanode):
    """Si el DataNode tiene bloques que el ControlNode no conoce, los reporta."""
    cn_transport = ASGITransport(app=controlnode)
    dn_transport = ASGITransport(app=datanode)
    headers_dn = {"X-DataNode-Key": "test-datanode-key"}

    async with AsyncClient(transport=cn_transport, base_url="http://cn") as cn:
        await cn.post(
            "/datanodes/register",
            headers=headers_dn,
            json={
                "node_id": "dn-1",
                "domain_id": "domain-test",
                "address": "http://dn-1:9000",
                "free_bytes": 1_000_000,
                "load_score": 0.0,
            },
        )

        # Subimos un bloque directamente al DataNode sin pasar por el ControlNode.
        data = b"orphan-block-content"
        bid = hashlib.sha256(data).hexdigest()
        async with AsyncClient(transport=dn_transport, base_url="http://dn") as dn:
            r = await dn.put(f"/blocks/{bid}", content=data)
            assert r.status_code == 201

        r = await cn.post(
            "/datanodes/dn-1/blocks",
            headers=headers_dn,
            json={"blocks": [{"block_id": bid, "checksum": bid}]},
        )
        assert r.status_code == 200
        assert bid in r.json()["unknown_blocks"]
