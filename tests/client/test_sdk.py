"""Pruebas end-to-end del DFShaClient usando ControlNode + 3 DataNodes."""

from __future__ import annotations

import hashlib
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from client import DFShaClient, DFShaClientError  # noqa: E402


def _mk_client(env) -> DFShaClient:
    return DFShaClient(
        controlnode_url="http://cn",
        token=env["token"],
        transport=env["cn_transport"],
        datanode_transport=env["dn_transport"],
    )


@pytest.mark.asyncio
async def test_put_get_roundtrip_bytes(registered_env):
    client = _mk_client(registered_env)
    content = b"hola DFSha!" * 2000  # ~22 KB

    await client.mkdir("/data")
    put_resp = await client.put(content, "/data/file.bin", block_size=4096)
    assert put_resp["version"] == 1

    fetched = await client.get("/data/file.bin")
    assert fetched == content


@pytest.mark.asyncio
async def test_put_get_small(registered_env):
    client = _mk_client(registered_env)
    content = b"small"
    await client.mkdir("/d")
    await client.put(content, "/d/small.txt")
    assert await client.get("/d/small.txt") == content


@pytest.mark.asyncio
async def test_ls_shows_directories_and_objects(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/a")
    await client.mkdir("/a/b")
    await client.put(b"x", "/a/hola.txt")

    entries = await client.ls("/a")
    names = {e["name"]: e["type"] for e in entries}
    assert names == {"b": "dir", "hola.txt": "object"}


@pytest.mark.asyncio
async def test_delete_hides_from_ls(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/d")
    put = await client.put(b"hola", "/d/f.txt")
    object_id = put["object_id"]

    await client.delete(object_id)
    # Reaparece como 404 en get.
    with pytest.raises(DFShaClientError):
        await client.get("/d/f.txt")


@pytest.mark.asyncio
async def test_get_falls_over_to_other_replica(registered_env):
    """Si borramos el bloque de un DataNode, el cliente usa la siguiente replica."""
    client = _mk_client(registered_env)
    await client.mkdir("/d")
    await client.put(b"replicado" * 500, "/d/rep.bin", block_size=1024)

    # Borrar los bloques del primer DataNode.
    first_store = registered_env["nodes"][0][2]
    deleted = 0
    for entry in list(first_store.iter_inventory()):
        first_store.delete(entry.block_id)
        deleted += 1
    assert deleted > 0

    # Aun deberiamos poder recuperar el contenido.
    data = await client.get("/d/rep.bin")
    assert data == b"replicado" * 500


@pytest.mark.asyncio
async def test_stat_returns_version_history(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/d")
    put1 = await client.put(b"v1", "/d/doc.txt")
    put2 = await client.put(b"v2-extended", "/d/doc.txt")

    info = await client.stat(put2["object_id"])
    versions = {v["version"]: v["status"] for v in info["versions"]}
    assert versions[1] == "COMMITTED"
    assert versions[2] == "COMMITTED"


@pytest.mark.asyncio
async def test_get_latest_version(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/d")
    await client.put(b"viejo", "/d/x.txt")
    await client.put(b"nuevo", "/d/x.txt")
    assert await client.get("/d/x.txt") == b"nuevo"


@pytest.mark.asyncio
async def test_rmdir_rejects_nonempty(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/d")
    await client.put(b"x", "/d/f.txt")
    with pytest.raises(DFShaClientError):
        await client.rmdir("/d")


@pytest.mark.asyncio
async def test_mkdir_rmdir(registered_env):
    client = _mk_client(registered_env)
    await client.mkdir("/data")
    await client.mkdir("/data/sub")
    await client.rmdir("/data/sub")
    entries = await client.ls("/data")
    assert entries == []
