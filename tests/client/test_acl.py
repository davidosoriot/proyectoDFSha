"""Pruebas de ACL granular por prefijo."""

from __future__ import annotations

import os
import sys

import pytest
from httpx import ASGITransport, AsyncClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from client import DFShaClient, DFShaClientError  # noqa: E402


async def _login(env, username: str, password: str) -> str:
    cn_transport = env["cn_transport"]
    async with AsyncClient(transport=cn_transport, base_url="http://cn") as c:
        await c.post("/auth/register", json={"username": username, "password": password})
        r = await c.post("/auth/login", json={"username": username, "password": password})
    return r.json()["access_token"]


def _client_with(env, token: str) -> DFShaClient:
    return DFShaClient(
        controlnode_url="http://cn",
        token=token,
        transport=env["cn_transport"],
        datanode_transport=env["dn_transport"],
    )


@pytest.mark.asyncio
async def test_other_user_cannot_read_without_acl(registered_env):
    """bob no puede leer archivos de alice sin ACL."""
    alice = _client_with(registered_env, registered_env["token"])
    await alice.mkdir("/fotos")
    await alice.put(b"secret", "/fotos/x.jpg")

    bob_token = await _login(registered_env, "bob", "p1234")
    bob = _client_with(registered_env, bob_token)

    # bob busca con owner=alice (que es "u", el usuario default del fixture) -> 403.
    with pytest.raises(DFShaClientError) as e:
        await bob.get("/fotos/x.jpg", owner="u")
    assert "403" in str(e.value)


@pytest.mark.asyncio
async def test_grant_read_then_access(registered_env):
    alice = _client_with(registered_env, registered_env["token"])
    await alice.mkdir("/fotos")
    await alice.put(b"compartida", "/fotos/x.jpg")

    bob_token = await _login(registered_env, "bob", "p1234")
    bob = _client_with(registered_env, bob_token)

    # alice concede READ a bob sobre /fotos.
    await alice.grant("bob", "/fotos", ["READ"])

    data = await bob.get("/fotos/x.jpg", owner="u")
    assert data == b"compartida"


@pytest.mark.asyncio
async def test_grant_specific_prefix_blocks_siblings(registered_env):
    """Un ACL sobre /fotos/compartidas no permite leer /fotos/privadas."""
    alice = _client_with(registered_env, registered_env["token"])
    await alice.mkdir("/fotos")
    await alice.mkdir("/fotos/compartidas")
    await alice.mkdir("/fotos/privadas")
    await alice.put(b"pub", "/fotos/compartidas/a.jpg")
    await alice.put(b"priv", "/fotos/privadas/b.jpg")

    bob_token = await _login(registered_env, "bob", "p1234")
    bob = _client_with(registered_env, bob_token)
    await alice.grant("bob", "/fotos/compartidas", ["READ"])

    assert await bob.get("/fotos/compartidas/a.jpg", owner="u") == b"pub"
    with pytest.raises(DFShaClientError):
        await bob.get("/fotos/privadas/b.jpg", owner="u")


@pytest.mark.asyncio
async def test_revoke_removes_access(registered_env):
    alice = _client_with(registered_env, registered_env["token"])
    await alice.mkdir("/d")
    await alice.put(b"x", "/d/a.txt")

    bob_token = await _login(registered_env, "bob", "p1234")
    bob = _client_with(registered_env, bob_token)
    await alice.grant("bob", "/d", ["READ"])
    assert await bob.get("/d/a.txt", owner="u") == b"x"

    await alice.revoke("bob", "/d")
    with pytest.raises(DFShaClientError):
        await bob.get("/d/a.txt", owner="u")


@pytest.mark.asyncio
async def test_longest_prefix_wins(registered_env):
    """ACL mas especifica gana: /fotos sin WRITE pero /fotos/sub con WRITE."""
    alice = _client_with(registered_env, registered_env["token"])
    await alice.mkdir("/fotos")
    await alice.mkdir("/fotos/sub")
    await alice.put(b"general", "/fotos/g.jpg")
    await alice.put(b"especifico", "/fotos/sub/s.jpg")

    bob_token = await _login(registered_env, "bob", "p1234")
    bob = _client_with(registered_env, bob_token)

    await alice.grant("bob", "/fotos", ["READ"])
    await alice.grant("bob", "/fotos/sub", ["READ", "WRITE"])

    acls = await alice.list_acls()
    perms = {a["path_prefix"]: a["permissions"] for a in acls}
    assert "WRITE" in perms["/fotos/sub"]
    assert "WRITE" not in perms["/fotos"]
    assert await bob.get("/fotos/g.jpg", owner="u") == b"general"
    assert await bob.get("/fotos/sub/s.jpg", owner="u") == b"especifico"


@pytest.mark.asyncio
async def test_cannot_grant_to_nonexistent_user(registered_env):
    alice = _client_with(registered_env, registered_env["token"])
    with pytest.raises(DFShaClientError):
        await alice.grant("ghost", "/", ["READ"])


@pytest.mark.asyncio
async def test_cannot_grant_to_self(registered_env):
    alice = _client_with(registered_env, registered_env["token"])
    with pytest.raises(DFShaClientError):
        await alice.grant("u", "/", ["READ"])
