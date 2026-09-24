"""Pruebas del espacio de nombres."""

from __future__ import annotations

import pytest


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_mkdir_creates_directory(client, user_token):
    r = await client.post(
        "/ns/mkdir", headers=_auth(user_token), json={"path": "/fotos"}
    )
    assert r.status_code == 201
    assert r.json()["path"] == "/fotos"

    r_ls = await client.get("/ns/ls?path=/", headers=_auth(user_token))
    names = [e["name"] for e in r_ls.json()["entries"]]
    assert "fotos" in names


@pytest.mark.asyncio
async def test_mkdir_duplicate_path(client, user_token):
    await client.post("/ns/mkdir", headers=_auth(user_token), json={"path": "/a"})
    r = await client.post(
        "/ns/mkdir", headers=_auth(user_token), json={"path": "/a"}
    )
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_mkdir_missing_parent(client, user_token):
    r = await client.post(
        "/ns/mkdir",
        headers=_auth(user_token),
        json={"path": "/nonexistent/child"},
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_ls_empty_directory(client, user_token):
    r = await client.get("/ns/ls?path=/", headers=_auth(user_token))
    assert r.status_code == 200
    assert r.json()["entries"] == []


@pytest.mark.asyncio
async def test_rmdir_nonempty_fails(client, user_token):
    await client.post("/ns/mkdir", headers=_auth(user_token), json={"path": "/p"})
    await client.post(
        "/ns/mkdir", headers=_auth(user_token), json={"path": "/p/child"}
    )
    r = await client.request(
        "DELETE", "/ns/rmdir?path=/p", headers=_auth(user_token)
    )
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_rmdir_nonexistent(client, user_token):
    r = await client.request(
        "DELETE", "/ns/rmdir?path=/ghost", headers=_auth(user_token)
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_rmdir_empty_success(client, user_token):
    await client.post("/ns/mkdir", headers=_auth(user_token), json={"path": "/empty"})
    r = await client.request(
        "DELETE", "/ns/rmdir?path=/empty", headers=_auth(user_token)
    )
    assert r.status_code == 204
