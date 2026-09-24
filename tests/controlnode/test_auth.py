"""Pruebas de autenticacion y aislamiento por usuario."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_register_creates_user(client):
    r = await client.post(
        "/auth/register", json={"username": "u1", "password": "s3cret"}
    )
    assert r.status_code == 201
    body = r.json()
    assert body["username"] == "u1"
    assert body["user_id"]


@pytest.mark.asyncio
async def test_register_duplicate_username(client):
    await client.post("/auth/register", json={"username": "u2", "password": "s3cret"})
    r = await client.post(
        "/auth/register", json={"username": "u2", "password": "otherpw"}
    )
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_login_returns_jwt(client):
    await client.post("/auth/register", json={"username": "u3", "password": "s3cret"})
    r = await client.post(
        "/auth/login", json={"username": "u3", "password": "s3cret"}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["access_token"]
    assert body["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_login_wrong_password(client):
    await client.post("/auth/register", json={"username": "u4", "password": "s3cret"})
    r = await client.post(
        "/auth/login", json={"username": "u4", "password": "bad"}
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_protected_endpoint_without_token(client):
    r = await client.get("/ns/ls")
    assert r.status_code == 401
