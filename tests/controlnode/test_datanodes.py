"""Pruebas del registro y reportes de DataNodes."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_register_datanode(client, datanode_headers):
    r = await client.post(
        "/datanodes/register",
        headers=datanode_headers,
        json={
            "node_id": "dn-x",
            "domain_id": "domain-test",
            "address": "http://dn-x:9000",
            "free_bytes": 100,
            "load_score": 0.0,
        },
    )
    assert r.status_code == 201
    assert r.json()["registered"] is True


@pytest.mark.asyncio
async def test_register_duplicate_datanode(client, datanode_headers):
    payload = {
        "node_id": "dn-dup",
        "domain_id": "domain-test",
        "address": "http://dn-dup:9000",
        "free_bytes": 100,
        "load_score": 0.0,
    }
    await client.post("/datanodes/register", headers=datanode_headers, json=payload)
    r = await client.post("/datanodes/register", headers=datanode_headers, json=payload)
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_heartbeat_updates_timestamp(client, datanode_headers, registered_datanodes):
    r = await client.post(
        "/datanodes/dn-1/heartbeat",
        headers=datanode_headers,
        json={"free_bytes": 555, "load_score": 0.4},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True


@pytest.mark.asyncio
async def test_heartbeat_unknown_node(client, datanode_headers):
    r = await client.post(
        "/datanodes/no-such/heartbeat",
        headers=datanode_headers,
        json={"free_bytes": 1, "load_score": 0.0},
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_block_report_unknown_blocks_returned(
    client, datanode_headers, registered_datanodes
):
    """Reportar un block_id que el ControlNode no conoce se devuelve como
    unknown_blocks para que el DataNode lo borre."""
    r = await client.post(
        "/datanodes/dn-1/blocks",
        headers=datanode_headers,
        json={"blocks": [{"block_id": "ghost-block_000000", "checksum": "x"}]},
    )
    assert r.status_code == 200
    assert r.json()["unknown_blocks"] == ["ghost-block_000000"]


@pytest.mark.asyncio
async def test_datanode_endpoints_require_api_key(client, registered_datanodes):
    r = await client.post(
        "/datanodes/dn-1/heartbeat",
        json={"free_bytes": 1, "load_score": 0.0},
    )
    assert r.status_code == 401
