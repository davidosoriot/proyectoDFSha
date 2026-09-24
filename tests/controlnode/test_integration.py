"""Pruebas de integracion multi-modulo del ControlNode."""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from sqlalchemy import select, update

from controlnode.core.catalog import CatalogService
from controlnode.core.health_monitor import PendingCleanupTask
from controlnode.db.connection import get_session_factory
from controlnode.db.models import ObjectRow, ObjectStatus
from controlnode.db.repository import Repository


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _manifest(object_key: str, n_blocks: int = 1, block_size: int = 64):
    object_id = str(uuid.uuid4())
    return {
        "object_key": object_key,
        "total_size": n_blocks * block_size,
        "block_size": block_size,
        "global_checksum": "b" * 64,
        "blocks": [
            {
                "block_id": f"{object_id}_{i:06d}",
                "index": i,
                "size": block_size,
                "checksum": "a" * 64,
            }
            for i in range(n_blocks)
        ],
    }


async def _confirm_and_commit(client, datanode_headers, token, object_id, block_ids):
    for block_id in block_ids:
        for node in ["dn-1", "dn-2", "dn-3"]:
            await client.post(
                f"/datanodes/{node}/blocks",
                headers=datanode_headers,
                json={"blocks": [{"block_id": block_id, "checksum": "a"}]},
            )
    return await client.post(f"/objects/{object_id}/commit", headers=_auth(token))


@pytest.mark.asyncio
async def test_two_concurrent_puts_same_key_consistent(
    client, user_token, datanode_headers, registered_datanodes
):
    """IT1: dos init+commit del mismo key en paralelo. Ambos terminan
    COMMITTED con versiones distintas y sin corromper el estado.
    """
    m1 = _manifest("/c.bin")
    m2 = _manifest("/c.bin")

    r1, r2 = await asyncio.gather(
        client.post("/objects/init", headers=_auth(user_token), json=m1),
        client.post("/objects/init", headers=_auth(user_token), json=m2),
    )
    assert r1.status_code == 201 and r2.status_code == 201
    obj1 = r1.json()["object_id"]
    obj2 = r2.json()["object_id"]

    r_c1, r_c2 = await asyncio.gather(
        _confirm_and_commit(client, datanode_headers, user_token, obj1, [b["block_id"] for b in m1["blocks"]]),
        _confirm_and_commit(client, datanode_headers, user_token, obj2, [b["block_id"] for b in m2["blocks"]]),
    )
    assert r_c1.status_code == 200 and r_c2.status_code == 200

    versions = sorted([r_c1.json()["version"], r_c2.json()["version"]])
    assert versions == [1, 2]


@pytest.mark.asyncio
async def test_pending_expiry_cleanup(
    client, user_token, datanode_headers, registered_datanodes
):
    """IT2: un PENDING con expires_at en el pasado se elimina del catalogo."""
    m = _manifest("/exp.bin")
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    object_id = r.json()["object_id"]
    block_ids = [b["block_id"] for b in m["blocks"]]

    # Adelantamos expires_at al pasado.
    factory = get_session_factory()
    async with factory() as session:
        await session.execute(
            update(ObjectRow)
            .where(ObjectRow.object_id == object_id)
            .values(expires_at=time.time() - 60)
        )
        await session.commit()

    task = PendingCleanupTask()
    await task._cleanup_once()

    # Ya no existe en el catalogo.
    async with factory() as session:
        obj = await session.get(ObjectRow, object_id)
        assert obj is None

    # Un reporte del DataNode con esos block_ids los devuelve como unknown.
    r_report = await client.post(
        "/datanodes/dn-1/blocks",
        headers=datanode_headers,
        json={"blocks": [{"block_id": bid, "checksum": "a"} for bid in block_ids]},
    )
    assert r_report.status_code == 200
    unknown = set(r_report.json()["unknown_blocks"])
    assert unknown == set(block_ids)


@pytest.mark.asyncio
async def test_delete_during_active_read(
    client, user_token, datanode_headers, registered_datanodes
):
    """IT3: GET devuelve ubicaciones; DELETE ocurre; las ubicaciones que ya
    tiene el cliente siguen siendo validas. Los bytes viven en los DataNodes,
    no en el ControlNode.
    """
    m = _manifest("/live.bin", n_blocks=1)
    r = await client.post("/objects/init", headers=_auth(user_token), json=m)
    object_id = r.json()["object_id"]
    await _confirm_and_commit(
        client, datanode_headers, user_token, object_id,
        [b["block_id"] for b in m["blocks"]],
    )

    r_get = await client.get(
        "/objects?key=/live.bin", headers=_auth(user_token)
    )
    assert r_get.status_code == 200
    # El cliente ya tiene las locations. Ahora borramos.
    r_del = await client.request(
        "DELETE", f"/objects/{object_id}", headers=_auth(user_token)
    )
    assert r_del.status_code == 204

    # La respuesta previa sigue teniendo direcciones (el cliente puede seguir).
    got = r_get.json()
    assert len(got["blocks"][0]["replicas"]) == 3
    # Y no hay bytes en ella (nunca los hubo).
    assert "data" not in got
    assert "content" not in got
