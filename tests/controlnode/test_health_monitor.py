"""Pruebas de la deteccion de caida de DataNodes.

Invocamos _check_once() directamente en vez de esperar el task de fondo, para
tener control determinstico del tiempo. Manipulamos last_heartbeat_at en la DB
para simular nodos sin heartbeat.
"""

from __future__ import annotations

import time

import pytest
from sqlalchemy import select, update

from controlnode.core.health_monitor import HealthMonitorTask
from controlnode.db.connection import get_session_factory
from controlnode.db.models import DataNode, Event, NodeStatus, Replica, ReplicaStatus


async def _override_heartbeat(node_id: str, offset_seconds: float):
    """Retrocede last_heartbeat_at de un nodo por offset_seconds."""
    factory = get_session_factory()
    async with factory() as session:
        await session.execute(
            update(DataNode)
            .where(DataNode.node_id == node_id)
            .values(last_heartbeat_at=time.time() - offset_seconds)
        )
        await session.commit()


async def _get_node_status(node_id: str) -> str:
    factory = get_session_factory()
    async with factory() as session:
        r = await session.execute(select(DataNode).where(DataNode.node_id == node_id))
        node = r.scalar_one()
        return node.status


@pytest.mark.asyncio
async def test_node_marked_dead_after_30s(
    client, datanode_headers, registered_datanodes
):
    """HM1: >30s sin heartbeat => DEAD."""
    await _override_heartbeat("dn-1", 35.0)
    monitor = HealthMonitorTask()
    await monitor._check_once()
    assert await _get_node_status("dn-1") == NodeStatus.DEAD


@pytest.mark.asyncio
async def test_node_suspected_between_20s_and_30s(
    client, datanode_headers, registered_datanodes
):
    """HM2: 20-30s sin heartbeat => SUSPECTED."""
    await _override_heartbeat("dn-2", 25.0)
    monitor = HealthMonitorTask()
    await monitor._check_once()
    assert await _get_node_status("dn-2") == NodeStatus.SUSPECTED


@pytest.mark.asyncio
async def test_alive_node_not_touched(
    client, datanode_headers, registered_datanodes
):
    """HM3: heartbeat reciente => sigue ALIVE."""
    monitor = HealthMonitorTask()
    await monitor._check_once()
    assert await _get_node_status("dn-3") == NodeStatus.ALIVE


@pytest.mark.asyncio
async def test_dead_node_replicas_marked_lost_and_event_fired(
    client, user_token, datanode_headers, registered_datanodes
):
    """HM4+HM5: al pasar a DEAD, replicas -> LOST y evento REPLICA_LOST."""
    # Creamos un objeto y confirmamos sus replicas.
    import uuid

    obj_id = str(uuid.uuid4())
    block_id = f"{obj_id}_000000"
    m = {
        "object_key": "/hm.bin",
        "total_size": 64,
        "block_size": 64,
        "global_checksum": "z" * 64,
        "blocks": [
            {"block_id": block_id, "index": 0, "size": 64, "checksum": "a" * 64}
        ],
    }
    r = await client.post(
        "/objects/init",
        headers={"Authorization": f"Bearer {user_token}"},
        json=m,
    )
    assert r.status_code == 201

    # Confirmamos las 3 replicas.
    for node in ["dn-1", "dn-2", "dn-3"]:
        await client.post(
            f"/datanodes/{node}/blocks",
            headers=datanode_headers,
            json={"blocks": [{"block_id": block_id, "checksum": "a"}]},
        )

    # Matamos dn-1.
    await _override_heartbeat("dn-1", 40.0)
    monitor = HealthMonitorTask()
    await monitor._check_once()
    assert await _get_node_status("dn-1") == NodeStatus.DEAD

    # Verificar que la replica en dn-1 quedo LOST y hay un evento REPLICA_LOST.
    factory = get_session_factory()
    async with factory() as session:
        r_repl = await session.execute(
            select(Replica).where(Replica.datanode_id == "dn-1")
        )
        replicas = list(r_repl.scalars().all())
        assert replicas, "deberia haber al menos una replica asignada a dn-1"
        assert all(r.status == ReplicaStatus.LOST for r in replicas)

        r_events = await session.execute(
            select(Event).where(Event.event_type == "REPLICA_LOST")
        )
        events = list(r_events.scalars().all())
        assert len(events) >= 1


@pytest.mark.asyncio
async def test_dead_node_reappears_becomes_alive(
    client, datanode_headers, registered_datanodes
):
    """DN5: DEAD + heartbeat nuevo => ALIVE."""
    await _override_heartbeat("dn-1", 40.0)
    monitor = HealthMonitorTask()
    await monitor._check_once()
    assert await _get_node_status("dn-1") == NodeStatus.DEAD

    # Heartbeat.
    r = await client.post(
        "/datanodes/dn-1/heartbeat",
        headers=datanode_headers,
        json={"free_bytes": 1000, "load_score": 0.0},
    )
    assert r.status_code == 200
    assert await _get_node_status("dn-1") == NodeStatus.ALIVE
