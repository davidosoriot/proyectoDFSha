"""Pruebas de la politica de colocacion de replicas."""

from __future__ import annotations

import pytest

from dfshacore.errors import InsufficientNodesError
from dfshacore.models import BlockInfo, DataNodeInfo, ObjectManifest
from dfshacore.placement import place_object


def _make_manifest(num_blocks: int, object_id: str = "obj-1") -> ObjectManifest:
    blocks = [
        BlockInfo(
            block_id=f"{object_id}_{i:06d}",
            index=i,
            size=64,
            checksum="x" * 64,
        )
        for i in range(num_blocks)
    ]
    return ObjectManifest(
        object_id=object_id,
        object_key="/x",
        total_size=num_blocks * 64,
        block_size=64,
        global_checksum="y" * 64,
        blocks=blocks,
    )


def test_no_replica_shares_node(sample_nodes):
    """P1: para cada bloque, las 3 replicas van a nodos distintos."""
    m = _make_manifest(num_blocks=5)
    result = place_object(m, sample_nodes, "domain-A", replication_factor=3, seed=1)
    for p in result.placements:
        assert len(p.replica_nodes) == 3
        assert len(set(p.replica_nodes)) == 3


def test_first_replica_in_writer_domain(sample_nodes):
    """P2: primera replica siempre en el dominio del escritor."""
    m = _make_manifest(num_blocks=5)
    result = place_object(m, sample_nodes, "domain-A", replication_factor=3, seed=1)
    node_by_id = {n.node_id: n for n in sample_nodes}
    for p in result.placements:
        first = node_by_id[p.replica_nodes[0]]
        assert first.domain_id == "domain-A"


def test_last_replica_prefers_other_domain(sample_nodes):
    """P3: cuando existen nodos en otro dominio, la ultima replica esta ahi."""
    m = _make_manifest(num_blocks=5)
    result = place_object(m, sample_nodes, "domain-A", replication_factor=3, seed=1)
    node_by_id = {n.node_id: n for n in sample_nodes}
    for p in result.placements:
        first_domain = node_by_id[p.replica_nodes[0]].domain_id
        last_domain = node_by_id[p.replica_nodes[-1]].domain_id
        assert last_domain != first_domain


def test_insufficient_nodes_raises():
    """P4: menos nodos disponibles que el factor => InsufficientNodesError."""
    m = _make_manifest(num_blocks=1)
    nodes = [
        DataNodeInfo("n1", "A", 100, 0.0),
        DataNodeInfo("n2", "A", 100, 0.0),
    ]
    with pytest.raises(InsufficientNodesError) as exc:
        place_object(m, nodes, "A", replication_factor=3, seed=1)
    assert exc.value.required == 3
    assert exc.value.available == 2


def test_deterministic_with_seed(sample_nodes):
    """P5: mismo seed produce el mismo resultado."""
    m = _make_manifest(num_blocks=4)
    r1 = place_object(m, sample_nodes, "domain-A", 3, seed=42)
    r2 = place_object(m, sample_nodes, "domain-A", 3, seed=42)
    ids1 = [p.replica_nodes for p in r1.placements]
    ids2 = [p.replica_nodes for p in r2.placements]
    assert ids1 == ids2


def test_no_writer_domain_nodes_fallback():
    """P6: sin nodos en el dominio del escritor no falla; usa cualquier disponible."""
    m = _make_manifest(num_blocks=1)
    nodes = [
        DataNodeInfo("b1", "domain-B", 100, 0.0),
        DataNodeInfo("b2", "domain-B", 100, 0.0),
        DataNodeInfo("b3", "domain-B", 100, 0.0),
    ]
    result = place_object(m, nodes, "domain-A", 3, seed=1)
    assert len(result.placements[0].replica_nodes) == 3
    # Los 3 nodos usados son todos de domain-B (unico dominio disponible).
    assert all(nid.startswith("b") for nid in result.placements[0].replica_nodes)


def test_blocks_spread_across_nodes(sample_nodes):
    """P7: objeto con >= 2 bloques usa >= 2 nodos primarios distintos.

    Cubre RNF4: los bloques del mismo objeto no se concentran en un unico nodo.
    """
    m = _make_manifest(num_blocks=6)
    result = place_object(m, sample_nodes, "domain-A", 3, seed=1)
    primaries = {p.replica_nodes[0] for p in result.placements}
    assert len(primaries) >= 2
