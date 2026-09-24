"""Pruebas de integracion multi-modulo de dfshacore."""

from __future__ import annotations

import dataclasses
import json

from dfshacore.assembler import assemble_from_directory
from dfshacore.chunker import chunk_file
from dfshacore.models import ObjectManifest, BlockInfo
from dfshacore.placement import place_object


def test_roundtrip_multi_block_exact_bytes(tmp_path, random_bytes):
    """I1: round-trip byte a byte con multiples bloques y contenido binario."""
    data = random_bytes(4096, seed=99)
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=1024)
    assert len(manifest.blocks) == 4

    assemble_from_directory(manifest, str(blocks_dir), str(dst))
    assert src.read_bytes() == dst.read_bytes()


def test_large_object_on_multiple_nodes(tmp_path, sample_nodes, random_bytes):
    """I2: objeto de 3 bloques va a >= 2 nodos primarios distintos. Cubre RNF4."""
    data = random_bytes(3072, seed=100)
    src = tmp_path / "src.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=1024)
    assert len(manifest.blocks) == 3

    placement = place_object(manifest, sample_nodes, "domain-A", 3, seed=1)
    primaries = {p.replica_nodes[0] for p in placement.placements}
    assert len(primaries) >= 2


def test_manifest_serializable_json(tmp_path):
    """I3: ObjectManifest serializable a JSON via dataclasses.asdict."""
    src = tmp_path / "src.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(b"hola")

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=64)
    payload = json.dumps(dataclasses.asdict(manifest))
    restored_dict = json.loads(payload)

    restored = ObjectManifest(
        object_id=restored_dict["object_id"],
        object_key=restored_dict["object_key"],
        total_size=restored_dict["total_size"],
        block_size=restored_dict["block_size"],
        global_checksum=restored_dict["global_checksum"],
        blocks=[BlockInfo(**b) for b in restored_dict["blocks"]],
        created_at=restored_dict["created_at"],
    )
    assert restored.object_id == manifest.object_id
    assert restored.global_checksum == manifest.global_checksum
    assert restored.blocks == manifest.blocks
