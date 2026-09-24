"""Pruebas del particionamiento y calculo de checksums."""

from __future__ import annotations

import hashlib
import os

from dfshacore.chunker import chunk_file


def _sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_empty_file(tmp_path, tmp_blocks_dir):
    """C1: archivo vacio produce manifiesto con 0 bloques y checksum del vacio."""
    src = tmp_path / "empty.bin"
    src.write_bytes(b"")

    manifest = chunk_file(str(src), "/empty.bin", tmp_blocks_dir, block_size=64)

    assert manifest.total_size == 0
    assert manifest.blocks == []
    assert manifest.global_checksum == hashlib.sha256(b"").hexdigest()


def test_smaller_than_block(tmp_path, tmp_blocks_dir):
    """C2: archivo menor al block_size cabe en 1 bloque."""
    data = b"hola mundo"
    src = tmp_path / "small.bin"
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/small.bin", tmp_blocks_dir, block_size=64)

    assert manifest.total_size == len(data)
    assert len(manifest.blocks) == 1
    assert manifest.blocks[0].size == len(data)
    assert manifest.blocks[0].checksum == _sha256_of_bytes(data)


def test_exact_multiple(tmp_path, tmp_blocks_dir):
    """C3: archivo de tamano N*block_size produce exactamente N bloques."""
    block_size = 16
    n = 4
    data = bytes(range(256))[: block_size * n]  # 64 bytes
    src = tmp_path / "exact.bin"
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/exact.bin", tmp_blocks_dir, block_size=block_size)

    assert manifest.total_size == block_size * n
    assert len(manifest.blocks) == n
    for b in manifest.blocks:
        assert b.size == block_size


def test_larger_than_block(tmp_path, tmp_blocks_dir):
    """C4: block_size + 1 produce 2 bloques, el segundo con size=1."""
    block_size = 16
    data = b"x" * (block_size + 1)
    src = tmp_path / "bigger.bin"
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/bigger.bin", tmp_blocks_dir, block_size=block_size)

    assert len(manifest.blocks) == 2
    assert manifest.blocks[0].size == block_size
    assert manifest.blocks[1].size == 1


def test_block_checksums_match_content(tmp_path, tmp_blocks_dir, random_bytes):
    """C5: checksum de cada bloque coincide con el SHA-256 del archivo escrito."""
    data = random_bytes(300, seed=7)
    src = tmp_path / "rand.bin"
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/rand.bin", tmp_blocks_dir, block_size=64)

    for b in manifest.blocks:
        path = os.path.join(tmp_blocks_dir, b.block_id)
        assert _sha256_of_file(path) == b.checksum
        assert os.path.getsize(path) == b.size


def test_global_checksum_matches_content(tmp_path, tmp_blocks_dir, random_bytes):
    """C6: global_checksum == SHA-256 del contenido original."""
    data = random_bytes(500, seed=11)
    src = tmp_path / "g.bin"
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/g.bin", tmp_blocks_dir, block_size=64)

    assert manifest.global_checksum == _sha256_of_bytes(data)


def test_block_ids_unique(tmp_path, tmp_blocks_dir, random_bytes):
    """C7: no hay block_ids repetidos."""
    data = random_bytes(200, seed=1)
    src = tmp_path / "u.bin"
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/u.bin", tmp_blocks_dir, block_size=32)

    ids = [b.block_id for b in manifest.blocks]
    assert len(ids) == len(set(ids))


def test_block_ids_not_content_based(tmp_path, tmp_blocks_dir):
    """C8: dos ejecuciones sobre el mismo archivo generan block_ids distintos.

    Verifica que no hay deduplicacion accidental. El object_id es UUID4, no
    depende del contenido, asi que los block_ids derivados tambien varian.
    """
    data = b"contenido identico" * 20
    src = tmp_path / "id.bin"
    src.write_bytes(data)

    m1 = chunk_file(str(src), "/id.bin", str(tmp_path / "b1"), block_size=32)
    m2 = chunk_file(str(src), "/id.bin", str(tmp_path / "b2"), block_size=32)

    ids1 = {b.block_id for b in m1.blocks}
    ids2 = {b.block_id for b in m2.blocks}
    assert ids1.isdisjoint(ids2)
    # Pero los checksums de bloque si coinciden (mismo contenido).
    csums1 = [b.checksum for b in m1.blocks]
    csums2 = [b.checksum for b in m2.blocks]
    assert csums1 == csums2
