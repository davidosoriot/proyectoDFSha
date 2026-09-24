"""Pruebas del reensamblado y verificacion de integridad."""

from __future__ import annotations

import os

import pytest

from dfshacore.assembler import assemble_from_directory
from dfshacore.chunker import chunk_file
from dfshacore.errors import BlockCorruptError, MissingBlockError


def _roundtrip(tmp_path, data: bytes, block_size: int):
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=block_size)
    assemble_from_directory(manifest, str(blocks_dir), str(dst))

    return src.read_bytes(), dst.read_bytes(), manifest


def test_roundtrip_small(tmp_path):
    """A1: round-trip byte a byte para archivo < block_size."""
    data = b"algo pequeno"
    original, restored, _ = _roundtrip(tmp_path, data, block_size=64)
    assert original == restored


def test_roundtrip_exact_multiple(tmp_path):
    """A2: round-trip byte a byte para archivo de tamano N*block_size."""
    block_size = 16
    data = b"0123456789ABCDEF" * 5  # 80 bytes
    original, restored, m = _roundtrip(tmp_path, data, block_size=block_size)
    assert original == restored
    assert len(m.blocks) == 5


def test_roundtrip_empty(tmp_path):
    """A3: round-trip archivo vacio."""
    original, restored, m = _roundtrip(tmp_path, b"", block_size=64)
    assert original == restored == b""
    assert m.blocks == []


def test_corrupt_block_detected(tmp_path, random_bytes):
    """A4: modificar 1 byte de un bloque produce BlockCorruptError."""
    data = random_bytes(200, seed=42)
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=64)

    # Corromper el segundo bloque.
    target = manifest.blocks[1]
    target_path = blocks_dir / target.block_id
    corrupted = bytearray(target_path.read_bytes())
    corrupted[0] ^= 0xFF
    target_path.write_bytes(bytes(corrupted))

    with pytest.raises(BlockCorruptError) as exc:
        assemble_from_directory(manifest, str(blocks_dir), str(dst))

    assert exc.value.block_id == target.block_id
    assert exc.value.index == target.index


def test_missing_block_detected(tmp_path, random_bytes):
    """A5: omitir un bloque produce MissingBlockError con block_id correcto."""
    data = random_bytes(200, seed=43)
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=64)

    missing = manifest.blocks[-1]
    os.remove(blocks_dir / missing.block_id)

    with pytest.raises(MissingBlockError) as exc:
        assemble_from_directory(manifest, str(blocks_dir), str(dst))

    assert exc.value.block_id == missing.block_id
    assert exc.value.index == missing.index


def test_wrong_order_still_works(tmp_path, random_bytes):
    """A6: bloques en orden distinto al indice se reordenan correctamente."""
    data = random_bytes(200, seed=44)
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    src.write_bytes(data)

    manifest = chunk_file(str(src), "/x", str(blocks_dir), block_size=64)
    # Invertimos la lista manualmente.
    manifest.blocks = list(reversed(manifest.blocks))

    assemble_from_directory(manifest, str(blocks_dir), str(dst))
    assert src.read_bytes() == dst.read_bytes()
