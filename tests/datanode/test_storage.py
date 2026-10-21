"""Pruebas de BlockStore: put/get/delete, idempotencia y deteccion de corrupcion."""

from __future__ import annotations

import hashlib
import os

import pytest

from datanode.crypto import BlockCipher
from datanode.storage import BlockCorrupted, BlockNotFound, BlockStore, CHECKSUM_SIZE


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_put_and_get_roundtrip(tmp_path):
    store = BlockStore(str(tmp_path))
    data = b"hello world"
    bid = _digest(data)
    store.put(bid, data)
    assert store.exists(bid)
    assert store.get(bid) == data


def test_put_rejects_wrong_checksum(tmp_path):
    store = BlockStore(str(tmp_path))
    with pytest.raises(ValueError):
        store.put("deadbeef", b"abc", expected_checksum="00" * 32)


def test_get_nonexistent_raises(tmp_path):
    store = BlockStore(str(tmp_path))
    with pytest.raises(BlockNotFound):
        store.get("00" * 32)


def test_delete_idempotent(tmp_path):
    store = BlockStore(str(tmp_path))
    data = b"x"
    bid = _digest(data)
    store.put(bid, data)
    assert store.delete(bid) is True
    assert store.delete(bid) is False


def test_put_idempotent_same_content(tmp_path):
    store = BlockStore(str(tmp_path))
    data = b"same"
    bid = _digest(data)
    store.put(bid, data)
    store.put(bid, data)
    assert store.get(bid) == data


def test_corruption_detected(tmp_path):
    store = BlockStore(str(tmp_path))
    data = b"payload"
    bid = _digest(data)
    store.put(bid, data)
    # corrompemos el contenido en disco (saltando el checksum header).
    path = tmp_path / bid
    raw = path.read_bytes()
    tampered = raw[:CHECKSUM_SIZE] + b"X" + raw[CHECKSUM_SIZE + 1:]
    path.write_bytes(tampered)
    with pytest.raises(BlockCorrupted):
        store.get(bid)


def test_inventory_lists_all(tmp_path):
    store = BlockStore(str(tmp_path))
    for i in range(3):
        data = f"block-{i}".encode()
        store.put(_digest(data), data)
    inv = list(store.iter_inventory())
    assert len(inv) == 3
    assert all(len(e.checksum) == 64 for e in inv)


def test_rejects_path_traversal(tmp_path):
    store = BlockStore(str(tmp_path))
    with pytest.raises(ValueError):
        store.put("../evil", b"x")
    with pytest.raises(ValueError):
        store.put("a/b", b"x")


def test_roundtrip_with_encryption(tmp_path):
    key = os.urandom(32).hex()
    cipher = BlockCipher.from_hex(key)
    store = BlockStore(str(tmp_path), cipher=cipher)
    data = b"secret payload" * 100
    bid = _digest(data)
    store.put(bid, data)

    # El archivo en disco no debe contener los bytes en claro.
    on_disk = (tmp_path / bid).read_bytes()
    assert data not in on_disk

    assert store.get(bid) == data


def test_encryption_tag_detects_tampering(tmp_path):
    key = os.urandom(32).hex()
    store = BlockStore(str(tmp_path), cipher=BlockCipher.from_hex(key))
    data = b"important"
    bid = _digest(data)
    store.put(bid, data)

    path = tmp_path / bid
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 0xFF  # flip last byte del ciphertext
    path.write_bytes(bytes(raw))
    with pytest.raises(BlockCorrupted):
        store.get(bid)


def test_used_bytes_grows_with_blocks(tmp_path):
    store = BlockStore(str(tmp_path))
    assert store.used_bytes() == 0
    data = b"x" * 1024
    store.put(_digest(data), data)
    assert store.used_bytes() >= 1024 + CHECKSUM_SIZE
