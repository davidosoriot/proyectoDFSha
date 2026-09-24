"""Fixtures compartidos para las pruebas de dfshacore."""

from __future__ import annotations

import os
import random
import sys
from typing import List

import pytest

# Aseguramos que el paquete raiz sea importable cuando pytest se corre desde el
# directorio del proyecto.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dfshacore.models import DataNodeInfo  # noqa: E402


@pytest.fixture
def tmp_blocks_dir(tmp_path):
    d = tmp_path / "blocks"
    d.mkdir()
    return str(d)


@pytest.fixture
def random_bytes():
    def _make(size: int, seed: int = 1234) -> bytes:
        rng = random.Random(seed)
        return bytes(rng.randrange(0, 256) for _ in range(size))

    return _make


@pytest.fixture
def sample_nodes() -> List[DataNodeInfo]:
    """Inventario de 6 nodos: 3 en 'A' y 3 en 'B'. Todos ALIVE."""
    return [
        DataNodeInfo("node-A1", "domain-A", free_bytes=1_000, load_score=0.1),
        DataNodeInfo("node-A2", "domain-A", free_bytes=900, load_score=0.2),
        DataNodeInfo("node-A3", "domain-A", free_bytes=800, load_score=0.3),
        DataNodeInfo("node-B1", "domain-B", free_bytes=1_000, load_score=0.1),
        DataNodeInfo("node-B2", "domain-B", free_bytes=900, load_score=0.2),
        DataNodeInfo("node-B3", "domain-B", free_bytes=800, load_score=0.3),
    ]
