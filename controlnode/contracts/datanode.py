"""Contrato que debe cumplir el DataNode.

El ControlNode no implementa el DataNode (esa parte la hace otro integrante).
Define aca la interfaz abstracta que espera y provee un cliente en memoria
para poder probar el ControlNode sin que exista todavia el DataNode real.

IMPORTANTE: este contrato NO se usa desde los endpoints REST del ControlNode.
La invariante del sistema es que el ControlNode nunca toca bytes de contenido.
Este contrato modela lo que el cliente hara contra los DataNodes; se expone
aca solo para permitir escribir tests de integracion end-to-end en memoria.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class DataNodeReport:
    """Snapshot del estado que el DataNode reporta al ControlNode."""

    free_bytes: int
    load_score: float
    block_ids: list[str]


class DataNodeClient(ABC):
    """Interfaz que consume el cliente de DFSha al hablar con un DataNode."""

    node_id: str
    address: str

    @abstractmethod
    async def store_block(self, block_id: str, data: bytes) -> None:
        """Guarda un bloque bajo el block_id dado."""

    @abstractmethod
    async def fetch_block(self, block_id: str) -> bytes:
        """Devuelve los bytes del bloque; lanza KeyError si no existe."""

    @abstractmethod
    async def delete_block(self, block_id: str) -> None:
        """Borra un bloque; noop si no existe."""

    @abstractmethod
    async def report(self) -> DataNodeReport:
        """Genera el reporte de inventario para el ControlNode."""


@dataclass
class InMemoryDataNode(DataNodeClient):
    """DataNode falso que guarda los bloques en memoria.

    Util para pruebas. En produccion el DataNode real es un proceso separado
    con su propio storage y su propia API gRPC.
    """

    node_id: str
    address: str = "in-memory"
    capacity_bytes: int = 10_000_000_000
    _storage: Dict[str, bytes] = field(default_factory=dict)

    async def store_block(self, block_id: str, data: bytes) -> None:
        self._storage[block_id] = data

    async def fetch_block(self, block_id: str) -> bytes:
        if block_id not in self._storage:
            raise KeyError(block_id)
        return self._storage[block_id]

    async def delete_block(self, block_id: str) -> None:
        self._storage.pop(block_id, None)

    async def report(self) -> DataNodeReport:
        used = sum(len(v) for v in self._storage.values())
        return DataNodeReport(
            free_bytes=max(0, self.capacity_bytes - used),
            load_score=0.0,
            block_ids=list(self._storage.keys()),
        )
