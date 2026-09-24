"""Estructuras de datos del modulo dfshacore.

Todos los campos son tipos primitivos para permitir serializacion trivial con
json.dumps(dataclasses.asdict(...)). El ControlNode elige su propio mecanismo
de persistencia sin necesidad de adaptadores.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class BlockInfo:
    """Describe un bloque dentro del manifiesto de un objeto."""

    block_id: str
    """Identificador unico del bloque, formato '{object_id}_{index:06d}'."""

    index: int
    """Posicion 0-based del bloque dentro del objeto."""

    size: int
    """Cantidad real de bytes del bloque. El ultimo bloque puede ser menor al
    block_size configurado; los anteriores son siempre iguales al block_size.
    """

    checksum: str
    """SHA-256 hexadecimal del contenido del bloque."""


@dataclass
class ObjectManifest:
    """Descripcion completa de un objeto particionado."""

    object_id: str
    """UUID4 generado al crear el objeto. No depende del contenido."""

    object_key: str
    """Ruta visible para el usuario, e.g. '/fotos/video.mp4'."""

    total_size: int
    """Bytes totales del objeto. 0 si el archivo esta vacio."""

    block_size: int
    """Tamano configurado de bloque en bytes."""

    global_checksum: str
    """SHA-256 del contenido completo del objeto, en orden."""

    blocks: List[BlockInfo] = field(default_factory=list)
    """Lista ordenada por index. Vacia si total_size == 0."""

    created_at: float = 0.0
    """Timestamp Unix del momento de particionamiento."""


@dataclass
class DataNodeInfo:
    """Entrada del inventario de DataNodes disponibles para colocacion."""

    node_id: str
    domain_id: str
    free_bytes: int
    load_score: float
    """Carga actual en [0.0, 1.0]; 0 = idle, 1 = saturado."""

    is_available: bool = True


@dataclass
class BlockPlacement:
    """Decision de colocacion para un bloque especifico.

    El orden de replica_nodes es significativo:
      [0]  -> primera replica (dominio del escritor)
      [-1] -> ultima replica (dominio distinto preferentemente)
    """

    block_id: str
    block_index: int
    replica_nodes: List[str]


@dataclass
class ObjectPlacement:
    """Resultado completo de la colocacion de un objeto."""

    object_id: str
    placements: List[BlockPlacement] = field(default_factory=list)
