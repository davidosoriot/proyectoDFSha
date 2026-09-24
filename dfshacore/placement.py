"""Politica de colocacion de replicas de bloques en DataNodes.

Reglas (fijadas por especificacion):
    1. Factor de replicacion: 3 (configurable).
    2. Las N replicas de un bloque van a N nodos distintos.
    3. Primera replica: en un nodo del dominio del escritor.
    4. Ultima replica: preferentemente en un dominio distinto al del escritor.
    5. Entre candidatos se elige por espacio libre y carga, con desempate
       aleatorio para evitar puntos calientes.
    6. Los bloques del mismo objeto se reparten entre nodos distintos.

Deterministico si se pasa un seed; de otro modo usa el generador global.
"""

from __future__ import annotations

import random
from typing import Dict, List, Optional

from .errors import InsufficientNodesError
from .models import BlockPlacement, DataNodeInfo, ObjectManifest, ObjectPlacement

W_FREE: float = 0.5
"""Peso del espacio libre en la puntuacion. Positivo: mas espacio, mejor."""

W_LOAD: float = 0.3
"""Peso de la carga reportada. Negativo: mas carga, peor."""

W_SPREAD: float = 0.2
"""Peso del uso previo dentro del mismo objeto. Negativo: mas replicas ya
asignadas de este objeto, peor. Sirve para distribuir bloques del mismo
objeto entre nodos distintos.
"""

JITTER_EPSILON: float = 0.01
"""Rango del jitter aleatorio para desempate. Pequeno para no dominar la
puntuacion cuando hay diferencias claras entre candidatos.
"""


def _normalize(value: float, maximum: float) -> float:
    """Escala value a [0, 1] contra maximum. Si maximum <= 0 devuelve 1.0."""
    if maximum <= 0:
        return 1.0
    return value / maximum


def _score(
    node: DataNodeInfo,
    usage: Dict[str, int],
    max_free: int,
    max_usage: int,
    rng: random.Random,
) -> float:
    free_norm = _normalize(node.free_bytes, max_free)
    usage_norm = _normalize(usage[node.node_id], max_usage)
    return (
        W_FREE * free_norm
        - W_LOAD * node.load_score
        - W_SPREAD * usage_norm
        + rng.uniform(0.0, JITTER_EPSILON)
    )


def _pick_best(
    candidates: List[DataNodeInfo],
    usage: Dict[str, int],
    rng: random.Random,
) -> DataNodeInfo:
    max_free = max((n.free_bytes for n in candidates), default=0)
    max_usage = max((usage[n.node_id] for n in candidates), default=0)
    return max(
        candidates,
        key=lambda n: _score(n, usage, max_free, max_usage, rng),
    )


def _place_block(
    block_id: str,
    block_index: int,
    available: List[DataNodeInfo],
    writer_domain: str,
    replication_factor: int,
    usage: Dict[str, int],
    rng: random.Random,
) -> BlockPlacement:
    selected_ids: List[str] = []

    # Replica 1: dominio del escritor. Fallback a cualquier nodo si el dominio
    # no tiene nodos disponibles.
    domain_candidates = [n for n in available if n.domain_id == writer_domain]
    if not domain_candidates:
        domain_candidates = list(available)
    r1 = _pick_best(domain_candidates, usage, rng)
    selected_ids.append(r1.node_id)
    r1_domain = r1.domain_id

    # Replicas intermedias: mejor puntuacion sobre cualquier nodo no seleccionado.
    for _ in range(replication_factor - 2):
        candidates = [n for n in available if n.node_id not in selected_ids]
        rx = _pick_best(candidates, usage, rng)
        selected_ids.append(rx.node_id)

    # Ultima replica: preferentemente en un dominio distinto al de la primera.
    if replication_factor >= 2:
        remaining = [n for n in available if n.node_id not in selected_ids]
        cross_domain = [n for n in remaining if n.domain_id != r1_domain]
        candidates_last = cross_domain if cross_domain else remaining
        r_last = _pick_best(candidates_last, usage, rng)
        selected_ids.append(r_last.node_id)

    # Actualizar contador de uso para spread entre bloques del mismo objeto.
    for node_id in selected_ids:
        usage[node_id] = usage.get(node_id, 0) + 1

    return BlockPlacement(
        block_id=block_id,
        block_index=block_index,
        replica_nodes=selected_ids,
    )


def place_object(
    manifest: ObjectManifest,
    nodes: List[DataNodeInfo],
    writer_domain: str,
    replication_factor: int = 3,
    seed: Optional[int] = None,
) -> ObjectPlacement:
    """Decide la colocacion de todas las replicas de todos los bloques.

    Errores:
        InsufficientNodesError si len(nodos disponibles) < replication_factor.

    El resultado es deterministico cuando se pasa seed.
    """
    if replication_factor <= 0:
        raise ValueError("replication_factor debe ser positivo")

    available = [n for n in nodes if n.is_available]
    if len(available) < replication_factor:
        raise InsufficientNodesError(
            required=replication_factor,
            available=len(available),
        )

    rng = random.Random(seed)
    usage: Dict[str, int] = {n.node_id: 0 for n in available}

    placements: List[BlockPlacement] = []
    for block in manifest.blocks:
        placement = _place_block(
            block_id=block.block_id,
            block_index=block.index,
            available=available,
            writer_domain=writer_domain,
            replication_factor=replication_factor,
            usage=usage,
            rng=rng,
        )
        placements.append(placement)

    return ObjectPlacement(object_id=manifest.object_id, placements=placements)
