"""PlacementService: adapta el inventario del catalogo al modulo dfshacore.

El ControlNode guarda los DataNodes en la tabla `datanodes`, pero el algoritmo
de colocacion vive en dfshacore y consume DataNodeInfo. Este servicio
convierte entre ambos.
"""

from __future__ import annotations

from typing import List, Optional

from dfshacore import DataNodeInfo, ObjectManifest, place_object
from dfshacore.errors import InsufficientNodesError as _DfshaInsufficient

from ..config import settings
from ..db.models import NodeStatus
from ..db.repository import Repository
from ..errors import InsufficientResourcesError
from .catalog import PlacementDecision


class PlacementService:
    def __init__(self, repo: Repository):
        self.repo = repo

    async def _current_inventory(self) -> List[DataNodeInfo]:
        """Nodos ALIVE en cualquier dominio, convertidos a DataNodeInfo.

        Los SUSPECTED se excluyen: no queremos asignar nuevas replicas a
        nodos que probablemente estan mal (aunque todavia no esten muertos).
        Esto reduce la ventana en que se hacen malas asignaciones.
        """
        alive = await self.repo.list_alive_datanodes()
        return [
            DataNodeInfo(
                node_id=n.node_id,
                domain_id=n.domain_id,
                free_bytes=n.free_bytes,
                load_score=n.load_score,
                is_available=True,
            )
            for n in alive
        ]

    async def decide(
        self,
        manifest: ObjectManifest,
        writer_domain: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> List[PlacementDecision]:
        writer_domain = writer_domain or settings.domain_id
        inventory = await self._current_inventory()

        try:
            placement = place_object(
                manifest=manifest,
                nodes=inventory,
                writer_domain=writer_domain,
                replication_factor=settings.replication_factor,
                seed=seed,
            )
        except _DfshaInsufficient as exc:
            raise InsufficientResourcesError(
                f"DataNodes insuficientes: se requieren {exc.required} "
                f"y hay {exc.available} disponibles"
            ) from exc

        return [
            PlacementDecision(
                block_id=p.block_id,
                replica_node_ids=list(p.replica_nodes),
            )
            for p in placement.placements
        ]
