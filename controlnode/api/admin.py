"""Endpoints administrativos del ControlNode.

Protegidos por la misma API key que los DataNodes (no son de uso de usuario
final). Hoy exponen un disparador manual de re-replicacion y una vista del
log de eventos pendientes.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from ..core.rereplication import RereplicationService
from ..db.repository import Repository
from .deps import get_repository, require_datanode_api_key

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_datanode_api_key)],
)


@router.post("/rereplicate")
async def trigger_rereplication(repo: Repository = Depends(get_repository)) -> dict:
    """Dispara una pasada de re-replicacion y devuelve estadisticas."""
    svc = RereplicationService(repo)
    stats = await svc.tick()
    return stats


@router.get("/events")
async def list_pending_events(
    limit: int = 50, repo: Repository = Depends(get_repository)
) -> dict:
    events = await repo.list_pending_events(limit=limit)
    return {
        "count": len(events),
        "events": [
            {
                "event_id": e.event_id,
                "event_type": e.event_type,
                "entity_id": e.entity_id,
                "created_at": e.created_at,
            }
            for e in events
        ],
    }
