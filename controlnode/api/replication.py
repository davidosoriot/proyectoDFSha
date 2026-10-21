"""Endpoints de replicacion del ControlNode.

- GET /replication/snapshot: devuelve el catalogo completo serializado.
- GET /replication/role: devuelve el rol actual del nodo (debug).
- Middleware/dependency `require_leader` que rechaza escrituras en followers.

Autenticacion: X-Replication-Key compartida entre leader y followers.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, status

from ..config import settings
from ..core.replication import export_snapshot
from ..db.connection import get_session
from ..db.repository import Repository
from .deps import get_db_session

router = APIRouter(prefix="/replication", tags=["replication"])


async def require_replication_key(x_replication_key: str = Header(default="")) -> str:
    if x_replication_key != settings.replication_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Replication-Key invalida",
        )
    return x_replication_key


async def require_leader() -> None:
    """Dependency que bloquea escrituras cuando el nodo es FOLLOWER."""
    if settings.role != "LEADER":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Este nodo es FOLLOWER. Enviar escrituras a {settings.leader_url}",
        )


@router.get("/role")
async def get_role() -> dict:
    return {
        "role": settings.role,
        "leader_url": settings.leader_url if settings.role == "FOLLOWER" else None,
    }


@router.get("/snapshot", dependencies=[Depends(require_replication_key)])
async def snapshot(session=Depends(get_db_session)) -> dict:
    """Devuelve snapshot completo del catalogo del leader."""
    if settings.role != "LEADER":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="snapshot solo disponible en el leader",
        )
    return await export_snapshot(session)
