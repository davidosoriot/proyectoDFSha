"""Endpoints por los que los DataNodes se registran y reportan estado.

Autenticados con la API key X-DataNode-Key. Ninguno recibe ni devuelve
contenido de bloques.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from ..core.catalog import CatalogService
from ..db.repository import Repository
from ..errors import ConflictError, NotFoundError
from .deps import get_repository, require_datanode_api_key
from .schemas import (
    BlockReportRequest,
    BlockReportResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    RegisterDataNodeRequest,
    RegisterDataNodeResponse,
)

router = APIRouter(
    prefix="/datanodes",
    tags=["datanodes"],
    dependencies=[Depends(require_datanode_api_key)],
)


@router.post(
    "/register",
    response_model=RegisterDataNodeResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_datanode(
    body: RegisterDataNodeRequest,
    repo: Repository = Depends(get_repository),
) -> RegisterDataNodeResponse:
    catalog = CatalogService(repo)
    try:
        node = await catalog.register_datanode(
            node_id=body.node_id,
            domain_id=body.domain_id,
            address=body.address,
            free_bytes=body.free_bytes,
            load_score=body.load_score,
        )
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return RegisterDataNodeResponse(registered=True, registered_at=node.registered_at)


@router.post("/{node_id}/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(
    node_id: str,
    body: HeartbeatRequest,
    repo: Repository = Depends(get_repository),
) -> HeartbeatResponse:
    catalog = CatalogService(repo)
    try:
        await catalog.receive_heartbeat(node_id, body.free_bytes, body.load_score)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    return HeartbeatResponse(ok=True)


@router.post("/{node_id}/blocks", response_model=BlockReportResponse)
async def block_report(
    node_id: str,
    body: BlockReportRequest,
    repo: Repository = Depends(get_repository),
) -> BlockReportResponse:
    node = await repo.get_datanode(node_id)
    if node is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DataNode {node_id} no existe",
        )

    catalog = CatalogService(repo)
    unknown: list[str] = []
    for entry in body.blocks:
        confirmed = await catalog.confirm_replica(entry.block_id, node_id)
        if confirmed is None:
            # El ControlNode no conoce esa asignacion: el DataNode debe borrar.
            unknown.append(entry.block_id)
    await repo.session.commit()
    return BlockReportResponse(unknown_blocks=unknown)
