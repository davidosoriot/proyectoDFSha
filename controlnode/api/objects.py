"""Endpoints del ciclo de vida de objetos.

Ninguno transporta contenido de bloques. Solo:
- Metadatos (checksums, tamanos, block_ids)
- Ubicaciones (node_id, address) para que el cliente hable directo con DataNodes
"""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from ..core.auth_svc import CurrentUser
from ..core.catalog import BlockInput, CatalogService, ObjectInitInput
from ..core.federation import FederationResolver
from ..core.placement_svc import PlacementService
from ..db.models import ObjectStatus, Replica, ReplicaStatus
from ..db.repository import Repository
from ..errors import (
    ConflictError,
    InsufficientResourcesError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
)
from dfshacore import BlockInfo, ObjectManifest
from .deps import get_current_user, get_repository
from .schemas import (
    BlockPlacementInfo,
    BlockReadInfo,
    CommitObjectResponse,
    DataNodeLocation,
    GetObjectResponse,
    InitObjectRequest,
    InitObjectResponse,
    ObjectVersionInfo,
    ReportFailureRequest,
    ReportFailureResponse,
    StatResponse,
)

router = APIRouter(prefix="/objects", tags=["objects"])


def _manifest_from_request(body: InitObjectRequest) -> ObjectManifest:
    """Convierte el request al ObjectManifest de dfshacore."""
    return ObjectManifest(
        object_id="",  # se ignora, se toma del prefijo de block_ids
        object_key=body.object_key,
        total_size=body.total_size,
        block_size=body.block_size,
        global_checksum=body.global_checksum,
        blocks=[
            BlockInfo(
                block_id=b.block_id,
                index=b.index,
                size=b.size,
                checksum=b.checksum,
            )
            for b in body.blocks
        ],
    )


@router.post(
    "/init",
    response_model=InitObjectResponse,
    status_code=status.HTTP_201_CREATED,
)
async def init_object(
    body: InitObjectRequest,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> InitObjectResponse:
    if not body.blocks and body.total_size > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Manifiesto sin bloques pero total_size > 0",
        )

    # Placement con el modulo dfshacore via PlacementService.
    manifest = _manifest_from_request(body)
    placement_svc = PlacementService(repo)
    try:
        decisions = await placement_svc.decide(manifest)
    except InsufficientResourcesError as exc:
        raise HTTPException(
            status_code=status.HTTP_507_INSUFFICIENT_STORAGE, detail=str(exc)
        )

    # Registro en el catalogo. init_object emite el evento OBJECT_INIT.
    catalog = CatalogService(repo)
    obj = await catalog.init_object(
        ObjectInitInput(
            owner_id=user.user_id,
            object_key=body.object_key,
            total_size=body.total_size,
            block_size=body.block_size,
            global_checksum=body.global_checksum,
            blocks=[
                BlockInput(
                    block_id=b.block_id,
                    index=b.index,
                    size=b.size,
                    checksum=b.checksum,
                )
                for b in body.blocks
            ],
        ),
        decisions,
    )

    # Construir respuesta con las ubicaciones (node_id + address).
    nodes_by_id = {}
    for d in decisions:
        for nid in d.replica_node_ids:
            if nid not in nodes_by_id:
                node = await repo.get_datanode(nid)
                nodes_by_id[nid] = node

    placements_resp: List[BlockPlacementInfo] = []
    for block, decision in zip(body.blocks, decisions):
        placements_resp.append(
            BlockPlacementInfo(
                block_id=block.block_id,
                index=block.index,
                replica_nodes=[
                    DataNodeLocation(
                        node_id=nid,
                        address=nodes_by_id[nid].address if nodes_by_id[nid] else "",
                    )
                    for nid in decision.replica_node_ids
                ],
            )
        )

    return InitObjectResponse(
        object_id=obj.object_id,
        status=obj.status,
        placements=placements_resp,
    )


@router.post(
    "/{object_id}/commit",
    response_model=CommitObjectResponse,
)
async def commit_object(
    object_id: str,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> CommitObjectResponse:
    catalog = CatalogService(repo)
    try:
        obj = await catalog.commit_object(object_id, user.user_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except PermissionDeniedError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    except InvalidStateError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        )
    except ConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=exc.args[0]
        )
    return CommitObjectResponse(
        object_id=obj.object_id, version=obj.version, status=obj.status
    )


@router.get("", response_model=GetObjectResponse)
async def get_object_by_key(
    key: str = Query(...),
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> GetObjectResponse:
    # Costura 3: pasamos por FederationResolver antes de tocar el catalogo local.
    resolver = FederationResolver()
    resolution = await resolver.resolve(key, user.user_id)
    # Hoy resolution.location siempre es 'local'; el bloque remote quedaria aca
    # (redirect al ControlNode remoto). Se deja preparado para Hito 3.

    obj = await repo.get_latest_committed(user.user_id, key, user.domain_id)
    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"objeto {key} no existe"
        )

    blocks = await repo.get_blocks_for_object(obj.object_id)
    replicas = await repo.get_replicas_for_object(obj.object_id)
    # Agrupar replicas por block_id.
    replicas_by_block: dict[str, List[Replica]] = {}
    for r in replicas:
        if r.status == ReplicaStatus.LOST:
            continue
        replicas_by_block.setdefault(r.block_id, []).append(r)

    # Cargar los datanodes referenciados una sola vez.
    node_cache = {}

    async def _location(datanode_id: str) -> DataNodeLocation:
        if datanode_id not in node_cache:
            node_cache[datanode_id] = await repo.get_datanode(datanode_id)
        n = node_cache[datanode_id]
        return DataNodeLocation(node_id=datanode_id, address=n.address if n else "")

    block_infos: List[BlockReadInfo] = []
    for b in blocks:
        locs = []
        # Preservar el orden por replica_index.
        for r in sorted(replicas_by_block.get(b.block_id, []), key=lambda x: x.replica_index):
            locs.append(await _location(r.datanode_id))
        block_infos.append(
            BlockReadInfo(
                block_id=b.block_id,
                index=b.block_index,
                size=b.size,
                checksum=b.checksum,
                replicas=locs,
            )
        )

    return GetObjectResponse(
        object_id=obj.object_id,
        object_key=obj.object_key,
        version=obj.version,
        total_size=obj.total_size,
        block_size=obj.block_size,
        global_checksum=obj.global_checksum,
        blocks=block_infos,
    )


@router.delete("/{object_id}")
async def delete_object(
    object_id: str,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> Response:
    catalog = CatalogService(repo)
    try:
        await catalog.delete_object(object_id, user.user_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except PermissionDeniedError:
        # Devolvemos 404 para no revelar la existencia del recurso a un usuario
        # que no es dueno (decision D en la guia).
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="objeto no existe"
        )
    except InvalidStateError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{object_id}/stat", response_model=StatResponse)
async def stat_object(
    object_id: str,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> StatResponse:
    obj = await repo.get_object(object_id)
    if obj is None or obj.owner_id != user.user_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="objeto no existe"
        )
    versions = await repo.get_all_versions(user.user_id, obj.object_key, user.domain_id)
    return StatResponse(
        object_key=obj.object_key,
        versions=[
            ObjectVersionInfo(
                object_id=v.object_id,
                version=v.version,
                status=v.status,
                total_size=v.total_size,
                created_at=v.created_at,
                committed_at=v.committed_at,
                deleted_at=v.deleted_at,
            )
            for v in versions
        ],
    )


@router.post(
    "/{object_id}/blocks/{block_id}/report-failure",
    response_model=ReportFailureResponse,
)
async def report_replica_failure(
    object_id: str,
    block_id: str,
    body: ReportFailureRequest,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> ReportFailureResponse:
    obj = await repo.get_object(object_id)
    if obj is None or obj.owner_id != user.user_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="objeto no existe"
        )
    catalog = CatalogService(repo)
    try:
        replica = await catalog.mark_replica_lost(block_id, body.node_id, body.reason)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    assert replica is not None
    return ReportFailureResponse(acknowledged=True, replica_id=replica.replica_id)
