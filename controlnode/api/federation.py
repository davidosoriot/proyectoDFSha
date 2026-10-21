"""Endpoints de federacion CN-to-CN.

Dos planos:

- Administrativos (api key de DataNode): registrar/listar peers, lanzar propagacion.
- Peer-to-peer (api key de federacion): recepcion de eventos, lookup de objetos.
"""

from __future__ import annotations

from typing import Any, List

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from pydantic import BaseModel

from ..config import settings
from ..core.federation_svc import FederationPropagator, PeerRegistry
from ..db.models import ObjectStatus, Replica, ReplicaStatus
from ..db.repository import Repository
from ..errors import ConflictError, NotFoundError
from .deps import get_repository, require_datanode_api_key


# --- Admin: registro de peers ---

admin_router = APIRouter(prefix="/federation", tags=["federation"])


class RegisterPeerRequest(BaseModel):
    domain_id: str
    endpoint: str
    api_key: str


class PeerResponse(BaseModel):
    domain_id: str
    endpoint: str
    status: str
    registered_at: float
    last_seen_at: float | None


class PeerListResponse(BaseModel):
    peers: List[PeerResponse]


@admin_router.post(
    "/peers",
    response_model=PeerResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_datanode_api_key)],
)
async def register_peer(
    body: RegisterPeerRequest, repo: Repository = Depends(get_repository)
) -> PeerResponse:
    registry = PeerRegistry(repo)
    try:
        peer = await registry.register(body.domain_id, body.endpoint, body.api_key)
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return PeerResponse(
        domain_id=peer.domain_id,
        endpoint=peer.endpoint,
        status=peer.status,
        registered_at=peer.registered_at,
        last_seen_at=peer.last_seen_at,
    )


@admin_router.delete(
    "/peers/{domain_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_datanode_api_key)],
)
async def remove_peer(domain_id: str, repo: Repository = Depends(get_repository)) -> Response:
    registry = PeerRegistry(repo)
    try:
        await registry.remove(domain_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@admin_router.get(
    "/peers",
    response_model=PeerListResponse,
    dependencies=[Depends(require_datanode_api_key)],
)
async def list_peers(repo: Repository = Depends(get_repository)) -> PeerListResponse:
    registry = PeerRegistry(repo)
    peers = await registry.list_all()
    return PeerListResponse(
        peers=[
            PeerResponse(
                domain_id=p.domain_id,
                endpoint=p.endpoint,
                status=p.status,
                registered_at=p.registered_at,
                last_seen_at=p.last_seen_at,
            )
            for p in peers
        ]
    )


@admin_router.post(
    "/propagate",
    dependencies=[Depends(require_datanode_api_key)],
)
async def propagate_once() -> dict:
    """Lanza una pasada manual de propagacion (util para tests)."""
    return await FederationPropagator().tick()


# --- Peer-to-peer: necesita X-Federation-Key ---

peer_router = APIRouter(prefix="/federation", tags=["federation"])


async def require_federation_key(x_federation_key: str = Header(default="")) -> str:
    if x_federation_key != settings.federation_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Federation-Key invalida",
        )
    return x_federation_key


class EventIn(BaseModel):
    event_id: int
    domain_id: str
    event_type: str
    entity_id: str
    payload: Any
    created_at: float


class EventsBatch(BaseModel):
    events: List[EventIn]


@peer_router.post(
    "/events",
    dependencies=[Depends(require_federation_key)],
)
async def receive_events(body: EventsBatch) -> dict:
    """Recibe eventos de otro dominio. Hoy solo los registra en logs; en
    produccion alimentaria la vista de federacion (objetos remotos cacheados,
    etc.). Devuelve el conteo recibido para que el emisor pueda confirmar.
    """
    import logging
    log = logging.getLogger("controlnode.federation")
    log.info("recibidos %d eventos de dominios peer", len(body.events))
    return {"received": len(body.events)}


@peer_router.get(
    "/objects",
    dependencies=[Depends(require_federation_key)],
)
async def lookup_object(
    key: str = Query(...),
    owner_username: str = Query(...),
    repo: Repository = Depends(get_repository),
) -> dict:
    """Resuelve un objeto para otro dominio. Devuelve metadatos+ubicaciones.

    Hace la misma logica que GET /objects pero autenticado por federacion y
    sin requerir JWT de usuario: la autorizacion la hace el dominio llamante.
    """
    owner = await repo.get_user_by_username(owner_username)
    if owner is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"usuario {owner_username} no existe"
        )
    obj = await repo.get_latest_committed(owner.user_id, key, settings.domain_id)
    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"objeto {key} no existe"
        )
    blocks = await repo.get_blocks_for_object(obj.object_id)
    replicas = await repo.get_replicas_for_object(obj.object_id)
    by_block: dict[str, List[Replica]] = {}
    for r in replicas:
        if r.status == ReplicaStatus.LOST:
            continue
        by_block.setdefault(r.block_id, []).append(r)

    node_cache: dict[str, Any] = {}

    async def _loc(datanode_id: str) -> dict:
        if datanode_id not in node_cache:
            node_cache[datanode_id] = await repo.get_datanode(datanode_id)
        n = node_cache[datanode_id]
        return {"node_id": datanode_id, "address": n.address if n else ""}

    block_infos = []
    for b in blocks:
        locs = []
        for r in sorted(by_block.get(b.block_id, []), key=lambda x: x.replica_index):
            locs.append(await _loc(r.datanode_id))
        block_infos.append(
            {
                "block_id": b.block_id,
                "index": b.block_index,
                "size": b.size,
                "checksum": b.checksum,
                "replicas": locs,
            }
        )

    return {
        "object_id": obj.object_id,
        "object_key": obj.object_key,
        "version": obj.version,
        "total_size": obj.total_size,
        "block_size": obj.block_size,
        "global_checksum": obj.global_checksum,
        "blocks": block_infos,
        "domain_id": settings.domain_id,
    }
