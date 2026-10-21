"""Endpoints de ACLs: conceder / revocar / listar permisos por prefijo."""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel

from ..core.acl_svc import ACLService
from ..core.auth_svc import CurrentUser
from ..db.repository import Repository
from ..errors import ConflictError, NotFoundError
from .deps import get_current_user, get_repository

router = APIRouter(prefix="/acl", tags=["acl"])


class GrantRequest(BaseModel):
    grantee_username: str
    path_prefix: str
    permissions: List[str]  # cualquiera de READ, WRITE, DELETE


class ACLItem(BaseModel):
    acl_id: str
    owner_username: str
    grantee_username: str
    path_prefix: str
    permissions: List[str]
    created_at: float


class ListResponse(BaseModel):
    acls: List[ACLItem]


@router.post("/grant", response_model=ACLItem, status_code=status.HTTP_201_CREATED)
async def grant(
    body: GrantRequest,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> ACLItem:
    svc = ACLService(repo)
    try:
        rec = await svc.grant(
            owner_id=user.user_id,
            grantee_username=body.grantee_username,
            path_prefix=body.path_prefix,
            permissions=body.permissions,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return ACLItem(
        acl_id=rec.acl_id,
        owner_username=rec.owner_username,
        grantee_username=rec.grantee_username,
        path_prefix=rec.path_prefix,
        permissions=rec.permissions,
        created_at=rec.created_at,
    )


@router.delete("/revoke", status_code=status.HTTP_204_NO_CONTENT)
async def revoke(
    grantee_username: str = Query(...),
    path_prefix: str = Query(...),
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> Response:
    svc = ACLService(repo)
    try:
        await svc.revoke(user.user_id, grantee_username, path_prefix)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/list", response_model=ListResponse)
async def list_mine(
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> ListResponse:
    svc = ACLService(repo)
    records = await svc.list_granted_by(user.user_id)
    return ListResponse(
        acls=[
            ACLItem(
                acl_id=r.acl_id,
                owner_username=r.owner_username,
                grantee_username=r.grantee_username,
                path_prefix=r.path_prefix,
                permissions=r.permissions,
                created_at=r.created_at,
            )
            for r in records
        ]
    )
