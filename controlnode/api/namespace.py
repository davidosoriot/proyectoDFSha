"""Endpoints del espacio de nombres."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from ..core.auth_svc import CurrentUser
from ..core.catalog import CatalogService
from ..core.namespace_svc import NamespaceService
from ..db.repository import Repository
from ..errors import ConflictError, NotFoundError
from .deps import get_current_user, get_repository
from .schemas import LsEntry, LsResponse, MkdirRequest, MkdirResponse

router = APIRouter(prefix="/ns", tags=["namespace"])


@router.get("/ls", response_model=LsResponse)
async def ls(
    path: str = Query("/"),
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> LsResponse:
    svc = NamespaceService(repo)
    try:
        entries = await svc.list_dir(user.user_id, path)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    return LsResponse(
        path=path,
        entries=[LsEntry(name=e.name, type=e.type, created_at=e.created_at) for e in entries],
    )


@router.post("/mkdir", response_model=MkdirResponse, status_code=status.HTTP_201_CREATED)
async def mkdir(
    body: MkdirRequest,
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> MkdirResponse:
    catalog = CatalogService(repo)
    try:
        d = await catalog.create_directory(user.user_id, body.path)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return MkdirResponse(dir_id=d.dir_id, path=d.full_path)


@router.delete("/rmdir")
async def rmdir(
    path: str = Query(...),
    user: CurrentUser = Depends(get_current_user),
    repo: Repository = Depends(get_repository),
) -> Response:
    catalog = CatalogService(repo)
    try:
        await catalog.remove_directory(user.user_id, path)
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return Response(status_code=status.HTTP_204_NO_CONTENT)
