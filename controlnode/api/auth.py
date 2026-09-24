"""Endpoints de autenticacion."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from ..core.auth_svc import AuthService
from ..core.catalog import CatalogService
from ..db.repository import Repository
from ..errors import AuthenticationError, ConflictError
from .deps import get_repository
from .schemas import (
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register(
    body: RegisterRequest,
    repo: Repository = Depends(get_repository),
) -> RegisterResponse:
    svc = AuthService(repo)
    try:
        user = await svc.register(body.username, body.password)
    except ConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        )
    # Al registrar creamos el directorio raiz para el usuario.
    await CatalogService(repo).ensure_root_directory(user.user_id)
    await repo.session.commit()
    return RegisterResponse(user_id=user.user_id, username=user.username)


@router.post("/login", response_model=LoginResponse)
async def login(
    body: LoginRequest,
    repo: Repository = Depends(get_repository),
) -> LoginResponse:
    svc = AuthService(repo)
    try:
        token = await svc.login(body.username, body.password)
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)
        )
    return LoginResponse(
        access_token=token.access_token,
        token_type=token.token_type,
        expires_in=token.expires_in,
    )
