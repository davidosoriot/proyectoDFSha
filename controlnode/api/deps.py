"""Dependencias de FastAPI compartidas por los routers."""

from __future__ import annotations

from typing import AsyncIterator

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..core.auth_svc import AuthService, CurrentUser
from ..db.connection import get_session
from ..db.repository import Repository
from ..errors import AuthenticationError


async def get_db_session() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


def get_repository(session: AsyncSession = Depends(get_db_session)) -> Repository:
    return Repository(session)


async def get_current_user(
    authorization: str | None = Header(default=None),
) -> CurrentUser:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
        )
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Expected Bearer token",
        )
    token = authorization.split(" ", 1)[1].strip()
    try:
        return AuthService.decode_token(token)
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)
        )


async def require_datanode_api_key(
    x_datanode_key: str | None = Header(default=None),
) -> None:
    if not x_datanode_key or x_datanode_key != settings.datanode_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-DataNode-Key",
        )
