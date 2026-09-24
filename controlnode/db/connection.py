"""Fabrica de sesiones SQLAlchemy async.

Cambiar de SQLite a PostgreSQL solo requiere cambiar DFSHA_DB_URL. Toda la
logica de negocio pasa por la capa de repositorio, no directamente por aqui.
"""

from __future__ import annotations

from typing import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _connect_args(url: str) -> dict:
    # aiosqlite requiere check_same_thread=False para uso con asyncio.
    if url.startswith("sqlite"):
        return {"check_same_thread": False}
    return {}


def get_engine() -> AsyncEngine:
    global _engine, _session_factory
    if _engine is None:
        _engine = create_async_engine(
            settings.db_url,
            connect_args=_connect_args(settings.db_url),
            echo=False,
            future=True,
        )
        _session_factory = async_sessionmaker(
            _engine, expire_on_commit=False, class_=AsyncSession
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        get_engine()
    assert _session_factory is not None
    return _session_factory


async def dispose_engine() -> None:
    """Cierra el engine. Usar en shutdown y en tests."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


async def get_session() -> AsyncIterator[AsyncSession]:
    """Dependency para FastAPI. Cada request abre y cierra su sesion."""
    factory = get_session_factory()
    async with factory() as session:
        yield session
