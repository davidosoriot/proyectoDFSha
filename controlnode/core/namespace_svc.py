"""Servicio del espacio de nombres virtual.

Los directorios se guardan explicitamente en la DB para que ls sea O(1) y
mkdir de directorios vacios tenga semantica. Los objetos aparecen en ls
cuando estan COMMITTED en el prefijo consultado.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from ..config import settings
from ..db.repository import Repository
from ..errors import NotFoundError
from .catalog import _normalize_path


@dataclass
class NamespaceEntry:
    name: str
    type: str  # 'dir' o 'object'
    created_at: float


class NamespaceService:
    def __init__(self, repo: Repository):
        self.repo = repo

    async def list_dir(self, owner_id: str, path: str) -> List[NamespaceEntry]:
        path = _normalize_path(path)
        # Validar que el directorio exista.
        dir_row = await self.repo.get_directory(owner_id, path, settings.domain_id)
        if dir_row is None:
            raise NotFoundError(f"directorio {path} no existe")

        entries: List[NamespaceEntry] = []

        # Subdirectorios.
        subdirs = await self.repo.list_directory_children(
            owner_id, path, settings.domain_id
        )
        for d in subdirs:
            name = d.full_path.rsplit("/", 1)[-1]
            entries.append(NamespaceEntry(name=name, type="dir", created_at=d.created_at))

        # Objetos COMMITTED en este directorio, ultima version por clave.
        objs = await self.repo.list_committed_objects_in_dir(
            owner_id, path, settings.domain_id
        )
        for obj in objs:
            name = obj.object_key.rsplit("/", 1)[-1]
            entries.append(
                NamespaceEntry(
                    name=name,
                    type="object",
                    created_at=obj.committed_at or obj.created_at,
                )
            )

        entries.sort(key=lambda e: (e.type != "dir", e.name))
        return entries
