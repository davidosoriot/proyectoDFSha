"""Servicio de ACLs por prefijo.

El dueño de un recurso puede conceder permisos (READ, WRITE, DELETE) a otro
usuario sobre un prefijo del espacio de nombres. La comprobacion de acceso
busca la coincidencia mas larga: la ACL mas especifica gana.

Ejemplo:
    alice tiene objetos bajo /fotos/.
    alice -> bob READ sobre /fotos
    alice -> bob WRITE sobre /fotos/compartidas

    bob entonces puede leer todo /fotos y ademas escribir en /fotos/compartidas.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import List, Optional

from sqlalchemy import and_, delete, select

from ..config import settings
from ..db.models import ACL, EventType, Permission, User
from ..db.repository import Repository
from ..errors import ConflictError, NotFoundError, PermissionDeniedError


@dataclass
class ACLRecord:
    acl_id: str
    owner_id: str
    owner_username: str
    grantee_id: str
    grantee_username: str
    path_prefix: str
    permissions: List[str]
    created_at: float


_VALID_PERMS = {Permission.READ, Permission.WRITE, Permission.DELETE}


def _normalize_prefix(prefix: str) -> str:
    if not prefix or not prefix.startswith("/"):
        prefix = "/" + (prefix or "")
    # Remover trailing slash salvo si es solo la raiz.
    if len(prefix) > 1 and prefix.endswith("/"):
        prefix = prefix.rstrip("/")
    return prefix


def _pack_perms(perms: List[str]) -> str:
    seen = []
    for p in perms:
        p = p.upper()
        if p not in _VALID_PERMS:
            raise ValueError(f"permiso invalido: {p}")
        if p not in seen:
            seen.append(p)
    if not seen:
        raise ValueError("se requiere al menos un permiso")
    return ",".join(sorted(seen))


def _unpack_perms(packed: str) -> List[str]:
    return [p for p in packed.split(",") if p]


class ACLService:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    async def grant(
        self,
        owner_id: str,
        grantee_username: str,
        path_prefix: str,
        permissions: List[str],
    ) -> ACLRecord:
        grantee = await self.repo.get_user_by_username(grantee_username)
        if grantee is None:
            raise NotFoundError(f"usuario destino '{grantee_username}' no existe")
        if grantee.user_id == owner_id:
            raise ConflictError("no tiene sentido darse permisos a si mismo")

        prefix = _normalize_prefix(path_prefix)
        packed = _pack_perms(permissions)

        # Upsert: si ya existe, actualizar permisos.
        rows = await self.repo.session.execute(
            select(ACL).where(
                and_(
                    ACL.owner_id == owner_id,
                    ACL.grantee_id == grantee.user_id,
                    ACL.path_prefix == prefix,
                    ACL.domain_id == settings.domain_id,
                )
            )
        )
        existing = rows.scalar_one_or_none()
        now = time.time()
        if existing is not None:
            existing.permissions = packed
            await self.repo.session.flush()
            acl = existing
        else:
            acl = ACL(
                acl_id=str(uuid.uuid4()),
                owner_id=owner_id,
                grantee_id=grantee.user_id,
                path_prefix=prefix,
                permissions=packed,
                domain_id=settings.domain_id,
                created_at=now,
            )
            self.repo.session.add(acl)
            await self.repo.session.flush()

        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.ACL_GRANTED,
            entity_id=acl.acl_id,
            payload={
                "owner_id": owner_id,
                "grantee_id": grantee.user_id,
                "path_prefix": prefix,
                "permissions": packed,
            },
        )
        await self.repo.session.commit()

        owner_user = await self.repo.get_user(owner_id)
        return _to_record(acl, owner_user, grantee)

    async def revoke(
        self, owner_id: str, grantee_username: str, path_prefix: str
    ) -> None:
        grantee = await self.repo.get_user_by_username(grantee_username)
        if grantee is None:
            raise NotFoundError(f"usuario destino '{grantee_username}' no existe")
        prefix = _normalize_prefix(path_prefix)

        rows = await self.repo.session.execute(
            select(ACL).where(
                and_(
                    ACL.owner_id == owner_id,
                    ACL.grantee_id == grantee.user_id,
                    ACL.path_prefix == prefix,
                    ACL.domain_id == settings.domain_id,
                )
            )
        )
        acl = rows.scalar_one_or_none()
        if acl is None:
            raise NotFoundError("ACL no existe")

        acl_id = acl.acl_id
        await self.repo.session.execute(
            delete(ACL).where(ACL.acl_id == acl_id)
        )
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.ACL_REVOKED,
            entity_id=acl_id,
            payload={
                "owner_id": owner_id,
                "grantee_id": grantee.user_id,
                "path_prefix": prefix,
            },
        )
        await self.repo.session.commit()

    async def list_granted_by(self, owner_id: str) -> List[ACLRecord]:
        rows = await self.repo.session.execute(
            select(ACL).where(
                and_(
                    ACL.owner_id == owner_id,
                    ACL.domain_id == settings.domain_id,
                )
            )
        )
        results: List[ACLRecord] = []
        for acl in rows.scalars().all():
            owner = await self.repo.get_user(acl.owner_id)
            grantee = await self.repo.get_user(acl.grantee_id)
            if owner and grantee:
                results.append(_to_record(acl, owner, grantee))
        return results

    async def check(
        self,
        actor_user_id: str,
        resource_owner_id: str,
        path: str,
        permission: str,
    ) -> bool:
        """Devuelve True si el actor tiene `permission` sobre `path` del owner."""
        if actor_user_id == resource_owner_id:
            return True

        path = _normalize_prefix(path) if path.startswith("/") else path
        # Buscar todas las ACLs del grantee desde el owner y quedarse con la
        # mas especifica (prefix mas largo) que cubra path.
        rows = await self.repo.session.execute(
            select(ACL).where(
                and_(
                    ACL.owner_id == resource_owner_id,
                    ACL.grantee_id == actor_user_id,
                    ACL.domain_id == settings.domain_id,
                )
            )
        )
        best: Optional[ACL] = None
        for acl in rows.scalars().all():
            if _path_matches_prefix(path, acl.path_prefix):
                if best is None or len(acl.path_prefix) > len(best.path_prefix):
                    best = acl
        if best is None:
            return False
        return permission.upper() in _unpack_perms(best.permissions)

    async def require(
        self,
        actor_user_id: str,
        resource_owner_id: str,
        path: str,
        permission: str,
    ) -> None:
        if not await self.check(actor_user_id, resource_owner_id, path, permission):
            raise PermissionDeniedError(
                f"usuario {actor_user_id} no tiene {permission} sobre {path}"
            )


def _to_record(acl: ACL, owner: User, grantee: User) -> ACLRecord:
    return ACLRecord(
        acl_id=acl.acl_id,
        owner_id=acl.owner_id,
        owner_username=owner.username,
        grantee_id=acl.grantee_id,
        grantee_username=grantee.username,
        path_prefix=acl.path_prefix,
        permissions=_unpack_perms(acl.permissions),
        created_at=acl.created_at,
    )


def _path_matches_prefix(path: str, prefix: str) -> bool:
    """path /a/b/c coincide con prefijos /, /a, /a/b, /a/b/c. No con /a/bb."""
    if prefix == "/":
        return True
    if path == prefix:
        return True
    return path.startswith(prefix + "/")
