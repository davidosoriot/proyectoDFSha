"""Servicio de federacion entre dominios.

Dos capas:

1. **PeerRegistry**: mantiene la lista de peers conocidos (otros ControlNodes
   de otros dominios), con su endpoint y api_key compartida.

2. **FederationPropagator**: lee los eventos del log (events con propagated=0)
   y los envia a cada peer activo. Marca propagated=1 cuando todos los peers
   confirman recepcion. El protocolo es best-effort: si un peer esta caido,
   el evento se vuelve a intentar en el proximo tick.

3. **FederationClient**: utilidades para hacer lookup de objetos remotos y
   consultar la ubicacion de bloques en otro dominio.

Autenticacion peer-to-peer: header `X-Federation-Key` compartido. En produccion
esto se reemplazaria por mTLS; aqui es suficiente para la entrega academica.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import List, Optional

import httpx
from sqlalchemy import and_, select, update

from ..config import settings
from ..db.connection import get_session_factory
from ..db.models import Event, EventType, PeerDomain
from ..db.repository import Repository
from ..errors import ConflictError, NotFoundError

log = logging.getLogger("controlnode.federation")


@dataclass
class PeerRecord:
    domain_id: str
    endpoint: str
    status: str
    registered_at: float
    last_seen_at: Optional[float]


class PeerRegistry:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    async def register(
        self, domain_id: str, endpoint: str, api_key: str
    ) -> PeerRecord:
        if domain_id == settings.domain_id:
            raise ConflictError("no se puede registrar el propio dominio como peer")

        existing = await self.repo.session.get(PeerDomain, domain_id)
        now = time.time()
        if existing is not None:
            existing.endpoint = endpoint
            existing.api_key = api_key
            existing.status = "ACTIVE"
            existing.last_seen_at = now
            peer = existing
        else:
            peer = PeerDomain(
                domain_id=domain_id,
                endpoint=endpoint.rstrip("/"),
                api_key=api_key,
                status="ACTIVE",
                registered_at=now,
                last_seen_at=now,
            )
            self.repo.session.add(peer)
        await self.repo.session.flush()
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.PEER_REGISTERED,
            entity_id=domain_id,
            payload={"domain_id": domain_id, "endpoint": endpoint},
        )
        await self.repo.session.commit()
        return _to_record(peer)

    async def remove(self, domain_id: str) -> None:
        peer = await self.repo.session.get(PeerDomain, domain_id)
        if peer is None:
            raise NotFoundError(f"peer {domain_id} no existe")
        await self.repo.session.delete(peer)
        await self.repo.append_event(
            domain_id=settings.domain_id,
            event_type=EventType.PEER_REMOVED,
            entity_id=domain_id,
            payload={"domain_id": domain_id},
        )
        await self.repo.session.commit()

    async def list_all(self) -> List[PeerRecord]:
        rows = await self.repo.session.execute(select(PeerDomain))
        return [_to_record(p) for p in rows.scalars().all()]

    async def get(self, domain_id: str) -> Optional[PeerDomain]:
        return await self.repo.session.get(PeerDomain, domain_id)

    async def mark_seen(self, domain_id: str) -> None:
        await self.repo.session.execute(
            update(PeerDomain)
            .where(PeerDomain.domain_id == domain_id)
            .values(last_seen_at=time.time(), status="ACTIVE")
        )
        await self.repo.session.commit()


def _to_record(p: PeerDomain) -> PeerRecord:
    return PeerRecord(
        domain_id=p.domain_id,
        endpoint=p.endpoint,
        status=p.status,
        registered_at=p.registered_at,
        last_seen_at=p.last_seen_at,
    )


# --- Propagador de eventos ---


class FederationPropagator:
    """Reparte el log de eventos pendientes a cada peer activo."""

    def __init__(self, client_factory=None) -> None:
        self._factory = client_factory or (lambda: httpx.AsyncClient(timeout=10))

    async def tick(self) -> dict:
        stats = {"pending": 0, "sent": 0, "failed": 0, "peers": 0}
        factory = get_session_factory()
        async with factory() as session:
            repo = Repository(session)
            registry = PeerRegistry(repo)
            peers = await registry.list_all()
            stats["peers"] = len(peers)
            if not peers:
                return stats

            events = await repo.list_pending_events(limit=200)
            stats["pending"] = len(events)
            if not events:
                return stats

            peer_rows = await repo.session.execute(select(PeerDomain))
            peer_models = {p.domain_id: p for p in peer_rows.scalars().all()}

            batch = [
                {
                    "event_id": e.event_id,
                    "domain_id": e.domain_id,
                    "event_type": e.event_type,
                    "entity_id": e.entity_id,
                    "payload": json.loads(e.payload),
                    "created_at": e.created_at,
                }
                for e in events
            ]

            all_ok = True
            for peer in peers:
                model = peer_models.get(peer.domain_id)
                if model is None or model.status != "ACTIVE":
                    continue
                try:
                    async with self._factory() as c:
                        r = await c.post(
                            f"{model.endpoint}/federation/events",
                            json={"events": batch},
                            headers={"X-Federation-Key": model.api_key},
                        )
                    if r.status_code >= 400:
                        raise RuntimeError(f"peer {peer.domain_id}: {r.status_code}")
                    stats["sent"] += len(batch)
                    await registry.mark_seen(peer.domain_id)
                except Exception as exc:
                    log.warning(
                        "propagacion a %s fallo: %s", peer.domain_id, exc
                    )
                    stats["failed"] += len(batch)
                    all_ok = False

            if all_ok:
                event_ids = [e.event_id for e in events]
                await repo.session.execute(
                    update(Event)
                    .where(Event.event_id.in_(event_ids))
                    .values(propagated=1)
                )
                await repo.session.commit()
        return stats


class FederationPropagatorTask:
    def __init__(self, interval_seconds: Optional[float] = None) -> None:
        self._interval = (
            interval_seconds or settings.federation_propagation_interval_seconds
        )
        self._prop = FederationPropagator()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._prop.tick()
            except Exception:
                log.exception("federation propagator: fallo en tick")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="federation-propagator")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None


# --- Cliente para lookups cross-domain ---

# Factory global; los tests pueden monkey-patchearla para inyectar transporte
# in-memory. En produccion devuelve un httpx normal.
_default_client_factory = lambda: httpx.AsyncClient(timeout=10)


def set_default_client_factory(factory) -> None:
    """Permite a los tests inyectar un transport custom para operaciones de
    federacion disparadas desde dentro de handlers (donde no es trivial
    pasar factories como parametro)."""
    global _default_client_factory
    _default_client_factory = factory


class FederationClient:
    """Cliente que resuelve objetos en peers remotos."""

    def __init__(self, repo: Repository, client_factory=None) -> None:
        self.repo = repo
        self._factory = client_factory or _default_client_factory

    async def resolve_remote(
        self, domain_id: str, object_key: str, owner_username: str
    ) -> dict:
        peer = await self.repo.session.get(PeerDomain, domain_id)
        if peer is None:
            raise NotFoundError(f"dominio peer {domain_id} desconocido")
        async with self._factory() as c:
            r = await c.get(
                f"{peer.endpoint}/federation/objects",
                params={"key": object_key, "owner_username": owner_username},
                headers={"X-Federation-Key": peer.api_key},
            )
        if r.status_code == 404:
            raise NotFoundError(f"objeto {object_key} no existe en {domain_id}")
        if r.status_code >= 400:
            raise RuntimeError(
                f"error remoto {domain_id}: {r.status_code} {r.text}"
            )
        return r.json()
