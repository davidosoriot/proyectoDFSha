"""API REST del DataNode: plano de datos para bloques.

Los clientes (y en replicacion peer-to-peer, otros DataNodes) hablan aqui.
El ControlNode nunca llama estos endpoints: no toca bytes de contenido.

Endpoints:
    PUT    /blocks/{block_id}?checksum=...   subir bloque (body = bytes crudos)
    GET    /blocks/{block_id}                descargar bloque
    DELETE /blocks/{block_id}                borrar bloque
    GET    /health                           liveness
    GET    /inventory                        snapshot local (debug)
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from pydantic import BaseModel

from .storage import BlockCorrupted, BlockNotFound, BlockStore


class ReplicateRequest(BaseModel):
    target_address: str

log = logging.getLogger("datanode.api")

# Firma: async callback invocado tras almacenar un bloque. Permite al DataNode
# notificar inmediatamente al ControlNode (REPLICA_CONFIRMED) en lugar de
# esperar al proximo block report periodico. Si falla, se registra y continua.
OnBlockStored = Callable[[str, str], Awaitable[None]]  # (block_id, checksum)


def build_router(
    store: BlockStore,
    node_id: str,
    on_block_stored: Optional[OnBlockStored] = None,
    http_client_factory=None,
) -> APIRouter:
    """Construye el router. http_client_factory permite inyectar un cliente
    httpx con transporte custom (p. ej. ASGI in-process) para pruebas de la
    operacion /replicate."""
    router = APIRouter()
    _http_client_factory = http_client_factory or (lambda: httpx.AsyncClient(timeout=30))

    @router.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "node_id": node_id,
            "used_bytes": store.used_bytes(),
        }

    @router.put("/blocks/{block_id}", status_code=status.HTTP_201_CREATED)
    async def put_block(
        block_id: str,
        request: Request,
        checksum: str | None = Query(default=None),
    ) -> dict:
        data = await request.body()
        if not data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="body vacio",
            )
        try:
            stored = store.put(block_id, data, expected_checksum=checksum)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            )
        if on_block_stored is not None:
            try:
                await on_block_stored(block_id, stored)
            except Exception:
                log.exception("fallo al notificar al ControlNode %s", block_id)
        return {"block_id": block_id, "size": len(data), "checksum": stored}

    @router.get("/blocks/{block_id}")
    async def get_block(block_id: str) -> Response:
        try:
            data = store.get(block_id)
        except BlockNotFound:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="bloque inexistente")
        except BlockCorrupted as exc:
            log.error("bloque %s corrupto: %s", block_id, exc)
            raise HTTPException(status_code=status.HTTP_410_GONE, detail="bloque corrupto")
        return Response(content=data, media_type="application/octet-stream")

    @router.delete("/blocks/{block_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_block(block_id: str) -> Response:
        store.delete(block_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @router.post("/blocks/{block_id}/replicate", status_code=status.HTTP_200_OK)
    async def replicate_block(block_id: str, body: ReplicateRequest) -> dict:
        """Copia un bloque local a otro DataNode (data-plane peer-to-peer).

        Invocado por el ControlNode durante la re-replicacion. El bloque se
        envia con su checksum para que el destino lo verifique.
        """
        try:
            data = store.get(block_id)
        except BlockNotFound:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="bloque inexistente")
        except BlockCorrupted as exc:
            raise HTTPException(status_code=status.HTTP_410_GONE, detail=str(exc))

        import hashlib as _hl
        checksum = _hl.sha256(data).hexdigest()

        target = body.target_address.rstrip("/")
        async with _http_client_factory() as client:
            r = await client.put(
                f"{target}/blocks/{block_id}",
                content=data,
                params={"checksum": checksum},
            )
        if r.status_code not in (200, 201):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"target respondio {r.status_code}: {r.text}",
            )
        return {"block_id": block_id, "copied_to": body.target_address, "size": len(data)}

    @router.get("/inventory")
    async def inventory() -> dict:
        entries = [
            {"block_id": e.block_id, "size": e.size, "checksum": e.checksum}
            for e in store.iter_inventory()
        ]
        return {"node_id": node_id, "count": len(entries), "blocks": entries}

    return router
