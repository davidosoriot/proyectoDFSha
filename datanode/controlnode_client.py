"""Cliente HTTP del DataNode hacia el ControlNode.

Encapsula los tres endpoints que consume el DataNode:
    POST /datanodes/register
    POST /datanodes/{id}/heartbeat
    POST /datanodes/{id}/blocks

Todas las llamadas van con el header X-DataNode-Key.
"""

from __future__ import annotations

import logging
from typing import Iterable

import httpx

log = logging.getLogger("datanode.client")


class ControlNodeClient:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: float = 10.0) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {"X-DataNode-Key": api_key}
        self._timeout = timeout_seconds

    async def register(
        self,
        node_id: str,
        domain_id: str,
        address: str,
        free_bytes: int,
        load_score: float = 0.0,
    ) -> dict:
        payload = {
            "node_id": node_id,
            "domain_id": domain_id,
            "address": address,
            "free_bytes": free_bytes,
            "load_score": load_score,
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f"{self._base}/datanodes/register",
                json=payload,
                headers=self._headers,
            )
        if resp.status_code == 409:
            log.info("DataNode %s ya estaba registrado; se continua", node_id)
            return {"registered": True, "already_present": True}
        resp.raise_for_status()
        return resp.json()

    async def heartbeat(self, node_id: str, free_bytes: int, load_score: float) -> dict:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f"{self._base}/datanodes/{node_id}/heartbeat",
                json={"free_bytes": free_bytes, "load_score": load_score},
                headers=self._headers,
            )
        resp.raise_for_status()
        return resp.json()

    async def block_report(
        self,
        node_id: str,
        entries: Iterable[tuple[str, str]],
    ) -> list[str]:
        """Envia reporte completo de inventario; devuelve bloques desconocidos."""
        body = {
            "blocks": [{"block_id": bid, "checksum": csum} for bid, csum in entries]
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f"{self._base}/datanodes/{node_id}/blocks",
                json=body,
                headers=self._headers,
            )
        resp.raise_for_status()
        return resp.json().get("unknown_blocks", [])
