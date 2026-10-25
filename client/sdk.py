"""SDK async para interactuar con DFSha.

Separa dos planos:
    - control (ControlNode): metadatos, namespace, ciclo de vida de objetos.
    - datos (DataNodes): PUT/GET/DELETE bloques, directo contra las replicas.

Si el cliente intenta leer un bloque y la primera replica responde 404/410,
prueba la siguiente y reporta el fallo al ControlNode via report-failure.
Esto alimenta la cola de REPLICA_LOST que consume la re-replicacion.
"""

from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable, Optional

import httpx

from dfshacore import chunk_stream

log = logging.getLogger("client.sdk")


class DFShaClientError(Exception):
    pass


@dataclass
class _BlockRaw:
    block_id: str
    index: int
    size: int
    checksum: str
    data: bytes


class DFShaClient:
    """Cliente async al ControlNode y los DataNodes.

    Para contextos de prueba (ASGI in-process) se puede pasar un transport
    custom; para uso real basta con la URL del ControlNode.
    """

    def __init__(
        self,
        controlnode_url: str,
        token: Optional[str] = None,
        timeout_seconds: float = 30.0,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        datanode_transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self._cn_url = controlnode_url.rstrip("/")
        self._token = token
        self._timeout = timeout_seconds
        self._cn_transport = transport
        self._dn_transport = datanode_transport or transport

    # ---------- helpers HTTP ----------

    def _auth_headers(self) -> dict:
        if not self._token:
            return {}
        return {"Authorization": f"Bearer {self._token}"}

    def _cn_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._cn_url, timeout=self._timeout, transport=self._cn_transport
        )

    def _dn_client(self, base_url: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=base_url, timeout=self._timeout, transport=self._dn_transport
        )

    # ---------- auth ----------

    async def register(self, username: str, password: str) -> dict:
        async with self._cn_client() as c:
            r = await c.post(
                "/auth/register", json={"username": username, "password": password}
            )
        _raise_for_status(r)
        return r.json()

    async def login(self, username: str, password: str) -> str:
        async with self._cn_client() as c:
            r = await c.post(
                "/auth/login", json={"username": username, "password": password}
            )
        _raise_for_status(r)
        self._token = r.json()["access_token"]
        return self._token

    # ---------- namespace ----------

    async def mkdir(self, path: str) -> dict:
        async with self._cn_client() as c:
            r = await c.post("/ns/mkdir", json={"path": path}, headers=self._auth_headers())
        _raise_for_status(r)
        return r.json()

    async def rmdir(self, path: str) -> None:
        async with self._cn_client() as c:
            r = await c.delete(
                "/ns/rmdir", params={"path": path}, headers=self._auth_headers()
            )
        _raise_for_status(r)

    async def ls(self, path: str) -> list[dict]:
        async with self._cn_client() as c:
            r = await c.get(
                "/ns/ls", params={"path": path}, headers=self._auth_headers()
            )
        _raise_for_status(r)
        return r.json()["entries"]

    # ---------- objects ----------

    async def put(
        self,
        source: BinaryIO | bytes | str | Path,
        object_key: str,
        block_size: int = 64 * 1024 * 1024,
    ) -> dict:
        """Sube un objeto. Devuelve {object_id, version}."""
        stream, closer = _as_stream(source)
        try:
            raw_blocks: dict[str, _BlockRaw] = {}

            def _writer(block_id: str, index: int):
                return _CapturingWriter(raw_blocks, block_id, index)

            manifest = chunk_stream(
                source=stream,
                object_key=object_key,
                block_writer=_writer,
                block_size=block_size,
            )
        finally:
            if closer is not None:
                closer()

        # Completar size/checksum en raw_blocks desde manifest.
        for b in manifest.blocks:
            raw_blocks[b.block_id].size = b.size
            raw_blocks[b.block_id].checksum = b.checksum
            raw_blocks[b.block_id].index = b.index

        # Init object.
        async with self._cn_client() as c:
            r = await c.post(
                "/objects/init",
                headers=self._auth_headers(),
                json={
                    "object_key": object_key,
                    "total_size": manifest.total_size,
                    "block_size": manifest.block_size,
                    "global_checksum": manifest.global_checksum,
                    "blocks": [
                        {
                            "block_id": b.block_id,
                            "index": b.index,
                            "size": b.size,
                            "checksum": b.checksum,
                        }
                        for b in manifest.blocks
                    ],
                },
            )
        _raise_for_status(r)
        init_resp = r.json()
        object_id = init_resp["object_id"]

        # Subir cada bloque a todas las replicas asignadas.
        for placement in init_resp["placements"]:
            blk = raw_blocks[placement["block_id"]]
            for replica in placement["replica_nodes"]:
                await self._upload_block_to_node(
                    replica["address"], blk.block_id, blk.data, blk.checksum
                )

        # Commit.
        async with self._cn_client() as c:
            r = await c.post(
                f"/objects/{object_id}/commit", headers=self._auth_headers()
            )
        _raise_for_status(r)
        return r.json()

    async def _upload_block_to_node(
        self, address: str, block_id: str, data: bytes, checksum: str
    ) -> None:
        async with self._dn_client(address) as c:
            r = await c.put(
                f"/blocks/{block_id}", content=data, params={"checksum": checksum}
            )
        if r.status_code not in (200, 201):
            raise DFShaClientError(
                f"PUT bloque {block_id} en {address}: {r.status_code} {r.text}"
            )

    # ---------- acls ----------

    async def grant(
        self, grantee_username: str, path_prefix: str, permissions: list[str]
    ) -> dict:
        async with self._cn_client() as c:
            r = await c.post(
                "/acl/grant",
                headers=self._auth_headers(),
                json={
                    "grantee_username": grantee_username,
                    "path_prefix": path_prefix,
                    "permissions": permissions,
                },
            )
        _raise_for_status(r)
        return r.json()

    async def revoke(self, grantee_username: str, path_prefix: str) -> None:
        async with self._cn_client() as c:
            r = await c.delete(
                "/acl/revoke",
                headers=self._auth_headers(),
                params={
                    "grantee_username": grantee_username,
                    "path_prefix": path_prefix,
                },
            )
        _raise_for_status(r)

    async def list_acls(self) -> list[dict]:
        async with self._cn_client() as c:
            r = await c.get("/acl/list", headers=self._auth_headers())
        _raise_for_status(r)
        return r.json()["acls"]

    async def locate(
        self,
        object_key: str,
        owner: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> dict:
        """Resuelve un objeto sin descargarlo: manifiesto + replicas por bloque.

        Con `domain` distinto al local, el ControlNode hace proxy al peer
        federado y devuelve las ubicaciones en los DataNodes de ese dominio.
        """
        params = {"key": object_key}
        if owner:
            params["owner"] = owner
        if domain:
            params["domain"] = domain
        async with self._cn_client() as c:
            r = await c.get(
                "/objects", params=params, headers=self._auth_headers()
            )
        _raise_for_status(r)
        return r.json()

    async def get(
        self,
        object_key: str,
        owner: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> bytes:
        meta = await self.locate(object_key, owner=owner, domain=domain)

        pieces: list[bytes] = []
        for block in sorted(meta["blocks"], key=lambda b: b["index"]):
            data = await self._fetch_block_with_failover(
                object_id=meta["object_id"],
                block_id=block["block_id"],
                replicas=block["replicas"],
                expected_checksum=block["checksum"],
            )
            pieces.append(data)

        full = b"".join(pieces)
        actual = hashlib.sha256(full).hexdigest()
        if actual != meta["global_checksum"]:
            raise DFShaClientError(
                f"checksum global no coincide: esperado={meta['global_checksum']} real={actual}"
            )
        return full

    async def _fetch_block_with_failover(
        self,
        object_id: str,
        block_id: str,
        replicas: list[dict],
        expected_checksum: str,
    ) -> bytes:
        last_error: Optional[Exception] = None
        for replica in replicas:
            try:
                async with self._dn_client(replica["address"]) as c:
                    r = await c.get(f"/blocks/{block_id}")
                if r.status_code == 200:
                    data = r.content
                    actual = hashlib.sha256(data).hexdigest()
                    if actual != expected_checksum:
                        raise DFShaClientError(
                            f"bloque {block_id} corrupto en {replica['node_id']}"
                        )
                    return data
                # 404/410: la replica no sirve. Reportar y seguir.
                await self._report_failure(
                    object_id, block_id, replica["node_id"], f"GET {r.status_code}"
                )
            except (httpx.HTTPError, DFShaClientError) as exc:
                last_error = exc
                try:
                    await self._report_failure(
                        object_id, block_id, replica["node_id"], str(exc)
                    )
                except Exception:
                    log.exception("fallo al reportar replica caida")

        raise DFShaClientError(
            f"ninguna replica sirvio el bloque {block_id}; ultimo error: {last_error}"
        )

    async def _report_failure(
        self, object_id: str, block_id: str, node_id: str, reason: str
    ) -> None:
        async with self._cn_client() as c:
            await c.post(
                f"/objects/{object_id}/blocks/{block_id}/report-failure",
                headers=self._auth_headers(),
                json={"node_id": node_id, "reason": reason},
            )

    async def delete(self, object_id: str) -> None:
        async with self._cn_client() as c:
            r = await c.delete(
                f"/objects/{object_id}", headers=self._auth_headers()
            )
        _raise_for_status(r)

    async def stat(self, object_id: str) -> dict:
        async with self._cn_client() as c:
            r = await c.get(
                f"/objects/{object_id}/stat", headers=self._auth_headers()
            )
        _raise_for_status(r)
        return r.json()


# ---------- utilidades ----------


def _raise_for_status(r: httpx.Response) -> None:
    if r.status_code >= 400:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise DFShaClientError(f"HTTP {r.status_code}: {detail}")


def _as_stream(src) -> tuple[BinaryIO, Optional[callable]]:
    if isinstance(src, (bytes, bytearray)):
        return io.BytesIO(src), None
    if isinstance(src, (str, Path)):
        f = open(src, "rb")
        return f, f.close
    return src, None


class _CapturingWriter(io.BytesIO):
    """BytesIO que, al cerrarse, deposita los bytes en el dict de bloques crudos."""

    def __init__(self, bucket: dict, block_id: str, index: int) -> None:
        super().__init__()
        self._bucket = bucket
        self._block_id = block_id
        self._index = index

    def close(self) -> None:  # type: ignore[override]
        self._bucket[self._block_id] = _BlockRaw(
            block_id=self._block_id,
            index=self._index,
            size=0,
            checksum="",
            data=self.getvalue(),
        )
        super().close()
