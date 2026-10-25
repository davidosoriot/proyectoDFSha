"""Almacenamiento en disco de bloques.

Cada bloque se persiste como un archivo plano en storage_path/<block_id>.
Los primeros 32 bytes de cada archivo son el SHA-256 del contenido (en
limpio, antes de cifrar si aplica). El resto es el payload: cifrado AES-GCM
si hay encryption_key, en claro si no.

Formato del archivo:
    cifrado:    [32B checksum][12B nonce][16B tag][ciphertext]
    sin cifrar: [32B checksum][payload]

El checksum guardado se verifica contra lo que reporta el cliente y contra
el contenido real al servir el bloque. Si no coincide, se marca corrupcion
y se devuelve error 410 GONE.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from .crypto import BlockCipher

CHECKSUM_SIZE = 32  # sha-256


class BlockNotFound(Exception):
    pass


class BlockCorrupted(Exception):
    pass


@dataclass
class BlockInventoryEntry:
    block_id: str
    size: int
    checksum: str


class BlockStore:
    """Almacen de bloques sobre un directorio local."""

    def __init__(self, root: str, cipher: Optional[BlockCipher] = None) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._cipher = cipher

    def _path(self, block_id: str) -> Path:
        # block_id es un hash hex seguro; aun asi evitamos path traversal.
        if "/" in block_id or "\\" in block_id or ".." in block_id:
            raise ValueError(f"block_id invalido: {block_id!r}")
        return self._root / block_id

    def exists(self, block_id: str) -> bool:
        return self._path(block_id).is_file()

    def put(self, block_id: str, data: bytes, expected_checksum: Optional[str] = None) -> str:
        """Guarda un bloque. Devuelve el checksum sha-256 hex del contenido en claro.

        Si expected_checksum viene, debe coincidir o se rechaza (ValueError).
        Idempotente: si ya existe con el mismo checksum, no reescribe.
        """
        checksum = hashlib.sha256(data).hexdigest()
        if expected_checksum is not None and expected_checksum.lower() != checksum:
            raise ValueError(
                f"checksum no coincide: esperado={expected_checksum} real={checksum}"
            )

        path = self._path(block_id)
        if path.exists():
            # Idempotencia solo si la copia local esta SANA: se verifica el
            # contenido completo (descifrado + sha256), no solo el header. Una
            # copia corrupta con header intacto se sobreescribe.
            try:
                if hashlib.sha256(self.get(block_id)).hexdigest() == checksum:
                    return checksum
            except Exception:
                pass  # archivo corrupto: lo sobreescribimos

        payload = data
        header_extra = b""
        if self._cipher is not None:
            nonce, tag, ciphertext = self._cipher.encrypt(data)
            header_extra = nonce + tag
            payload = ciphertext

        tmp = path.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            f.write(bytes.fromhex(checksum))
            if header_extra:
                f.write(header_extra)
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return checksum

    def get(self, block_id: str) -> bytes:
        """Lee un bloque, verifica checksum y lo devuelve en claro."""
        path = self._path(block_id)
        if not path.is_file():
            raise BlockNotFound(block_id)
        with open(path, "rb") as f:
            stored_checksum = f.read(CHECKSUM_SIZE)
            if len(stored_checksum) != CHECKSUM_SIZE:
                raise BlockCorrupted(f"{block_id}: header truncado")

            if self._cipher is not None:
                nonce = f.read(self._cipher.NONCE_SIZE)
                tag = f.read(self._cipher.TAG_SIZE)
                ciphertext = f.read()
                try:
                    data = self._cipher.decrypt(nonce, tag, ciphertext)
                except Exception as exc:
                    raise BlockCorrupted(f"{block_id}: fallo descifrado: {exc}") from exc
            else:
                data = f.read()

        real = hashlib.sha256(data).digest()
        if real != stored_checksum:
            raise BlockCorrupted(
                f"{block_id}: checksum stored={stored_checksum.hex()} real={real.hex()}"
            )
        return data

    def delete(self, block_id: str) -> bool:
        path = self._path(block_id)
        if path.is_file():
            path.unlink()
            return True
        return False

    def iter_inventory(self) -> Iterator[BlockInventoryEntry]:
        """Recorre todos los bloques del disco y devuelve metadatos."""
        for path in self._root.iterdir():
            if not path.is_file() or path.suffix == ".tmp":
                continue
            try:
                checksum = self._read_checksum(path)
            except Exception:
                continue
            size = path.stat().st_size - CHECKSUM_SIZE
            if self._cipher is not None:
                size -= self._cipher.NONCE_SIZE + self._cipher.TAG_SIZE
            yield BlockInventoryEntry(
                block_id=path.name,
                size=max(0, size),
                checksum=checksum,
            )

    def used_bytes(self) -> int:
        total = 0
        for path in self._root.iterdir():
            if path.is_file() and path.suffix != ".tmp":
                total += path.stat().st_size
        return total

    def _read_checksum(self, path: Path) -> str:
        with open(path, "rb") as f:
            raw = f.read(CHECKSUM_SIZE)
        if len(raw) != CHECKSUM_SIZE:
            raise ValueError("header truncado")
        return raw.hex()
