"""Cifrado AES-GCM para bloques en reposo.

Clave de 32 bytes (hex) via la env DFSHA_DN_ENCRYPTION_KEY. Si viene vacia,
los bloques se guardan en claro. Cada bloque usa un nonce aleatorio de 12
bytes y guarda el tag de autenticacion de 16 bytes al lado.

Esta implementacion exige la libreria `cryptography` (incluida en
requirements.txt). No se usa criptografia hecha a mano: AES-GCM provee
confidencialidad + autenticacion simultaneamente y es un primitivo bien
analizado.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CryptoError(Exception):
    pass


@dataclass
class BlockCipher:
    """Cifrador simetrico AES-GCM por bloque.

    Produce (nonce, tag, ciphertext). El tag detecta cualquier modificacion
    del ciphertext o del nonce; descifrar con tag inválido lanza CryptoError.
    """

    key: bytes
    NONCE_SIZE: int = 12
    TAG_SIZE: int = 16

    def __post_init__(self) -> None:
        if len(self.key) != 32:
            raise CryptoError("la clave debe ser de 32 bytes (256 bits)")

    @classmethod
    def from_hex(cls, hex_key: str) -> "BlockCipher":
        try:
            raw = bytes.fromhex(hex_key)
        except ValueError as exc:
            raise CryptoError(f"clave hex invalida: {exc}") from exc
        return cls(key=raw)

    def encrypt(self, plaintext: bytes) -> Tuple[bytes, bytes, bytes]:
        nonce = os.urandom(self.NONCE_SIZE)
        aes = AESGCM(self.key)
        ct_and_tag = aes.encrypt(nonce, plaintext, associated_data=None)
        ciphertext = ct_and_tag[: -self.TAG_SIZE]
        tag = ct_and_tag[-self.TAG_SIZE:]
        return nonce, tag, ciphertext

    def decrypt(self, nonce: bytes, tag: bytes, ciphertext: bytes) -> bytes:
        aes = AESGCM(self.key)
        try:
            return aes.decrypt(nonce, ciphertext + tag, associated_data=None)
        except Exception as exc:
            raise CryptoError(f"fallo descifrado: {exc}") from exc
