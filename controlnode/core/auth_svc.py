"""Servicio de autenticacion.

- Registro de usuarios con hash pbkdf2_hmac (stdlib, sin dependencias nativas).
- Login: verifica credenciales y emite JWT firmado HS256.
- Verificacion de token para el resto del sistema.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
import uuid
from dataclasses import dataclass

import jwt

from ..config import settings
from ..db.models import User
from ..db.repository import Repository
from ..errors import AuthenticationError, ConflictError

_PBKDF2_ITERATIONS = 200_000
_PBKDF2_ALGORITHM = "sha256"


def _hash_password(password: str, salt: bytes | None = None) -> str:
    if salt is None:
        salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac(
        _PBKDF2_ALGORITHM, password.encode("utf-8"), salt, _PBKDF2_ITERATIONS
    )
    return f"pbkdf2${_PBKDF2_ALGORITHM}${_PBKDF2_ITERATIONS}${salt.hex()}${dk.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        scheme, algo, iters_s, salt_hex, dk_hex = stored.split("$")
        if scheme != "pbkdf2":
            return False
        iters = int(iters_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(dk_hex)
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac(algo, password.encode("utf-8"), salt, iters)
    return hmac.compare_digest(actual, expected)


@dataclass
class TokenInfo:
    access_token: str
    token_type: str
    expires_in: int


@dataclass
class CurrentUser:
    user_id: str
    username: str
    domain_id: str


class AuthService:
    def __init__(self, repo: Repository):
        self.repo = repo

    async def register(self, username: str, password: str) -> User:
        if not username or not password:
            raise AuthenticationError("username y password son requeridos")
        if len(password) < 4:
            raise AuthenticationError("password demasiado corta")

        existing = await self.repo.get_user_by_username(username)
        if existing is not None:
            raise ConflictError(f"username '{username}' ya esta en uso")

        user = User(
            user_id=str(uuid.uuid4()),
            username=username,
            password_hash=_hash_password(password),
            domain_id=settings.domain_id,
            created_at=time.time(),
        )
        await self.repo.create_user(user)
        return user

    async def login(self, username: str, password: str) -> TokenInfo:
        user = await self.repo.get_user_by_username(username)
        if user is None or not _verify_password(password, user.password_hash):
            raise AuthenticationError("credenciales invalidas")

        now = int(time.time())
        payload = {
            "sub": user.user_id,
            "username": user.username,
            "domain_id": user.domain_id,
            "iat": now,
            "exp": now + settings.jwt_expire_seconds,
        }
        token = jwt.encode(
            payload, settings.jwt_secret, algorithm=settings.jwt_algorithm
        )
        return TokenInfo(
            access_token=token,
            token_type="bearer",
            expires_in=settings.jwt_expire_seconds,
        )

    @staticmethod
    def decode_token(token: str) -> CurrentUser:
        try:
            payload = jwt.decode(
                token, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
            )
        except jwt.PyJWTError as exc:
            raise AuthenticationError(f"token invalido: {exc}") from exc

        try:
            return CurrentUser(
                user_id=payload["sub"],
                username=payload["username"],
                domain_id=payload["domain_id"],
            )
        except KeyError as exc:
            raise AuthenticationError(f"token sin campo {exc}") from exc
