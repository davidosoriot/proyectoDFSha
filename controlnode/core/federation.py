"""FederationResolver: costura para el Hito 3.

Hoy siempre responde 'local'. Cuando se implemente la federacion entre
dominios, este resolver consultara el indice de federacion y podra devolver
'remote' con el endpoint del ControlNode del otro dominio. El handler de
GET /objects ya esta preparado para actuar sobre ambos resultados.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..config import settings


@dataclass
class Resolution:
    location: str  # 'local' o 'remote'
    domain_id: str
    endpoint: Optional[str] = None


class FederationResolver:
    async def resolve(self, object_key: str, user_id: str) -> Resolution:
        # Hoy siempre local. El campo endpoint queda para el Hito 3.
        return Resolution(
            location="local", domain_id=settings.domain_id, endpoint=None
        )
