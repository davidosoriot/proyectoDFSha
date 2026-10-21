"""Configuracion centralizada del ControlNode.

Todos los valores se leen de variables de entorno con defaults razonables para
desarrollo. En produccion se sobrescriben via .env o el entorno del contenedor.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Identidad del dominio.
    domain_id: str = os.getenv("DFSHA_DOMAIN_ID", "domain-local")

    # Persistencia. Formato SQLAlchemy async.
    db_url: str = os.getenv("DFSHA_DB_URL", "sqlite+aiosqlite:///./controlnode.db")

    # Autenticacion.
    jwt_secret: str = os.getenv("DFSHA_JWT_SECRET", "dev-secret-change-me")
    jwt_algorithm: str = "HS256"
    jwt_expire_seconds: int = int(os.getenv("DFSHA_JWT_EXPIRE_SECONDS", "86400"))

    # Api key para autenticar DataNodes contra el ControlNode.
    datanode_api_key: str = os.getenv("DFSHA_DATANODE_API_KEY", "dev-datanode-key")

    # Colocacion.
    replication_factor: int = int(os.getenv("DFSHA_REPLICATION_FACTOR", "3"))
    block_size_default: int = int(os.getenv("DFSHA_BLOCK_SIZE_DEFAULT", str(64 * 1024 * 1024)))

    # Ciclo de vida de objetos PENDING.
    pending_ttl_seconds: int = int(os.getenv("DFSHA_PENDING_TTL_SECONDS", "3600"))

    # Detecccion de caida de DataNodes.
    heartbeat_interval_seconds: int = int(os.getenv("DFSHA_HEARTBEAT_INTERVAL", "5"))
    suspect_after_seconds: int = int(os.getenv("DFSHA_SUSPECT_AFTER", "20"))
    dead_after_seconds: int = int(os.getenv("DFSHA_DEAD_AFTER", "30"))
    health_check_interval_seconds: int = int(os.getenv("DFSHA_HEALTH_CHECK_INTERVAL", "5"))

    # Limpieza de PENDING expirados.
    pending_cleanup_interval_seconds: int = int(os.getenv("DFSHA_PENDING_CLEANUP_INTERVAL", "300"))

    # Re-replicacion (restablecer RF cuando una replica se pierde).
    rereplication_interval_seconds: int = int(os.getenv("DFSHA_REREP_INTERVAL", "30"))

    # Federacion: clave compartida entre peers (ControlNodes de distintos dominios).
    federation_api_key: str = os.getenv("DFSHA_FEDERATION_KEY", "dev-federation-key")
    federation_propagation_interval_seconds: int = int(
        os.getenv("DFSHA_FEDERATION_INTERVAL", "10")
    )

    # Replicacion del ControlNode (leader-follower). LEADER acepta escrituras,
    # FOLLOWER solo sirve lecturas y sincroniza desde el leader.
    role: str = os.getenv("DFSHA_ROLE", "LEADER").upper()  # LEADER | FOLLOWER
    leader_url: str = os.getenv("DFSHA_LEADER_URL", "http://localhost:8000")
    replication_api_key: str = os.getenv("DFSHA_REPLICATION_KEY", "dev-replication-key")
    replication_poll_interval_seconds: int = int(
        os.getenv("DFSHA_REPLICATION_POLL", "5")
    )


settings = Settings()


def reload_settings() -> Settings:
    """Util para tests: recarga la configuracion desde las env vars actuales."""
    global settings
    settings = Settings()
    return settings
