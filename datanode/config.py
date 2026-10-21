"""Configuracion del DataNode via variables de entorno."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class DataNodeSettings:
    node_id: str = os.getenv("DFSHA_DN_NODE_ID", "dn-local")
    domain_id: str = os.getenv("DFSHA_DN_DOMAIN_ID", "domain-local")
    bind_host: str = os.getenv("DFSHA_DN_HOST", "0.0.0.0")
    bind_port: int = int(os.getenv("DFSHA_DN_PORT", "9000"))

    # Direccion publica reportada al ControlNode (puede diferir de bind si hay NAT).
    public_address: str = os.getenv(
        "DFSHA_DN_PUBLIC_ADDRESS", "http://localhost:9000"
    )

    # Donde guardar los bloques en disco.
    storage_path: str = os.getenv("DFSHA_DN_STORAGE", "./dn_storage")

    # ControlNode al que se registra.
    controlnode_url: str = os.getenv(
        "DFSHA_DN_CONTROLNODE_URL", "http://localhost:8000"
    )
    controlnode_api_key: str = os.getenv(
        "DFSHA_DN_CONTROLNODE_KEY", "dev-datanode-key"
    )

    # Capacidad reportada (bytes). En produccion se calcula del filesystem.
    capacity_bytes: int = int(
        os.getenv("DFSHA_DN_CAPACITY", str(10 * 1024 * 1024 * 1024))  # 10 GB
    )

    # Cadencia de heartbeat / block report.
    heartbeat_interval_seconds: int = int(os.getenv("DFSHA_DN_HEARTBEAT", "5"))
    block_report_interval_seconds: int = int(os.getenv("DFSHA_DN_BLOCK_REPORT", "60"))

    # Cifrado en reposo (opcional). Clave base64/hex de 32 bytes; vacio = sin cifrado.
    encryption_key_hex: str = os.getenv("DFSHA_DN_ENCRYPTION_KEY", "")


settings = DataNodeSettings()


def reload_settings() -> DataNodeSettings:
    global settings
    settings = DataNodeSettings()
    return settings
