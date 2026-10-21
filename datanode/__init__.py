"""DataNode (Worker) de DFSha.

Almacena, sirve, replica y verifica bloques. Reporta periodicamente su
inventario al ControlNode. No guarda estado autoritativo: si pierde su
registro, se reconstruye desde el siguiente reporte de bloques.

API publica exportada:
    - BlockStore, BlockNotFound, BlockCorrupted: almacen de bloques en disco.
    - BlockCipher: cifrado AES-GCM opcional por bloque.
    - ControlNodeClient: cliente HTTP para hablar con el ControlNode.
    - HeartbeatTask, BlockReportTask: loops en background.
    - build_router: router FastAPI con la API REST de bloques.
    - settings: configuracion via variables de entorno.
"""

from .api import build_router
from .background import BlockReportTask, HeartbeatTask
from .config import DataNodeSettings, settings
from .controlnode_client import ControlNodeClient
from .crypto import BlockCipher, CryptoError
from .storage import BlockCorrupted, BlockNotFound, BlockStore

__all__ = [
    "BlockCipher",
    "BlockCorrupted",
    "BlockNotFound",
    "BlockReportTask",
    "BlockStore",
    "ControlNodeClient",
    "CryptoError",
    "DataNodeSettings",
    "HeartbeatTask",
    "build_router",
    "settings",
]
