"""dfshacore: particionamiento, manifiesto, reensamblado y colocacion.

API publica exportada:
    - Modelos: BlockInfo, ObjectManifest, DataNodeInfo, BlockPlacement, ObjectPlacement
    - Errores: DFShaError, BlockCorruptError, MissingBlockError,
               InsufficientNodesError, ManifestInvalidError
    - Chunker: chunk_stream, chunk_file, BLOCK_SIZE_DEFAULT
    - Assembler: assemble_stream, assemble_from_directory
    - Placement: place_object
"""

from .assembler import assemble_from_directory, assemble_stream
from .chunker import BLOCK_SIZE_DEFAULT, chunk_file, chunk_stream
from .errors import (
    BlockCorruptError,
    DFShaError,
    InsufficientNodesError,
    ManifestInvalidError,
    MissingBlockError,
)
from .models import (
    BlockInfo,
    BlockPlacement,
    DataNodeInfo,
    ObjectManifest,
    ObjectPlacement,
)
from .placement import place_object

__all__ = [
    "BLOCK_SIZE_DEFAULT",
    "BlockCorruptError",
    "BlockInfo",
    "BlockPlacement",
    "DFShaError",
    "DataNodeInfo",
    "InsufficientNodesError",
    "ManifestInvalidError",
    "MissingBlockError",
    "ObjectManifest",
    "ObjectPlacement",
    "assemble_from_directory",
    "assemble_stream",
    "chunk_file",
    "chunk_stream",
    "place_object",
]
