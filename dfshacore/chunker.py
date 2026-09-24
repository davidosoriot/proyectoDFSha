"""Particionamiento por streaming de un archivo en bloques con SHA-256.

Un archivo de 5 GB no se puede cargar completo en memoria. Este modulo lee el
archivo en fragmentos pequenos (READ_CHUNK_SIZE) y acumula hasta completar el
tamano de bloque logico configurado (BLOCK_SIZE_DEFAULT = 64 MB).
"""

from __future__ import annotations

import hashlib
import os
import time
import uuid
from typing import BinaryIO, Iterator, Optional, Tuple

from .models import BlockInfo, ObjectManifest

BLOCK_SIZE_DEFAULT: int = 64 * 1024 * 1024  # 64 MiB
"""Tamano por defecto de bloque logico."""

READ_CHUNK_SIZE: int = 1024 * 1024  # 1 MiB
"""Tamano interno de lectura por syscall.

Balance entre overhead de read() y consumo de memoria pico. El bloque logico
sigue siendo BLOCK_SIZE_DEFAULT, este valor solo controla cuanta memoria usa
el chunker por iteracion.
"""


def _format_block_id(object_id: str, index: int) -> str:
    return f"{object_id}_{index:06d}"


def _iter_block_chunks(
    source: BinaryIO, block_size: int
) -> Iterator[Tuple[int, bytes]]:
    """Genera trozos de READ_CHUNK_SIZE hasta completar un bloque logico.

    Emite tuplas (bytes_del_bloque_acumulados, chunk_leido) para permitir al
    llamador escribir cada chunk conforme se lee, sin acumular el bloque
    completo en memoria.
    """
    remaining = block_size
    while remaining > 0:
        to_read = min(READ_CHUNK_SIZE, remaining)
        chunk = source.read(to_read)
        if not chunk:
            return
        remaining -= len(chunk)
        yield len(chunk), chunk


def chunk_stream(
    source: BinaryIO,
    object_key: str,
    block_writer,
    block_size: int = BLOCK_SIZE_DEFAULT,
    object_id: Optional[str] = None,
) -> ObjectManifest:
    """Particiona un stream binario en bloques y calcula todos los checksums.

    Parametros:
        source: stream binario abierto en modo lectura ('rb').
        object_key: ruta visible del objeto, se guarda en el manifiesto.
        block_writer: callable (block_id, index) -> BinaryIO. El llamador
            provee el destino donde escribir el bloque. El stream retornado
            se cierra automaticamente al terminar el bloque.
        block_size: tamano logico de bloque en bytes.
        object_id: opcional; si no se provee se genera un UUID4.

    Retorna:
        ObjectManifest con la lista completa de bloques y los checksums.

    El particionamiento es por streaming: en ningun momento se mantiene un
    bloque completo en memoria. Se leen chunks de READ_CHUNK_SIZE, se
    actualizan los hashers y se escriben directamente al destino.
    """
    if block_size <= 0:
        raise ValueError("block_size debe ser positivo")

    obj_id = object_id or str(uuid.uuid4())
    global_hasher = hashlib.sha256()
    blocks: list[BlockInfo] = []
    total_size = 0
    index = 0

    while True:
        block_hasher = hashlib.sha256()
        block_size_accum = 0
        block_id = _format_block_id(obj_id, index)

        # Solicitamos el destino solo si empieza a haber datos que escribir.
        writer = None
        first_chunk = True

        for read_len, chunk in _iter_block_chunks(source, block_size):
            if first_chunk:
                writer = block_writer(block_id, index)
                first_chunk = False
            writer.write(chunk)
            block_hasher.update(chunk)
            global_hasher.update(chunk)
            block_size_accum += read_len

        if block_size_accum == 0:
            # No mas datos. Terminamos.
            break

        if writer is not None:
            close = getattr(writer, "close", None)
            if callable(close):
                close()

        blocks.append(
            BlockInfo(
                block_id=block_id,
                index=index,
                size=block_size_accum,
                checksum=block_hasher.hexdigest(),
            )
        )
        total_size += block_size_accum
        index += 1

        # Si el bloque quedo menor que el block_size, el archivo se acabo.
        if block_size_accum < block_size:
            break

    return ObjectManifest(
        object_id=obj_id,
        object_key=object_key,
        total_size=total_size,
        block_size=block_size,
        global_checksum=global_hasher.hexdigest(),
        blocks=blocks,
        created_at=time.time(),
    )


def chunk_file(
    file_path: str,
    object_key: str,
    output_dir: str,
    block_size: int = BLOCK_SIZE_DEFAULT,
    object_id: Optional[str] = None,
) -> ObjectManifest:
    """Conveniencia: particiona un archivo en disco escribiendo cada bloque
    como un archivo separado en output_dir.

    Cada bloque queda en output_dir/{block_id}. Utilizado por tests y por el
    cliente en flujos de desarrollo. En produccion, el cliente pasa un
    block_writer que envia el bloque directamente al DataNode.
    """
    os.makedirs(output_dir, exist_ok=True)

    def _writer(block_id: str, index: int):
        return open(os.path.join(output_dir, block_id), "wb")

    with open(file_path, "rb") as source:
        return chunk_stream(
            source=source,
            object_key=object_key,
            block_writer=_writer,
            block_size=block_size,
            object_id=object_id,
        )
