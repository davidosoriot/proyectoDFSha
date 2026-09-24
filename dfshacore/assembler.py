"""Reensamblado por streaming de un objeto a partir del manifiesto y bloques.

Verifica integridad bloque a bloque contra los checksums SHA-256 del manifiesto.
Ningun bloque completo se mantiene en memoria: se lee en fragmentos, se
actualiza el hasher, y se escribe al destino.
"""

from __future__ import annotations

import hashlib
from typing import BinaryIO, Callable, Iterable

from .chunker import READ_CHUNK_SIZE
from .errors import BlockCorruptError, ManifestInvalidError, MissingBlockError
from .models import BlockInfo, ObjectManifest

BlockOpener = Callable[[BlockInfo], BinaryIO]
"""Firma del callable que abre un stream de lectura para un bloque dado.

El llamador es quien decide de donde vienen los bytes: disco local, un
DataNode remoto, memoria, etc. Este modulo no conoce la fuente.
"""


def assemble_stream(
    manifest: ObjectManifest,
    block_opener: BlockOpener,
    destination: BinaryIO,
) -> str:
    """Reensambla un objeto en el stream de destino, verificando integridad.

    Parametros:
        manifest: manifiesto del objeto con la lista de bloques y sus checksums.
        block_opener: callable que abre un stream de lectura para un BlockInfo.
            Debe lanzar cualquier excepcion adecuada si el bloque no existe;
            este modulo la convierte en MissingBlockError.
        destination: stream binario abierto en modo escritura ('wb').

    Retorna:
        El SHA-256 del contenido reensamblado. Debe coincidir con
        manifest.global_checksum; si no, se lanza BlockCorruptError sobre el
        primer bloque en el que se detecto la divergencia.

    Errores:
        MissingBlockError si un bloque no se puede abrir.
        BlockCorruptError si el checksum de un bloque no coincide.
    """
    if manifest.total_size == 0:
        # Archivo vacio: no hay nada que escribir ni verificar.
        if manifest.blocks:
            raise ManifestInvalidError(
                "Manifiesto invalido: total_size=0 pero blocks no esta vacio"
            )
        return hashlib.sha256().hexdigest()

    # Reordenamos por index por robustez (el llamador podria pasarlos desordenados).
    ordered_blocks = sorted(manifest.blocks, key=lambda b: b.index)

    # Validamos indices contiguos empezando en 0.
    for expected_idx, block in enumerate(ordered_blocks):
        if block.index != expected_idx:
            raise ManifestInvalidError(
                f"Indices no contiguos en el manifiesto: esperado {expected_idx}, "
                f"encontrado {block.index}"
            )

    global_hasher = hashlib.sha256()

    for block in ordered_blocks:
        try:
            stream = block_opener(block)
        except FileNotFoundError as exc:
            raise MissingBlockError(block.block_id, block.index) from exc
        except MissingBlockError:
            raise
        except Exception as exc:
            raise MissingBlockError(block.block_id, block.index) from exc

        block_hasher = hashlib.sha256()
        bytes_read = 0
        try:
            while True:
                chunk = stream.read(READ_CHUNK_SIZE)
                if not chunk:
                    break
                destination.write(chunk)
                block_hasher.update(chunk)
                global_hasher.update(chunk)
                bytes_read += len(chunk)
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

        if bytes_read != block.size:
            raise BlockCorruptError(
                block_id=block.block_id,
                index=block.index,
                expected=f"{block.size} bytes",
                actual=f"{bytes_read} bytes",
            )

        actual_checksum = block_hasher.hexdigest()
        if actual_checksum != block.checksum:
            raise BlockCorruptError(
                block_id=block.block_id,
                index=block.index,
                expected=block.checksum,
                actual=actual_checksum,
            )

    final_checksum = global_hasher.hexdigest()
    if final_checksum != manifest.global_checksum:
        # Este caso solo se alcanza si algun bloque paso su verificacion
        # individual pero la concatenacion no cuadra con el global. Indica
        # inconsistencia en el propio manifiesto.
        raise BlockCorruptError(
            block_id="<global>",
            index=-1,
            expected=manifest.global_checksum,
            actual=final_checksum,
        )

    return final_checksum


def assemble_from_directory(
    manifest: ObjectManifest,
    input_dir: str,
    destination_path: str,
) -> str:
    """Conveniencia: reensambla desde un directorio donde cada bloque es un
    archivo cuyo nombre coincide con su block_id.
    """
    import os

    def _opener(block: BlockInfo) -> BinaryIO:
        return open(os.path.join(input_dir, block.block_id), "rb")

    with open(destination_path, "wb") as dst:
        return assemble_stream(manifest, _opener, dst)
