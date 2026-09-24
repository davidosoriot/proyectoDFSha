"""Excepciones explicitas del modulo dfshacore.

Todas las condiciones anomalas del particionamiento, reensamblado y colocacion
se reportan levantando alguna de estas excepciones. Nunca se falla en silencio.
"""


class DFShaError(Exception):
    """Clase base para los errores del modulo."""


class BlockCorruptError(DFShaError):
    """Se detecta que un bloque tiene un contenido distinto a su checksum."""

    def __init__(self, block_id: str, index: int, expected: str, actual: str):
        self.block_id = block_id
        self.index = index
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Bloque corrupto: block_id={block_id} index={index} "
            f"expected_sha256={expected} actual_sha256={actual}"
        )


class MissingBlockError(DFShaError):
    """Falta un bloque necesario para el reensamblado."""

    def __init__(self, block_id: str, index: int):
        self.block_id = block_id
        self.index = index
        super().__init__(
            f"Bloque faltante: block_id={block_id} index={index}"
        )


class InsufficientNodesError(DFShaError):
    """Hay menos DataNodes disponibles que el factor de replicacion requerido."""

    def __init__(self, required: int, available: int):
        self.required = required
        self.available = available
        super().__init__(
            f"Nodos insuficientes: se requieren {required} y hay {available} disponibles"
        )


class ManifestInvalidError(DFShaError):
    """El manifiesto contiene datos inconsistentes (indices fuera de rango,
    tamanos que no cuadran, etc.).
    """
