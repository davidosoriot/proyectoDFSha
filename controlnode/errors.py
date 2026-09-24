"""Excepciones del ControlNode.

Se mapean a codigos HTTP en la capa API. Los servicios del core nunca lanzan
HTTPException directamente, solo estas.
"""


class ControlNodeError(Exception):
    """Base."""


class NotFoundError(ControlNodeError):
    """Recurso solicitado no existe."""


class ConflictError(ControlNodeError):
    """Estado o restriccion en conflicto (por ejemplo, duplicado)."""


class PermissionDeniedError(ControlNodeError):
    """El usuario no tiene permiso sobre el recurso."""


class InvalidStateError(ControlNodeError):
    """La operacion no es valida en el estado actual del recurso."""


class InsufficientResourcesError(ControlNodeError):
    """No hay recursos suficientes (por ejemplo, DataNodes) para la operacion."""


class AuthenticationError(ControlNodeError):
    """Credenciales invalidas o token no valido."""
