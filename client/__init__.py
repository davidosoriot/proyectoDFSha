"""Cliente de DFSha.

Implementa el flujo completo visto por el usuario final:
    - login/register contra el ControlNode
    - mkdir/rmdir/ls en el namespace virtual
    - put: partir, subir bloques a los DataNodes, commitear
    - get: resolver, bajar bloques, verificar y reensamblar
    - stat/delete

El cliente nunca pasa bytes por el ControlNode. Resuelve ubicaciones y
habla directamente con los DataNodes. Si una replica falla, prueba la
siguiente en orden y reporta el fallo al ControlNode.
"""

from .sdk import DFShaClient, DFShaClientError

__all__ = ["DFShaClient", "DFShaClientError"]
