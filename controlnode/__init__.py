"""ControlNode del sistema DFSha.

Cerebro del dominio: mantiene el catalogo, el espacio de nombres, el registro
de DataNodes y decide donde colocar las replicas. No maneja bytes de contenido
(la invariante crucial del diseno).
"""
