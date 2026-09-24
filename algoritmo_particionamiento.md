# Algoritmo de particionamiento, manifiesto, reensamblado y colocación — DFSha

## Alcance del módulo

Este módulo cubre cuatro responsabilidades, todas sin efectos de red ni dependencias externas:

1. **Particionamiento y checksums** — partir el archivo en bloques por streaming, calcular SHA-256 por bloque y global.
2. **Manifiesto** — generar la estructura ordenada de bloques con índice, tamaño y checksum.
3. **Reensamblado** — reconstruir el archivo a partir del manifiesto y los bloques, verificando integridad bloque por bloque.
4. **Política de colocación de réplicas** — dado un manifiesto y un inventario de DataNodes, decidir a qué nodos va cada réplica de cada bloque.

**Fuera del alcance:** comunicación de red, ControlNode, DataNode como servicio, persistencia real del catálogo, autenticación, cifrado y federación.

---

## 1. Estructura de archivos y módulos

```
dfshacore/
├── __init__.py          exporta la API pública del módulo
├── errors.py            excepciones explícitas del módulo
├── models.py            dataclasses: BlockInfo, ObjectManifest, DataNodeInfo, BlockPlacement
├── chunker.py           streaming file → bloques + checksum SHA-256
├── assembler.py         streaming bloques + manifiesto → file + verificación de integridad
└── placement.py         política de colocación de réplicas

tests/
├── conftest.py          fixtures compartidos: directorios temporales, inventario de nodos de prueba
├── test_chunker.py      pruebas de particionamiento
├── test_assembler.py    pruebas de reensamblado e integridad
├── test_placement.py    pruebas de política de colocación
└── test_integration.py  round-trip byte a byte y pruebas multi-módulo
```

---

## 2. Estructuras de datos

### `BlockInfo` — describe un bloque dentro del manifiesto

```python
@dataclass(frozen=True)
class BlockInfo:
    block_id:  str   # "{object_id}_{index:06d}"
    index:     int   # posición 0-based dentro del objeto
    size:      int   # bytes reales del bloque (≤ block_size; el último puede ser menor)
    checksum:  str   # SHA-256 hex del contenido del bloque
```

### `ObjectManifest` — descripción completa del objeto

```python
@dataclass
class ObjectManifest:
    object_id:       str             # UUID4 generado al crear el objeto
    object_key:      str             # ruta visible para el usuario, e.g. "/fotos/video.mp4"
    total_size:      int             # bytes totales del objeto (0 para archivo vacío)
    block_size:      int             # tamaño configurado de bloque en bytes (64 MB por defecto)
    global_checksum: str             # SHA-256 del contenido completo en orden
    blocks:          list[BlockInfo] # lista ordenada por index; vacía si total_size == 0
    created_at:      float           # timestamp Unix del momento de particionamiento
```

### `DataNodeInfo` — entrada del inventario de nodos disponibles

```python
@dataclass
class DataNodeInfo:
    node_id:      str    # identificador único del nodo
    domain_id:    str    # dominio al que pertenece, e.g. "domain-david"
    free_bytes:   int    # espacio libre reportado en bytes
    load_score:   float  # carga actual en [0.0, 1.0]; 0 = idle, 1 = saturado
    is_available: bool   # True si el nodo respondió el último heartbeat
```

### `BlockPlacement` — decisión de colocación para un bloque

```python
@dataclass
class BlockPlacement:
    block_id:      str        # block_id del bloque en el manifiesto
    block_index:   int        # redundante con el manifiesto, útil para depuración
    replica_nodes: list[str]  # node_ids, longitud == replication_factor
                              # [0]  = primera réplica (dominio escritor)
                              # [-1] = última réplica (dominio distinto preferentemente)
```

### `ObjectPlacement` — resultado completo para un objeto

```python
@dataclass
class ObjectPlacement:
    object_id:  str
    placements: list[BlockPlacement]  # una entrada por bloque, orden == manifest.blocks
```

---

## 3. Generación de identificadores de bloque

**Formato:** `{object_id}_{index:06d}`

Ejemplo: `object_id = "7f3a2b1c-..."`, bloque en posición 2 → `block_id = "7f3a2b1c-..._000002"`.

El `object_id` es un **UUID4** generado al llamar a `chunk_object()`, sin relación con el contenido.

### Por qué esta estrategia y no otras

| Alternativa | Problema |
|---|---|
| SHA-256 del contenido del bloque | Deduplicación accidental: dos objetos con bloques idénticos compartirían `block_id`. La especificación excluye explícitamente la deduplicación. |
| UUID4 puro por bloque | Funciona, pero el `block_id` no lleva relación con su objeto ni su posición, dificultando el diagnóstico. |
| `hash(object_key + index)` | Si el mismo `object_key` se usa para versiones distintas del archivo, hay colisiones. |

Con `UUID4_index` se garantiza:
- Unicidad global incluso para objetos con contenido idéntico.
- El `block_id` lleva información de debug útil.
- Sin deduplicación implícita porque el UUID4 del objeto es opaco e independiente del contenido.

---

## 4. Algoritmo de colocación

**Firma:**
```
place_object(manifest, nodes, writer_domain, replication_factor=3, seed=None) → ObjectPlacement
```

### Paso 0 — Validación previa

```
available = [n for n in nodes if n.is_available]
if len(available) < replication_factor:
    raise InsufficientNodesError(...)
```

Falla explícita. No se intenta colocar con menos réplicas que las requeridas.

### Paso 1 — Inicializar RNG determinista

```
rng = random.Random(seed)
```

Con `seed=None` el comportamiento es no determinista (producción).
Con `seed=42` es reproducible (tests).

### Paso 2 — Inicializar contador de uso por nodo

```
usage: dict[str, int] = {n.node_id: 0 for n in available}
```

Rastrea cuántas réplicas se han asignado a cada nodo durante la colocación del objeto actual. Sirve para distribuir los bloques del mismo objeto entre nodos distintos en vez de concentrarlos.

### Paso 3 — Por cada bloque, colocar réplicas

#### Función de puntuación de un nodo candidato

```
score(node) = W_FREE   * normalize(node.free_bytes)
            - W_LOAD   * node.load_score
            - W_SPREAD * normalize(usage[node.node_id])
            + rng.uniform(0, epsilon)      # jitter para desempate aleatorio
```

Constantes: `W_FREE=0.5`, `W_LOAD=0.3`, `W_SPREAD=0.2`, `epsilon=0.01`.

`normalize(value)` escala el valor al rango [0, 1] sobre el máximo del conjunto candidato actual. Si todos son iguales, devuelve 1.0.

#### Réplica 1 — dominio del escritor

```
candidates = [n for n in available if n.domain_id == writer_domain]
if not candidates:
    candidates = available          # fallback si el dominio no tiene nodos
r1 = max(candidates, key=score)
selected = {r1.node_id}
```

#### Réplicas intermedias (2 .. replication_factor - 1)

```
for _ in range(replication_factor - 2):
    candidates = [n for n in available if n.node_id not in selected]
    rx = max(candidates, key=score)
    selected.add(rx.node_id)
```

#### Última réplica — dominio distinto preferentemente

```
remaining    = [n for n in available if n.node_id not in selected]
cross_domain = [n for n in remaining if n.domain_id != r1.domain_id]
candidates   = cross_domain if cross_domain else remaining
rN = max(candidates, key=score)
selected.add(rN.node_id)
```

#### Actualizar contadores

```
for node_id in replica_nodes:
    usage[node_id] += 1
```

### Cómo cumple cada regla de la especificación

| Regla | Mecanismo |
|---|---|
| Factor 3, nodos distintos por bloque | `selected` es un `set`; nunca se elige el mismo nodo dos veces para un bloque. |
| Primera réplica en dominio del escritor | Paso 3 — réplica 1 filtra por `domain_id == writer_domain` antes de puntuar. |
| Tercera réplica en dominio distinto preferentemente | Último paso filtra `cross_domain` con prioridad; cae a `remaining` solo si no hay opción. |
| Selección por espacio libre y carga, desempate aleatorio | Función `score()` con `free_bytes`, `load_score` y jitter RNG. |
| Bloques del mismo objeto repartidos entre nodos distintos | `W_SPREAD * usage[node_id]` penaliza nodos ya cargados del objeto actual. |

---

## 5. Lista de pruebas

### `test_chunker.py`

| # | Nombre | Qué verifica |
|---|---|---|
| C1 | `test_empty_file` | 0 bytes → `total_size=0`, `blocks=[]`, `global_checksum` del string vacío. |
| C2 | `test_smaller_than_block` | Archivo < block_size → exactamente 1 bloque, `block.size == total_size`. |
| C3 | `test_exact_multiple` | Tamaño exactamente `N * block_size` → exactamente N bloques, todos de tamaño `block_size`. |
| C4 | `test_larger_than_block` | `block_size + 1` bytes → 2 bloques; el segundo tiene `size=1`. |
| C5 | `test_block_checksums_match_content` | SHA-256 del contenido de cada bloque == `block.checksum`. |
| C6 | `test_global_checksum_matches_content` | SHA-256 de la concatenación de todos los bloques en orden == `global_checksum`. |
| C7 | `test_block_ids_unique` | No hay `block_id` repetidos en el manifiesto. |
| C8 | `test_block_ids_not_content_based` | Dos llamadas con el mismo archivo producen `block_id`s distintos (sin deduplicación accidental). |

### `test_assembler.py`

| # | Nombre | Qué verifica |
|---|---|---|
| A1 | `test_roundtrip_small` | Round-trip byte a byte para archivo < block_size. |
| A2 | `test_roundtrip_exact_multiple` | Round-trip byte a byte para archivo de tamaño `N * block_size`. |
| A3 | `test_roundtrip_empty` | Round-trip archivo vacío: salida de 0 bytes. |
| A4 | `test_corrupt_block_detected` | Modificar 1 byte de un bloque → `BlockCorruptError` con `block_id` e índice del bloque corrupto. |
| A5 | `test_missing_block_detected` | Omitir un bloque del conjunto → `MissingBlockError` con el `block_id` faltante. |
| A6 | `test_wrong_order_still_works` | Bloques en orden distinto al índice → el reensamblado los reordena y el resultado es correcto. |

### `test_placement.py`

| # | Nombre | Qué verifica |
|---|---|---|
| P1 | `test_no_replica_shares_node` | Para todo bloque, `replica_nodes` es un conjunto sin duplicados de tamaño `replication_factor`. |
| P2 | `test_first_replica_in_writer_domain` | `replica_nodes[0]` pertenece al `writer_domain` cuando hay al menos un nodo disponible en ese dominio. |
| P3 | `test_last_replica_prefers_other_domain` | Cuando existen nodos en otro dominio, `replica_nodes[-1]` está en un dominio diferente al de `replica_nodes[0]`. |
| P4 | `test_insufficient_nodes_raises` | `len(available) < replication_factor` → `InsufficientNodesError` explícito. |
| P5 | `test_deterministic_with_seed` | Misma llamada con mismo `seed` → resultado idéntico. |
| P6 | `test_no_writer_domain_nodes_fallback` | Sin nodos en `writer_domain`, no falla; usa cualquier nodo disponible para la primera réplica. |
| P7 | `test_blocks_spread_across_nodes` | Objeto con ≥ 2 bloques → réplicas primarias en ≥ 2 nodos distintos. Cubre RNF4. |

### `test_integration.py`

| # | Nombre | Qué verifica |
|---|---|---|
| I1 | `test_roundtrip_multi_block_exact_bytes` | Archivo con `block_size` pequeño (1 KB) y contenido binario aleatorio de varios KB → round-trip byte a byte. Cubre RNF3. |
| I2 | `test_large_object_on_multiple_nodes` | Objeto con 3 bloques (usando `block_size=1_024`) → réplicas primarias en ≥ 2 nodos distintos. Cubre RNF4. |
| I3 | `test_manifest_serializable` | `ObjectManifest` serializable a JSON con `dataclasses.asdict()` y deserializable sin pérdida. |

---

## 6. Decisiones propias no definidas en la especificación

| Decisión | Justificación |
|---|---|
| `READ_CHUNK_SIZE = 1 MiB` interno en el chunker | El bloque lógico es 64 MB, pero se lee en fragmentos de 1 MiB para equilibrar overhead de syscalls y consumo de memoria. Expuesto como constante ajustable. |
| Fallback cuando no hay nodos en el dominio del escritor | En vez de fallar, el algoritmo usa cualquier nodo disponible y lo advierte. El módulo no tiene contexto suficiente para saber si es un error de configuración o un estado transitorio; esa decisión la toma el ControlNode. |
| `global_checksum` sobre el contenido completo, no sobre checksums de bloques | Más estándar y verificable con `sha256sum` externo. La verificación de integridad global ya está cubierta por checksums individuales de bloque. |
| Campos de `ObjectManifest` todos tipos primitivos | Serialización trivial con `json.dumps(dataclasses.asdict(m))` sin adaptador especial, para que el ControlNode elija su mecanismo de persistencia libremente. |
| Pesos de puntuación como constantes de módulo (`W_FREE=0.5`, `W_LOAD=0.3`, `W_SPREAD=0.2`) | No están en la especificación. `W_FREE > W_LOAD` porque el espacio libre es el factor limitante más común. Fácilmente promovibles a parámetros de configuración si el ControlNode necesita ajustarlos. |
| `replica_nodes` como lista ordenada, no conjunto | El orden importa: posición 0 es la réplica que recibe el bloque primero; posición -1 es la réplica cross-domain. El ControlNode necesita este orden para implementar la replicación en cadena. |

---

## 7. Ambigüedades identificadas en el documento

**A1 — ¿Quién hace el particionamiento, el cliente o el ControlNode?**
El documento menciona que el cliente parte en bloques y reensambla, pero también que el cliente pide al ControlNode dónde escribir antes de subir. La interpretación coherente: el módulo de particionamiento vive en el cliente y el de colocación en el ControlNode. El cliente crea el manifiesto localmente, lo envía al ControlNode, recibe el `ObjectPlacement` y sube los bloques a los DataNodes indicados.

**A2 — "Bloques del mismo objeto en nodos distintos" vs. factibilidad con pocos nodos**
Con 4 nodos y factor 3, un objeto de 10 bloques genera 30 asignaciones sobre 4 nodos. Es imposible evitar que un nodo tenga más de un bloque del objeto. La regla se interpreta como "maximizar distribución" (via `W_SPREAD`), no como prohibición estricta de nodo repetido entre bloques distintos.

**A3 — `load_score`: métrica y escala no definidas**
La especificación menciona "carga reportada" sin definir unidades. Se asume `[0.0, 1.0]` (fracción de capacidad). Si el DataNode reporta otra escala, habría que normalizar antes de pasarlo al inventario. Queda pendiente de definirse en el protocolo del heartbeat.

**A4 — El manifiesto se crea antes de la transferencia**
La especificación dice que el objeto se publica en el catálogo solo cuando todas las réplicas confirmaron (RNF3). Pero el manifiesto (con checksums) se crea durante el particionamiento, antes de la transferencia. Este módulo genera el manifiesto; el ControlNode decide cuándo publicarlo en el catálogo.
