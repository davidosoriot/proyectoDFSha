# Guía de sustentación — DFSha

Documento de referencia rápida para la defensa oral. Por cada módulo lista
**qué decidí** y **qué alternativa descarté y por qué**. Las preguntas
difíciles de sustentación casi siempre son "¿por qué así y no asá?".

Autores: David Osorio · Esteban Villa.
Materia: Tópicos Especiales en Telemática.

---

## 0. Decisiones transversales

### Lenguaje y stack: Python 3.11+ con FastAPI + SQLAlchemy async
- **Elegí** Python por velocidad de desarrollo y porque permite expresar
  los algoritmos de particionamiento y colocación de forma legible.
  FastAPI por su documentación OpenAPI automática y por su modelo async
  nativo. SQLAlchemy async porque es el ORM estándar y abstrae SQLite
  (dev) / PostgreSQL (prod).
- **Descarté Go/Rust**: más rápidos pero su curva de setup habría comido
  tiempo que gasté en features del Hito 3 (federación, replicación).
- **Descarté gRPC**: la especificación lo mencionaba pero HTTP+JSON es
  más fácil de depurar, inspeccionar con `curl`, documentar con OpenAPI,
  y testear in-process con `ASGITransport`. El tradeoff es overhead de
  serialización, aceptable para la demo.

### Plano de datos vs plano de control
- **Invariante del sistema**: el ControlNode nunca transporta bytes.
  Todas sus respuestas devuelven *ubicaciones* (`node_id`, `address`),
  no contenido. El cliente conecta directo al DataNode para subir/bajar
  bloques. Esto desacopla el crecimiento del catálogo del ancho de banda.
- **Descartado**: hacer que el ControlNode proxee bytes "para simplificar
  el cliente". Lo rechacé porque convierte al CN en cuello de botella y
  rompe la separación de planos del diseño HDFS que inspiró el proyecto.

### Autenticación por capa
| Capa | Mecanismo | Razón |
|---|---|---|
| Usuarios → CN | JWT HS256 | Es stateless; el CN valida sin tocar DB |
| DataNodes → CN | API key `X-DataNode-Key` | Simple, no hay flujo humano |
| CN → CN (federación) | API key `X-Federation-Key` | Idem, peer-to-peer |
| Leader → Follower | API key `X-Replication-Key` | Separada por rol |
- **Descarté mTLS** porque exige PKI y rotación de certificados; es lo
  correcto en producción pero no aporta a demostrar el diseño.

---

## 1. `dfshacore/` — Particionamiento, colocación, reensamblado

### Streaming vs cargar en memoria
- **Elegí** `chunk_stream` con lectura por chunks de 1 MiB sobre bloques
  lógicos de 64 MiB. El archivo nunca entra entero en RAM.
- **Descarté** `source.read()` completo porque un archivo de 5 GB
  tumbaría al proceso. El patrón "callback `block_writer`" permite al
  llamador decidir el destino (archivo local, HTTP PUT, memoria).

### `block_id = {object_id}_{index:06d}`
- **Elegí** un formato predecible y ordenable lexicográficamente.
- **Descarté UUID por bloque** porque habría requerido una tabla extra
  para mapear índice→id y perdería la posibilidad de recuperar el
  `object_id` del prefijo (ver `CatalogService._object_id_from_block_ids`).

### Algoritmo de colocación: random con restricciones
- **Elegí** elegir réplicas priorizando (1) un cross-domain, (2) resto en
  dominio local, (3) nodos con más espacio libre, con shuffle semilla-
  dependiente para repartir carga.
- **Descarté round-robin estricto** porque crea hot-spots cuando los
  nodos entran/salen. Descarté también placement por rendezvous hashing
  — es mejor para rebalanceo pero sobrepasa el alcance.

### Factor de replicación por defecto: 3
- Patrón estándar HDFS/Ceph: 2 réplicas aguantan la caída de 1 nodo con
  tiempo para recuperarse; 3 aguantan 2 caídas simultáneas y es el
  mínimo para que "cross-domain" tenga sentido con RF local=2.

---

## 2. `controlnode/` — Catálogo + orquestación

### SQLite por defecto, cadena de conexión intercambiable
- **Elegí** SQLite+aiosqlite para dev y tests (sin servidor, in-memory
  para tests). La config permite pasar a PostgreSQL cambiando
  `DFSHA_DB_URL`. El Repository encapsula todo el SQL.
- **Descarté** usar un KV (Redis, etcd) desde el inicio porque el modelo
  es relacional (usuarios → directorios → objetos → bloques → réplicas),
  y los índices parciales únicos de SQL son clave para versionado.

### `CatalogService` como ÚNICO punto de mutación
- **Elegí** que cualquier cambio al catálogo pase por `CatalogService.X`,
  que en la misma transacción escribe la mutación + el evento al log.
- **Descarté** mutaciones dispersas en los handlers REST. La ventaja de
  centralizar: cualquier evento está garantizado de existir si la
  mutación tuvo éxito; esto es lo que permite propagación a peers y
  replicación leader→follower sin perder cambios.

### Versionado: índice único parcial sobre COMMITTED
- **Elegí** un `UniqueConstraint` con `sqlite_where="status='COMMITTED'"`
  sobre `(domain_id, owner_id, object_key, version)`. Los PENDING
  coexisten con la misma key hasta que uno hace commit.
- **Descarté** generar version como `max(version)+1` sin índice: dos
  commits concurrentes de la misma key podrían asignar la misma versión.
  Agregué un `asyncio.Lock` por `(owner, key, domain)` para serializar
  la asignación intra-proceso. Entre procesos (CN replicado), la carga
  cae en el leader, así que no hay carrera.

### Objetos PENDING con TTL
- **Elegí** marcar objetos como PENDING al init y pasarlos a COMMITTED
  solo cuando `confirmed_replicas >= RF` por bloque. `PendingCleanupTask`
  borra los que no se commitearon a tiempo.
- **Descarté** compromise 2PC con los DataNodes: sobredimensionado para
  este alcance. El modelo "lazy commit" se recupera solo del fallo del
  cliente (que no commitee).

### Detección de caída: SUSPECTED → DEAD por timeouts separados
- **Elegí** dos umbrales (20 s suspect, 30 s dead) para no tirar un nodo
  por una pausa de GC o un hiccup de red.
- **Al pasar a SUSPECTED**: dejamos de asignarle NUEVAS réplicas pero no
  tiramos las existentes.
- **Al pasar a DEAD**: sus réplicas ASSIGNED/CONFIRMED pasan a LOST y
  emitimos un `REPLICA_LOST` por cada una. La `RereplicationTask` los
  consume.

### Log de eventos como "costura" para Hito 3
- **Elegí** una tabla `events(event_id auto, propagated=0)` donde cada
  mutación deja huella. Permite reconstruir estado y propagar.
- **Descarté** change-data-capture con triggers de SQLite: menos portable.
  También descarté publicar eventos vía message broker (Kafka, Redis
  streams): dependencia extra sin justificación en el alcance.

---

## 3. `datanode/` — Worker de bloques

### Formato de archivo: `[32B checksum][12B nonce][16B tag][ciphertext]`
- El checksum SHA-256 va en claro al inicio para verificar el bloque sin
  descifrarlo (útil para `/inventory` y para el block-report rápido).
- El payload va cifrado AES-GCM si hay `DFSHA_DN_ENCRYPTION_KEY`.
- **Alternativa descartada**: metadatos en un archivo lateral. Más
  trabajo, dos escrituras no atómicas, más puntos de corrupción.

### Escritura atómica: write-to-tmp + fsync + rename
- Garantiza que un `kill -9` a mitad de escritura deja o el archivo
  anterior o nada: nunca un archivo corrupto con nombre válido.

### Cifrado AES-GCM
- **Elegí** AES-GCM (vía `cryptography`): confidencialidad + integridad
  autenticada en un primitivo bien analizado.
- **Rechacé** criptografía hecha a mano. Un fallback XOR+HMAC que había
  escrito "por si no hay cryptography" lo eliminé: es correcto para la
  demo pero una invitación a preguntas peligrosas de defensa.
- **Rechacé AES-CBC**: necesita HMAC separado para integridad; es la vía
  tradicional pero AES-GCM lo supera en todo.

### Verificación de corrupción al leer
- Cada GET recalcula el SHA-256 del plaintext y lo compara con el
  checksum almacenado. Si no coincide, respondemos **410 GONE**. El
  cliente marca la réplica como caída y pide la siguiente.
- **Rechacé 404**: 404 oculta la causa; 410 ("gone") comunica al cliente
  que el bloque **existía** pero no es utilizable.

### Confirmación inmediata al almacenar (`on_block_stored`)
- Al recibir un bloque, el DataNode notifica al ControlNode con un
  block-report de ese solo bloque, para que la réplica pase a CONFIRMED
  sin esperar al tick periódico.
- **Rechacé** depender solo del block-report periódico: el `commit`
  tardaría hasta un minuto, inaceptable para el flujo put/commit.

### Operación `POST /blocks/{id}/replicate`
- El ControlNode no mueve bytes; cuando necesita re-replicar un bloque
  llama a este endpoint en el DataNode fuente, que hace PUT al destino.
- **Rechacé "pull desde el destino"**: implica que el destino conozca al
  fuente y tenga permisos de lectura. Push es más simple y la fuente
  controla la operación.

---

## 4. `client/` — SDK y CLI

### Cliente async + CLI delgado
- El SDK (`DFShaClient`) es la lógica real; `cli.py` solo parsea argv.
  Permite tests exhaustivos sin levantar procesos.
- **Rechacé** `requests` sync porque `httpx` async con `ASGITransport`
  permite probar el flujo end-to-end sin red real (y sin fakes).

### Failover de lectura
- Al leer un bloque, si la primera réplica devuelve 404/410 o falla, el
  cliente pide la siguiente y **reporta la caída** al ControlNode via
  `/objects/{id}/blocks/{bid}/report-failure`. Esto alimenta
  `REPLICA_LOST` y dispara re-replicación.
- **Rechacé** retry en la misma réplica: si la primera falló, el bloque
  probablemente está corrupto o el nodo caído; sumar RTT no ayuda.

### Verificación extremo a extremo
- El cliente recalcula el checksum global del contenido reensamblado y
  lo compara con el que firmó el put. Si el ControlNode fuera comprometido
  y devolviera checksums alterados, el cliente lo detectaría al reensamblar.

---

## 5. Re-replicación — `controlnode/core/rereplication.py`

### Scanning vs event-driven
- **Elegí** un scan periódico de bloques con `confirmed_count < RF`.
- **Rechacé** consumir el log de REPLICA_LOST directamente porque la
  misma condición ocurre también por 1) bloque que nunca llegó a RF
  (fallo de cliente), 2) corrupción detectada a la lectura, 3) nodo
  recuperado con menos espacio. El scan los cubre todos uniformemente.

### Elección de fuente y destino
- **Fuente**: réplica CONFIRMED en nodo ALIVE (no SUSPECTED). Random
  para repartir carga de egreso.
- **Destino**: nodo ALIVE que NO sea ya portador, ordenado por `free_bytes`
  descendente.
- **Rechacé** destinos SUSPECTED: no queremos agregar réplicas a nodos
  que probablemente están mal.

### Fallo durante la copia
- Si la copia falla, la réplica recién asignada se marca LOST para que
  el siguiente tick vuelva a intentar con otro destino. Idempotente.

---

## 6. ACLs granulares — `controlnode/core/acl_svc.py`

### Longest-prefix match
- **Elegí**: entre todas las ACLs que coinciden con el path, gana la de
  prefijo más largo. Permite reglas específicas que anulan generales.
- **Rechacé "unión de permisos"** (todas las ACLs coincidentes suman
  permisos). Es más permisivo y crea sorpresas: si doy READ en /fotos y
  luego revoco en /fotos/privadas, con "unión" la revocación no haría
  nada. Longest-prefix match es el modelo de rutas HTTP y de IAM.

### Permisos: READ, WRITE, DELETE separados
- **Elegí** una lista de permisos por ACL (ej. `[READ, WRITE]`).
- **Rechacé** un bitmask: menos legible en JSON y no gana nada a esta
  escala.

### No se puede dar ACL a uno mismo
- La operación lanza 409. Rechaza "autoconcederse" para que el log de
  ACLs sea útil (todas las ACLs listadas son concesiones reales).

---

## 7. Federación — `controlnode/core/federation_svc.py`

### Peers registrados explícitamente, no auto-descubrimiento
- **Elegí** registro manual (`POST /federation/peers`) con api_key
  compartida.
- **Rechacé auto-discovery vía multicast/mDNS**: fallaría en WAN y
  requiere infraestructura. Rechacé también DNS-SD: dependencia extra.

### Propagación: pull desde cada peer con marca `propagated=1`
- `FederationPropagator.tick()` lee eventos pendientes, los POSTea a cada
  peer, y SOLO si TODOS confirmaron marca `propagated=1`.
- **Rechacé "marcar propagated inmediatamente al enviar"**: si un peer
  rechaza, el evento se pierde para él. El esquema actual reintenta
  mientras algún peer esté caído.

### Lectura cross-domain por proxy
- Un cliente en domain-A pide `GET /objects?key=/x&owner=alice&domain=B`.
  Su CN consulta `/federation/objects` del CN remoto, recibe las
  ubicaciones (DataNodes de domain-B) y las devuelve al cliente. El
  cliente luego habla directo con los DataNodes remotos.
- **Rechacé** copiar el objeto a domain-A: redundaría el almacenamiento
  y rompería la autoridad del dominio dueño sobre sus datos.

### Catálogo compartido vs federado
- **Elegí** federado: cada CN sabe solo de sus propios objetos; los
  remotos se consultan por lookup. Los eventos propagados dan contexto
  pero NO se usan para resolver reads (son logs, no índice).
- **Rechacé** un índice global replicado: implicaría consenso entre
  dominios, lo cual es Hito 4 y más.

---

## 8. Replicación del ControlNode (leader-follower)

### Snapshot completo vs event-replay
- **Elegí** snapshot completo periódico. El leader expone un dump JSON
  de todas las tablas; el follower borra y reinserta.
- **Rechacé event-replay** porque exigiría que los eventos codifiquen la
  mutación exacta (OBJECT_INIT con todos los blocks y replicas, etc.),
  lo cual es trabajo serio y propenso a bugs. Snapshot completo es
  O(catálogo) por tick pero simple y correcto. Para catálogo real
  (millones de objetos) migraría a event-replay con snapshot de base.

### Follower rechaza escrituras con 503 + URL del leader
- El dependency `require_leader` se usaría en handlers de escritura;
  hoy la decisión del rol es por env y el cliente puede redirigir.
- **Rechacé "escrituras locales que se mergean"**: entra en consenso
  distribuido, Hito 4.

### Promoción manual
- Si el leader cae, un operador cambia `DFSHA_ROLE=LEADER` en un
  follower, lo reinicia, y actualiza `DFSHA_LEADER_URL` en los demás.
- **Rechacé elección automática (Raft)**: tiene su propio proyecto; lo
  dejé explícito como "fuera del alcance" en README.

---

## 9. Testing (110 tests)

### ASGITransport en memoria vs levantar sockets
- **Elegí** `httpx.ASGITransport` para hablar con las apps FastAPI dentro
  del mismo proceso: tests deterministas, no abren puertos, no colisionan
  entre sí.
- **Para multi-nodo** (3 DataNodes, 2 ControlNodes): escribí un
  `_MultiNodeTransport` que enruta por el `host` de la URL a la app
  correspondiente. Vale como maqueta sin red real.
- **Rechacé pytest-docker / testcontainers**: lento (minutos vs segundos)
  y oculta errores de concurrencia bajo capas de infraestructura.

### SQLite in-memory por test con `StaticPool`
- Cada test crea su engine propio; el `StaticPool` fuerza a usar la
  misma conexión para todas las sesiones (requisito de SQLite in-memory).
- **Rechacé** DB en disco por test: 10x más lento y requiere limpieza.

---

## Preguntas probables y respuesta corta

| Pregunta | Respuesta |
|---|---|
| ¿Qué pasa si el cliente muere a mitad de un put? | El objeto queda PENDING; `PendingCleanupTask` lo borra tras `pending_ttl_seconds`. Los bloques en los DataNodes serán "huérfanos" y se borrarán al próximo block-report. |
| ¿Por qué HTTP y no gRPC? | Debugabilidad, OpenAPI gratis, in-process testing con ASGI. gRPC es más eficiente en wire pero no cambia la correcitud del diseño. |
| ¿Cómo garantizan "exactly-once" en el commit? | No lo hacemos: el commit es idempotente (reintentar un commit ya hecho es error 422, no corrupción). El cliente debe reintentar hasta éxito. |
| ¿Qué pasa si dos clientes suben la misma key a la vez? | Los dos crean objetos PENDING. Cuando el primero commitea, obtiene version=1. Cuando el segundo commitea, obtiene version=2. El lock intra-proceso serializa la asignación de versión. |
| ¿Qué pasa si el leader del CN se cae? | Followers siguen sirviendo lecturas con el último snapshot. Escrituras se rechazan con 503. Operador promueve un follower manualmente. |
| ¿Y si un peer de federación miente sobre un objeto? | Hoy confiamos en la api_key. En producción, mTLS + firmas en los manifiestos. El cliente igual verifica checksum global al reensamblar. |
| ¿Cómo evitan ataques de replay sobre JWT? | El token tiene `exp` (default 24 h). Para revocación granular habría que agregar lista de revocación o acortar `exp` a minutos y rotar refresh tokens; fuera del alcance. |
| ¿Por qué no RAFT para el CN? | Alcance. La replicación leader-follower sin elección automática ya cubre durabilidad y lectura escalable; failover es manual. |
| ¿Qué garantías tiene el cifrado en reposo? | AES-GCM 256 bits. Clave única por despliegue del DataNode. Si el disco se roba, los bloques son inservibles. **Limitación**: la clave vive en env del proceso; en prod iría a KMS/HSM. |
| ¿Qué tan grande puede ser un archivo? | Limitado por capacidad del DataNode y por `int` SQL (hasta ~2 GB por campo en SQLite; en PostgreSQL BIGINT es TB). El chunker es streaming, nunca carga el archivo en RAM. |
