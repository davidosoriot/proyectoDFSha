# DFSha — Sistema de archivos distribuido

Proyecto de la materia **Tópicos Especiales en Telemática**.
Autores: David Osorio · Esteban Villa.

DFSha es un servicio de almacenamiento distribuido de objetos con
particionamiento en bloques y replicación. El sistema se organiza como una
federación de dominios autónomos; este repositorio contiene el **ControlNode
(SuperPeer)** de un dominio y el módulo compartido de particionamiento y
colocación de réplicas.

---

## Estructura del repositorio

```
dfshacore/                Módulo puro: particionamiento, manifiesto,
                          reensamblado, colocación de réplicas.
controlnode/              ControlNode del dominio: API REST + catálogo +
                          registro de DataNodes + orquestación +
                          re-replicación + ACLs + federación + replicación
                          leader-follower.
datanode/                 DataNode (worker): almacenamiento en disco,
                          verificación por checksum, cifrado AES-GCM
                          opcional, heartbeat, block report, copia P2P.
client/                   SDK async y CLI del cliente (put/get/ls/grant/...).
tests/                    Pruebas de dfshacore, ControlNode, DataNode,
                          cliente, re-replicación, ACL, federación y
                          replicación.
algoritmo_particionamiento.md   Diseño del módulo dfshacore.
control_node_guia.md      Guía de implementación del ControlNode para
                          sustentación.
ProyectoDFShaTopTel.docx  Especificación del proyecto.
```

---

## Alcance

**Dentro del alcance (Hito 2 + Hito 3):**
- Módulo `dfshacore` completo (chunker, assembler, placement, models, errors).
- ControlNode de un dominio: namespace, catálogo, registro de DataNodes,
  ciclo de vida de objetos (PENDING → COMMITTED → DELETED), detección de
  caída de nodos, autenticación JWT, aislamiento por usuario, API REST
  documentada con OpenAPI.
- DataNode (worker): almacenamiento en disco con verificación por SHA-256,
  cifrado AES-GCM opcional por bloque, API REST para `PUT/GET/DELETE
  /blocks/{id}`, operación `/blocks/{id}/replicate` para copia peer-to-peer,
  registro contra el ControlNode y loops de heartbeat y block report.
  Detecta corrupción y bloques huérfanos.
- **Cliente SDK + CLI (`client/`):** `register`, `login`, `mkdir`, `rmdir`,
  `ls`, `put`, `get`, `stat`, `delete`, `grant`, `revoke`, `list_acls`.
  Nunca pasa bytes por el ControlNode; sube/baja bloques directo a los
  DataNodes. Si una réplica falla, prueba la siguiente y reporta el fallo.
- **Re-replicación:** `RereplicationService` detecta bloques con factor de
  réplica bajo, elige un nodo fuente CONFIRMED, elige un destino, y pide
  copia P2P vía `/blocks/{id}/replicate`. Se dispara periódicamente
  (`RereplicationTask`) o on-demand (`POST /admin/rereplicate`).
- **ACLs granulares por prefijo:** `/acl/grant`, `/acl/revoke`, `/acl/list`.
  Permisos `READ`, `WRITE`, `DELETE`. Resolución por *longest-prefix match*.
- **Federación entre dominios:** registro de peers (`/federation/peers`),
  propagador de eventos (`FederationPropagator`), lookup remoto de objetos
  (`/federation/objects?key=&owner_username=`). `GET /objects?domain=X`
  hace proxy al dominio X y devuelve ubicaciones de los DataNodes de ese
  dominio para que el cliente baje los bloques directo.
- **Replicación del ControlNode (leader-follower):** `DFSHA_ROLE=LEADER|FOLLOWER`,
  snapshot completo (`GET /replication/snapshot`), `FollowerSyncTask` que
  descarga y aplica el snapshot periódicamente. Followers rechazan escrituras
  con `503` y redirigen al leader. Promoción manual vía reconfiguración.
- **Cifrado en reposo:** AES-GCM opcional por bloque en el DataNode
  (`DFSHA_DN_ENCRYPTION_KEY`).

**Fuera del alcance:**
- Elección de líder automática (Raft/Paxos). Promoción es manual.
- Plano de datos en gRPC (es HTTP; el ControlNode nunca transporta bytes).
- mTLS entre nodos (hoy es `X-...-Key` compartida; adecuado para red de
  confianza o para la entrega académica).

---

## Requisitos

- Python 3.11 o superior.
- Windows, Linux o macOS.

---

## Cómo levantar el ControlNode

### 1. Instalar dependencias

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

En Linux/macOS:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Correr el servidor

```powershell
uvicorn controlnode.main:app --reload
```

El servidor queda disponible en `http://localhost:8000`. La documentación
interactiva de la API (OpenAPI/Swagger) está en `http://localhost:8000/docs`.

### 3. Configuración por variables de entorno

Los valores por defecto sirven para desarrollo. Para personalizar:

| Variable | Default | Descripción |
|---|---|---|
| `DFSHA_DOMAIN_ID` | `domain-local` | Identificador del dominio |
| `DFSHA_DB_URL` | `sqlite+aiosqlite:///./controlnode.db` | Cadena de conexión SQLAlchemy |
| `DFSHA_JWT_SECRET` | `dev-secret-change-me` | Secreto para firmar JWT (cambiar en producción) |
| `DFSHA_JWT_EXPIRE_SECONDS` | `86400` | Vigencia del token en segundos |
| `DFSHA_DATANODE_API_KEY` | `dev-datanode-key` | API key para autenticar DataNodes |
| `DFSHA_REPLICATION_FACTOR` | `3` | Réplicas por bloque |
| `DFSHA_BLOCK_SIZE_DEFAULT` | `67108864` | Tamaño de bloque por defecto (64 MiB) |
| `DFSHA_PENDING_TTL_SECONDS` | `3600` | TTL de objetos PENDING sin commit |
| `DFSHA_SUSPECT_AFTER` | `20` | Segundos sin heartbeat antes de SUSPECTED |
| `DFSHA_DEAD_AFTER` | `30` | Segundos sin heartbeat antes de DEAD |
| `DFSHA_REREP_INTERVAL` | `30` | Cadencia del loop de re-replicación |
| `DFSHA_FEDERATION_KEY` | `dev-federation-key` | API key compartida entre peers de federación |
| `DFSHA_FEDERATION_INTERVAL` | `10` | Cadencia del propagador de eventos a peers |
| `DFSHA_ROLE` | `LEADER` | `LEADER` o `FOLLOWER` |
| `DFSHA_LEADER_URL` | `http://localhost:8000` | URL del leader (solo followers) |
| `DFSHA_REPLICATION_KEY` | `dev-replication-key` | API key entre leader y followers |
| `DFSHA_REPLICATION_POLL` | `5` | Cadencia de pull de snapshots en followers |

---

## Cómo levantar un DataNode

En otra terminal (con el ControlNode ya corriendo en localhost:8000):

```powershell
$env:DFSHA_DN_NODE_ID = "dn-1"
$env:DFSHA_DN_DOMAIN_ID = "domain-local"
$env:DFSHA_DN_PORT = "9000"
$env:DFSHA_DN_PUBLIC_ADDRESS = "http://localhost:9000"
$env:DFSHA_DN_STORAGE = "./dn1_storage"
uvicorn datanode.main:app --port 9000
```

Para levantar varios DataNodes, basta con repetir el comando cambiando
`DFSHA_DN_NODE_ID`, `DFSHA_DN_PORT`, `DFSHA_DN_PUBLIC_ADDRESS` y
`DFSHA_DN_STORAGE`. El DataNode se registra automáticamente contra el
ControlNode al arrancar y después manda heartbeat y block report
periódicamente.

### Variables de entorno del DataNode

| Variable | Default | Descripción |
|---|---|---|
| `DFSHA_DN_NODE_ID` | `dn-local` | Identificador del DataNode |
| `DFSHA_DN_DOMAIN_ID` | `domain-local` | Dominio al que pertenece |
| `DFSHA_DN_HOST` | `0.0.0.0` | Interfaz de bind |
| `DFSHA_DN_PORT` | `9000` | Puerto HTTP del plano de datos |
| `DFSHA_DN_PUBLIC_ADDRESS` | `http://localhost:9000` | URL que se reporta al ControlNode |
| `DFSHA_DN_STORAGE` | `./dn_storage` | Directorio donde se guardan los bloques |
| `DFSHA_DN_CONTROLNODE_URL` | `http://localhost:8000` | ControlNode al que se registra |
| `DFSHA_DN_CONTROLNODE_KEY` | `dev-datanode-key` | API key compartida con el ControlNode |
| `DFSHA_DN_CAPACITY` | `10737418240` | Capacidad reportada en bytes (10 GiB) |
| `DFSHA_DN_HEARTBEAT` | `5` | Cadencia de heartbeat en segundos |
| `DFSHA_DN_BLOCK_REPORT` | `60` | Cadencia de block report en segundos |
| `DFSHA_DN_ENCRYPTION_KEY` | *(vacío)* | Clave hex de 32 bytes para cifrado en reposo (opcional) |

### Endpoints REST del DataNode

El cliente (y en replicación peer-to-peer, otros DataNodes) consume estos
endpoints. El ControlNode **nunca** llama al DataNode: el plano de datos y
el plano de control viven separados.

- `PUT /blocks/{block_id}?checksum=...` — subir bloque; body = bytes crudos.
  Si el checksum calculado no coincide, responde `400`.
- `GET /blocks/{block_id}` — descargar bloque. Verifica checksum y, si el
  bloque está corrupto, responde `410 GONE`.
- `DELETE /blocks/{block_id}` — borrar bloque. Idempotente.
- `GET /health` — liveness y bytes usados.
- `GET /inventory` — snapshot local del inventario (debug).

---

## Cómo correr las pruebas

```powershell
python -m pytest tests/ -v
```

Salida esperada: **110 tests pasando**:

| Suite | Tests |
|---|---|
| `tests/test_*.py` (dfshacore) | 24 |
| `tests/controlnode/` | 35 |
| `tests/datanode/` | 23 |
| `tests/client/` (SDK + re-replicación + ACL) | 19 |
| `tests/federation/` | 4 |
| `tests/replication/` (leader-follower) | 5 |

Para correr solo un subconjunto:

```powershell
python -m pytest tests/controlnode/ -v
python -m pytest tests/datanode/ -v
python -m pytest tests/client/ -v
python -m pytest tests/federation/ -v
python -m pytest tests/replication/ -v
python -m pytest tests/test_chunker.py -v       # solo particionamiento
```

---

## Cliente CLI

```powershell
python -m client.cli register alice secreta1234
python -m client.cli login    alice secreta1234
python -m client.cli mkdir    /fotos
python -m client.cli put      ./paisaje.jpg /fotos/paisaje.jpg
python -m client.cli ls       /fotos
python -m client.cli get      /fotos/paisaje.jpg ./bajada.jpg
python -m client.cli stat     <object_id>
python -m client.cli delete   <object_id>
```

Variables:

| Variable | Default | Descripción |
|---|---|---|
| `DFSHA_CN_URL` | `http://localhost:8000` | ControlNode al que conectar |
| `DFSHA_TOKEN` | *(archivo `~/.dfsha_token`)* | JWT (ignora el archivo si está) |

---

## Endpoints REST del ControlNode

Todos los endpoints (excepto `/auth/*` y `/datanodes/*`) requieren un JWT en el
header `Authorization: Bearer <token>`. Los endpoints de `/datanodes/*` usan la
API key en `X-DataNode-Key`.

**Ninguno transporta bytes de contenido** — el ControlNode maneja solo
metadatos y ubicaciones. Los bytes viajan directo entre el cliente y los
DataNodes.

### Autenticación
- `POST /auth/register` — crear usuario
- `POST /auth/login` — obtener JWT

### Namespace
- `GET /ns/ls?path=/fotos` — listar directorio
- `POST /ns/mkdir` — crear directorio
- `DELETE /ns/rmdir?path=...` — borrar directorio vacío

### Objetos
- `POST /objects/init` — iniciar carga: recibe manifiesto, devuelve dónde subir cada bloque
- `POST /objects/{id}/commit` — confirmar carga (transición PENDING → COMMITTED)
- `GET /objects?key=...` — resolver clave: devuelve ubicaciones de bloques
- `DELETE /objects/{id}` — borrado lógico
- `GET /objects/{id}/stat` — metadatos e historial de versiones
- `POST /objects/{id}/blocks/{bid}/report-failure` — reportar réplica caída

### DataNodes
- `POST /datanodes/register` — dar de alta un DataNode
- `POST /datanodes/{id}/heartbeat` — heartbeat periódico
- `POST /datanodes/{id}/blocks` — reporte completo de inventario

### ACLs
- `POST /acl/grant` — conceder READ/WRITE/DELETE a otro usuario sobre un prefijo
- `DELETE /acl/revoke?grantee_username=&path_prefix=` — revocar
- `GET /acl/list` — listar ACLs concedidas por el usuario actual
- `GET /objects?key=/foo&owner=alice` — leer objeto de otro usuario (requiere ACL READ)

### Admin
- `POST /admin/rereplicate` — disparar una pasada manual de re-replicación
- `GET /admin/events` — ver el log de eventos no propagados

### Federación entre dominios
- `POST /federation/peers` — registrar otro dominio como peer
- `DELETE /federation/peers/{domain_id}` — eliminar peer
- `GET /federation/peers` — listar peers conocidos
- `POST /federation/propagate` — pasada manual del propagador de eventos
- `POST /federation/events` *(peer-to-peer)* — recepción de eventos remotos
- `GET /federation/objects?key=&owner_username=` *(peer-to-peer)* — lookup remoto
- `GET /objects?key=/x&owner=alice&domain=domain-B` — leer un objeto en otro dominio

### Replicación del ControlNode
- `GET /replication/role` — rol actual (LEADER o FOLLOWER)
- `GET /replication/snapshot` *(leader only)* — snapshot completo del catálogo

---

## Documentos de diseño

- [algoritmo_particionamiento.md](algoritmo_particionamiento.md) —
  Diseño detallado del módulo `dfshacore`: modelos, algoritmo de colocación,
  lista de pruebas, decisiones propias y ambigüedades.
- [control_node_guia.md](control_node_guia.md) —
  Guía de implementación del ControlNode pensada para sustentación:
  qué hace cada pieza, por qué se diseñó así, casos borde, preguntas
  frecuentes de defensa.
- [sustentacion.md](sustentacion.md) —
  Guía rápida para la defensa oral: por cada módulo, qué decisión tomamos
  y qué alternativa descartamos y por qué. Incluye tabla de preguntas
  probables con respuestas cortas.
