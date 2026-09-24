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
                          registro de DataNodes + orquestación.
tests/                    Pruebas de dfshacore y del ControlNode.
algoritmo_particionamiento.md   Diseño del módulo dfshacore.
control_node_guia.md      Guía de implementación del ControlNode para
                          sustentación.
ProyectoDFShaTopTel.docx  Especificación del proyecto.
```

---

## Alcance de esta entrega (Hito 2)

**Dentro del alcance:**
- Módulo `dfshacore` completo (chunker, assembler, placement, models, errors).
- ControlNode de un dominio: namespace, catálogo, registro de DataNodes,
  ciclo de vida de objetos (PENDING → COMMITTED → DELETED), detección de
  caída de nodos, autenticación JWT, aislamiento por usuario, API REST
  documentada con OpenAPI.

**Fuera del alcance de esta entrega:**
- Federación entre dominios (Hito 3).
- Replicación del ControlNode, elección de líder y failover (Hito 3).
- Implementación del DataNode (otro integrante del equipo).
- Plano de datos en gRPC (el ControlNode nunca transporta bytes).
- Cifrado en reposo, mTLS entre nodos, ACL granular por prefijo.

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

---

## Cómo correr las pruebas

```powershell
python -m pytest tests/ -v
```

Salida esperada: **59 tests pasando** (24 de `dfshacore` + 35 del ControlNode).

Para correr solo un subconjunto:

```powershell
python -m pytest tests/controlnode/ -v         # solo ControlNode
python -m pytest tests/test_chunker.py -v       # solo particionamiento
```

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

---

## Documentos de diseño

- [algoritmo_particionamiento.md](algoritmo_particionamiento.md) —
  Diseño detallado del módulo `dfshacore`: modelos, algoritmo de colocación,
  lista de pruebas, decisiones propias y ambigüedades.
- [control_node_guia.md](control_node_guia.md) —
  Guía de implementación del ControlNode pensada para sustentación:
  qué hace cada pieza, por qué se diseñó así, casos borde, preguntas
  frecuentes de defensa.
