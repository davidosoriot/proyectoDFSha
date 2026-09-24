# Guía de implementación del ControlNode — DFSha

> **Para quién es esta guía:** cualquier integrante del equipo que vaya a sustentar el
> proyecto y necesite explicar con claridad qué hace el ControlNode, por qué se diseñó
> así y qué decisiones técnicas hay detrás de cada parte.

---

## ¿Qué es el ControlNode y por qué existe?

En DFSha los archivos no viven en una sola máquina. Se parten en bloques que se
distribuyen entre varios **DataNodes** (nodos de almacenamiento). Alguien tiene que
saber dónde está cada bloque, quién tiene permiso de leerlo y si los nodos siguen vivos.
Ese alguien es el **ControlNode**.

El ControlNode es el **cerebro del dominio**: no guarda bytes de archivos, solo guarda
*información sobre* esos bytes. A esto en arquitecturas distribuidas se le llama
**plano de control** (vs. plano de datos). Esta separación es fundamental:

- El plano de control (ControlNode) maneja metadatos y decisiones → tráfico pequeño.
- El plano de datos (DataNodes) maneja los bytes reales → puede escalar horizontalmente.

Si el ControlNode manejara el contenido, su ancho de banda sería el techo de todo el
sistema. Al separar los dos planos, añadir más DataNodes aumenta la capacidad sin
tocar el ControlNode.

---

## Arquitectura general del ControlNode

```
  Cliente
    │
    │  REST / HTTPS  (solo metadatos, nunca bytes de archivos)
    ▼
┌─────────────────────────────────────────┐
│              ControlNode                │
│                                         │
│  API REST (FastAPI)                     │
│     │                                   │
│  Servicios de negocio (core/)           │
│     │                                   │
│  Repositorio de datos (SQLite)          │
│     │                                   │
│  Módulo de colocación (dfshacore)       │
└─────────────────────────────────────────┘
    │
    │  El cliente contacta directamente a los DataNodes para subir/bajar bloques
    ▼
DataNode 1 · DataNode 2 · DataNode 3
```

**La invariante más importante:** el ControlNode **nunca toca un byte de contenido**.
No existe ningún endpoint que reciba o devuelva el contenido de un bloque. Si alguien
pregunta "¿por qué el ControlNode no descarga el archivo por mí?", la respuesta es:
porque haría que todo el tráfico pasara por un solo punto y limitaría el rendimiento
de todo el sistema.

---

## Estructura de archivos y qué hace cada módulo

```
controlnode/
├── main.py              Arranca el servidor FastAPI y registra las tareas de fondo
├── config.py            Configuración centralizada (domain_id, JWT secret, timeouts)
│
├── api/                 Capa HTTP — solo valida que el request tenga buen formato
│   ├── deps.py          Dependencias: extrae el usuario del JWT, abre la DB
│   ├── auth.py          Endpoints de registro e inicio de sesión
│   ├── namespace.py     Endpoints del sistema de archivos (ls, mkdir, rmdir)
│   ├── objects.py       Endpoints del ciclo de vida de objetos (subir, confirmar, leer, borrar)
│   └── datanodes.py     Endpoints que usan los DataNodes para reportar su estado
│
├── core/                Lógica de negocio — no sabe nada de HTTP ni de SQL directamente
│   ├── catalog.py       ÚNICO punto de mutación del catálogo: toda escritura pasa por aquí
│   ├── namespace_svc.py Lógica de directorios virtuales
│   ├── placement_svc.py Adapta la información del catálogo al módulo de colocación
│   ├── auth_svc.py      Registro, hash de contraseñas, emisión y validación de JWT
│   ├── health_monitor.py Tarea de fondo que detecta nodos caídos
│   └── federation.py   Preparado para conectar dominios en el Hito 3 (hoy siempre "local")
│
├── db/
│   ├── connection.py    Fábrica de conexiones (SQLite hoy, Postgres mañana)
│   ├── models.py        Tablas ORM con SQLAlchemy
│   └── repository.py   Todas las consultas SQL en un solo lugar
│
├── contracts/
│   └── datanode.py     Interfaz abstracta del DataNode + versión falsa para tests
│
└── tests/              Pruebas automáticas de cada módulo
```

**Por qué esta separación en capas:**

| Capa | Qué sabe | Qué no sabe |
|---|---|---|
| `api/` | HTTP, JSON, códigos de estado | SQL, lógica de negocio |
| `core/` | Reglas del negocio | HTTP, SQL específico |
| `db/` | SQL, SQLite | HTTP, reglas del negocio |

Esto se llama **arquitectura en capas** o *layered architecture*. La ventaja es que
se puede cambiar una capa sin tocar las otras. Por ejemplo, para cambiar de SQLite a
PostgreSQL solo se modifica `connection.py`; la lógica de negocio no cambia.

---

## El modelo de datos: qué guarda el ControlNode

### Diagrama de relaciones

```
users ──────< objects ──────< blocks ──────< replicas >────── datanodes
  │
  └──────< directories
```

### Tabla por tabla

#### `users` — los usuarios del sistema
Guarda el username y el hash de la contraseña (nunca la contraseña en texto plano).
El campo `domain_id` identifica a qué dominio pertenece el usuario, necesario para
cuando se conecten dos dominios en el Hito 3.

#### `directories` — el sistema de archivos virtual
Los "carpetas" no existen físicamente en ningún nodo. Son filas en esta tabla con un
`full_path` como `/fotos/vacaciones`. Cuando el usuario hace `ls /fotos`, el ControlNode
busca en esta tabla, no en los DataNodes.

**Por qué directorios explícitos y no inferidos de los nombres de archivo:**
Si solo guardáramos el nombre del archivo `/fotos/vacaciones/video.mp4` y quisiéramos
hacer `ls /fotos`, tendríamos que escanear todos los objetos del usuario buscando el
prefijo `/fotos/`. Con directorios explícitos la consulta es O(1) y `mkdir` de
carpetas vacías tiene sentido.

#### `objects` — los metadatos de cada archivo subido
Un "objeto" en DFSha es un archivo inmutable. Si el usuario vuelve a subir el mismo
archivo, se crea una nueva fila con `version=2`, la anterior no se modifica. Esto
es el modelo **WORM** (*Write Once, Read Many*).

El campo más importante es `status`, que puede ser:
- `PENDING`: el archivo se está subiendo, no se puede leer
- `COMMITTED`: el archivo está completo y disponible
- `DELETED`: el archivo fue borrado lógicamente

#### `blocks` — la lista de fragmentos de cada objeto
Un archivo de 200 MB se parte en 4 bloques de 50 MB cada uno (con el tamaño de
bloque configurado). Esta tabla guarda el `checksum` SHA-256 de cada bloque para
poder detectar corrupción al momento de leer.

#### `replicas` — dónde está cada copia de cada bloque
Cada bloque tiene 3 réplicas en 3 DataNodes distintos. Esta tabla sabe exactamente
en qué DataNode está cada réplica y si fue confirmada o se perdió.

Estados de una réplica:
- `ASSIGNED`: el ControlNode decidió que va en ese nodo, pero el DataNode todavía no confirmó
- `CONFIRMED`: el DataNode reportó que recibió el bloque
- `LOST`: el DataNode cayó o el cliente reportó que la réplica falló

#### `datanodes` — registro de los nodos de almacenamiento
Guarda el estado de cada DataNode: cuánto espacio libre tiene, qué tan cargado está
y cuándo fue su último heartbeat. Con esto el ControlNode decide dónde colocar las
próximas réplicas.

#### `events` — el log de todo lo que ocurre (costura para Hito 3)
Cada vez que algo cambia en el catálogo (un objeto se commit, un nodo cae, una
réplica se pierde), se escribe un evento en esta tabla. Hoy nadie consume estos
eventos, pero en el Hito 3 este log será la base para sincronizar dos dominios y
para replicar el ControlNode. **Todo cambio del catálogo pasa primero por aquí.**

---

## El ciclo de vida de un objeto: cómo se sube un archivo

Este es el flujo completo de un `put`. Es importante entenderlo bien para la sustentación.

```
Cliente                    ControlNode                    DataNodes
   │                            │                             │
   │── POST /objects/init ──────►│                             │
   │   (manifiesto con bloques)  │  1. Valida que hay ≥ 3 nodos│
   │                             │  2. Llama a place_object()  │
   │                             │  3. Crea objeto en PENDING  │
   │                             │  4. Guarda réplicas ASSIGNED│
   │◄── {placements} ────────────│                             │
   │                             │                             │
   │── bytes del bloque 0 ───────────────────────────────────►│ DataNode A
   │                             │                DataNode A──►│ DataNode B (cadena)
   │                             │                DataNode B──►│ DataNode C
   │                             │                             │
   │                             │◄── POST /blocks (reporte) ──│ DataNode A, B, C
   │                             │  réplicas pasan a CONFIRMED │
   │                             │                             │
   │── POST /objects/{id}/commit►│                             │
   │                             │  5. Verifica: ¿todas las    │
   │                             │     réplicas confirmadas?   │
   │                             │  6. Asigna versión          │
   │                             │  7. Status → COMMITTED      │
   │◄── {version, COMMITTED} ────│                             │
```

**Puntos clave para la sustentación:**
1. El cliente nunca envía el contenido al ControlNode, solo el manifiesto (metadatos).
2. La replicación es en **cadena** (A→B→C), no en estrella (cliente→A, cliente→B, cliente→C). Esto reduce el ancho de banda del cliente a 1x en vez de 3x.
3. El objeto solo pasa a COMMITTED cuando **todas** las réplicas confirmaron. Garantiza que nunca se lee un archivo incompleto.

---

## La máquina de estados del objeto

```
                          commit (todas réplicas OK)
   POST /init         ┌─────────────────────────────► COMMITTED
       │              │                                    │
       ▼              │                                    │ DELETE
    PENDING ──────────┘                                    ▼
       │                                                DELETED
       │ TTL expirado (1 hora sin commit)
       ▼
  (se borra del catálogo)
```

**Por qué esta máquina de estados:**
El estado PENDING resuelve el problema de consistencia más importante: ¿qué pasa si
el cliente se cae a mitad de una subida? El archivo queda PENDING pero nunca COMMITTED,
así que es invisible para otros usuarios. Después de 1 hora el ControlNode lo limpia.
Sin este estado, un usuario podría ver un archivo parcialmente subido.

### Casos borde importantes

**¿Qué pasa si el cliente sube un archivo y nunca llama a commit?**
El objeto queda PENDING. La tarea `PendingCleanupTask` lo detecta cuando
`expires_at = created_at + 1 hora` haya pasado. Lo borra del catálogo y le dice al
DataNode en el próximo reporte de bloques que esos bloques son huérfanos y puede
eliminarlos.

**¿Qué pasa si dos clientes suben el mismo archivo al mismo tiempo?**
Ambos obtienen un `object_id` distinto y quedan PENDING. El que commita primero
obtiene `version=1`, el segundo obtiene `version=2`. Ambos son COMMITTED y coexisten.
`ls` muestra solo la última versión. Esto es **consistente** con el modelo WORM.

**¿Qué pasa si se borra un archivo mientras otro cliente lo está leyendo?**
El borrado es **lógico**: cambia `status=DELETED` en la tabla pero los bytes en los
DataNodes siguen ahí. El cliente que ya tenía la lista de ubicaciones de los bloques
puede completar su descarga sin interrupciones. La liberación de espacio en los
DataNodes es diferida. Esto es posible precisamente porque el ControlNode no es
intermediario del contenido.

**¿Qué pasa si el commit llega pero faltan réplicas?**
El ControlNode responde 409 con el detalle de qué bloques tienen réplicas sin confirmar.
El cliente puede reintentar con backoff mientras espera que los DataNodes terminen
de replicar en cadena.

---

## Autenticación y aislamiento entre usuarios

Se usa **JWT (JSON Web Token)** para autenticación. El flujo es:

```
1. POST /auth/login  →  ControlNode verifica usuario/contraseña
2. ControlNode emite un token JWT firmado con su clave secreta
3. Todas las peticiones siguientes llevan el token en el header: Authorization: Bearer <token>
4. El ControlNode verifica la firma del token sin necesidad de consultar la DB
```

**Por qué JWT en vez de sesiones en base de datos:**
Los tokens JWT son autocontenidos — llevan el `user_id` dentro del propio token
firmado. El ControlNode puede verificar la identidad del usuario sin hacer una
consulta SQL en cada petición. Esto es escalable y sin estado.

**Aislamiento:** cada query al catálogo filtra siempre por `owner_id = current_user.user_id`.
Un usuario nunca puede ver ni modificar objetos de otro, porque la condición de
propiedad está en todas las consultas del repositorio.

---

## Detección de caída de DataNodes

Los DataNodes envían un **heartbeat** cada 5 segundos al ControlNode. Si un nodo
deja de enviar heartbeats, el ControlNode lo detecta y actúa.

```
Tiempo sin heartbeat    Estado del nodo    Acción
────────────────────    ───────────────    ──────
< 20 segundos           ALIVE              nada
20 - 30 segundos        SUSPECTED          deja de recibir nuevas asignaciones
> 30 segundos           DEAD               réplicas marcadas LOST, evento disparado
```

**La tarea `HealthMonitorTask` corre cada 5 segundos en segundo plano** dentro del
mismo proceso del ControlNode. Revisa todos los nodos ALIVE y SUSPECTED y aplica
la lógica de transición de estado.

**¿Por qué 30 segundos (6 heartbeats)?** Para tolerar jitter de red y picos de carga
sin declarar un nodo caído precipitadamente. 6 heartbeats perdidos en fila son una
señal mucho más confiable que 1 ó 2 perdidos.

**¿Qué pasa si un nodo "muerto" reaparece?**
El primer heartbeat que llega lo vuelve a marcar ALIVE. En el siguiente reporte de
bloques, el ControlNode compara su inventario con el del nodo:
- Si un bloque ya fue re-replicado en otros nodos → le dice al DataNode que lo borre.
- Si un bloque sigue siendo necesario → se marca de nuevo como CONFIRMED.

---

## Modelo de concurrencia

FastAPI usa **asyncio** — un solo hilo del sistema operativo maneja miles de peticiones
concurrentes alternando entre ellas mientras esperan I/O (base de datos, red). No es
paralelismo real, es concurrencia cooperativa.

**Secciones críticas** (puntos donde dos peticiones concurrentes podrían interferir):

| Operación | Riesgo | Solución |
|---|---|---|
| Dos `commit` del mismo objeto a la vez | Ambos podrían asignar `version=1` | Transacción DB con `SELECT ... FOR UPDATE` |
| Dos `mkdir` del mismo path a la vez | Ambos pasarían el check de existencia | Constraint `UNIQUE` en la DB — la segunda falla con `IntegrityError` |
| Heartbeat llega mientras se marca DEAD | Inconsistencia de estado | Ambas operaciones son `UPDATE` atómicos; la DB serializa el acceso |

**Nota sobre SQLite:** SQLite en modo WAL (*Write-Ahead Log*) permite lecturas concurrentes
pero serializa escrituras. Para las pruebas de carga del proyecto (20 clientes
concurrentes) es suficiente. Para producción real se migraría a PostgreSQL, pero
la capa de repositorio hace que ese cambio sea mínimo.

---

## Las tres costuras para el Hito 3

El diseño deja tres puntos preparados para el siguiente hito sin implementarlos todavía.

### Costura 1: El log de eventos (para replicación del ControlNode y federación)

Toda mutación del catálogo pasa por un único método:

```python
class CatalogService:
    async def apply(self, event_type, entity_id, payload):
        # 1. Escribe el evento en la tabla events
        # 2. Aplica la mutación (UPDATE, INSERT, DELETE)
        # Todo en una sola transacción atómica
```

Hoy los eventos se guardan pero nadie los consume. En el Hito 3, un componente
leerá `SELECT * FROM events WHERE propagated=0` y los enviará al otro dominio o a
los seguidores del ControlNode replicado. El beneficio de tener esto ya: las
mutaciones del catálogo son trazables, ordenadas y reproducibles.

### Costura 2: `domain_id` en todas las entidades

Cada fila en cada tabla lleva `domain_id`. Hoy siempre vale lo mismo (el dominio
propio). En el Hito 3, cuando lleguen objetos del dominio del compañero, su
`domain_id` distinto los distingue en las mismas tablas sin cambiar el esquema.

### Costura 3: El `FederationResolver`

```python
class FederationResolver:
    async def resolve(self, object_key, user_id):
        return Resolution(location="local")   # hoy siempre
```

Antes de buscar un objeto en el catálogo local, el handler llama a este resolver.
Hoy siempre responde "local". En el Hito 3 puede responder "remote, pregúntale a
este endpoint". El handler ya está escrito para manejar ambas respuestas.

---

## El contrato con el DataNode

El ControlNode no implementa el DataNode (lo hace otro integrante del equipo), pero
define exactamente qué debe hacer:

```python
class DataNodeClient(ABC):             # interfaz abstracta
    async def store_block(...)         # guardar un bloque
    async def fetch_block(...)         # servir un bloque
    async def delete_block(...)        # borrar un bloque huérfano
```

Para las pruebas automáticas existe `InMemoryDataNodeClient`: un DataNode falso que
guarda los bloques en un diccionario en memoria. Esto permite probar todo el
ControlNode sin que el DataNode real exista todavía.

---

## Resumen de endpoints REST

> Ningún endpoint transporta contenido de bloques. Solo metadatos.

| Grupo | Método | Ruta | Para qué sirve |
|---|---|---|---|
| Auth | POST | `/auth/register` | Crear cuenta de usuario |
| Auth | POST | `/auth/login` | Obtener token JWT |
| Namespace | GET | `/ns/ls` | Listar directorio |
| Namespace | POST | `/ns/mkdir` | Crear directorio |
| Namespace | DELETE | `/ns/rmdir` | Borrar directorio vacío |
| Objetos | POST | `/objects/init` | Iniciar subida: recibe manifiesto, devuelve dónde subir cada bloque |
| Objetos | POST | `/objects/{id}/commit` | Confirmar que la subida terminó → objeto pasa a COMMITTED |
| Objetos | GET | `/objects` | Resolver un objeto por clave: devuelve ubicaciones de bloques para descargar |
| Objetos | DELETE | `/objects/{id}` | Borrado lógico |
| Objetos | GET | `/objects/{id}/stat` | Metadatos e historial de versiones |
| Objetos | POST | `/objects/{id}/blocks/{bid}/report-failure` | Cliente reporta que una réplica falló |
| DataNodes | POST | `/datanodes/register` | DataNode se da de alta |
| DataNodes | POST | `/datanodes/{id}/heartbeat` | DataNode reporta que sigue vivo |
| DataNodes | POST | `/datanodes/{id}/blocks` | DataNode reporta su inventario completo de bloques |

---

## Preguntas frecuentes en una sustentación

**¿Por qué el ControlNode usa SQLite y no una base de datos distribuida?**
Porque en este hito hay un solo ControlNode por dominio. SQLite es simple, no
requiere infraestructura adicional y la capa de repositorio permite migrar a
PostgreSQL para el hito de producción. Usar una base de datos distribuida ahora
añadiría complejidad sin beneficio.

**¿Qué garantiza que nunca se lea un archivo corrupto?**
Tres niveles: (1) cada bloque tiene un checksum SHA-256 que el cliente verifica al
descargarlo, (2) un objeto solo pasa a COMMITTED cuando todas sus réplicas fueron
confirmadas por los DataNodes, (3) el `global_checksum` del objeto permite verificar
la integridad de la descarga completa.

**¿Qué pasa si el ControlNode se cae?**
En este hito, el servicio no está disponible hasta que el ControlNode vuelva (no hay
replicación del ControlNode todavía). Los datos en los DataNodes son durables; cuando
el ControlNode vuelve, los DataNodes reportan sus bloques y el catálogo se reconstruye.
La replicación del ControlNode es parte del Hito 3.

**¿Por qué el borrado es diferido?**
Marcar un bloque como borrado en el catálogo es O(1). Ir a cada DataNode a borrarlo
requiere comunicación de red con cada nodo, que puede estar caído. El borrado diferido
desacopla la respuesta inmediata al usuario de la limpieza eventual de espacio en los
nodos.

**¿Cómo escala el sistema si se añaden más DataNodes?**
El ControlNode lee el inventario de nodos de su propia base de datos. Añadir un nuevo
DataNode es registrarlo con `POST /datanodes/register`. Inmediatamente queda disponible
como destino de nuevas réplicas en las siguientes operaciones de `init`. No hay
redistribución ni reinicio.

**¿Por qué la replicación es en cadena y no en estrella?**
En estrella, el cliente enviaría el mismo bloque a los 3 DataNodes, usando 3x su
ancho de banda de subida. En cadena (A→B→C), el cliente envía el bloque una sola vez
a A, y A lo propaga a B y C usando el ancho de banda del servidor, que es mucho mayor.
El ancho de banda del cliente solo se usa 1 vez por bloque.
