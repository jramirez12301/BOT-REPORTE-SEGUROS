# DOCUMENTACION BORRADOR

## 1) Contexto y alcance

Este documento define un estandar tecnico reutilizable para auditar ejecuciones de automatizaciones Python en la empresa.

Objetivo del estandar:
- Centralizar trazabilidad en base de datos.
- Evitar dependencia exclusiva de logs locales.
- Mantener un contrato uniforme entre equipos.
- Permitir que cualquier automatizacion nueva use la misma base tecnica.

Alcance de este borrador:
- `core/db_utils.py`
- `core/audit_logger.py`
- `core/template_automatizacion.py`

Queda fuera de alcance en este paso:
- Logica funcional especifica de cada automatizacion (ETL, API, scraping, etc.).
- Dashboards/BI de auditoria.

---

## 2) Reglas de negocio consolidadas

1. **No rollback funcional**: si una automatizacion ya aplico cambios exitosos, esos cambios se mantienen.
2. **Persistencia de auditoria al final**: se ejecuta en `finally`.
3. **Transaccion de auditoria desde Python**:
   - 1 `INSERT` en `EJECUCION`
   - N `INSERT` en `LOG_PROCESOS` (chunking)
4. **Campo de detalle oficial**: `LOG_PROCESOS.detalle`.
5. **Estados permitidos**: `EXITO`, `ADVERTENCIA`, `ERROR`.
6. **Tags obligatorios**: `[INICIO]`, `[RESUMEN]`, `[FIN]`.
7. **Tags opcionales (si hay contenido)**: `[INFO]`, `[INSERT]`, `[UPDATE]`, `[DELETE]`, `[WARNING]`, `[ERROR]`.
8. **Zona horaria**: Argentina por defecto (`America/Argentina/Buenos_Aires`), configurable.
9. **Agnostico al dominio**: no usar conceptos de una automatizacion puntual (ej. prereserva).

---

## 3) Arquitectura objetivo por automatizacion

Estructura base:

```text
/automatizaciones
├── /core
│   ├── __init__.py
│   ├── db_utils.py
│   ├── audit_logger.py
│   └── template_automatizacion.py
├── /etl_seguros
└── /aut_stock_web
```

### Responsabilidades

- `core/db_utils.py`
  - Cargar variables de entorno con `python-dotenv`.
  - Exponer factories de conexion (`db_conn_factory`) para auditoria.
  - Soportar entornos (`TEST`, `PROD`) sin cambiar codigo de negocio.

- `core/audit_logger.py`
  - Capturar eventos en memoria.
  - Construir `resumen` y `detalle` con contrato de tags.
  - Particionar detalle por longitud maxima.
  - Persistir `EJECUCION` + `LOG_PROCESOS` dentro de una transaccion.

- `core/template_automatizacion.py`
  - Boilerplate oficial para nuevos desarrollos.
  - Patron `try/except/finally` + `AuditLogger`.

---

## 4) Especificacion tecnica de `AuditLogger`

### 4.1 Objetivo

Encapsular toda la auditoria de ejecucion para que la automatizacion solo reporte eventos, sin armar strings ni SQL de auditoria manualmente.

### 4.2 Flujo de vida

1. `__init__`
2. `start()`
3. Metodos de tracking (`record_*`)
4. `mark_error()` si aplica
5. `persist()` siempre en `finally`

### 4.3 API propuesta (borrador)

- `start()`
- `set_metric(name: str, value: int)`
- `record_info(message: str)`
- `record_insert(entity_ids: list[str])`
- `record_update(entity_id: str, changes: list[str])`
- `record_delete(entity_ids: list[str])`
- `record_warning(message: str)`
- `record_error(message: str)`
- `mark_success()`
- `mark_warning()`
- `mark_error(message: str, exc: Exception | None = None)`
- `build_summary() -> str`
- `build_detail_blocks() -> list[str]` (aplica chunking)
- `persist() -> int` (retorna `id_ejecucion`)

### 4.4 Estado interno minimo

- Identidad de proceso: `id_proceso` (o `process_code` para resolverlo)
- Timestamps: `fecha_inicio`, `fecha_fin`
- Estado final: `EXITO|ADVERTENCIA|ERROR`
- Metricas: `inserts`, `updates`, `deletes`, `warnings`, `errors` + extras (`extraidos`, `procesados`, etc.)
- Acumuladores de detalle por tag
- Control de idempotencia: evitar doble `persist()`

---

## 5) Contrato de tags y orden de salida

Orden del detalle (si existe contenido en cada seccion):

1. `[INICIO]` (obligatorio)
2. `[RESUMEN]` (obligatorio)
3. `[INFO]` (opcional)
4. `[INSERT]` (opcional)
5. `[UPDATE]` (opcional)
6. `[DELETE]` (opcional)
7. `[WARNING]` (opcional)
8. `[ERROR]` (opcional)
9. `[FIN]` (obligatorio)

Regla de legibilidad:
- Si no hay datos para un tag opcional, se omite.

Ejemplo de detalle (una sola pieza):

```text
[INICIO] 2026-04-15 09:00:00
[RESUMEN] extraidos=120, procesados=100, inserts=80, updates=20, deletes=0, warnings=1, errors=0
[INFO] Fuente=API_X, batch=2026-04-15T09:00
[INSERT] ID_1001, ID_1002, ID_1003
[UPDATE] ID_2001 (2 campos), ID_2002 (1 campo)
- ID_2001.precio: '100' -> '95'
- ID_2001.estado: 'PENDIENTE' -> 'OK'
[WARNING] ID_7788: timeout de lectura, reintento exitoso
[FIN] ADVERTENCIA
```

---

## 6) Estrategia de particionado (chunking)

### 6.1 Motivacion

Evitar registros de log gigantes para mantener performance y legibilidad en MySQL.

### 6.2 Regla

- Definir `max_chunk_chars` configurable (recomendado: `10_000`).
- El detalle completo se arma en memoria.
- Luego se divide en bloques de hasta `max_chunk_chars`, preservando lineas completas cuando sea posible.
- Cada bloque se inserta como una fila en `LOG_PROCESOS` con el mismo `id_ejecucion`.

### 6.3 Politica para listas muy largas

- Si una lista de IDs supera 50 elementos, se puede:
  - agrupar por bloques en multiples lineas, o
  - resumir parcialmente por linea y continuar en el siguiente bloque.
- El objetivo es **no perder informacion**, solo distribuirla en varios registros.

### 6.4 Convencion recomendada para continuidad

- Bloque 1: incluir `[INICIO]` y `[RESUMEN]`.
- Bloques intermedios: contenido de continuidad.
- Ultimo bloque: incluir `[FIN]`.

---

## 7) Modelo de persistencia transaccional

Persistencia en `finally`:

1. Abrir conexion via `db_conn_factory()`.
2. Iniciar transaccion (`autocommit=False`).
3. Insertar en `EJECUCION`:
   - `id_proceso`, `fecha_inicio`, `fecha_fin`, `resumen`, `estado`.
4. Obtener `id_ejecucion` generado.
5. Insertar N filas en `LOG_PROCESOS`:
   - (`id_ejecucion`, `detalle`) por cada bloque.
6. `commit`.
7. Si algo falla: `rollback` de esta transaccion de auditoria.

Notas:
- Esta transaccion es de auditoria, no deshace cambios funcionales externos.
- Si falla auditoria, registrar error tecnico local para soporte.

---

## 8) Diseno de `db_utils.py`

### 8.1 Objetivo

Centralizar configuracion de conexiones y evitar repetir lectura de `.env` en cada automatizacion.

### 8.2 Principios

- Carga unica de `.env` al importar modulo (o bajo funcion de inicializacion).
- Variables por entorno, por ejemplo:
  - `APP_ENV=TEST|PROD`
  - `AUDIT_DB_HOST_TEST`, `AUDIT_DB_USER_TEST`, ...
  - `AUDIT_DB_HOST_PROD`, `AUDIT_DB_USER_PROD`, ...
- Fallback opcional a variables sin sufijo (`AUDIT_DB_HOST`, etc.).

### 8.3 API sugerida

- `get_app_env() -> str`
- `build_audit_db_config(env: str | None = None) -> dict`
- `create_audit_connection(env: str | None = None)`
- `get_audit_db_connection_factory(env: str | None = None)`

---

## 9) Plantilla oficial: `template_automatizacion.py`

Objetivo de la plantilla:
- Acelerar inicio de nuevas automatizaciones.
- Reducir errores de implementacion.
- Forzar mismo patron de auditoria en toda la empresa.

Estructura esperada de flujo:

```python
audit = AuditLogger(...)
try:
    audit.start()
    # logica principal
    # record_insert / record_update / record_warning / record_error
    # mark_success o mark_warning
except Exception as e:
    audit.mark_error("Error fatal", exc=e)
    raise
finally:
    audit.persist()
```

Guias para desarrolladores:
- Identificar el ID principal de su entidad y reportarlo en `record_insert`/`record_update`/`record_delete`.
- En updates, reportar resumen y detalle por campo (`old -> new`) en `changes`.
- No construir manualmente el string final de auditoria.

---

## 10) Ejemplo de ejecucion (multi-bloque)

Escenario:
- 1,200 inserts
- 80 updates (con detalle por campo)
- 2 warnings
- 1 error no fatal
- `max_chunk_chars=10_000`

Resultado:
- `EJECUCION`:
  - `estado=ADVERTENCIA`
  - `resumen="inserts=1200, updates=80, deletes=0, warnings=2, errors=1"`
- `LOG_PROCESOS`:
  - bloque 1: `[INICIO]`, `[RESUMEN]`, comienzo `[INSERT]`
  - bloques intermedios: continuidad de `[INSERT]` y `[UPDATE]` + detalle de campos
  - bloque final: `[WARNING]`, `[ERROR]`, `[FIN] ADVERTENCIA`

---

## 11) Decisiones abiertas (para cerrar antes de implementacion)

1. Tamano final de `LOG_PROCESOS.detalle` en DDL (`TEXT` o `MEDIUMTEXT`).
2. Politica exacta de troceo:
   - por linea,
   - por palabra,
   - o hibrida (recomendada).
3. Si `persist()` falla, definir politica operativa:
   - solo log local,
   - reintento,
   - cola de reproceso.
4. Estandar de nomenclatura de `PROCESOS` (`codigo_proceso` unico recomendado).

---

## 12) Proximos pasos

1. Implementar `core/db_utils.py`.
2. Implementar `core/audit_logger.py` con tests unitarios basicos.
3. Crear `core/template_automatizacion.py`.
4. Integrar primero en `etl_seguros` como piloto.
5. Ajustar borrador a version estable luego del piloto.
