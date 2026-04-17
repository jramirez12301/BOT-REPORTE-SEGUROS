# Memoria de fase - Planificacion de la sesion

## Proposito de esta memoria

Registrar las decisiones de planificacion aprobadas en la sesion para ejecutar cambios de forma controlada y por fases.

## Fases acordadas

### Fase 0 - Orden y seguridad inicial

- Mantener operativa actual sin romper `etl.py`.
- Ordenar higiene de repositorio (`.gitignore`, archivos locales sensibles y temporales).
- Consolidar documentacion de diseno en `docs/`.

### Fase 1 - Base compartida (`core`)

Crear capa reusable:

- `core/__init__.py`
- `core/db_utils.py`
- `core/audit_logger.py`
- `core/template_automatizacion.py`

Objetivo: desacoplar auditoria y conexion a DB de cada automatizacion puntual.

### Fase 2 - Encapsular automatizacion actual

Preparar estructura modular sin alterar reglas de negocio:

- Crear `automatizaciones/etl_seguros/`
- Mover `etl.py`, `dataset/`, `test/` a su carpeta de automatizacion.

Nota: `dataset/` y `test/` pertenecen al ETL actual por decision explicita.

### Fase 3 - Integracion de auditoria nueva

Refactor de la automatizacion piloto para usar `AuditLogger`:

- ciclo de vida: `start` -> tracking -> `mark_*` -> `persist` en `finally`
- persistencia transaccional final (`EJECUCION` + `LOG_PROCESOS`)
- chunking del campo detalle para volumen alto

### Fase 4 - Escalado a nuevas automatizaciones

Aplicar plantilla en nuevas carpetas, por ejemplo:

- `automatizaciones/aut_stock_web/`

Todas las automatizaciones nuevas deben iniciar desde `core/template_automatizacion.py`.

### Fase 5 - Calidad y operacion

- Tests unitarios de `core/audit_logger.py`.
- Estandar de ejecucion/documentacion por automatizacion.
- Scripts de ejecucion compartidos en `scripts/`.

## Reglas tecnicas principales cerradas

1. Tabla de detalle usa columna `detalle`.
2. Persistencia final con transaccion Python.
3. N inserts en `LOG_PROCESOS` permitidos por chunking.
4. Tags obligatorios: `[INICIO]`, `[RESUMEN]`, `[FIN]`.
5. Tags opcionales se omiten si no hay contenido.
6. Estados finales permitidos: `EXITO`, `ADVERTENCIA`, `ERROR`.
7. Zona horaria por defecto: `America/Argentina/Buenos_Aires`.

## Riesgos identificados y mitigacion prevista

- Logs demasiado largos -> chunking configurable (recomendado 10,000 chars).
- Ambiguedad de reglas -> memoria en docs y plantilla unica para equipos.
- Diferencias de entornos -> `db_utils.py` con factories por `APP_ENV`.
- Fallo de auditoria al cierre -> manejo de excepcion tecnica sin ocultar error funcional.

## Entregables comprometidos para la siguiente etapa de construccion

- `core/db_utils.py`
- `core/audit_logger.py`
- `core/template_automatizacion.py`

## Estado de avance de la sesion

### Fase 0 - COMPLETADA

Se realizo orden y seguridad inicial sin romper el flujo funcional actual.

Cambios aplicados:
- Se normalizaron nombres de memoria para agentes en `docs/memoria_agentes/`.
- Se elimino carpeta duplicada con espacios para evitar confusion de contexto.
- Se actualizo `.gitignore` para cubrir mejor secretos y temporales:
  - `.env.*` (manteniendo `!.env.example`)
  - `credentials.*.json` (manteniendo `!credentials.json.example`)
  - `*.log`, `logs/`
  - `**/watermark.json`
  - `**/__pycache__/`

### Fase 1 - COMPLETADA

Se creo la base compartida en `core/`.

Archivos creados:
- `core/__init__.py`
- `core/db_utils.py`
- `core/audit_logger.py`
- `core/template_automatizacion.py`

### Fase 2 - COMPLETADA

Se encapsulo la automatizacion actual en su carpeta dedicada.

Cambios aplicados:
- Se creo `automatizaciones/etl_seguros/`.
- Se movio `etl.py` a `automatizaciones/etl_seguros/etl.py`.
- Se movio `test/` a `automatizaciones/etl_seguros/test/`.
- Se movio `dataset/` a `automatizaciones/etl_seguros/dataset/` (movimiento por subcarpetas para evitar bloqueo del sistema de archivos).

Resultado:
- La raiz queda preparada para escalar a multiples automatizaciones.
- `dataset` y `test` quedaron correctamente asociados a `etl_seguros`.

### Fase 3 - COMPLETADA

Se integro la auditoria en `automatizaciones/etl_seguros/etl.py` y se agrego modo dual de ejecucion.

Cambios aplicados:
- Integracion de `AuditLogger` con `id_proceso=1`, tracking de metricas/eventos y `persist()` en `finally`.
- Modo dual por CLI con `argparse`:
  - Produccion por defecto.
  - Testing con `--testing`.
- Nuevos argumentos documentados en `--help`:
  - `--start-date YYYYMMDD` para carga historica en produccion.
  - `--reset-watermark` para testing no interactivo.
  - `--batch-size` y `--sleep-seconds` para control operativo de lotes.
  - `--dry-run` para simulacro sin escrituras reales.
- Produccion sin watermark y extraccion desde `Vista_Seguros`.
- Testing con watermark incremental y actualizacion solo al final exitoso.
- Proteccion de API de Google Sheets:
  - particionado de payload por lotes,
  - reintentos simples con backoff para 429/5xx,
  - espera entre lotes para reducir riesgo de throttling.
- Rutas robustas por ubicacion del script (incluye inyeccion de `sys.path` para importar `core`).
- Se mantuvieron logs locales en paralelo a la auditoria de base de datos.

## Proxima sesion recomendada

1. Validar ejecucion end-to-end con datos reales en ambos modos (`PRODUCCION` y `--testing`).
2. Ejecutar pruebas de carga para ajustar `--batch-size` y `--sleep-seconds` segun cuota real de Google API.
3. Confirmar en base de auditoria que se cumpla: 1 fila en `EJECUCION` y N filas en `LOG_PROCESOS` por chunking.
4. Iniciar Fase 4 con scaffold de `automatizaciones/aut_stock_web/` usando `core/template_automatizacion.py`.
5. Avanzar Fase 5 con tests unitarios para `core/audit_logger.py` y pruebas de smoke del ETL.

## Nuevas fases acordadas (Produccion SQL Server)

### Fase 3.1 - Conexion SQL Server para produccion

- Actualizar `core/db_utils.py` para soportar conexion a SQL Server con `pyodbc`.
- Incorporar variables de entorno de SQL Server (`SQLSERVER_HOST`, `SQLSERVER_PORT`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`, `SQLSERVER_DRIVER`).
- Mantener testing con la logica actual (MySQL + watermark).

### Fase 3.2 - Query productiva multi-sucursal (piloto TOP 50)

- Agregar constante `PROD_EXTRACT_QUERY` en `automatizaciones/etl_seguros/etl.py`.
- Implementar `UNION ALL` entre `ProyautMonti.dbo.Vista_Seguros` y `ProyautAuto.dbo.Vista_Seguros`.
- Inyectar columnas fijas de origen:
  - `'FORD' AS Sucursal_origen`
  - `'HYUNDAI' AS Sucursal_origen`
- Aplicar `ORDER BY fechaprereserva ASC` externo para orden determinista.

### Fase 3.3 - Adaptacion de schema y hoja

- Agregar `Sucursal_origen` a `SHEET_COLUMNS` (nuevo total: 28 columnas).
- Conservar regla actual:
  - hoja vacia => crea encabezado automaticamente,
  - encabezado existente invalido => error explicito.

### Fase 3.4 - Manejo de errores SQL y auditoria

- Envolver extraccion de produccion en `try/except` de errores operativos de SQL Server.
- Registrar el error en `AuditLogger` con mensaje claro para soporte.
- Mantener `persist()` en `finally` para garantizar trazabilidad.

### Fase 3.5 - Validacion operativa previa a cron

- Ejecutar pruebas en `--dry-run` con query piloto (TOP 50) sin escritura real en Sheets.
- Validar que la auditoria cumpla:
  - 1 fila en `EJECUCION`,
  - N filas en `LOG_PROCESOS` por chunking.
- Confirmar que la nueva columna `Sucursal_origen` se refleje correctamente en Google Sheets.
