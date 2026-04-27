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

Estado: COMPLETADA.

Implementado:
- `core/db_utils.py` ahora soporta SQL Server con `pyodbc`.
- Nuevas funciones: `build_sqlserver_config`, `create_sqlserver_connection`, `get_sqlserver_connection_factory`.
- `requirements.txt` actualizado con `pyodbc>=5.1.0`.
- `.env.example` actualizado con variables SQL Server adicionales (`SQLSERVER_DATABASE`, `SQLSERVER_DRIVER`, etc.).

### Fase 3.2 - Query productiva multi-sucursal (piloto TOP 50)

- Agregar constante `PROD_EXTRACT_QUERY` en `automatizaciones/etl_seguros/etl.py`.
- Implementar `UNION ALL` entre `ProyautMonti.dbo.Vista_Seguros` y `ProyautAuto.dbo.Vista_Seguros`.
- Inyectar columnas fijas de origen:
  - `'FORD' AS Sucursal_origen`
  - `'HYUNDAI' AS Sucursal_origen`
- Aplicar `ORDER BY fechaprereserva ASC` externo para orden determinista.

Estado: COMPLETADA.

Implementado:
- Se agrego constante `PROD_EXTRACT_QUERY` en `automatizaciones/etl_seguros/etl.py`.
- Incluye `UNION ALL` con `TOP 50` por sucursal (FORD/HYUNDAI).
- Incluye `ORDER BY fechaprereserva ASC` externo.

### Fase 3.3 - Adaptacion de schema y hoja

- Agregar `Sucursal_origen` a `SHEET_COLUMNS` (nuevo total: 28 columnas).
- Conservar regla actual:
  - hoja vacia => crea encabezado automaticamente,
  - encabezado existente invalido => error explicito.

Estado: COMPLETADA.

Implementado:
- Se agrego `Sucursal_origen` a `SHEET_COLUMNS` (28 columnas).
- Se adapto rango de filtro para que sea dinamico segun cantidad real de columnas (ya no queda fijo en `AA`).
- Se mantiene regla de encabezado: autocreacion solo en hoja vacia/fila 1 vacia y error si existe encabezado invalido.

### Fase 3.4 - Manejo de errores SQL y auditoria

- Envolver extraccion de produccion en `try/except` de errores operativos de SQL Server.
- Registrar el error en `AuditLogger` con mensaje claro para soporte.
- Mantener `persist()` en `finally` para garantizar trazabilidad.

Estado: COMPLETADA.

Implementado:
- En produccion, la extraccion SQL Server ahora captura `pyodbc.Error`.
- Se registra mensaje operativo claro en auditoria (`AuditLogger.record_error`) antes de propagar error.
- Se mantiene `persist()` en `finally` para trazabilidad aun en fallos.

### Fase 3.5 - Validacion operativa previa a cron

- Ejecutar pruebas en `--dry-run` con query piloto (TOP 50) sin escritura real en Sheets.
- Validar que la auditoria cumpla:
  - 1 fila en `EJECUCION`,
  - N filas en `LOG_PROCESOS` por chunking.
- Confirmar que la nueva columna `Sucursal_origen` se refleje correctamente en Google Sheets.

Estado: PENDIENTE DE EJECUCION OPERATIVA.

Checklist de validacion recomendado:
- Ejecutar `python automatizaciones/etl_seguros/etl.py --dry-run --start-date 20260101`.
- Verificar en auditoria: 1 fila en `EJECUCION` y N filas en `LOG_PROCESOS`.
- Confirmar que el encabezado en hoja nueva incluya `Sucursal_origen`.
- Confirmar que el orden y conteos de clasificacion sean consistentes para piloto TOP 50.

### Fase 3.6 - Simplificacion final de query productiva

- Unificar extraccion de produccion en una sola query parametrizada por `FechaPrereserva`.
- Definir comportamiento operativo estable:
  - `python etl.py` => usa fecha actual en formato `YYYYMMDD`.
  - `python etl.py --start-date YYYYMMDD` => usa fecha definida por usuario (zero day/historico).
- Eliminar ramas redundantes de query en produccion para simplificar mantenimiento.

Estado: COMPLETADA.

Implementado:
- Se reemplazaron queries separadas por una unica constante `PROD_EXTRACT_QUERY`.
- `build_runtime_config` ahora asigna fecha actual automaticamente en produccion si no se informa `--start-date`.
- `extract_from_sqlserver_production` ejecuta siempre la query parametrizada con 2 parametros (Ford/Hyundai).
- Se actualizo `--help` para reflejar el comportamiento real.

### Fase 3.7 - Clave compuesta por origen de sucursal

- Resolver colision de `Prereserva` entre bases (Ford/Hyundai) usando clave compuesta de negocio.
- Incorporar columna `Sucursal_origen` en la extraccion SQL Server.
- Ajustar clasificacion en ETL para comparar por `Prereserva + Sucursal_origen`.

Estado: COMPLETADA.

Implementado:
- Query productiva agrega `Sucursal_origen` estatico por fuente (`FORD` / `HYUNDAI`).
- Se agrego `Sucursal_origen` a `SHEET_COLUMNS_DB`.
- Se agrego helper `build_entity_key(prereserva, sucursal_origen)`.
- `read_sheet_snapshot` y `classify_records` migraron a indice por clave compuesta.
- Auditoria de inserts/updates ahora reporta `entity_id` compuesto para trazabilidad correcta.

### Fase 6 - Contenerizacion del ETL (Docker)

- Crear una imagen Docker para `automatizaciones/etl_seguros/etl.py` con dependencias aisladas.
- Estandarizar ejecucion en Linux para evitar diferencias entre ejecucion manual y cron.
- Mantener conectividad a SQL Server externo + Google Sheets + DB de auditoria sin instalar dependencias globales en el host.

Estado: PLANIFICADA.

Objetivo tecnico:
- Ejecutar ETL en contenedor reproducible, portable y con dependencias consistentes.
- Reducir incidentes por entorno (drivers ODBC ausentes, rutas de credenciales, diferencias de interpreter/path).

Alcance acordado:
- Crear `Dockerfile` para ETL con Python y ODBC.
- Definir `docker-compose.yml` para ejecucion operativa con `.env` y volumenes.
- Mantener `credentials.json` fuera de la imagen (montado por volumen, solo lectura).
- Definir forma de programacion recomendada:
  - cron del host invocando `docker run` o `docker compose run`,
  - sin cron interno dentro del contenedor para evitar complejidad operativa.

Dependencias de sistema que debe instalar la imagen:
- Base Python (`python:3.x-slim` o equivalente estable).
- Paquetes Linux requeridos para `pyodbc`:
  - `unixodbc`
  - `unixodbc-dev`
  - `curl`
  - `gnupg`
  - `ca-certificates`
  - `apt-transport-https`
- Repositorio Microsoft para Ubuntu/Debian compatible con la imagen.
- Driver SQL Server:
  - `msodbcsql18`

Dependencias Python dentro de la imagen:
- Instalar todo `requirements.txt` del proyecto.
- Verificar presencia de `pyodbc` y driver detectable con `pyodbc.drivers()`.

Instalaciones/configuracion necesarias al correr la imagen (runtime):
- Variables de entorno (`.env`):
  - SQL Server: `SQLSERVER_HOST_1`, `SQLSERVER_HOST_2`, `SQLSERVER_PORT_*`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`, `SQLSERVER_DRIVER`, `SQLSERVER_ENCRYPT`, `SQLSERVER_TRUST_SERVER_CERTIFICATE`.
  - Google Sheets: `SPREADSHEET_ID`, `SHEET_NAME`, `GOOGLE_CREDENTIALS_FILE`.
  - Auditoria: `AUDIT_DB_HOST`, `AUDIT_DB_PORT`, `AUDIT_DB_USER`, `AUDIT_DB_PASSWORD`, `AUDIT_DB_NAME`.
- Volumenes:
  - Montar `credentials.json` en ruta fija del contenedor (read-only).
  - Montar salida de logs (`etl.log`) para persistencia fuera del contenedor.
  - Montar `watermark.json` solo para modo testing, no requerido en produccion.
- Red:
  - Salida a SQL Server remoto (`tcp/1433` o puerto configurado por host).
  - Salida HTTPS a APIs de Google.

Lo que NO se instalara dentro del contenedor:
- SQL Server local (el ETL consume servidores externos existentes).
- Cron del sistema dentro del contenedor (el scheduling queda en host/orquestador).

Riesgos y mitigacion:
- Diferencias TLS/SSL entre hosts SQL Server:
  - Mantener configuracion por entorno y evaluar soporte por host.
  - En etapa siguiente, habilitar parametros por host (`SQLSERVER_DRIVER_1`, `SQLSERVER_ENCRYPT_1`, etc.) si se confirma necesidad.
- Secretos expuestos en imagen:
  - No copiar `credentials.json` ni `.env` al build context final.
  - Inyectar secretos por volumen/variables en runtime.
- Drift de ejecucion cron vs manual:
  - Usar comando docker unico y deterministico.
  - Registrar en auditoria los mismos tags y metrica que en ejecucion local.

Criterios de aceptacion de la fase:
- El ETL corre en contenedor en Linux con resultado equivalente a ejecucion manual.
- `python -c "import pyodbc; print(pyodbc.drivers())"` devuelve `ODBC Driver 18 for SQL Server` (o 17) dentro del contenedor.
- Se puede ejecutar en modo produccion y testing sin cambios de codigo.
- Auditoria persiste correctamente (`EJECUCION` + `LOG_PROCESOS`) y `etl.log` queda disponible en volumen montado.

Checklist operativo previo a cierre de fase:
- Build de imagen OK.
- Prueba de conectividad SQL Server por cada host group.
- Prueba de escritura controlada a Google Sheets (`--dry-run` y corrida real).
- Prueba programada por cron del host invocando contenedor.
- Documentacion de runbook de despliegue y rollback.

### Cierre del proyecto

Estado general: CERRADO A NIVEL FUNCIONAL.

Alcance completado:
- Arquitectura modular por fases.
- Integracion de auditoria transaccional.
- Soporte de entornos testing/produccion.
- Conexion productiva a SQL Server.
- Estabilizacion de query multi-sucursal y reglas operativas para cron + zero day.

Ultimo paso pendiente (proxima etapa):
- Refactorizar el codigo para reducir complejidad ciclomatica, separar funciones largas y mejorar mantenibilidad sin cambiar comportamiento.

Nota de seguimiento:
- A futuro, para sumar nuevas sucursales (Peugeot/Jeep/Fiat), agregar nuevos bloques `UNION ALL` con su `Sucursal_origen`.
- Si vuelve `FechaPatentamiento`, se incorpora en columnas esperadas y en hoja sin cambiar el enfoque de clave compuesta.
