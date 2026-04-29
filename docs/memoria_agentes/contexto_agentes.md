# Memoria de contexto de agentes

## Estado final del proyecto

- Proyecto principal estabilizado: `automatizaciones/etl_seguros/etl.py`.
- Estructura modular implementada con `core/` compartido y `automatizaciones/` por dominio.
- La automatizacion soporta:
  - Testing incremental (MySQL + watermark).
  - Produccion SQL Server (query multi-sucursal por rango de fecha).
  - Auditoria transaccional en base de datos.

## Objetivo estrategico cumplido

- Estandarizar auditoria para automatizaciones con un modulo reusable en `core/`.
- Centralizar trazabilidad en BD y mantener logs locales de respaldo operativo.

## Modelo de auditoria implementado

Tablas:

1. `PROCESOS`
2. `EJECUCION` (`id_proceso`, `fecha_inicio`, `fecha_fin`, `resumen`, `estado`)
3. `LOG_PROCESOS` (`id_ejecucion`, `detalle`)

## Reglas de negocio vigentes

- No rollback funcional: los cambios exitosos se mantienen.
- Persistencia de auditoria al final del flujo (`finally`).
- Persistencia transaccional desde Python:
  - 1 insert en `EJECUCION`
  - N inserts en `LOG_PROCESOS` por chunking.
- Estados validos: `EXITO`, `ADVERTENCIA`, `ERROR`.
- Tags de detalle aplicados con contrato acordado.
- Zona horaria por defecto: `America/Argentina/Buenos_Aires`.
- Clave de comparacion en ETL: `Prereserva + Sucursal_origen` para evitar colisiones entre bases.

## Estado de `AuditLogger`

- Implementado y operativo en `core/audit_logger.py`.
- Agnostico de dominio, con inyeccion de dependencias y chunking de detalle.
- Integrado en ETL con tracking de inserts/updates/errores y `persist()` en `finally`.

## Estructura objetivo acordada

```text
/Entorno
├── core/
│   ├── __init__.py
│   ├── db_utils.py
│   ├── audit_logger.py
│   └── template_automatizacion.py
├── automatizaciones/
│   ├── etl_seguros/
│   └── aut_stock_web/
├── docs/
└── scripts/
```

## Criterio de entornos

- Desarrollo: `venv` local.
- Testing: MySQL incremental + watermark.
- Produccion: SQL Server multi-sucursal por rango de fecha (`YYYYMMDD`), con ejecucion cron esperada cada 4 horas.
- Si se ejecuta sin flags en produccion: usa hoy -> hoy.
- Si se ejecuta con `--start-date` y opcional `--end-date`: usa rango indicado.
- Auditoria: MySQL por entorno (`AUDIT_DB_*`).

## Decisiones de lenguaje

- Se adopta el termino formal: **automatizacion** (en lugar de bot).

## Cierre y siguiente etapa

- Proyecto dado por finalizado a nivel funcional.
- Ultima etapa pendiente: refactorizacion del codigo para mejorar legibilidad, separacion por modulos y mantenibilidad sin alterar comportamiento.

## Estado final del alcance multi-sucursal

- Query productiva incluye `Sucursal_origen` estatico por base (`FORD` / `HYUNDAI`).
- La clasificacion en hoja se realiza por clave compuesta para evitar updates falsos por `Prereserva` repetida entre sucursales.
- Preparado para ampliar a nuevas sucursales agregando bloques `UNION ALL` con su origen.

## Cambios recientes (2026-04-29)

- Se corrigio bug de indexacion en `automatizaciones/etl_seguros/etl.py` que armaba la clave compuesta con columna incorrecta (`row[0]`).
- Se reemplazo acceso por posicion fija por indices explicitos de `SHEET_COLUMNS_DB` para `Prereserva` y `Sucursal_origen` en:
  - `read_sheet_snapshot`
  - `classify_records`
- Impacto esperado:
  - Evita colisiones de clave por `TipoVenta|Sucursal_origen`.
  - Restaura el matching correcto por `Prereserva|Sucursal_origen` entre origen SQL Server y Google Sheets.
  - Reduce updates cruzados/desordenados entre sucursales.

## Verificacion operativa (2026-04-29)

- Se ejecuto ETL en `PRODUCCION` con `--dry-run --start-date 20260101 --end-date 20260201`.
- Resultado auditoria:
  - `extraidos=304`
  - `deduplicados=258`
  - `inserts=258`
  - `updates=0`
  - `noop=0`
  - `warnings=0`, `errors=0`
- Se confirmo presencia de claves compuestas multi-sucursal en formato `Prereserva|Sucursal_origen` (ej: `26010001|FORD`, `26010001|FIAT`, `26010001|JEEP`, `26010001|HYUNDAI`).
- Extraccion por origen informada en auditoria:
  - FORD=125
  - HYUNDAI=17
  - JEEP=29
  - FIAT=133

## Planificacion por fases (fechas y UX CLI)

1. Fase 1 - Formato fecha visible en hoja
   - Convertir columnas de fecha (`FechaEntrega`, `FechaPrereserva`, `FechaVenta`) desde `YYYYMMDD` a `dd/mm/YYYY` antes de escribir en Sheets.
   - Mantener compatibilidad: si una fecha de origen es invalida, conservar texto original y registrar warning.
2. Fase 2 - Comparacion canonica de fechas
   - Evitar updates falsos por formato visual comparando fechas por valor canonico (`YYYYMMDD`) entre BD y Sheets.
3. Fase 3 - Mensajes de error CLI mas claros
   - Diferenciar validacion de `--start-date` y `--end-date` con mensaje preciso cuando la fecha no existe o no cumple formato.
4. Fase 4 - Verificacion operativa
   - Ejecutar `dry-run` y luego corrida real de control para validar formato visual y ausencia de updates innecesarios.

## Ejecucion secuencial de fases (2026-04-29)

- Fase 1 aplicada en `automatizaciones/etl_seguros/etl.py`:
  - Se formatean fechas validas a `dd/mm/YYYY` en `classify_records` mediante `format_date_for_sheet`.
  - Se conserva valor original si la fecha de origen es invalida.
- Fase 2 aplicada en `automatizaciones/etl_seguros/etl.py`:
  - Se agrega `normalize_date_for_comparison` para comparar fechas equivalentes (`YYYYMMDD`, `dd/mm/YYYY`, `YYYY-MM-DD`) en formato canonico.
- Fase 3 aplicada en `automatizaciones/etl_seguros/etl.py`:
  - Se separan validadores de `--start-date` y `--end-date`.
  - Mensaje de error ahora indica argumento correcto y causa (formato/calendario).
- Fase 4 verificada operativamente:
  - `dry-run` productivo ejecutado con `--start-date 20260201 --end-date 20260301`.
  - Resultado: `extraidos=269`, `deduplicados=218`, `append=218`, `update=0`, `noop=0`, `FIN=EXITO`.
  - Se valido mensaje de UX CLI para fecha invalida:
    - `argument --end-date: --end-date invalido: '20260230'. Formato requerido YYYYMMDD y fecha calendario valida`

## Ajuste de usabilidad Google Sheets (2026-04-29)

- Se agrego script operativo `scripts/sheet_styling_seguros.py` para aplicar estilo y reglas de uso en `Hoja 1`.
- Acciones aplicadas en la hoja:
  - Formato de encabezado (fondo azul, texto blanco, negrita, centrado).
  - Formato de fecha `dd/mm/yyyy` en `FechaEntrega`, `FechaPrereserva`, `FechaVenta`.
  - Formato numerico para `PrecioVenta` (`$ #,##0`).
  - Freeze de fila 1 y filtro basico al rango total.
  - Alta automatica de columnas usuario faltantes al final:
    - `Primer contacto`
    - `Segundo contacto`
    - `Vendido / No vendido`
  - Validaciones:
    - `Primer contacto` y `Segundo contacto`: fecha valida.
    - `Vendido / No vendido`: lista (`Vendido`, `No vendido`, `Pendiente`).
  - Formato condicional por fila segun `Vendido / No vendido`.

## Estilizado UX avanzado (2026-04-29)

- Se actualizo `scripts/sheet_styling_seguros.py` con plan A/B/C/D/E ejecutado y aplicado en hoja real.
- Criterios UX implementados:
  - Prioridad visual de estado: color de fila completa por `Vendido / No vendido` (`Vendido`, `No vendido`, `Pendiente`).
  - Banding blanco/gris alternado para mejorar lectura en filas sin estado.
  - Freeze en L: fila 1 y columnas `A:F`.
  - Tipografia base `Arial`.
  - Ajuste de anchos estrategicos por columna con `updateDimensionProperties`.
  - `wrapStrategy=WRAP` en `Email` y `Domicilio`.
  - Idempotencia activa: limpieza previa de `conditionalFormats` y `bandedRanges` antes de re-aplicar.
