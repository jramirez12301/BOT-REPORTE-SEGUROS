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
