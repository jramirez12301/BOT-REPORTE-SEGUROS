# GEMINI - Memoria de contexto

## Estado actual del repositorio

- Proyecto principal vigente: sincronizacion MySQL -> Google Sheets en `etl.py`.
- Estructura actual relevante en raiz: `etl.py`, `dataset/`, `test/`, `docs/`, `requirements.txt`.
- `dataset/` y `test/` pertenecen funcionalmente al ETL actual.
- Hay documentacion en borrador y posibilidad de ajustar reglas sin restricciones legacy fuertes.

## Objetivo estrategico acordado

Estandarizar auditoria para todas las automatizaciones de la empresa con un modulo reusable en `core/`, evitando logica duplicada y manteniendo trazabilidad centralizada en base de datos.

## Modelo de auditoria acordado

Tablas:

1. `PROCESOS`
2. `EJECUCION` (`id_proceso`, `fecha_inicio`, `fecha_fin`, `resumen`, `estado`)
3. `LOG_PROCESOS` (`id_ejecucion`, `detalle`)

## Reglas de negocio vigentes

- No rollback funcional: los cambios exitosos se mantienen.
- Persistencia de auditoria al final del flujo (`finally`).
- Persistencia transaccional desde Python:
  - 1 insert en `EJECUCION`
  - N inserts en `LOG_PROCESOS` por chunking
- Estados validos: `EXITO`, `ADVERTENCIA`, `ERROR`.
- Tags en el detalle:
  - Obligatorios: `[INICIO]`, `[RESUMEN]`, `[FIN]`
  - Opcionales: `[INFO]`, `[INSERT]`, `[UPDATE]`, `[DELETE]`, `[WARNING]`, `[ERROR]`
- Zona horaria por defecto: `America/Argentina/Buenos_Aires` (configurable).

## Directrices de diseno para `AuditLogger`

- Agnostico de dominio: metodos con IDs y entidades genericas.
- Inyeccion de dependencias: `db_conn_factory` en constructor.
- Construccion de detalle en memoria y particionado por tamano.
- Limite de bloque recomendado: `10_000` caracteres por fila de `LOG_PROCESOS`.
- Debe permitir resumen de updates y detalle por campo (`old -> new`).

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

- Desarrollo: usar `venv` local.
- Produccion: aislamiento obligatorio por `venv` o contenedor.
- `db_utils.py` definira factories por entorno (`TEST`/`PROD`) via `.env`.

## Decisiones de lenguaje

- Se adopta el termino formal: **automatizacion** (en lugar de bot).
