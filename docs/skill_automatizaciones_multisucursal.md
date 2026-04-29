# Skill Operativa: Automatizaciones Multi-sucursal

## Proposito

Definir un estandar operativo para crear y mantener automatizaciones robustas, modulares y seguras en el ecosistema multi-concesionaria.

Esta skill aplica a procesos como:

- Extraccion y carga de datos (Sheets, DB, API).
- Sincronizacion entre sistemas.
- Actualizaciones operativas con reglas CRUD.
- Procesos de soporte para herramientas internas.

## Regla Cero: Inicializacion de contexto

Antes de ejecutar cualquier tarea, leer en silencio:

1. `docs/memoria_agentes/contexto_agentes.md`
2. `docs/memoria_fase_planificacion.md`

No asumir contexto sin leer ambos archivos.

## Arquitectura obligatoria

Estructura que no se altera:

- `automatizaciones/`: codigo de negocio por automatizacion (una carpeta por proceso).
- `core/`: utilidades transversales, sin logica de marca/sucursal.
- `deploy/`: infraestructura y despliegue.
- `docs/`: documentacion funcional y memoria de agentes.
- `scripts/`: utilidades operativas.

Regla de aislamiento:

- Cada automatizacion nueva debe ser autocontenida en su propia carpeta.
- No mezclar logica de una automatizacion con otra.

## Flujo de trabajo obligatorio

1. Planificar en `docs/memoria_fase_planificacion.md`.
2. Implementar por fases con checklist `[ ]` / `[x]`.
3. Validar con dry-run y verificacion tecnica.
4. Documentar modulo en su `README.md` local.
5. Actualizar `docs/memoria_agentes/contexto_agentes.md` con:
   - que se hizo,
   - por que se hizo,
   - que queda pendiente.

## Guardrails de seguridad

- Usar `get_app_env()` para detectar entorno.
- Si `APP_ENV != PROD`, forzar `dry_run=True`.
- En dry-run no se aplican escrituras reales en `apply_actions()`.
- No hardcodear credenciales, secretos o IDs sensibles.

## Contrato de auditoria transversal

Uso obligatorio de `core/audit_logger.py`.

Ciclo minimo:

`start -> record_* / set_metric -> mark_* -> persist`

Metricas obligatorias:

- `extraidos`
- `procesados`
- `noop`
- `inserts`
- `updates`
- `deletes`
- `warnings`
- `errors`

Eventos recomendados:

- `record_info`
- `record_insert`
- `record_update`
- `record_delete`
- `record_warning`
- `record_error`
- `record_detail_line`

Cierre obligatorio:

- `mark_success()` o `mark_warning()` o `mark_error()`
- `persist()` en `finally`

## Convencion de detalle en LOG_PROCESOS

Etiquetas estandar:

- `[INICIO]`
- `[RESUMEN]`
- `[INFO]`
- `[SOURCE_START]`
- `[SOURCE_SUMMARY]`
- `[SOURCE_ERROR]`
- `[INSERT]`
- `[UPDATE]`
- `[DELETE]`
- `[WARNING]`
- `[ERROR]`
- `[FIN]`

Nota:

- `LOG_PROCESOS` puede tener una o varias filas por `id_ejecucion` por chunking.

## Patron recomendado de implementacion

Usar Strategy/POO para mantener flujo limpio:

- `BaseExtractor`
- `BaseTransformer`
- `BaseLoader`

Flujo sugerido:

`extract -> transform -> plan_actions -> apply_actions -> finalize`

Clave de negocio:

- Usar identificador estable por entidad.
- Si aplica multi-sucursal, usar clave compuesta (ej: `Prereserva|Sucursal_origen`).

## Definition of Done (DoD)

Una tarea se considera terminada solo si cumple todo:

- [ ] Implementacion completa por fases.
- [ ] Dry-run ejecutado y analizado.
- [ ] Validacion tecnica realizada (sintaxis/tests/comando operativo).
- [ ] README del modulo actualizado.
- [ ] `docs/memoria_fase_planificacion.md` actualizado.
- [ ] `docs/memoria_agentes/contexto_agentes.md` actualizado.
- [ ] Riesgos/puntos pendientes informados al usuario.

## Plantilla de cierre para respuesta al usuario

Entregar siempre:

1. Que se cambio.
2. Por que se cambio.
3. Como se valido.
4. Riesgos/supuestos.
5. Siguientes pasos sugeridos.
