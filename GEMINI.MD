# Memoria del Proyecto: ETL Seguros (MySQL -> Google Sheets)

## Objetivo General
Automatizar la sincronización de un log transaccional de ventas desde MySQL hacia una vista maestra consolidada en Google Sheets, con ejecución periódica, trazabilidad auditada y cero sobreingeniería.

---

## 1) Estado Consolidado Actual

### ETL Fase 2 (implementada y validada)
- Flujo `append/update/noop` funcionando en `etl.py`.
- Lectura completa de hoja (`get_all_values`) + índice en memoria `Prereserva -> fila`.
- Comparación total de 27 campos para detectar updates reales.
- Escritura eficiente:
  - Nuevos: `append_rows()`
  - Existentes modificados: `batch_update()` en una sola llamada.
- Watermark (`watermark.json`) se actualiza solo al finalizar exitosamente.
- Logging locuaz por `Prereserva` y por campo mutado (`old -> new`).

### Robustez ya aplicada
- Normalización defensiva para comparar/escribir (`NaN`, `NaT`, `None`, `null` -> `""`).
- Tolerancia de encabezados en Sheets (`Anio`/`Año`) con validación canónica.
- Manejo de UI de hoja no bloqueante:
  - `freeze(rows=1)`
  - `set_basic_filter("A1:AA{last_row}")`
  - si falla, solo `WARNING` (no frena datos ni watermark).

### Simulador de concesionaria (validado)
- `test/simulador_concesionaria.py` soporta `source=faker|replay` y `mode=log|db`.
- Modo interactivo disponible con `--interactive` (wizard por consola para cargar parámetros).
- Modo mantenimiento: `--reset-table` solo resetea tabla (`DROP + CREATE`) y termina.
- QA markdown mejorado:
  - Tabla de Updates incluye `Cliente`.
  - Updates ordenados por `Prereserva` y `seq`.
  - Separador visual entre grupos de Prereserva.

---

## 2) Decisiones Técnicas Innegociables
- `Prereserva` es clave de negocio inmutable para cruce y actualización.
- No deduplicar eventos transaccionales en simulación/replay.
- En ETL, comparar fila completa (27 campos) para detectar diferencias reales.
- Anulaciones no se borran: se tratan como update de estado.
- Logs deben contar historia exacta por registro para auditoría humana.

---

## 3) Flujo ETL Actual (Fase 2)
1. Leer watermark local.
2. Extraer incremental desde MySQL (`id > last_id`).
3. Deduplicar por `Prereserva` (`keep='last'`).
4. Leer estado completo de Sheets e indexar filas.
5. Clasificar en memoria: `append`, `update`, `noop`.
6. Ejecutar escrituras por lote (`batch_update` + `append_rows`).
7. Aplicar configuración visual no bloqueante (`freeze + filter`).
8. Actualizar watermark solo si todo lo crítico fue exitoso.

---

## 4) Próxima Implementación (Fase 3)

### Meta
Reducir dependencia de `watermark.json` para tolerar mejor fallos operativos y recuperar estado automáticamente.

### Dirección acordada
- Diseñar modo de sincronización **sin dependencia fuerte de watermark** (o con watermark como optimización, no como fuente única de verdad).
- Priorizar consistencia: el bot debe poder reconstruir decisiones de `append/update/noop` aunque el watermark se corrompa o falte.
- Mantener simplicidad operativa y logs auditables.

### Criterios de éxito Fase 3
- Recuperación automática ante watermark ausente/inválido.
- Cero duplicados por `Prereserva` en destino.
- Mismo comportamiento funcional validado con simulador (`faker` y `replay`).

---

## Estructura de Datos (Schema - 27 columnas)
1. Prereserva (PK)
2. Rubro
3. Precioventa
4. Cliente
5. Cuitcliente
6. Email
7. Telefono
8. Domicilio
9. Localidad
10. Provincia
11. Codigounidad
12. Marca
13. Marcamodelo
14. Anio
15. Color
16. Vin
17. Patente
18. Vendedor
19. Sucursal
20. Origen
21. Estado
22. Estadoavance
23. Fechaprereserva
24. Fechaventa
25. Fechaentrega
26. Fechapatentamiento
27. Fechaproceso
