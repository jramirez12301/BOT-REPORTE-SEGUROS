# Plan de Trabajo: Fase 2 - Sincronización Total (Updates)

Esta fase se centra en permitir que el bot no solo inserte registros nuevos, sino que también detecte y actualice cambios en registros existentes, manteniendo Google Sheets como un espejo fiel del estado transaccional en MySQL.

---

## 1. Simulador de Datos (`simulador_concesionaria.py`)
**Objetivo:** Crear un entorno controlado para probar actualizaciones dinámicas.

- **Tecnología:** Librería `Faker` para datos realistas.
- **Acciones:**
    - Crear la tabla `Seguros` con el schema de 27 columnas si no existe.
    - Insertar registros iniciales.
    - Simular actualizaciones insertando nuevas filas en MySQL con la misma `Prereserva` pero con cambios en campos como `Estado`, `Vendedor`, `VIN`, o correcciones de datos del cliente.

---

## 2. Configuración (`.env`)
- Añadir la variable `SYNC_INTERVAL_MINUTES`.
- Valor `> 0`: El script entrará en un bucle infinito con esperas (`time.sleep`).
- Valor `0` o inexistente: El script corre una sola vez (comportamiento actual para Cron).

---

## 3. Refactorización de `etl.py` (Lógica de Negocio)

### A. Lectura de Datos Existentes
- Cambiar la lectura de "solo Columna A" por una lectura completa usando `worksheet.get_all_values()`.
- Cargar estos datos en un DataFrame de Pandas (`df_sheet`).
- Generar un diccionario de mapeo: `{"Prereserva": Numero_Fila_GSheets}`.

### B. Comparación y Clasificación (Cerebro del Bot)
Tras obtener la foto final de MySQL (`df_mysql` deduplicado):
1. **Nuevos:** `Prereserva` no está en el Sheet -> Se envían vía `append_rows()`.
2. **Existentes:** `Prereserva` ya está en el Sheet -> Se procede a la **Comparación Total**.

### C. Comparación Total (Fila a Fila)
- Por cada `Prereserva` existente:
    - Se comparan todos los campos del registro de MySQL contra los del Sheet (Pandas).
    - Si se detecta una diferencia en cualquier columna:
        - Se registra el cambio (Modo Locuaz): `[INFO] Cambio detectado en 'Vendedor': Juan -> Pedro`.
        - Se añade a la lista de `batch_update`.

### D. Actualización en Bloque (`batch_update`)
- Agrupar todas las filas que sufrieron cambios.
- Ejecutar una única llamada a la API de Google Sheets para actualizar todas las celdas necesarias de forma eficiente.

---

## 4. Auditoría y Logging (Modo Locuaz)
El bot debe narrar cada paso en `etl.log` y consola:
- `[INFO] Evaluando Prereserva 26030056...`
- `[INFO] ¡HUBO UN CAMBIO en 'Vendedor'! Modificando de 'Juan' a 'Pedro'`
- `[INFO] Ningún cambio detectado en Prereserva 26010102. Ignorando.`

---

## 5. Checklist de Verificación
1. [ ] El simulador crea la tabla y pobla datos iniciales.
2. [ ] El bot inserta los nuevos datos en GSheets.
3. [ ] El simulador inserta un cambio en MySQL para una Prereserva ya existente en el Sheet.
4. [ ] El bot detecta el cambio, loguea la diferencia y actualiza la fila correcta en GSheets sin duplicarla.
5. [ ] El bucle `SYNC_INTERVAL_MINUTES` funciona correctamente.
