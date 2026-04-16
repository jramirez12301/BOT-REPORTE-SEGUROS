import os
import json
import logging
import unicodedata
import mysql.connector
import pandas as pd
import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Configuracion del entorno de ejecucion

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de los logs para depuración
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constantes y Rutas de Archivos
WATERMARK_FILE = 'watermark.json' # Representa la última id que proceso
CREDENTIALS_FILE = 'credentials.json' # Configuración google sheet
# 
WATERMARK_CANDIDATES = ['id_interno', 'id']

# Orden exacto requerido para Google Sheets (segun especificacion)
SHEET_COLUMNS = [
    'Prereserva', 'Rubro', 'Precioventa', 'Cliente', 'Cuitcliente', 'Email', 'Telefono',
    'Domicilio', 'Localidad', 'Provincia', 'Codigounidad', 'Marca', 'Marcamodelo', 'Anio',
    'Color', 'Vin', 'Patente', 'Vendedor', 'Sucursal', 'Origen', 'Estado', 'Estadoavance',
    'Fechaprereserva', 'Fechaventa', 'Fechaentrega', 'Fechapatentamiento', 'Fechaproceso'
]


def canonicalize_header_name(value: str) -> str:
    """Normaliza encabezados para tolerar acentos y variaciones menores."""
    text = normalize_cell_value(value)
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace(' ', '')

    # Alias explícitos de encabezados válidos en negocio.
    aliases = {
        'ano': 'anio',
    }
    return aliases.get(text, text)

# Función para leer última id procesada del archivo watermark.json
def get_watermark() -> int:
    """Lee el archivo watermark.json para obtener el último id procesado."""
    try:
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, 'r') as f:
                data = json.load(f)
                last_id = data.get('last_id', 0)
                logging.info(f"Marca de agua leída correctamente. last_id={last_id}")
                return last_id
        else:
            logging.info("El archivo watermark.json no existe. Asumiendo last_id=0 para primera ejecución.")
            return 0
    except Exception as e:
        logging.error(f"Error al leer la marca de agua: {e}", exc_info=True)
        raise

# Función para actualizar última id procesada del archivo watermark.json
def update_watermark(new_last_id: int):
    """Sobrescribe el archivo watermark.json con el nuevo id más alto procesado."""
    try:
        with open(WATERMARK_FILE, 'w') as f:
            json.dump({'last_id': new_last_id}, f)
        logging.info(f"Marca de agua actualizada correctamente. Nuevo last_id={new_last_id}")
    except Exception as e:
        logging.error(f"Error al actualizar la marca de agua: {e}", exc_info=True)
        raise

def resolve_watermark_field(connection) -> str:
    """Detecta la columna incremental disponible para la marca de agua."""
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW COLUMNS FROM Seguros")
        cols = {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()

    for candidate in WATERMARK_CANDIDATES:
        if candidate in cols:
            return candidate

    raise KeyError(f"No se encontró columna incremental. Probadas: {WATERMARK_CANDIDATES}")


def extract_from_mysql(last_id: int) -> tuple[pd.DataFrame, str]:
    """Extrae registros nuevos desde MySQL superiores a last_id en orden ascendente."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '3306')),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            connection_timeout=10,
            use_pure=True,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci',
            use_unicode=True,
        )

        # Asegura que la sesion use UTF-8 completo para acentos y ñ
        connection.set_charset_collation(charset='utf8mb4', collation='utf8mb4_unicode_ci')

        watermark_field = resolve_watermark_field(connection)
        logging.info(f"Columna incremental detectada: {watermark_field}")
        
        # Ordenamos por campo incremental ASC para conservar incrementalidad por marca de agua
        columns_query = ', '.join([watermark_field] + SHEET_COLUMNS)
        query = f"SELECT {columns_query} FROM Seguros WHERE {watermark_field} > %s ORDER BY {watermark_field} ASC"
        
        # Pandas read_sql puede manejar los parámetros de forma segura
        df = pd.read_sql(query, connection, params=(last_id,))
        logging.info(f"Extracción exitosa desde MySQL. {len(df)} registros obtenidos.")
        
        return df, watermark_field
    except Exception as e:
        logging.error(f"Error al extraer datos desde MySQL: {e}", exc_info=True)
        raise
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina registros duplicados conservando el estado más reciente."""
    if df.empty:
        logging.info("El DataFrame está vacío, no hay datos para procesar.")
        return df
        
    try:
        if 'Prereserva' not in df.columns:
            raise KeyError("No se encontró la columna requerida 'Prereserva' en los datos extraídos.")

        # Dado que extrajimos con ORDER BY id_interno ASC, la última ocurrencia es la más nueva.
        initial_count = len(df)
        df_clean = df.drop_duplicates(subset=['Prereserva'], keep='last')
        final_count = len(df_clean)
        
        logging.info(f"Deduplicación completada: {initial_count} registros iniciales -> {final_count} registros únicos.")
        return df_clean
    except Exception as e:
        logging.error(f"Error en la transformación de datos: {e}", exc_info=True)
        raise

def normalize_cell_value(value) -> str:
    """Normaliza valores para comparación y serialización segura hacia Google Sheets."""
    if pd.isna(value):
        return ''

    text = str(value).strip()
    if text.lower() in {'nan', 'nat', 'none', 'null'}:
        return ''
    return text


def read_sheet_snapshot(worksheet) -> tuple[dict, dict]:
    """Lee toda la hoja y construye índice Prereserva->fila y snapshot por Prereserva."""
    try:
        all_values = worksheet.get_all_values()

        if not all_values:
            logging.info("La hoja está vacía. Se asume sin encabezado ni registros existentes.")
            return {}, {}

        header = [normalize_cell_value(col) for col in all_values[0]]
        expected_header = SHEET_COLUMNS
        received_canonical = [canonicalize_header_name(col) for col in header[:len(expected_header)]]
        expected_canonical = [canonicalize_header_name(col) for col in expected_header]

        if received_canonical != expected_canonical:
            raise ValueError(
                "Encabezado inválido en Google Sheets. "
                f"Esperado={expected_header} | Recibido={header[:len(expected_header)]}"
            )

        if header[:len(expected_header)] != expected_header:
            logging.info(
                "Encabezado compatible detectado con variaciones de formato/acentos. "
                "Se continúa usando el orden canónico interno."
            )

        index_by_prereserva = {}
        row_values_by_prereserva = {}
        duplicates = 0

        # Índice en memoria: DataFrame/base-0 + 2 para mapear a fila real en Sheets (base-1 + header).
        for row_number, row in enumerate(all_values[1:], start=2):
            row_27 = (row + [''] * len(SHEET_COLUMNS))[:len(SHEET_COLUMNS)]
            normalized_row = [normalize_cell_value(value) for value in row_27]
            prereserva = normalized_row[0]

            if not prereserva:
                continue

            if prereserva in index_by_prereserva:
                duplicates += 1

            index_by_prereserva[prereserva] = row_number
            row_values_by_prereserva[prereserva] = normalized_row

        if duplicates > 0:
            logging.warning(
                "Se detectaron %s Prereservas duplicadas en la hoja. Se utilizará la última fila encontrada.",
                duplicates,
            )

        logging.info(
            "Estado de la hoja cargado. Filas con Prereserva=%s",
            len(index_by_prereserva),
        )
        return index_by_prereserva, row_values_by_prereserva
    except Exception as e:
        logging.error(f"Error al leer snapshot completo de Google Sheets: {e}", exc_info=True)
        raise


def classify_records(df: pd.DataFrame, sheet_index: dict, sheet_rows: dict) -> tuple[list, list, int]:
    """Clasifica registros deduplicados en insercion, actualizacion y sin cambios."""
    if df.empty:
        return [], [], 0

    append_rows = []
    updates = []
    noop_count = 0

    for _, row in df.iterrows():
        mysql_row = [normalize_cell_value(row.get(col, '')) for col in SHEET_COLUMNS]
        prereserva = mysql_row[0]

        if not prereserva:
            logging.warning("Registro omitido: Prereserva vacío tras normalización.")
            continue

        if prereserva not in sheet_index:
            append_rows.append(mysql_row)
            logging.info("Prereserva %s -> INSERCION (no existe en Sheets).", prereserva)
            continue

        sheet_row = sheet_rows.get(prereserva, [''] * len(SHEET_COLUMNS))
        changed_fields = []
        for column_name, old_value, new_value in zip(SHEET_COLUMNS, sheet_row, mysql_row):
            if old_value != new_value:
                changed_fields.append((column_name, old_value, new_value))

        if changed_fields:
            updates.append(
                {
                    'prereserva': prereserva,
                    'row_number': sheet_index[prereserva],
                    'values': mysql_row,
                    'changes': changed_fields,
                }
            )
            logging.info(
                "Prereserva %s -> ACTUALIZACION (%s campos modificados).",
                prereserva,
                len(changed_fields),
            )
            for field, old_value, new_value in changed_fields:
                logging.info(
                    "Prereserva %s | Campo '%s' | '%s' -> '%s'",
                    prereserva,
                    field,
                    old_value,
                    new_value,
                )
        else:
            noop_count += 1
            logging.info("Prereserva %s -> SIN_CAMBIOS (sin cambios).", prereserva)

    return append_rows, updates, noop_count


def batch_update_existing_rows(worksheet, updates: list):
    """Actualiza filas existentes en una sola llamada por lote."""
    if not updates:
        logging.info("No hay updates para ejecutar en Google Sheets.")
        return

    try:
        data = []

        # Escritura eficiente: un único payload con múltiples rangos A1, uno por fila modificada.
        for item in updates:
            row_number = int(item['row_number'])
            start_a1 = rowcol_to_a1(row_number, 1)
            end_a1 = rowcol_to_a1(row_number, len(SHEET_COLUMNS))
            range_a1 = f"{start_a1}:{end_a1}"
            data.append({'range': range_a1, 'values': [item['values']]})

        worksheet.batch_update(data, value_input_option='USER_ENTERED')
        logging.info("Actualizacion por lote ejecutada. Filas actualizadas=%s", len(updates))
    except Exception as e:
        logging.error(f"Error al ejecutar batch_update en Google Sheets: {e}", exc_info=True)
        raise


def append_new_rows(worksheet, rows: list):
    """Inserta registros nuevos en lote con append_rows."""
    if not rows:
        logging.info("No hay filas nuevas para insertar en Google Sheets.")
        return

    try:
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')
        logging.info("Insercion por lote ejecutada. Filas insertadas=%s", len(rows))
    except Exception as e:
        logging.error(f"Error al ejecutar append_rows en Google Sheets: {e}", exc_info=True)
        raise


def ensure_sheet_filter(worksheet, total_data_rows: int):
    """Asegura encabezado congelado y filtro activo sin afectar el flujo de negocio."""
    try:
        # Rango dinámico del modelo: encabezado + 27 columnas (A:AA).
        last_row = max(1, int(total_data_rows) + 1)
        filter_range = f"A1:AA{last_row}"

        worksheet.freeze(rows=1)
        worksheet.set_basic_filter(filter_range)
        logging.info("UI de hoja aplicada: freeze fila 1 + filtro %s", filter_range)
    except Exception as e:
        logging.warning(
            "No se pudo aplicar configuración visual de la hoja (freeze/filter). "
            "Se continúa porque los datos ya fueron escritos. Detalle: %s",
            e,
        )

def main():
    logging.info("--- Iniciando ciclo de sincronización ETL ---")
    print("[INFO] Iniciando ciclo ETL...")
    try:
        # 1. Configurar conexión a la API de Google
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"No se encontró el archivo de credenciales: {CREDENTIALS_FILE}")

        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            cred_json = json.load(f)
            service_account_email = cred_json.get('client_email', 'N/D')
        print(f"[INFO] Cuenta de servicio en uso: {service_account_email}")
             
        credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        gc = gspread.authorize(credentials)
        
        # Abrir el documento
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        sheet_name = os.getenv('SHEET_NAME', 'Hoja 1')
        
        if not spreadsheet_id:
            raise ValueError("SPREADSHEET_ID no configurado en variables de entorno.")

        print(f"[INFO] SPREADSHEET_ID: {spreadsheet_id}")
        print(f"[INFO] SHEET_NAME: {sheet_name}")
             
        sh = gc.open_by_key(spreadsheet_id)
        worksheet = sh.worksheet(sheet_name)
        print("[INFO] Conexion con Google Sheets OK.")
        
        # 2. Leer marca de agua
        last_id = get_watermark()
        print(f"[INFO] Marca de agua actual: {last_id}")
        
        # 3. Extraer desde MySQL
        df, watermark_field = extract_from_mysql(last_id)
        print(f"[INFO] Registros extraidos desde MySQL: {len(df)}")
        
        if not df.empty:
            if watermark_field not in df.columns:
                raise KeyError(f"No se encontró la columna de marca de agua '{watermark_field}'.")

            # Capturar el id maximo procesado antes de cualquier transformacion
            new_last_id = int(df[watermark_field].max())
            
            # 4. Transformar y Deduplicar
            df_clean = process_data(df)
            print(f"[INFO] Registros luego de deduplicar: {len(df_clean)}")

            # 5. Snapshot completo de Sheets + índice en memoria Prereserva -> fila real.
            sheet_index, sheet_rows = read_sheet_snapshot(worksheet)

            # 6. Clasificacion por regla de negocio: insercion / actualizacion / sin cambios comparando 27 campos.
            append_rows_payload, updates_payload, noop_count = classify_records(df_clean, sheet_index, sheet_rows)
            print(f"[INFO] Clasificacion: append={len(append_rows_payload)} update={len(updates_payload)} noop={noop_count}")

            # 7. Escritura por lotes: updates existentes y luego nuevos.
            batch_update_existing_rows(worksheet, updates_payload)
            append_new_rows(worksheet, append_rows_payload)
            print("[INFO] Escritura a Google Sheets completada.")

            # 7.1 Calidad de vida UI: encabezado congelado + filtro activo (no bloqueante).
            total_data_rows = len(sheet_index) + len(append_rows_payload)
            ensure_sheet_filter(worksheet, total_data_rows)
                 
            # 8. Actualizar Marca de Agua (solo si todo fue exitoso)
            update_watermark(new_last_id)
            print(f"[INFO] Marca de agua actualizada a: {new_last_id}")
        else:
            logging.info("No se encontraron registros nuevos en la base de datos.")
            print("[INFO] No hay registros nuevos para procesar.")
            
        logging.info("--- Sincronización ETL finalizada con éxito ---\n")
        print("[OK] ETL finalizado con exito.")
        
    except SpreadsheetNotFound:
        msg = (
            "[ERROR] Spreadsheet no encontrado (404). "
            "Valida SPREADSHEET_ID y comparte el archivo con el email de la cuenta de servicio."
        )
        logging.error(msg, exc_info=True)
        print(msg)

    except WorksheetNotFound:
        msg = (
            "[ERROR] La hoja/pestaña no existe. "
            "Revisa SHEET_NAME en .env exactamente igual al nombre de la pestaña."
        )
        logging.error(msg, exc_info=True)
        print(msg)

    except APIError as e:
        status_code = getattr(getattr(e, 'response', None), 'status_code', 'N/D')
        response_text = getattr(getattr(e, 'response', None), 'text', '')
        msg = f"[ERROR] API Google Sheets HTTP {status_code}: {e}"
        logging.error(f"{msg} | response={response_text}", exc_info=True)
        print(msg)
        if response_text:
            print(f"[ERROR] Detalle API: {response_text}")

    except Exception as e:
        # Si un error ocurre, el update_watermark no se ejecutará
        logging.error(f"--- Sincronización abortada debido a un error crítico: {e} ---\n", exc_info=True)
        print(f"[ERROR] ETL abortado: {e}")

if __name__ == "__main__":
    main()
