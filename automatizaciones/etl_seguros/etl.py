import argparse
import json
import logging
import os
import sys
import time
import unicodedata
from decimal import Decimal, InvalidOperation
from datetime import datetime
from pathlib import Path

import gspread
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import rowcol_to_a1


# Permite importar modulos de /core aunque el script se ejecute desde su carpeta.
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.audit_logger import AuditLogger
from core.db_utils import get_audit_db_connection_factory


# Cargamos .env del proyecto para ambos modos de ejecucion.
load_dotenv(PROJECT_ROOT / ".env")

# Constantes del modelo de hoja.
WATERMARK_CANDIDATES = ["id_interno", "id"]
SHEET_COLUMNS = [
    "Prereserva",
    "Rubro",
    "Precioventa",
    "Cliente",
    "Cuitcliente",
    "Email",
    "Telefono",
    "Domicilio",
    "Localidad",
    "Provincia",
    "Codigounidad",
    "Marca",
    "Marcamodelo",
    "Anio",
    "Color",
    "Vin",
    "Patente",
    "Vendedor",
    "Sucursal",
    "Origen",
    "Estado",
    "Estadoavance",
    "Fechaprereserva",
    "Fechaventa",
    "Fechaentrega",
    "Fechapatentamiento",
    "Fechaproceso",
]


def parse_args() -> argparse.Namespace:
    """Parámetros de línea de comandos para producción y testing."""
    parser = argparse.ArgumentParser(
        description=(
            "Sincroniza datos hacia Google Sheets con auditoría transaccional. "
            "Modo por defecto: PRODUCCION."
        )
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            "Activa modo TESTING. Usa DB_NAME_TEST y SPREADSHEET_ID_TEST de forma estricta, "
            "habilita watermark y mantiene extracción incremental."
        ),
    )
    parser.add_argument(
        "--reset-watermark",
        action="store_true",
        help=(
            "Solo en TESTING. Resetea watermark a 0 automáticamente sin pedir confirmación por input()."
        ),
    )
    parser.add_argument(
        "--start-date",
        type=parse_start_date,
        help=(
            "Solo en PRODUCCION. Formato YYYYMMDD para carga histórica. "
            "Si se informa, ignora filtro por año actual y usa fechaprereserva >= fecha indicada."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Tamaño máximo de lote para escrituras a Google Sheets (default: 2000).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Segundos de espera entre lotes/reintentos contra Google Sheets (default: 2).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Modo simulacro: clasifica, registra auditoría y logs, pero no escribe en Google Sheets "
            "ni actualiza watermark."
        ),
    )
    return parser.parse_args()


def parse_start_date(value: str) -> str:
    """Valida YYYYMMDD y devuelve YYYY-MM-DD para SQL parametrizado."""
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--start-date debe tener formato YYYYMMDD") from exc
    return parsed.strftime("%Y-%m-%d")


def setup_logging(log_file: Path) -> None:
    """Configura logging local para diagnóstico operativo."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        encoding="utf-8",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def get_env_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Variable de entorno requerida no configurada: {name}")
    return value.strip()


def get_env_optional(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def resolve_credentials_file() -> Path:
    """Resuelve ruta de credenciales intentando carpeta local y raíz del proyecto."""
    env_value = get_env_optional("GOOGLE_CREDENTIALS_FILE", "")
    if env_value:
        return Path(env_value).expanduser().resolve()

    local_candidate = CURRENT_DIR / "credentials.json"
    root_candidate = PROJECT_ROOT / "credentials.json"
    if local_candidate.exists():
        return local_candidate
    return root_candidate


def build_runtime_config(args: argparse.Namespace) -> dict:
    """Arma configuración de ejecución según modo PRODUCCION/TESTING."""
    is_testing = bool(args.testing)

    if args.batch_size <= 0:
        raise ValueError("--batch-size debe ser mayor a 0")
    if args.sleep_seconds < 0:
        raise ValueError("--sleep-seconds no puede ser negativo")
    if args.reset_watermark and not is_testing:
        raise ValueError("--reset-watermark solo puede usarse junto con --testing")
    if args.start_date and is_testing:
        raise ValueError("--start-date solo aplica en modo PRODUCCION")

    db_name = get_env_required("DB_NAME_TEST") if is_testing else get_env_required("DB_NAME")
    spreadsheet_id = (
        get_env_required("SPREADSHEET_ID_TEST") if is_testing else get_env_required("SPREADSHEET_ID")
    )

    def pick_db_value(test_key: str, prod_key: str, default: str = "") -> str:
        if is_testing:
            test_value = get_env_optional(test_key, "")
            if test_value:
                return test_value
        if default:
            return get_env_optional(prod_key, default)
        return get_env_required(prod_key)

    db_config = {
        "host": pick_db_value("DB_HOST_TEST", "DB_HOST"),
        "port": int(pick_db_value("DB_PORT_TEST", "DB_PORT", default="3306")),
        "user": pick_db_value("DB_USER_TEST", "DB_USER"),
        "password": pick_db_value("DB_PASSWORD_TEST", "DB_PASSWORD"),
        "database": db_name,
        "connection_timeout": 10,
        "use_pure": True,
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
        "use_unicode": True,
    }

    runtime_dir = CURRENT_DIR
    return {
        "is_testing": is_testing,
        "db_config": db_config,
        "sheet_name": get_env_optional("SHEET_NAME", "Hoja 1"),
        "spreadsheet_id": spreadsheet_id,
        "credentials_file": resolve_credentials_file(),
        "watermark_file": runtime_dir / "watermark.json",
        "log_file": runtime_dir / "etl.log",
        "batch_size": args.batch_size,
        "sleep_seconds": args.sleep_seconds,
        "dry_run": bool(args.dry_run),
        "start_date": args.start_date,
        "audit_env": "TEST" if is_testing else "PROD",
    }


def create_source_connection(db_config: dict):
    connection = mysql.connector.connect(**db_config)
    connection.set_charset_collation(charset="utf8mb4", collation="utf8mb4_unicode_ci")
    return connection


def canonicalize_header_name(value: str) -> str:
    """Normaliza encabezados para tolerar acentos y variaciones menores."""
    text = normalize_cell_value(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace(" ", "")
    aliases = {"ano": "anio"}
    return aliases.get(text, text)


def get_watermark(file_path: Path) -> int:
    """Lee watermark.json para obtener el último id procesado en TESTING."""
    try:
        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            last_id = int(data.get("last_id", 0))
            logging.info("Marca de agua leida correctamente. last_id=%s", last_id)
            return last_id

        logging.info("No existe watermark.json. Se asume last_id=0 para testing.")
        return 0
    except Exception as e:
        logging.error("Error al leer la marca de agua: %s", e, exc_info=True)
        raise


def update_watermark(new_last_id: int, file_path: Path) -> None:
    """Actualiza watermark.json con el nuevo id máximo procesado en TESTING."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as f:
            json.dump({"last_id": int(new_last_id)}, f)
        logging.info("Marca de agua actualizada correctamente. last_id=%s", new_last_id)
    except Exception as e:
        logging.error("Error al actualizar la marca de agua: %s", e, exc_info=True)
        raise


def resolve_watermark_field(connection) -> str:
    """Detecta la columna incremental disponible para testing incremental."""
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW COLUMNS FROM Seguros")
        cols = {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()

    for candidate in WATERMARK_CANDIDATES:
        if candidate in cols:
            return candidate

    raise KeyError(f"No se encontro columna incremental. Probadas: {WATERMARK_CANDIDATES}")


def extract_from_mysql_testing(last_id: int, db_config: dict) -> tuple[pd.DataFrame, str]:
    """Extrae incremental desde tabla Seguros para entorno TESTING."""
    try:
        connection = create_source_connection(db_config)
        watermark_field = resolve_watermark_field(connection)
        logging.info("Columna incremental detectada: %s", watermark_field)

        columns_query = ", ".join([watermark_field] + SHEET_COLUMNS)
        query = (
            f"SELECT {columns_query} FROM Seguros "
            f"WHERE {watermark_field} > %s ORDER BY {watermark_field} ASC"
        )
        df = pd.read_sql(query, connection, params=(last_id,))
        logging.info("Extraccion testing exitosa. Registros obtenidos=%s", len(df))
        return df, watermark_field
    except Exception as e:
        logging.error("Error al extraer datos en testing: %s", e, exc_info=True)
        raise
    finally:
        if "connection" in locals() and connection.is_connected():
            connection.close()


def extract_from_mysql_production(db_config: dict, start_date: str | None) -> pd.DataFrame:
    """Extrae datos desde Vista_Seguros para entorno PRODUCCION."""
    try:
        connection = create_source_connection(db_config)

        if start_date:
            query = (
                "SELECT * FROM Vista_Seguros "
                "WHERE fechaprereserva >= %s "
                "ORDER BY fechaprereserva ASC, prereserva ASC"
            )
            params = (start_date,)
            logging.info("Extraccion produccion en modo historico desde fecha=%s", start_date)
        else:
            current_year = datetime.now().year
            query = (
                "SELECT * FROM Vista_Seguros "
                "WHERE anio >= %s "
                "ORDER BY fechaprereserva ASC, prereserva ASC"
            )
            params = (current_year,)
            logging.info("Extraccion produccion por anio actual=%s", current_year)

        df = pd.read_sql(query, connection, params=params)
        logging.info("Extraccion produccion exitosa. Registros obtenidos=%s", len(df))
        return df
    except Exception as e:
        logging.error("Error al extraer datos en produccion: %s", e, exc_info=True)
        raise
    finally:
        if "connection" in locals() and connection.is_connected():
            connection.close()


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina duplicados por Prereserva conservando el registro mas reciente."""
    if df.empty:
        logging.info("El DataFrame esta vacio, no hay datos para procesar.")
        return df

    try:
        if "Prereserva" not in df.columns:
            raise KeyError("No se encontro la columna requerida 'Prereserva' en los datos extraidos.")

        initial_count = len(df)
        df_clean = df.drop_duplicates(subset=["Prereserva"], keep="last")
        final_count = len(df_clean)
        logging.info("Deduplicacion completada: %s -> %s", initial_count, final_count)
        return df_clean
    except Exception as e:
        logging.error("Error en la transformacion de datos: %s", e, exc_info=True)
        raise


def normalize_cell_value(value) -> str:
    """Normaliza valores para comparar y serializar hacia Google Sheets."""
    if pd.isna(value):
        return ""

    text = str(value).strip()
    if text.lower() in {"nan", "nat", "none", "null"}:
        return ""
    return text


def normalize_number_text(text: str) -> str:
    """Normaliza textos numericos para comparar valores equivalentes."""
    raw = text.strip().replace(" ", "")
    if not raw:
        return ""

    # Si viene con formato latino 51.060.000,00 -> 51060000.00
    if "." in raw and "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")

    try:
        number = Decimal(raw)
    except InvalidOperation:
        return text

    if number == number.to_integral_value():
        return str(int(number))

    normalized = format(number.normalize(), "f")
    return normalized.rstrip("0").rstrip(".")


def normalize_for_comparison(column_name: str, value) -> str:
    """Aplica normalizacion por campo para evitar updates por formato."""
    text = normalize_cell_value(value)
    if not text:
        return ""

    numeric_like_columns = {"Precioventa", "Anio", "Origen"}
    if column_name in numeric_like_columns:
        return normalize_number_text(text)

    return text


def read_sheet_snapshot(worksheet, sleep_seconds: float) -> tuple[dict, dict]:
    """Lee la hoja, valida encabezado y arma índice Prereserva->fila."""
    try:
        all_values = worksheet.get_all_values()
        if not all_values:
            logging.info("La hoja esta vacia. Se crea encabezado canonico automaticamente.")

            # Si es una hoja nueva, inicializamos la fila de encabezados.
            header_end = rowcol_to_a1(1, len(SHEET_COLUMNS))
            header_range = f"A1:{header_end}"
            execute_with_retry(
                action=lambda: worksheet.update(
                    header_range,
                    [SHEET_COLUMNS],
                    value_input_option="USER_ENTERED",
                ),
                action_name="creacion de encabezado en hoja vacia",
                sleep_seconds=sleep_seconds,
            )

            # Intentamos dejar la hoja usable para humanos desde el inicio.
            try:
                execute_with_retry(
                    action=lambda: worksheet.freeze(rows=1),
                    action_name="freeze encabezado inicial",
                    sleep_seconds=sleep_seconds,
                )
                execute_with_retry(
                    action=lambda: worksheet.set_basic_filter("A1:AA1"),
                    action_name="filtro inicial",
                    sleep_seconds=sleep_seconds,
                )
            except Exception as ui_exc:
                logging.warning(
                    "No se pudo aplicar freeze/filtro al crear encabezado inicial: %s",
                    ui_exc,
                )

            return {}, {}

        # Caso especial: hay filas en la hoja pero la fila 1 esta vacia.
        # Lo tratamos como hoja sin encabezado y la inicializamos.
        first_row = all_values[0] if all_values else []
        if not first_row or all(not normalize_cell_value(col) for col in first_row):
            logging.info(
                "Se detecto fila de encabezado vacia. Se crea encabezado canonico automaticamente."
            )

            header_end = rowcol_to_a1(1, len(SHEET_COLUMNS))
            header_range = f"A1:{header_end}"
            execute_with_retry(
                action=lambda: worksheet.update(
                    header_range,
                    [SHEET_COLUMNS],
                    value_input_option="USER_ENTERED",
                ),
                action_name="creacion de encabezado en fila 1 vacia",
                sleep_seconds=sleep_seconds,
            )

            try:
                execute_with_retry(
                    action=lambda: worksheet.freeze(rows=1),
                    action_name="freeze encabezado inicial",
                    sleep_seconds=sleep_seconds,
                )
                execute_with_retry(
                    action=lambda: worksheet.set_basic_filter("A1:AA1"),
                    action_name="filtro inicial",
                    sleep_seconds=sleep_seconds,
                )
            except Exception as ui_exc:
                logging.warning(
                    "No se pudo aplicar freeze/filtro al crear encabezado inicial: %s",
                    ui_exc,
                )

            return {}, {}

        header = [normalize_cell_value(col) for col in all_values[0]]
        expected_header = SHEET_COLUMNS
        received_canonical = [canonicalize_header_name(col) for col in header[: len(expected_header)]]
        expected_canonical = [canonicalize_header_name(col) for col in expected_header]

        if received_canonical != expected_canonical:
            raise ValueError(
                "Encabezado invalido en Google Sheets. "
                f"Esperado={expected_header} | Recibido={header[:len(expected_header)]}"
            )

        if header[: len(expected_header)] != expected_header:
            logging.info(
                "Encabezado compatible detectado con variaciones de formato/acentos. "
                "Se continua con el orden canonico interno."
            )

        index_by_prereserva = {}
        row_values_by_prereserva = {}
        duplicates = 0

        for row_number, row in enumerate(all_values[1:], start=2):
            row_27 = (row + [""] * len(SHEET_COLUMNS))[: len(SHEET_COLUMNS)]
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
                "Se detectaron %s Prereservas duplicadas en la hoja. Se usa la ultima fila encontrada.",
                duplicates,
            )

        logging.info("Estado de la hoja cargado. Filas indexadas=%s", len(index_by_prereserva))
        return index_by_prereserva, row_values_by_prereserva
    except Exception as e:
        logging.error("Error al leer snapshot de Google Sheets: %s", e, exc_info=True)
        raise


def classify_records(df: pd.DataFrame, sheet_index: dict, sheet_rows: dict) -> tuple[list, list, int]:
    """Clasifica registros en insercion, actualizacion y sin cambios."""
    if df.empty:
        return [], [], 0

    append_rows = []
    updates = []
    noop_count = 0

    for _, row in df.iterrows():
        mysql_row = [normalize_cell_value(row.get(col, "")) for col in SHEET_COLUMNS]
        prereserva = mysql_row[0]

        if not prereserva:
            logging.warning("Registro omitido: Prereserva vacio tras normalizacion.")
            continue

        if prereserva not in sheet_index:
            append_rows.append(mysql_row)
            logging.info("Prereserva %s -> INSERCION", prereserva)
            continue

        sheet_row = sheet_rows.get(prereserva, [""] * len(SHEET_COLUMNS))
        changed_fields = []
        for column_name, old_value, new_value in zip(SHEET_COLUMNS, sheet_row, mysql_row):
            old_cmp = normalize_for_comparison(column_name, old_value)
            new_cmp = normalize_for_comparison(column_name, new_value)
            if old_cmp != new_cmp:
                changed_fields.append((column_name, old_value, new_value))

        if changed_fields:
            updates.append(
                {
                    "entity_id": prereserva,
                    "row_number": sheet_index[prereserva],
                    "values": mysql_row,
                    "changes": changed_fields,
                }
            )
            logging.info("Prereserva %s -> ACTUALIZACION (%s campos)", prereserva, len(changed_fields))
        else:
            noop_count += 1
            logging.info("Prereserva %s -> SIN_CAMBIOS", prereserva)

    return append_rows, updates, noop_count


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Divide una lista en bloques para envíos seguros a la API."""
    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor a 0")
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def execute_with_retry(action, action_name: str, sleep_seconds: float, max_retries: int = 3):
    """Reintenta acciones de Google Sheets ante 429 y errores 5xx."""
    attempt = 0
    while True:
        try:
            return action()
        except APIError as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            retryable = status_code == 429 or (status_code is not None and int(status_code) >= 500)
            if not retryable or attempt >= max_retries:
                raise
            wait_time = sleep_seconds * (2**attempt)
            logging.warning(
                "%s fallo con APIError HTTP %s. Reintento %s/%s en %.1fs",
                action_name,
                status_code,
                attempt + 1,
                max_retries,
                wait_time,
            )
            time.sleep(wait_time)
            attempt += 1


def batch_update_existing_rows(worksheet, updates: list, batch_size: int, sleep_seconds: float) -> None:
    """Actualiza filas existentes en lotes seguros para la API."""
    if not updates:
        logging.info("No hay actualizaciones para ejecutar en Google Sheets.")
        return

    try:
        if len(updates) > batch_size:
            msg = (
                f"[INFO] Actualizaciones masivas detectadas ({len(updates)}). "
                f"Se particiona en lotes de {batch_size} por seguridad."
            )
            print(msg)
            logging.info(msg)

        update_batches = chunk_list(updates, batch_size)
        for idx, batch in enumerate(update_batches, start=1):
            data = []
            for item in batch:
                row_number = int(item["row_number"])
                start_a1 = rowcol_to_a1(row_number, 1)
                end_a1 = rowcol_to_a1(row_number, len(SHEET_COLUMNS))
                range_a1 = f"{start_a1}:{end_a1}"
                data.append({"range": range_a1, "values": [item["values"]]})

            execute_with_retry(
                action=lambda payload=data: worksheet.batch_update(payload, value_input_option="USER_ENTERED"),
                action_name=f"batch_update lote {idx}/{len(update_batches)}",
                sleep_seconds=sleep_seconds,
            )
            logging.info("Lote UPDATE %s/%s enviado. Filas=%s", idx, len(update_batches), len(batch))
            if idx < len(update_batches):
                time.sleep(sleep_seconds)
    except Exception as e:
        logging.error("Error al ejecutar actualizacion por lotes en Google Sheets: %s", e, exc_info=True)
        raise


def append_new_rows(worksheet, rows: list, batch_size: int, sleep_seconds: float) -> None:
    """Inserta filas nuevas en lotes seguros para la API."""
    if not rows:
        logging.info("No hay filas nuevas para insertar en Google Sheets.")
        return

    try:
        if len(rows) > batch_size:
            msg = (
                f"[INFO] Inserciones masivas detectadas ({len(rows)}). "
                f"Se particiona en lotes de {batch_size} por seguridad."
            )
            print(msg)
            logging.info(msg)

        row_batches = chunk_list(rows, batch_size)
        for idx, batch in enumerate(row_batches, start=1):
            execute_with_retry(
                action=lambda payload=batch: worksheet.append_rows(payload, value_input_option="USER_ENTERED"),
                action_name=f"append_rows lote {idx}/{len(row_batches)}",
                sleep_seconds=sleep_seconds,
            )
            logging.info("Lote APPEND %s/%s enviado. Filas=%s", idx, len(row_batches), len(batch))
            if idx < len(row_batches):
                time.sleep(sleep_seconds)
    except Exception as e:
        logging.error("Error al ejecutar insercion por lotes en Google Sheets: %s", e, exc_info=True)
        raise


def ensure_sheet_filter(worksheet, total_data_rows: int, sleep_seconds: float) -> tuple[bool, str]:
    """Aplica freeze/filtro como mejora visual sin bloquear el proceso."""
    try:
        last_row = max(1, int(total_data_rows) + 1)
        filter_range = f"A1:AA{last_row}"
        execute_with_retry(
            action=lambda: worksheet.freeze(rows=1),
            action_name="freeze encabezado",
            sleep_seconds=sleep_seconds,
        )
        execute_with_retry(
            action=lambda: worksheet.set_basic_filter(filter_range),
            action_name="set_basic_filter",
            sleep_seconds=sleep_seconds,
        )
        logging.info("UI aplicada: freeze fila 1 + filtro %s", filter_range)
        return True, ""
    except Exception as e:
        msg = (
            "No se pudo aplicar configuracion visual de la hoja (freeze/filter). "
            "Se continua porque los datos ya fueron escritos. "
            f"Detalle: {e}"
        )
        logging.warning(msg)
        return False, msg


def confirm_or_reset_watermark(args: argparse.Namespace, watermark_file: Path, current_last_id: int) -> int:
    """En testing permite reiniciar watermark con flag o confirmación interactiva."""
    if current_last_id <= 0:
        return current_last_id

    if args.reset_watermark:
        update_watermark(0, watermark_file)
        print("[INFO] Watermark reiniciado a 0 por --reset-watermark.")
        return 0

    answer = input(
        f"[PREGUNTA] El watermark actual es {current_last_id}. Deseas reiniciarlo a 0? (s/N): "
    ).strip().lower()
    if answer in {"s", "si", "y", "yes"}:
        update_watermark(0, watermark_file)
        print("[INFO] Watermark reiniciado a 0 por confirmacion de usuario.")
        return 0
    return current_last_id


def map_updates_for_audit(updates_payload: list[dict]) -> list[tuple[str, list[str]]]:
    """Convierte updates al formato genérico de auditoría."""
    mapped = []
    for item in updates_payload:
        entity_id = str(item.get("entity_id", "")).strip()
        changes = item.get("changes", [])
        change_lines = [f"{field}: '{old}' -> '{new}'" for field, old, new in changes]
        if entity_id:
            mapped.append((entity_id, change_lines))
    return mapped


def main():
    args = parse_args()
    runtime = build_runtime_config(args)
    setup_logging(runtime["log_file"])

    audit = AuditLogger(
        db_conn_factory=get_audit_db_connection_factory(env=runtime["audit_env"]),
        process_name="ETL_SEGUROS",
        process_description="Sincroniza operaciones de seguros MySQL -> Google Sheets",
        create_process_if_missing=True,
        timezone_name="America/Argentina/Buenos_Aires",
        max_chunk_chars=10_000,
    )

    warning_count = 0
    final_ok = False

    logging.info("--- Iniciando ciclo de sincronizacion ETL ---")
    print("[INFO] Iniciando ciclo ETL...")

    try:
        audit.start()
        audit.record_info(f"Modo de ejecucion={'TESTING' if runtime['is_testing'] else 'PRODUCCION'}")
        if runtime["dry_run"]:
            audit.record_info("Modo simulacro activo: no se escribira en Google Sheets")

        # 1) Configurar conexión a Google Sheets.
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials_file = runtime["credentials_file"]
        if not credentials_file.exists():
            raise FileNotFoundError(f"No se encontro archivo de credenciales: {credentials_file}")

        with credentials_file.open("r", encoding="utf-8") as f:
            cred_json = json.load(f)
            service_account_email = cred_json.get("client_email", "N/D")
        print(f"[INFO] Cuenta de servicio en uso: {service_account_email}")

        credentials = Credentials.from_service_account_file(str(credentials_file), scopes=scopes)
        gc = gspread.authorize(credentials)

        spreadsheet_id = runtime["spreadsheet_id"]
        sheet_name = runtime["sheet_name"]
        print(f"[INFO] SPREADSHEET_ID: {spreadsheet_id}")
        print(f"[INFO] SHEET_NAME: {sheet_name}")

        sh = gc.open_by_key(spreadsheet_id)
        worksheet = sh.worksheet(sheet_name)
        print("[INFO] Conexion con Google Sheets OK.")

        # Validamos encabezado al inicio. Si la hoja esta vacia, lo crea automaticamente.
        sheet_index, sheet_rows = read_sheet_snapshot(
            worksheet=worksheet,
            sleep_seconds=runtime["sleep_seconds"],
        )

        # 2) Extraer datos según entorno.
        new_last_id = None
        if runtime["is_testing"]:
            last_id = get_watermark(runtime["watermark_file"])
            last_id = confirm_or_reset_watermark(args, runtime["watermark_file"], last_id)
            print(f"[INFO] Marca de agua actual: {last_id}")

            df, watermark_field = extract_from_mysql_testing(last_id, runtime["db_config"])
            if not df.empty:
                if watermark_field not in df.columns:
                    raise KeyError(f"No se encontro la columna de marca de agua '{watermark_field}'.")
                new_last_id = int(df[watermark_field].max())
        else:
            df = extract_from_mysql_production(runtime["db_config"], runtime["start_date"])
            if runtime["start_date"]:
                audit.record_info(f"Carga historica habilitada desde {runtime['start_date']}")

        audit.set_metric("extraidos", len(df))
        print(f"[INFO] Registros extraidos desde MySQL: {len(df)}")

        if not df.empty:
            # 3) Transformar y deduplicar.
            df_clean = process_data(df)
            audit.set_metric("deduplicados", len(df_clean))
            print(f"[INFO] Registros luego de deduplicar: {len(df_clean)}")

            # 4) Clasificar contra el estado actual de la hoja.
            append_rows_payload, updates_payload, noop_count = classify_records(df_clean, sheet_index, sheet_rows)
            audit.set_metric("noop", noop_count)
            print(
                f"[INFO] Clasificacion: append={len(append_rows_payload)} "
                f"update={len(updates_payload)} noop={noop_count}"
            )

            # 5) Registrar eventos en auditoria (detalle de inserts/updates).
            append_ids = [row[0] for row in append_rows_payload if row and row[0]]
            if append_ids:
                audit.record_insert(append_ids)

            for entity_id, changes in map_updates_for_audit(updates_payload):
                audit.record_update(entity_id=entity_id, changes=changes)

            # 6) Escribir a Sheets o simular (dry-run).
            if runtime["dry_run"]:
                print("[INFO] DRY-RUN: se omite escritura en Google Sheets.")
                logging.info("Dry-run activo: no se ejecutaron operaciones de escritura en Sheets.")
            else:
                batch_update_existing_rows(
                    worksheet=worksheet,
                    updates=updates_payload,
                    batch_size=runtime["batch_size"],
                    sleep_seconds=runtime["sleep_seconds"],
                )
                append_new_rows(
                    worksheet=worksheet,
                    rows=append_rows_payload,
                    batch_size=runtime["batch_size"],
                    sleep_seconds=runtime["sleep_seconds"],
                )
                print("[INFO] Escritura a Google Sheets completada.")

                total_data_rows = len(sheet_index) + len(append_rows_payload)
                filter_ok, filter_msg = ensure_sheet_filter(
                    worksheet=worksheet,
                    total_data_rows=total_data_rows,
                    sleep_seconds=runtime["sleep_seconds"],
                )
                if not filter_ok:
                    warning_count += 1
                    audit.record_warning(filter_msg)

                if runtime["is_testing"] and new_last_id is not None:
                    update_watermark(new_last_id, runtime["watermark_file"])
                    print(f"[INFO] Marca de agua actualizada a: {new_last_id}")
        else:
            audit.set_metric("deduplicados", 0)
            audit.set_metric("noop", 0)
            logging.info("No se encontraron registros nuevos para procesar.")
            print("[INFO] No hay registros nuevos para procesar.")

        if warning_count > 0:
            audit.mark_warning()
        else:
            audit.mark_success()
        final_ok = True

        logging.info("--- Sincronizacion ETL finalizada con exito ---")
        print("[OK] ETL finalizado con exito.")

    except SpreadsheetNotFound as e:
        msg = (
            "[ERROR] Spreadsheet no encontrado (404). "
            "Valida SPREADSHEET_ID y comparte el archivo con la cuenta de servicio."
        )
        audit.mark_error(msg, exc=e)
        logging.error(msg, exc_info=True)
        print(msg)

    except WorksheetNotFound as e:
        msg = (
            "[ERROR] La hoja/pestana no existe. "
            "Revisa SHEET_NAME en .env exactamente igual al nombre de la pestana."
        )
        audit.mark_error(msg, exc=e)
        logging.error(msg, exc_info=True)
        print(msg)

    except APIError as e:
        status_code = getattr(getattr(e, "response", None), "status_code", "N/D")
        response_text = getattr(getattr(e, "response", None), "text", "")
        msg = f"[ERROR] API Google Sheets HTTP {status_code}: {e}"
        audit.mark_error(msg)
        logging.error("%s | response=%s", msg, response_text, exc_info=True)
        print(msg)
        if response_text:
            print(f"[ERROR] Detalle API: {response_text}")

    except Exception as e:
        msg = f"[ERROR] ETL abortado: {e}"
        audit.mark_error("Sincronizacion abortada por error critico", exc=e)
        logging.error("--- Sincronizacion abortada por error critico: %s ---", e, exc_info=True)
        print(msg)

    finally:
        try:
            id_ejecucion = audit.persist()
            logging.info("Auditoria persistida correctamente. id_ejecucion=%s", id_ejecucion)
            print(f"[INFO] Auditoria registrada. id_ejecucion={id_ejecucion}")
        except Exception as audit_exc:
            logging.error("No se pudo persistir la auditoria: %s", audit_exc, exc_info=True)
            print(f"[ERROR] No se pudo persistir la auditoria: {audit_exc}")

        if not final_ok:
            logging.info("El ciclo finalizo con errores. Revisar detalle de auditoria y logs locales.")


if __name__ == "__main__":
    main()
