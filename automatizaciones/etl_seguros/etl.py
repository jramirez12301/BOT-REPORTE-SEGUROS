import argparse
import concurrent.futures
import json
import logging
import os
import re
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
from core.db_utils import create_sqlserver_connection_from_config, get_audit_db_connection_factory


# Cargamos .env del proyecto para ambos modos de ejecucion.
load_dotenv(PROJECT_ROOT / ".env")

# Constantes del modelo de hoja.
WATERMARK_CANDIDATES = ["id_interno", "id"]

# Columnas canonicas de la hoja (orden final de escritura/comparacion).
# - Sucursal_origen: marca de negocio (FORD/HYUNDAI/JEEP/FIAT) derivada en ETL.
# - Marca: viene de la vista y se mostrara como "Modelo" en encabezado visible.
# - NroSucursal: se mantiene por compatibilidad historica de la hoja.
# Columnas canonicas de la hoja (orden final de escritura/comparacion).
SHEET_COLUMNS_DB = [
    "TipoVenta",
    "FechaEntrega",
    "Prereserva",
    "Rubro",
    "PrecioVenta",
    "ClienteRazonSocial",
    "CuitCuil",
    "Email",
    "Telefono",
    "Domicilio",
    "Localidad",
    "Provincia",
    "CodigoUnidad",
    "Sucursal_origen",
    "Marca",
    "MarcaModelo",
    "Anio",
    "Color",
    "Vin",
    "Patente",
    "Vendedor",
    "NroSucursal",
    "FechaPrereserva",
    "FechaVenta",
]

# Encabezados visibles en Google Sheets.
SHEET_COLUMNS_SHEET = [
    "Tipo de Venta" if col == "TipoVenta" else
    "Fecha de Entrega" if col == "FechaEntrega" else
    "Cliente - Razón Social" if col == "ClienteRazonSocial" else
    "CUIT - CUIL" if col == "CuitCuil" else
    "Año" if col == "Anio" else
    "Marca" if col == "Sucursal_origen" else
    "Modelo" if col == "Marca" else
    col
    for col in SHEET_COLUMNS_DB
]


# Mapeo de negocio por sucursal.
# La infraestructura (hosts/credenciales) vive en .env y NO se ata a marcas.
BRANCH_SOURCES = [
    {
        "sucursal_origen": "FORD",
        "host_group": 1,
        "database": "ProyautMonti",
        "view": "dbo.Vista_Seguros",
    },
    {
        "sucursal_origen": "HYUNDAI",
        "host_group": 1,
        "database": "ProyautAuto",
        "view": "dbo.Vista_Seguros",
    },
    {
        "sucursal_origen": "JEEP",
        "host_group": 2,
        "database": "ProyautLand",
        "view": "dbo.Vista_Seguros",
    },
    {
        "sucursal_origen": "FIAT",
        "host_group": 2,
        "database": "ProyautPine",
        "view": "dbo.Vista_Seguros",
    },
]


# Alias para tolerar diferencias de nombre entre origenes legacy y vista nueva.
COLUMN_ALIASES = {
    "PrecioVenta": ["PrecioVenta", "Precioventa"],
    "CuitCliente": ["CuitCliente", "Cuitcliente"],
    "CodigoUnidad": ["CodigoUnidad", "Codigounidad"],
    "MarcaModelo": ["MarcaModelo", "Marcamodelo"],
    "NroSucursal": ["NroSucursal", "Sucursal", "nrosucursal", "sucursal"],
    "FechaPrereserva": ["FechaPrereserva", "Fechaprereserva"],
    "FechaVenta": ["FechaVenta", "Fechaventa"],
    "FechaEntrega": ["FechaEntrega", "Fechaentrega"],
    "Sucursal_origen": ["Sucursal_origen", "sucursal_origen", "SUCURSAL_ORIGEN"],
}


def build_entity_key(prereserva: str, sucursal_origen: str) -> str:
    """Genera clave compuesta de negocio para evitar cruces entre sucursales."""
    return f"{normalize_cell_value(prereserva)}|{normalize_cell_value(sucursal_origen)}"


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
            "Solo en PRODUCCION. Formato YYYYMMDD para definir fecha inicial de carga. "
            "Si no se informa, se usa la fecha de hoy."
        ),
    )
    parser.add_argument(
        "--end-date",
        type=parse_start_date,
        help=(
            "Solo en PRODUCCION y junto con --start-date. Formato YYYYMMDD para definir fecha final. "
            "Si no se informa, se usa la fecha de hoy."
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
    """Valida YYYYMMDD y devuelve el mismo formato para SQL Server."""
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--start-date debe tener formato YYYYMMDD") from exc
    return parsed.strftime("%Y%m%d")


def setup_logging(log_file: Path) -> None:
    """Configura logging local para diagnóstico operativo."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    rotate_log_file_by_runs(log_file, keep_runs=3)
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        encoding="utf-8",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def rotate_log_file_by_runs(log_file: Path, keep_runs: int = 3) -> None:
    """Mantiene hasta N ejecuciones de log (actual + historicos)."""
    if keep_runs < 1:
        return

    try:
        oldest_backup_index = keep_runs - 1
        oldest_backup = log_file.with_name(f"{log_file.name}.{oldest_backup_index}")
        if oldest_backup.exists():
            oldest_backup.unlink()

        for index in range(oldest_backup_index - 1, 0, -1):
            current_backup = log_file.with_name(f"{log_file.name}.{index}")
            next_backup = log_file.with_name(f"{log_file.name}.{index + 1}")
            if current_backup.exists():
                current_backup.replace(next_backup)

        if log_file.exists():
            log_file.replace(log_file.with_name(f"{log_file.name}.1"))
    except OSError:
        # Si el log esta montado como archivo bind, rename puede fallar.
        # Fallback seguro: truncar para evitar crecimiento indefinido.
        if log_file.exists():
            log_file.write_text("", encoding="utf-8")


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


def get_env_int_optional(name: str, default: int) -> int:
    value = get_env_optional(name, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Variable de entorno invalida (int): {name}={value}") from exc


def build_sqlserver_host_config(host_group: int) -> dict:
    """Construye configuracion SQL Server por grupo de host desde .env."""
    suffix = str(host_group)
    user = get_env_optional(f"SQLSERVER_USER_{suffix}", "") or get_env_required("SQLSERVER_USER")
    password = get_env_optional(f"SQLSERVER_PASSWORD_{suffix}", "") or get_env_required(
        "SQLSERVER_PASSWORD"
    )

    driver = get_env_optional(f"SQLSERVER_DRIVER_{suffix}", "") or get_env_optional(
        "SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server"
    )
    connection_timeout = get_env_int_optional(
        f"SQLSERVER_CONNECTION_TIMEOUT_{suffix}",
        get_env_int_optional("SQLSERVER_CONNECTION_TIMEOUT", 10),
    )
    query_timeout = get_env_int_optional(
        f"SQLSERVER_QUERY_TIMEOUT_{suffix}",
        get_env_int_optional("SQLSERVER_QUERY_TIMEOUT", 60),
    )
    encrypt = get_env_optional(f"SQLSERVER_ENCRYPT_{suffix}", "") or get_env_optional(
        "SQLSERVER_ENCRYPT", "no"
    )
    trust_server_certificate = get_env_optional(
        f"SQLSERVER_TRUST_SERVER_CERTIFICATE_{suffix}",
        "",
    ) or get_env_optional("SQLSERVER_TRUST_SERVER_CERTIFICATE", "yes")

    return {
        "host_group": host_group,
        "host": get_env_required(f"SQLSERVER_HOST_{suffix}"),
        "port": get_env_int_optional(
            f"SQLSERVER_PORT_{suffix}",
            get_env_int_optional("SQLSERVER_PORT", 1433),
        ),
        "user": user,
        "password": password,
        "driver": driver,
        "timeout": connection_timeout,
        "query_timeout": query_timeout,
        "encrypt": encrypt,
        "trust_server_certificate": trust_server_certificate,
    }


def build_sqlserver_host_map() -> dict[int, dict]:
    """Define hosts productivos disponibles para extraccion por paralelo."""
    host_groups = sorted({int(source["host_group"]) for source in BRANCH_SOURCES})
    return {group: build_sqlserver_host_config(group) for group in host_groups}


def select_columns_without_source() -> list[str]:
    """Columnas explicitas de vista, excluyendo la columna derivada Sucursal_origen."""
    return [col for col in SHEET_COLUMNS_DB if col != "Sucursal_origen"]


def build_sqlserver_select_expression(column_name: str) -> str:
    """Mapea columnas canonicas a expresiones SQL Server explicitas por vista."""
    if column_name == "NroSucursal":
        # La vista no define NroSucursal en todas las bases. Se conserva columna por compatibilidad.
        return "CAST(NULL AS NVARCHAR(50)) AS [NroSucursal]"
    return f"v.[{column_name}] AS [{column_name}]"


def build_branch_query_sql(source: dict, select_columns: list[str]) -> str:
    """Arma SELECT explicito por sucursal sin usar SELECT *."""
    columns_sql = ",\n        ".join([build_sqlserver_select_expression(col) for col in select_columns])
    database = source["database"]
    view_name = source["view"]
    sucursal_origen = source["sucursal_origen"]
    return f"""
SELECT
    {columns_sql},
    '{sucursal_origen}' AS Sucursal_origen
FROM {database}.{view_name} AS v
WHERE v.FechaPrereserva >= ?
  AND v.FechaPrereserva <= ?
""".strip()


def build_host_union_query(sources: list[dict], select_columns: list[str]) -> str:
    """Combina sucursales del mismo host en un unico UNION ALL local."""
    if not sources:
        raise ValueError("No hay sucursales configuradas para el host")
    parts = [build_branch_query_sql(source, select_columns) for source in sources]
    union_sql = "\nUNION ALL\n".join(parts)
    return f"""
SELECT
    {", ".join(f"[{col}]" for col in SHEET_COLUMNS_DB)}
FROM (
{union_sql}
) AS src
""".strip()


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
    if args.end_date and is_testing:
        raise ValueError("--end-date solo aplica en modo PRODUCCION")
    if args.end_date and not args.start_date:
        raise ValueError("--end-date requiere informar tambien --start-date")

    db_name = get_env_required("DB_NAME_TEST") if is_testing else None
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

    mysql_testing_config = None
    if is_testing:
        mysql_testing_config = {
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

    runtime_dir = Path(get_env_optional("ETL_RUNTIME_DIR", str(CURRENT_DIR))).expanduser().resolve()
    runtime_dir.mkdir(parents=True, exist_ok=True)
    start_date = args.start_date
    end_date = args.end_date
    date_range_origin = "PARAMETROS" if bool(args.start_date) else "DEFAULT_HOY"
    sqlserver_host_map = None
    if not is_testing and not start_date:
        # En produccion sin --start-date, usamos fecha actual en formato YYYYMMDD.
        start_date = datetime.now().strftime("%Y%m%d")
    if not is_testing and not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    if not is_testing and start_date and end_date and start_date > end_date:
        raise ValueError("--start-date no puede ser mayor que --end-date")
    if not is_testing:
        sqlserver_host_map = build_sqlserver_host_map()

    return {
        "is_testing": is_testing,
        "mysql_testing_config": mysql_testing_config,
        "sqlserver_host_map": sqlserver_host_map,
        "sheet_name": get_env_optional("SHEET_NAME", "Hoja 1"),
        "spreadsheet_id": spreadsheet_id,
        "credentials_file": resolve_credentials_file(),
        "watermark_file": runtime_dir / "watermark.json",
        "log_file": runtime_dir / "etl.log",
        "batch_size": args.batch_size,
        "sleep_seconds": args.sleep_seconds,
        "dry_run": bool(args.dry_run),
        "start_date": start_date,
        "end_date": end_date,
        "date_range_origin": date_range_origin,
        "audit_env": "TEST" if is_testing else "PROD",
    }


def create_source_connection_mysql(db_config: dict):
    """Conexion MySQL para entorno testing."""
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
    aliases.update(
        {
            "sucursalorigen": "sucursal_origen",
        }
    )
    return aliases.get(text, text)


def validate_sheet_header(header: list[str], expected_header: list[str]) -> None:
    """Valida encabezado de hoja con compatibilidad para nombres legacy."""
    received_canonical = [canonicalize_header_name(col) for col in header[: len(expected_header)]]
    expected_canonical = [canonicalize_header_name(col) for col in expected_header]

    # Compatibilidad por posicion:
    # - Columna Sucursal_origen puede venir como Marca (nuevo) o Sucursal (legacy).
    # - Columna Marca puede venir como Modelo (nuevo) o Marca (legacy).
    # - Columna NroSucursal puede venir como NroSucursal o Sucursal (legacy).
    idx_origen = SHEET_COLUMNS_DB.index("Sucursal_origen")
    idx_marca = SHEET_COLUMNS_DB.index("Marca")
    idx_nro_sucursal = SHEET_COLUMNS_DB.index("NroSucursal")

    for idx, received in enumerate(received_canonical):
        if idx == idx_origen:
            if received not in {"marca", "sucursal_origen", "sucursal"}:
                raise ValueError(
                    "Encabezado invalido en Google Sheets. "
                    f"Columna {idx + 1} esperada Marca/Sucursal_origen y recibida='{header[idx]}'"
                )
            continue

        if idx == idx_marca:
            if received not in {"modelo", "marca"}:
                raise ValueError(
                    "Encabezado invalido en Google Sheets. "
                    f"Columna {idx + 1} esperada Modelo/Marca y recibida='{header[idx]}'"
                )
            continue

        if idx == idx_nro_sucursal:
            if received not in {"nrosucursal", "sucursal"}:
                raise ValueError(
                    "Encabezado invalido en Google Sheets. "
                    f"Columna {idx + 1} esperada NroSucursal/Sucursal y recibida='{header[idx]}'"
                )
            continue

        expected = expected_canonical[idx]
        if received != expected:
            raise ValueError(
                "Encabezado invalido en Google Sheets. "
                f"Esperado={expected_header} | Recibido={header[:len(expected_header)]}"
            )


def resolve_source_value(row: pd.Series, target_column: str) -> str:
    """Resuelve valor por columna usando aliases tolerantes entre vistas."""
    candidates = COLUMN_ALIASES.get(target_column, [target_column])
    for candidate in candidates:
        if candidate in row:
            value = normalize_cell_value(row.get(candidate, ""))
            if target_column == "PrecioVenta":
                return normalize_number_text(value)
            return value
    return ""


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
        connection = create_source_connection_mysql(db_config)
        watermark_field = resolve_watermark_field(connection)
        logging.info("Columna incremental detectada: %s", watermark_field)

        cursor = connection.cursor()
        try:
            cursor.execute("SHOW COLUMNS FROM Seguros")
            available_cols = {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()

        select_parts = [f"`{watermark_field}`"]
        for target_col in SHEET_COLUMNS_DB:
            candidates = COLUMN_ALIASES.get(target_col, [target_col])
            source_col = next((candidate for candidate in candidates if candidate in available_cols), None)

            if source_col:
                if source_col == target_col:
                    select_parts.append(f"`{target_col}`")
                else:
                    select_parts.append(f"`{source_col}` AS `{target_col}`")
                continue

            if target_col == "Sucursal_origen":
                # En testing puede no existir columna de origen; usamos valor fijo para clave compuesta.
                select_parts.append("'TEST' AS `Sucursal_origen`")
                continue

            # Mantiene shape estable del DataFrame en testing aunque falte alguna columna en origen.
            select_parts.append(f"NULL AS `{target_col}`")

        columns_query = ", ".join(select_parts)
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


def group_sources_by_host() -> dict[int, list[dict]]:
    """Agrupa el mapeo de negocio por host para consultas concurrentes."""
    grouped: dict[int, list[dict]] = {}
    for source in BRANCH_SOURCES:
        host_group = int(source["host_group"])
        grouped.setdefault(host_group, []).append(source)
    return grouped


def extract_host_group(
    host_group: int,
    host_config: dict,
    sources: list[dict],
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, list[dict], str]:
    """Extrae datos de un host usando UNION ALL local para sus sucursales."""
    select_columns = select_columns_without_source()
    query = build_host_union_query(sources=sources, select_columns=select_columns)
    params = []
    for _ in sources:
        params.extend([start_date, end_date])

    source_summaries = []
    source_names = ",".join(source["sucursal_origen"] for source in sources)
    host_label = f"host_group={host_group} host={host_config['host']}"

    try:
        connection = create_sqlserver_connection_from_config(host_config)
        cursor = connection.cursor()
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        host_df = pd.DataFrame.from_records(rows, columns=columns)

        for source in sources:
            sucursal = source["sucursal_origen"]
            source_count = int((host_df["Sucursal_origen"] == sucursal).sum()) if not host_df.empty else 0
            source_summaries.append(
                {
                    "host_group": host_group,
                    "host": host_config["host"],
                    "sucursal_origen": sucursal,
                    "database": source["database"],
                    "rows": source_count,
                }
            )

        logging.info(
            "Extraccion SQL Server %s OK. Sucursales=%s registros=%s",
            host_label,
            source_names,
            len(host_df),
        )
        return host_df, source_summaries, host_label
    finally:
        if "connection" in locals():
            try:
                if "cursor" in locals():
                    cursor.close()
                connection.close()
            except Exception:
                pass


def extract_from_sqlserver_production(
    start_date: str | None,
    end_date: str | None,
    host_map: dict[int, dict],
) -> tuple[pd.DataFrame, list[dict], list[str]]:
    """Extrae datos productivos en paralelo por host y unifica resultados."""
    if not start_date or not end_date:
        raise ValueError("El rango de fechas para produccion no puede estar vacio")

    grouped_sources = group_sources_by_host()
    if not grouped_sources:
        raise ValueError("No hay sucursales configuradas para extraccion productiva")

    extracted_frames: list[pd.DataFrame] = []
    source_summaries: list[dict] = []
    source_errors: list[str] = []

    max_workers = min(2, len(grouped_sources))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for host_group, sources in grouped_sources.items():
            host_config = host_map.get(host_group)
            if not host_config:
                raise ValueError(f"No hay configuracion de host para host_group={host_group}")

            source_names = ",".join(source["sucursal_origen"] for source in sources)
            future = executor.submit(
                extract_host_group,
                host_group,
                host_config,
                sources,
                start_date,
                end_date,
            )
            future_map[future] = (host_group, host_config["host"], source_names)

        for future in concurrent.futures.as_completed(future_map):
            host_group, host, source_names = future_map[future]
            try:
                host_df, host_summaries, _host_label = future.result()
                extracted_frames.append(host_df)
                source_summaries.extend(host_summaries)
            except Exception as exc:
                error_msg = (
                    f"[SOURCE_ERROR] host_group={host_group} host={host} "
                    f"sucursales={source_names} error={type(exc).__name__}: {exc}"
                )
                source_errors.append(error_msg)
                logging.error(error_msg, exc_info=True)

    if not extracted_frames:
        if source_errors:
            details = summarize_source_errors(source_errors)
            if is_missing_odbc_driver_error(source_errors):
                raise RuntimeError(
                    "Extraccion productiva fallida en todos los hosts. "
                    "Causa probable: no hay driver ODBC de SQL Server instalado en Linux "
                    "(pyodbc.drivers() vacio). "
                    f"Detalle: {details}"
                )
            raise RuntimeError(
                "Extraccion productiva fallida en todos los hosts. "
                f"Detalle: {details}"
            )
        return pd.DataFrame(), source_summaries, source_errors

    final_df = pd.concat(extracted_frames, ignore_index=True)
    # Orden obligatorio para mantener insercion cronologica tras concat concurrente.
    final_df = final_df.sort_values(
        by=["FechaPrereserva", "Prereserva", "Sucursal_origen"],
        kind="mergesort",
    ).reset_index(drop=True)
    return final_df, source_summaries, source_errors


def summarize_source_errors(source_errors: list[str], max_items: int = 2) -> str:
    """Resume errores por host para mensajes operativos cortos."""
    if not source_errors:
        return ""
    compact = source_errors[:max_items]
    remaining = len(source_errors) - len(compact)
    suffix = "" if remaining <= 0 else f" ... (+{remaining} mas)"
    return " | ".join(compact) + suffix


def is_missing_odbc_driver_error(source_errors: list[str]) -> bool:
    """Detecta falla por ausencia de driver ODBC SQL Server."""
    if not source_errors:
        return False
    text = " ".join(source_errors).lower()
    return (
        "no se encontro un driver odbc compatible para sql server" in text
        or "drivers instalados: []" in text
    )


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina duplicados por clave compuesta conservando el registro mas reciente."""
    if df.empty:
        logging.info("El DataFrame esta vacio, no hay datos para procesar.")
        return df

    try:
        if "Prereserva" not in df.columns:
            raise KeyError("No se encontro la columna requerida 'Prereserva' en los datos extraidos.")

        origen_candidates = COLUMN_ALIASES.get("Sucursal_origen", ["Sucursal_origen"])
        origen_col = next((col for col in origen_candidates if col in df.columns), None)
        if not origen_col:
            raise KeyError(
                "No se encontro la columna requerida 'Sucursal_origen' en los datos extraidos."
            )

        initial_count = len(df)
        df_clean = df.drop_duplicates(subset=["Prereserva", origen_col], keep="last")
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

    # Limpia simbolos comunes de moneda/etiquetas para comparar valor real.
    raw = raw.upper().replace("ARS", "").replace("$", "")
    raw = re.sub(r"[^0-9,\.\-]", "", raw)
    if not raw or raw in {"-", ".", ","}:
        return ""

    # Resolver separadores para formatos latinos y anglos.
    if "." in raw and "," in raw:
        last_dot = raw.rfind(".")
        last_comma = raw.rfind(",")
        if last_comma > last_dot:
            # Formato latino: 50.160.000,00
            raw = raw.replace(".", "").replace(",", ".")
        else:
            # Formato anglo: 50,160,000.00
            raw = raw.replace(",", "")
    elif "," in raw:
        # Solo comas: puede ser miles o decimal.
        if re.match(r"^-?\d{1,3}(,\d{3})+$", raw):
            raw = raw.replace(",", "")
        else:
            raw = raw.replace(",", ".")
    elif "." in raw:
        # Solo puntos: puede ser miles o decimal.
        if re.match(r"^-?\d{1,3}(\.\d{3})+$", raw):
            raw = raw.replace(".", "")

    try:
        number = Decimal(raw)
    except InvalidOperation:
        return text

    if number == number.to_integral_value():
        return str(int(number))

    return str(int(round(number)))


def normalize_for_comparison(column_name: str, value) -> str:
    """Aplica normalizacion por campo para evitar updates por formato."""
    text = normalize_cell_value(value)
    if not text:
        return ""

    numeric_like_columns = {"PrecioVenta", "Anio"}
    if column_name in numeric_like_columns:
        return normalize_number_text(text)

    # Algunas columnas llegan con ceros a la izquierda desde BD
    # (ej: CodigoUnidad=00061256, NroSucursal=01), pero Sheets suele mostrarlas sin cero.
    # Para evitar updates innecesarios, comparamos estas columnas por valor numerico.
    leading_zero_columns = {"CodigoUnidad", "NroSucursal", "MarcaModelo"}
    if column_name in leading_zero_columns and text.isdigit():
        return str(int(text))

    return text


def read_sheet_snapshot(worksheet, sleep_seconds: float) -> tuple[dict, dict]:
    """Lee la hoja, valida encabezado y arma índice Prereserva->fila."""
    try:
        all_values = worksheet.get_all_values()
        if not all_values:
            logging.info("La hoja esta vacia. Se crea encabezado canonico automaticamente.")

            # Si es una hoja nueva, inicializamos la fila de encabezados.
            header_end = rowcol_to_a1(1, len(SHEET_COLUMNS_SHEET))
            header_range = f"A1:{header_end}"
            execute_with_retry(
                action=lambda: worksheet.update(
                    range_name=header_range,
                    values=[SHEET_COLUMNS_SHEET],
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
                filter_end = rowcol_to_a1(1, len(SHEET_COLUMNS_SHEET))
                execute_with_retry(
                    action=lambda: worksheet.set_basic_filter(f"A1:{filter_end}"),
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

            header_end = rowcol_to_a1(1, len(SHEET_COLUMNS_SHEET))
            header_range = f"A1:{header_end}"
            execute_with_retry(
                action=lambda: worksheet.update(
                    range_name=header_range,
                    values=[SHEET_COLUMNS_SHEET],
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
                filter_end = rowcol_to_a1(1, len(SHEET_COLUMNS_SHEET))
                execute_with_retry(
                    action=lambda: worksheet.set_basic_filter(f"A1:{filter_end}"),
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
        expected_header = SHEET_COLUMNS_SHEET
        validate_sheet_header(header, expected_header)

        if header[: len(expected_header)] != expected_header:
            logging.info(
                "Encabezado compatible detectado con variaciones de formato/acentos. "
                "Se continua con el orden canonico interno."
            )

        index_by_entity = {}
        row_values_by_entity = {}
        duplicates = 0

        for row_number, row in enumerate(all_values[1:], start=2):
            row_27 = (row + [""] * len(SHEET_COLUMNS_SHEET))[: len(SHEET_COLUMNS_SHEET)]
            normalized_row = [normalize_cell_value(value) for value in row_27]
            prereserva = normalized_row[0]
            sucursal_origen = normalized_row[SHEET_COLUMNS_DB.index("Sucursal_origen")]
            entity_key = build_entity_key(prereserva, sucursal_origen)

            if not prereserva or not sucursal_origen:
                continue
            if entity_key in index_by_entity:
                duplicates += 1

            index_by_entity[entity_key] = row_number
            row_values_by_entity[entity_key] = normalized_row

        if duplicates > 0:
            logging.warning(
                "Se detectaron %s claves compuestas duplicadas en la hoja. Se usa la ultima fila encontrada.",
                duplicates,
            )

        logging.info("Estado de la hoja cargado. Filas indexadas=%s", len(index_by_entity))
        return index_by_entity, row_values_by_entity
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
        mysql_row = [resolve_source_value(row, col) for col in SHEET_COLUMNS_DB]
        prereserva = mysql_row[0]
        sucursal_origen = mysql_row[SHEET_COLUMNS_DB.index("Sucursal_origen")]
        entity_key = build_entity_key(prereserva, sucursal_origen)

        if not prereserva:
            logging.warning("Registro omitido: Prereserva vacio tras normalizacion.")
            continue
        if not sucursal_origen:
            logging.warning("Registro omitido: Sucursal_origen vacio tras normalizacion.")
            continue

        if entity_key not in sheet_index:
            append_rows.append(mysql_row)
            logging.info("Entidad %s -> INSERCION", entity_key)
            continue

        sheet_row = sheet_rows.get(entity_key, [""] * len(SHEET_COLUMNS_SHEET))
        changed_fields = []
        for column_name, old_value, new_value in zip(SHEET_COLUMNS_DB, sheet_row, mysql_row):
            old_cmp = normalize_for_comparison(column_name, old_value)
            new_cmp = normalize_for_comparison(column_name, new_value)
            if old_cmp != new_cmp:
                changed_fields.append((column_name, old_value, new_value))

        if changed_fields:
            updates.append(
                {
                    "entity_id": entity_key,
                    "row_number": sheet_index[entity_key],
                    "values": mysql_row,
                    "changes": changed_fields,
                }
            )
            logging.info("Entidad %s -> ACTUALIZACION (%s campos)", entity_key, len(changed_fields))
        else:
            noop_count += 1
            logging.info("Entidad %s -> SIN_CAMBIOS", entity_key)

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
                end_a1 = rowcol_to_a1(row_number, len(SHEET_COLUMNS_SHEET))
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
        filter_end = rowcol_to_a1(last_row, len(SHEET_COLUMNS_SHEET))
        filter_range = f"A1:{filter_end}"
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
        if not runtime["is_testing"]:
            date_range_info = (
                f"FechaPrereserva desde={runtime['start_date']} "
                f"hasta={runtime['end_date']} "
                f"origen={runtime['date_range_origin']}"
            )
            logging.info("Rango productivo aplicado: %s", date_range_info)
            print(f"[INFO] Rango productivo aplicado: {date_range_info}")
            audit.record_info(f"Rango productivo aplicado: {date_range_info}")
            audit.record_detail_line(
                "[RANGO_CARGA] "
                "campo=FechaPrereserva "
                f"desde={runtime['start_date']} "
                f"hasta={runtime['end_date']} "
                f"origen={runtime['date_range_origin']}"
            )

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

            df, watermark_field = extract_from_mysql_testing(last_id, runtime["mysql_testing_config"])
            if not df.empty:
                if watermark_field not in df.columns:
                    raise KeyError(f"No se encontro la columna de marca de agua '{watermark_field}'.")
                new_last_id = int(df[watermark_field].max())
        else:
            grouped_sources = group_sources_by_host()
            for host_group, sources in sorted(grouped_sources.items()):
                host_cfg = runtime["sqlserver_host_map"][host_group]
                source_names = ",".join(source["sucursal_origen"] for source in sources)
                source_dbs = ",".join(source["database"] for source in sources)
                audit.record_detail_line(
                    "[SOURCE_START] "
                    f"host_group={host_group} host={host_cfg['host']} "
                    f"sucursales={source_names} databases={source_dbs} "
                    f"rango={runtime['start_date']}->{runtime['end_date']}"
                )
            try:
                df, source_summaries, source_errors = extract_from_sqlserver_production(
                    start_date=runtime["start_date"],
                    end_date=runtime["end_date"],
                    host_map=runtime["sqlserver_host_map"],
                )
            except Exception as sql_exc:
                sql_exc_text = str(sql_exc).lower()
                if (
                    "driver odbc" in sql_exc_text
                    or "drivers instalados: []" in sql_exc_text
                    or "pyodbc.drivers() vacio" in sql_exc_text
                ):
                    sql_msg = (
                        "Error operativo SQL Server en extraccion de produccion. "
                        "No se detecta driver ODBC en el entorno Linux. "
                        "Instala msodbcsql18 y unixODBC en el servidor/contenedor."
                    )
                else:
                    sql_msg = (
                        "Error operativo SQL Server en extraccion de produccion. "
                        "Verifica acceso, nombre de base y permisos de lectura."
                    )
                audit.record_error(f"{sql_msg} Detalle: {sql_exc}")
                raise

            for source_summary in source_summaries:
                audit.record_detail_line(
                    "[SOURCE_SUMMARY] "
                    f"host_group={source_summary['host_group']} "
                    f"host={source_summary['host']} "
                    f"sucursal={source_summary['sucursal_origen']} "
                    f"database={source_summary['database']} "
                    f"extraidos={source_summary['rows']}"
                )

            if source_errors:
                warning_count += len(source_errors)
                for source_error in source_errors:
                    audit.record_detail_line(source_error)
                    audit.record_warning(source_error)

        audit.set_metric("extraidos", len(df))
        print(f"[INFO] Registros extraidos desde origen: {len(df)}")

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
            idx_prereserva = SHEET_COLUMNS_DB.index("Prereserva")
            idx_origen = SHEET_COLUMNS_DB.index("Sucursal_origen")
            append_ids = [
                build_entity_key(row[idx_prereserva], row[idx_origen])
                for row in append_rows_payload
                if row and row[idx_prereserva] and row[idx_origen]
            ]
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
