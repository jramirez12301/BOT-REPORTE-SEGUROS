import argparse
import json
import logging
import os
import random
import re
import string
import sys
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

try:
    from faker import Faker
except Exception:  # pragma: no cover
    Faker = None


CANONICAL_COLUMNS = [
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

NUMERIC_COLUMNS = {"Prereserva", "Rubro", "Telefono", "Codigounidad", "Anio", "Sucursal", "Origen"}
STATUS_FLOW = ["Asignado", "Autorizado", "Facturada", "Turno Entrega", "Entregada"]
TABLE_NAME = "Seguros"
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOGGER = logging.getLogger("simulador_concesionaria")
ACTION_TEXT = {
    "append": "registro nuevo",
    "update": "actualizacion detectada",
    "noop": "sin cambios",
}


def setup_logging(output_dir: Path) -> Path:
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "simulador.log"

    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    LOGGER.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    LOGGER.addHandler(sh)

    return log_file


def now_date() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def iso_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def build_empty_event() -> Dict[str, str]:
    return {col: "" for col in CANONICAL_COLUMNS}


def random_plate(rng: random.Random) -> str:
    if rng.random() < 0.5:
        return "".join(rng.choices(string.ascii_uppercase, k=2)) + "".join(rng.choices(string.digits, k=3)) + "".join(rng.choices(string.ascii_uppercase, k=2))
    return "".join(rng.choices(string.ascii_uppercase, k=3)) + "".join(rng.choices(string.digits, k=3))


def random_vin(rng: random.Random) -> str:
    chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    return "".join(rng.choices(chars, k=17))


def mutate_typo(value: str, rng: random.Random) -> str:
    text = normalize_text(value)
    if not text:
        return text
    idx = rng.randrange(len(text))
    replacement = rng.choice(string.ascii_uppercase)
    return text[:idx] + replacement + text[idx + 1 :]


def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    for col in CANONICAL_COLUMNS:
        if col in df.columns:
            out[col] = df[col].map(normalize_text)
        else:
            out[col] = ""
    return out


def create_faker_new_event(fake: Faker, rng: random.Random, seq: int, used_prereservas: set) -> Dict[str, str]:
    event = build_empty_event()

    while True:
        pr = str(26000000 + seq + rng.randint(0, 99999))
        if pr not in used_prereservas:
            used_prereservas.add(pr)
            break

    event.update(
        {
            "Prereserva": pr,
            "Rubro": str(rng.choice([11, 12, 13, 14, 19, 53, 76])),
            "Precioventa": f"{rng.randint(23000000, 78000000)}",
            "Cliente": fake.name().upper(),
            "Cuitcliente": f"{rng.randint(20, 33)}-{rng.randint(10000000, 99999999)}-{rng.randint(0, 9)}",
            "Email": fake.email().upper(),
            "Telefono": str(rng.randint(540000000000, 549999999999)),
            "Domicilio": fake.street_address().upper(),
            "Localidad": fake.city().upper(),
            "Provincia": fake.province().upper(),
            "Codigounidad": str(rng.randint(100, 99999)),
            "Marca": rng.choice(["Pinerolo", "Montironi", "Automont"]),
            "Marcamodelo": rng.choice(
                [
                    "1253-FIAT FASTBACK TURBO 270 AT MY 26",
                    "1211-FIAT CRONOS PRECISION 1.3 GSE CVT",
                    "4829-FORD TERRITORY TREND 1.5L HIBRIDA AT",
                    "0804-HYUNDAI HB20 PLATINUM SAFETY AT 1.6",
                ]
            ),
            "Anio": str(rng.choice([2025, 2026])),
            "Color": rng.choice(["GRIS SILVERSTONE", "BLANCO OXFORD", "NEGRO", "PLATA BARI"]),
            "Vin": "" if rng.random() < 0.55 else random_vin(rng),
            "Patente": "" if rng.random() < 0.75 else random_plate(rng),
            "Vendedor": fake.name().upper(),
            "Sucursal": str(rng.choice([0, 1, 3, 6, 13, 32])),
            "Origen": "0",
            "Estado": "Ingresado",
            "Estadoavance": "Asignado",
            "Fechaprereserva": now_date(),
            "Fechaventa": "",
            "Fechaentrega": "",
            "Fechapatentamiento": "",
            "Fechaproceso": now_date(),
        }
    )
    return event


def evolve_event(prev: Dict[str, str], rng: random.Random) -> Dict[str, str]:
    event = deepcopy(prev)
    current = event.get("Estadoavance", "Asignado")
    try:
        idx = STATUS_FLOW.index(current)
    except ValueError:
        idx = 0

    if idx < len(STATUS_FLOW) - 1:
        event["Estadoavance"] = STATUS_FLOW[idx + 1]

    if event["Estadoavance"] in {"Facturada", "Turno Entrega", "Entregada"} and not event.get("Fechaventa"):
        event["Fechaventa"] = now_date()
    if event["Estadoavance"] in {"Turno Entrega", "Entregada"} and not event.get("Fechaentrega"):
        event["Fechaentrega"] = now_date()
    if event["Estadoavance"] == "Entregada" and not event.get("Fechapatentamiento"):
        event["Fechapatentamiento"] = now_date()

    if rng.random() < 0.15:
        mutation = rng.choice(["vin", "patente", "vendedor", "cliente"])
        if mutation == "vin":
            event["Vin"] = event["Vin"] or random_vin(rng)
        elif mutation == "patente":
            event["Patente"] = event["Patente"] or random_plate(rng)
        elif mutation == "vendedor":
            event["Vendedor"] = mutate_typo(event.get("Vendedor", ""), rng)
        elif mutation == "cliente":
            event["Cliente"] = mutate_typo(event.get("Cliente", ""), rng)

    event["Fechaproceso"] = now_date()
    return event


def build_faker_stream(new_count: int, repeat_count: int, seed: Optional[int]) -> List[Tuple[Dict[str, str], bool]]:
    if Faker is None:
        raise RuntimeError("Faker no está instalado. Ejecuta: pip install Faker")

    rng = random.Random(seed)
    Faker.seed(seed)
    fake = Faker("es_AR")

    used_prereservas = set()
    latest_by_prereserva: Dict[str, Dict[str, str]] = {}
    stream: List[Tuple[Dict[str, str], bool]] = []

    for i in range(new_count):
        event = create_faker_new_event(fake, rng, i, used_prereservas)
        latest_by_prereserva[event["Prereserva"]] = event
        stream.append((event, False))

    keys = list(latest_by_prereserva.keys())
    for _ in range(repeat_count):
        pr = rng.choice(keys)
        evolved = evolve_event(latest_by_prereserva[pr], rng)
        latest_by_prereserva[pr] = evolved
        stream.append((evolved, True))

    return stream


def load_replay_rows(path: Path) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró archivo replay: {path}")

    if path.suffix.lower() in {".xlsx", ".xls"}:
        LOGGER.info("Leyendo replay Excel: %s", path)
        df = pd.read_excel(path, dtype=str, keep_default_na=False, na_filter=False)
        df = ensure_required_columns(df)
        rows = [row for row in df.to_dict(orient="records") if normalize_text(row.get("Prereserva", "")) != ""]
        replay_meta = {
            "encoding": "excel-native",
            "strategy": "read_excel",
            "read_attempts": 1,
            "replay_path": str(path),
        }
        return rows, replay_meta

    # Priorizamos cp1252/latin1 por ser exportes legacy en Windows.
    encodings = ["cp1252", "latin1", "utf-8-sig", "utf-8"]
    parse_strategies = [
        {"sep": ",", "engine": "python"},
        {"sep": None, "engine": "python"},
        {"sep": ";", "engine": "python"},
        {"sep": "|", "engine": "python"},
        {"sep": None, "engine": "python", "on_bad_lines": "skip"},
    ]

    last_error = None
    df = None
    attempts = 0
    selected_encoding = None
    selected_strategy = None
    for enc in encodings:
        for strategy in parse_strategies:
            attempts += 1
            try:
                df = pd.read_csv(
                    path,
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                    encoding=enc,
                    **strategy,
                )
                LOGGER.info("Replay leído OK | archivo=%s | encoding=%s | strategy=%s", path.name, enc, strategy)
                selected_encoding = enc
                selected_strategy = strategy
                break
            except Exception as exc:
                last_error = exc
                LOGGER.warning("Falló lectura replay | archivo=%s | encoding=%s | strategy=%s | error=%s", path.name, enc, strategy, exc)

        if df is not None:
            break

    if df is None:
        raise RuntimeError(f"No se pudo leer replay con encodings {encodings}: {last_error}")

    df = ensure_required_columns(df)
    rows = [row for row in df.to_dict(orient="records") if normalize_text(row.get("Prereserva", "")) != ""]
    replay_meta = {
        "encoding": selected_encoding,
        "strategy": selected_strategy,
        "read_attempts": attempts,
        "replay_path": str(path),
    }
    return rows, replay_meta


def build_replay_stream(
    path: Path, new_count: int, repeat_count: int, seed: Optional[int]
) -> Tuple[List[Tuple[Dict[str, str], bool]], Dict[str, object]]:
    rng = random.Random(seed)
    rows, replay_meta = load_replay_rows(path)
    if not rows:
        raise RuntimeError("El replay no contiene filas utilizables con Prereserva.")

    unique_ids: List[str] = []
    seen = set()
    for row in rows:
        pr = row["Prereserva"]
        if pr not in seen:
            seen.add(pr)
            unique_ids.append(pr)

    if len(unique_ids) < new_count:
        raise RuntimeError(
            f"No hay suficientes Prereservas únicas en replay. Solicitadas={new_count}, disponibles={len(unique_ids)}"
        )

    selected_ids = set(rng.sample(unique_ids, new_count))
    stream: List[Tuple[Dict[str, str], bool]] = []
    emitted_new = set()
    emitted_repeat = 0
    latest_by_id: Dict[str, Dict[str, str]] = {}

    for row in rows:
        pr = row["Prereserva"]
        if pr not in selected_ids:
            continue
        if pr not in emitted_new:
            emitted_new.add(pr)
            event = deepcopy(row)
            stream.append((event, False))
            latest_by_id[pr] = event
        elif emitted_repeat < repeat_count:
            event = deepcopy(row)
            stream.append((event, True))
            emitted_repeat += 1
            latest_by_id[pr] = event

        if len(emitted_new) == new_count and emitted_repeat == repeat_count:
            break

    while emitted_repeat < repeat_count:
        pr = rng.choice(list(selected_ids))
        base = latest_by_id.get(pr)
        if base is None:
            continue
        evolved = evolve_event(base, rng)
        stream.append((evolved, True))
        emitted_repeat += 1
        latest_by_id[pr] = evolved

    return stream, replay_meta


def clean_numeric(value: str) -> Optional[str]:
    text = normalize_text(value)
    if text == "":
        return None
    digits = re.sub(r"[^0-9]", "", text)
    return digits if digits else None


def prepare_db_row(event: Dict[str, str]) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for col in CANONICAL_COLUMNS:
        value = normalize_text(event.get(col, ""))
        if col in NUMERIC_COLUMNS:
            out[col] = clean_numeric(value)
        else:
            out[col] = value if value != "" else None
    return out


def connect_db():
    load_dotenv()
    return mysql.connector.connect(
        host=os.getenv("DB_HOST_TEST", "localhost"),
        port=int(os.getenv("DB_PORT_TEST", "3306")),
        user=os.getenv("DB_USER_TEST"),
        password=os.getenv("DB_PASSWORD_TEST"),
        database=os.getenv("DB_NAME_TEST"),
        connection_timeout=10,
        use_pure=True,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        use_unicode=True,
    )


def ensure_table(cursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            Prereserva BIGINT NULL,
            Rubro INT NULL,
            Precioventa VARCHAR(50) NULL,
            Cliente VARCHAR(255) NULL,
            Cuitcliente VARCHAR(50) NULL,
            Email VARCHAR(255) NULL,
            Telefono BIGINT NULL,
            Domicilio VARCHAR(255) NULL,
            Localidad VARCHAR(255) NULL,
            Provincia VARCHAR(255) NULL,
            Codigounidad BIGINT NULL,
            Marca VARCHAR(100) NULL,
            Marcamodelo VARCHAR(255) NULL,
            Anio INT NULL,
            Color VARCHAR(120) NULL,
            Vin VARCHAR(80) NULL,
            Patente VARCHAR(30) NULL,
            Vendedor VARCHAR(255) NULL,
            Sucursal INT NULL,
            Origen INT NULL,
            Estado VARCHAR(120) NULL,
            Estadoavance VARCHAR(120) NULL,
            Fechaprereserva VARCHAR(50) NULL,
            Fechaventa VARCHAR(50) NULL,
            Fechaentrega VARCHAR(50) NULL,
            Fechapatentamiento VARCHAR(50) NULL,
            Fechaproceso VARCHAR(50) NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def reset_table_hard() -> None:
    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        ensure_table(cursor)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def insert_event(cursor, event: Dict[str, Optional[str]]) -> None:
    columns_sql = ", ".join(CANONICAL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(CANONICAL_COLUMNS))
    query = f"INSERT INTO {TABLE_NAME} ({columns_sql}) VALUES ({placeholders})"
    values = [event.get(c) for c in CANONICAL_COLUMNS]
    cursor.execute(query, values)


def compare_events(previous: Dict[str, str], current: Dict[str, str]) -> List[Dict[str, str]]:
    changes: List[Dict[str, str]] = []
    for field in CANONICAL_COLUMNS:
        old = normalize_text(previous.get(field, ""))
        new = normalize_text(current.get(field, ""))
        if old != new:
            changes.append({"field": field, "old": old, "new": new})
    return changes


def compute_expected_changes(stream: List[Tuple[Dict[str, str], bool]]) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    last_by_prereserva: Dict[str, Dict[str, str]] = {}
    records: List[Dict[str, object]] = []
    counts = {"append": 0, "update": 0, "noop": 0}

    for seq, (event, is_repeat) in enumerate(stream, start=1):
        pr = normalize_text(event.get("Prereserva", ""))
        previous = last_by_prereserva.get(pr)
        if previous is None:
            action = "append"
            changes = []
        else:
            changes = compare_events(previous, event)
            action = "update" if changes else "noop"

        counts[action] += 1
        record = {
            "seq": seq,
            "prereserva": pr,
            "is_repeat_input": bool(is_repeat),
            "expected_action": action,
            "accion_esperada_texto": ACTION_TEXT[action],
            "changed_fields_count": len(changes),
            "changes": changes,
            "estadoavance_actual": normalize_text(event.get("Estadoavance", "")),
            "cliente": normalize_text(event.get("Cliente", "")),
            "marca": normalize_text(event.get("Marca", "")),
            "marcamodelo": normalize_text(event.get("Marcamodelo", "")),
            "anio": normalize_text(event.get("Anio", "")),
        }
        records.append(record)
        last_by_prereserva[pr] = deepcopy(event)

    return records, counts


def print_expected_change_console(record: Dict[str, object]) -> None:
    pr = record["prereserva"]
    action = record["expected_action"]
    text = record["accion_esperada_texto"]
    changes = record["changes"]

    print(f"[QA] Prereserva {pr} | accion esperada: {action} ({text}) | cambios: {len(changes)}")
    for ch in changes:
        print(f"[QA]   - Campo '{ch['field']}': '{ch['old']}' -> '{ch['new']}'")


def write_expected_changes_file(output_dir: Path, run_id: str, args: argparse.Namespace, expected_records: List[Dict[str, object]], counts: Dict[str, int]) -> Path:
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    path = output_dir / f"expected_changes_{run_id}.json"
    payload = {
        "run_id": run_id,
        "created_at": iso_now(),
        "config": {
            "source": args.source,
            "mode": args.mode,
            "new_count": args.new_count,
            "repeat_count": args.repeat_count,
            "delay": args.delay,
            "seed": args.seed,
            "max_events": args.max_events,
            "reset_table": bool(args.reset_table),
            "replay_file": args.replay_file,
        },
        "summary": {
            "total_simulados": len(expected_records),
            "total_expected_appends": counts["append"],
            "total_expected_updates": counts["update"],
            "total_expected_noops": counts["noop"],
        },
        "expected_changes": expected_records,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def escape_md(text: object) -> str:
    value = normalize_text(text)
    value = value.replace("|", "\\|")
    return value


def write_qa_markdown_summary(
    output_dir: Path,
    run_id: str,
    args: argparse.Namespace,
    input_meta: Dict[str, object],
    expected_records: List[Dict[str, object]],
    counts: Dict[str, int],
) -> Path:
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    md_path = output_dir / f"qa_summary_{run_id}.md"

    update_entries: List[Tuple[str, int, str]] = []
    for record in expected_records:
        if record.get("expected_action") != "update":
            continue
        pr = escape_md(record.get("prereserva", ""))
        cliente = escape_md(record.get("cliente", ""))
        seq = int(record.get("seq", 0))
        for ch in record.get("changes", []):
            field = escape_md(ch.get("field", ""))
            old = escape_md(ch.get("old", ""))
            new = escape_md(ch.get("new", ""))
            update_entries.append((pr, seq, f"| {pr} | {cliente} | {field} | {old} | {new} |"))

    # Ordenamos por Prereserva y luego por secuencia para mantener evolución de estados agrupada.
    sorted_updates = sorted(update_entries, key=lambda item: (item[0], item[1]))
    update_rows: List[str] = []
    last_pr = None
    for pr, _, row in sorted_updates:
        if last_pr is not None and pr != last_pr:
            update_rows.append("|  |  |  |  |  |")
        update_rows.append(row)
        last_pr = pr

    lines = [
        f"# QA Summary - {run_id}",
        "",
        "## Encabezado",
        f"- Fecha: {iso_now()}",
        f"- Source: {args.source}",
        f"- Mode: {args.mode}",
        f"- new_count: {args.new_count}",
        f"- repeat_count: {args.repeat_count}",
        f"- delay: {args.delay}",
    ]

    if args.source == "replay":
        lines.extend(
            [
                f"- replay_path: {input_meta.get('replay_path', '')}",
                f"- replay_encoding: {input_meta.get('encoding', '')}",
                f"- replay_strategy: {input_meta.get('strategy', '')}",
                f"- replay_read_attempts: {input_meta.get('read_attempts', '')}",
            ]
        )

    lines.extend(
        [
            "",
            "## Resumen de Expectativas",
            f"- Total simulados: {len(expected_records)}",
            f"- Appends esperados: {counts.get('append', 0)}",
            f"- Updates esperados: {counts.get('update', 0)}",
            f"- Noops esperados: {counts.get('noop', 0)}",
            "",
            "## Tabla de Updates",
            "| Prereserva | Cliente | Campo Modificado | Valor Anterior | Valor Nuevo |",
            "|---|---|---|---|---|",
        ]
    )

    if update_rows:
        lines.extend(update_rows)
    else:
        lines.append("| - | - | - | - | - |")

    insert_rows: List[str] = []
    for record in expected_records:
        if record.get("expected_action") != "append":
            continue

        pr = escape_md(record.get("prereserva", ""))
        cliente = escape_md(record.get("cliente", ""))
        marca = normalize_text(record.get("marca", ""))
        modelo = normalize_text(record.get("marcamodelo", ""))
        anio = normalize_text(record.get("anio", ""))

        auto_base = " ".join([x for x in [marca, modelo] if x]).strip()
        auto_label = f"{auto_base} ({anio})" if auto_base and anio else auto_base or anio
        auto = escape_md(auto_label)

        insert_rows.append(f"| {pr} | {cliente} | {auto} |")

    lines.extend(
        [
            "",
            "## Tabla de Inserts",
            "| Prereserva | Cliente | Auto |",
            "|---|---|---|",
        ]
    )

    if insert_rows:
        lines.extend(insert_rows)
    else:
        lines.append("| - | - | - |")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return md_path


def run_log_mode(stream: List[Tuple[Dict[str, str], bool]], delay: float, expected_records: List[Dict[str, object]]) -> Dict:
    status_counter = Counter()
    repeat_total = 0
    for idx, ((event, is_repeat), qa) in enumerate(zip(stream, expected_records), start=1):
        if is_repeat:
            repeat_total += 1
        status_counter[event.get("Estadoavance", "")] += 1
        payload = {
            "seq": idx,
            "action": "insert",
            "is_repeat": is_repeat,
            "prereserva": event.get("Prereserva"),
            "expected_action": qa["expected_action"],
            "accion_esperada_texto": qa["accion_esperada_texto"],
            "changed_fields_count": qa["changed_fields_count"],
            "changes": qa["changes"],
            "data": event,
        }
        print(json.dumps(payload, ensure_ascii=False))
        if delay > 0:
            time.sleep(delay)

    summary = {
        "total": len(stream),
        "new": len(stream) - repeat_total,
        "repeat": repeat_total,
        "status_distribution": dict(status_counter),
        "inserted_in_db": 0,
    }
    print(json.dumps({"summary": summary}, ensure_ascii=False))
    return summary


def run_db_mode(
    stream: List[Tuple[Dict[str, str], bool]],
    delay: float,
    reset_table: bool,
    expected_records: List[Dict[str, object]],
) -> Dict:
    conn = connect_db()
    cursor = conn.cursor()
    inserted = 0
    status_counter = Counter()
    repeat_total = 0
    try:
        ensure_table(cursor)
        conn.commit()

        if reset_table:
            print(f"[INFO] Ejecutando TRUNCATE TABLE {TABLE_NAME}...")
            cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")
            conn.commit()

        for idx, ((event, is_repeat), qa) in enumerate(zip(stream, expected_records), start=1):
            if is_repeat:
                repeat_total += 1
            row = prepare_db_row(event)
            insert_event(cursor, row)
            conn.commit()  # commit por fila, simulación transaccional real
            inserted += 1
            status_counter[event.get("Estadoavance", "")] += 1
            print(f"[DB] seq={idx} inserted id_prereserva={event.get('Prereserva')} is_repeat={is_repeat} estadoavance={event.get('Estadoavance','')}")
            print_expected_change_console(qa)
            if delay > 0:
                time.sleep(delay)
    finally:
        cursor.close()
        conn.close()

    print(f"[OK] Inserciones completadas: {inserted}")
    print(f"[OK] Distribución Estadoavance: {dict(status_counter)}")
    return {
        "total": len(stream),
        "new": len(stream) - repeat_total,
        "repeat": repeat_total,
        "status_distribution": dict(status_counter),
        "inserted_in_db": inserted,
    }


# SECCIÓN INTERFAZ
def prompt_text_choice(prompt: str, choices: List[str], default: str) -> str:
    choices_norm = [c.lower() for c in choices]
    default_norm = default.lower()
    while True:
        raw = input(f"{prompt} [{' / '.join(choices)}] (default: {default}): ").strip().lower()
        if raw == "":
            return default_norm
        if raw in choices_norm:
            return raw
        print(f"[ERROR] Opción inválida. Valores permitidos: {', '.join(choices)}")


def prompt_int_value(prompt: str, default: int, min_value: Optional[int] = None) -> int:
    while True:
        raw = input(f"{prompt} (default: {default}): ").strip()
        if raw == "":
            return default
        try:
            value = int(raw)
        except ValueError:
            print("[ERROR] Valor inválido. Debe ser un número entero.")
            continue
        if min_value is not None and value < min_value:
            print(f"[ERROR] El valor debe ser >= {min_value}.")
            continue
        return value


def prompt_float_value(prompt: str, default: float, min_value: Optional[float] = None) -> float:
    while True:
        raw = input(f"{prompt} (default: {default}): ").strip()
        if raw == "":
            return default
        try:
            value = float(raw)
        except ValueError:
            print("[ERROR] Valor inválido. Debe ser un número (ej: 0 o 0.5).")
            continue
        if min_value is not None and value < min_value:
            print(f"[ERROR] El valor debe ser >= {min_value}.")
            continue
        return value


def prompt_optional_int(prompt: str, default: Optional[int]) -> Optional[int]:
    default_label = "vacío" if default is None else str(default)
    while True:
        raw = input(f"{prompt} (default: {default_label}): ").strip()
        if raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            print("[ERROR] Valor inválido. Debe ser un número entero o vacío.")


def prompt_yes_no(prompt: str, default: bool = False, attention: bool = False) -> bool:
    default_hint = "S/n" if default else "s/N"
    prefix = "[ATENCION] " if attention else ""
    while True:
        raw = input(f"{prefix}{prompt} [{default_hint}]: ").strip().lower()
        if raw == "":
            return default
        if raw in {"s", "si", "y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("[ERROR] Respuesta inválida. Escribe 's' o 'n'.")


def prompt_interactive_args(args: argparse.Namespace) -> argparse.Namespace:
    print("=== Simulador Concesionaria | Modo Interactivo ===")
    print("(Enter para aceptar valor por defecto)")

    args.source = prompt_text_choice("Fuente de datos", ["faker", "replay"], args.source)
    args.mode = prompt_text_choice("Modo de salida", ["log", "db"], args.mode)
    args.new_count = prompt_int_value("Cantidad de nuevos (new-count)", args.new_count, min_value=0)
    args.repeat_count = prompt_int_value("Cantidad de repetidos (repeat-count)", args.repeat_count, min_value=0)
    args.delay = prompt_float_value("Delay en segundos", args.delay, min_value=0.0)
    args.seed = prompt_optional_int("Seed opcional (Enter = aleatorio)", args.seed)

    if args.source == "replay":
        replay_default = args.replay_file
        replay_input = input(f"Replay file (default: {replay_default}): ").strip()
        args.replay_file = replay_input if replay_input else replay_default

    output_default = args.output_dir
    output_input = input(f"Output dir (default: {output_default}): ").strip()
    args.output_dir = output_input if output_input else output_default

    if args.mode == "db":
        print("\n[ATENCION] Reset de tabla en MySQL (Seguros)")
        print("- Esto elimina datos de testing y reinicia id.")
        args.reset_table = prompt_yes_no("¿Deseas resetear antes de simular?", default=args.reset_table, attention=True)
        if args.reset_table:
            args.reset_only = prompt_yes_no("¿Reset solamente y salir (sin simular)?", default=False)
        else:
            args.reset_only = False
    else:
        args.reset_table = False
        args.reset_only = False

    print("\n--- Resumen de configuración ---")
    print(f"source       : {args.source}")
    print(f"mode         : {args.mode}")
    print(f"new_count    : {args.new_count}")
    print(f"repeat_count : {args.repeat_count}")
    print(f"delay        : {args.delay}")
    print(f"seed         : {args.seed}")
    print(f"replay_file  : {args.replay_file}")
    print(f"output_dir   : {args.output_dir}")
    print(f"reset_table  : {args.reset_table}")
    print(f"reset_only   : {args.reset_only}")

    args.abort_run = not prompt_yes_no("¿Ejecutar simulación con esta configuración?", default=True)
    if args.abort_run:
        print("[INFO] Ejecución cancelada por el usuario.")

    return args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulador concesionaria: faker/replay con salida log o DB.")
    parser.add_argument("--source", choices=["faker", "replay"], default="faker", help="Fuente de eventos.")
    parser.add_argument("--mode", choices=["log", "db"], default="log", help="Destino de eventos: consola o MySQL.")
    parser.add_argument("--new-count", type=int, default=20, help="Cantidad de eventos nuevos (Prereserva única).")
    parser.add_argument("--repeat-count", type=int, default=10, help="Cantidad total de eventos repetidos.")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay en segundos entre eventos.")
    parser.add_argument("--seed", type=int, default=None, help="Semilla para reproducibilidad.")
    parser.add_argument("--max-events", type=int, default=2000, help="Límite de seguridad de eventos totales.")
    parser.add_argument("--reset-table", action="store_true", help="Resetea Seguros (DROP+CREATE). Si se usa solo este flag, no simula eventos.")
    parser.add_argument("--reset-only", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--interactive", action="store_true", help="Abre interfaz guiada por consola para cargar parámetros.")
    parser.add_argument(
        "--replay-file",
        default="dataset/NORMALIZADO/historial_completo_normalizado.csv",
        help="Ruta al archivo normalizado para source=replay.",
    )
    parser.add_argument(
        "--output-dir",
        default="dataset/SIMULACIONES",
        help="Carpeta para guardar historial JSON de cada corrida.",
    )
    return parser.parse_args()


def build_stream(args: argparse.Namespace) -> Tuple[List[Tuple[Dict[str, str], bool]], Dict[str, object]]:
    if args.source == "faker":
        stream = build_faker_stream(args.new_count, args.repeat_count, args.seed)
        return stream, {"source": "faker", "read_attempts": 0}

    replay_path = Path(args.replay_file)
    if not replay_path.is_absolute():
        # Permite ejecutar desde cualquier carpeta (ej: ./test)
        cwd_candidate = Path.cwd() / replay_path
        root_candidate = PROJECT_ROOT / replay_path
        if cwd_candidate.exists():
            replay_path = cwd_candidate
        else:
            replay_path = root_candidate

    stream, replay_meta = build_replay_stream(replay_path, args.new_count, args.repeat_count, args.seed)
    return stream, replay_meta


def write_run_history(
    output_dir: Path,
    run_id: str,
    args: argparse.Namespace,
    stream: List[Tuple[Dict[str, str], bool]],
    execution_summary: Dict,
    input_meta: Dict[str, object],
    expected_records: List[Dict[str, object]],
) -> Path:
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir

    output_dir.mkdir(parents=True, exist_ok=True)
    history_path = output_dir / f"simulacion_{run_id}_{args.source}_{args.mode}.json"

    events = []
    for idx, (event, is_repeat) in enumerate(stream, start=1):
        events.append(
            {
                "seq": idx,
                "is_repeat": is_repeat,
                "prereserva": event.get("Prereserva"),
                "estadoavance": event.get("Estadoavance", ""),
                "expected_action": expected_records[idx - 1]["expected_action"],
                "accion_esperada_texto": expected_records[idx - 1]["accion_esperada_texto"],
                "changed_fields_count": expected_records[idx - 1]["changed_fields_count"],
                "changes": expected_records[idx - 1]["changes"],
                "data": event,
            }
        )

    payload = {
        "run_id": run_id,
        "created_at": iso_now(),
        "config": {
            "source": args.source,
            "mode": args.mode,
            "new_count": args.new_count,
            "repeat_count": args.repeat_count,
            "delay": args.delay,
            "seed": args.seed,
            "max_events": args.max_events,
            "reset_table": bool(args.reset_table),
            "replay_file": args.replay_file,
        },
        "summary": execution_summary,
        "input_meta": input_meta,
        "events": events,
    }

    history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return history_path


def main() -> None:
    args = parse_args()
    if args.interactive:
        args = prompt_interactive_args(args)
        if getattr(args, "abort_run", False):
            return

    log_file = setup_logging(Path(args.output_dir))
    LOGGER.info("Iniciando simulador | source=%s mode=%s", args.source, args.mode)

    try:
        # Soporta mantenimiento directo: python .\test\simulador_concesionaria.py --reset-table
        if args.reset_only or (args.reset_table and len(sys.argv) == 2):
            print(f"[INFO] Reseteando tabla {TABLE_NAME} (DROP + CREATE)...")
            reset_table_hard()
            print(f"[OK] Tabla {TABLE_NAME} reseteada. id vuelve a iniciar desde 1.")
            LOGGER.info("Tabla %s reseteada en modo mantenimiento (--reset-table solo).", TABLE_NAME)
            return

        total = args.new_count + args.repeat_count
        if total <= 0:
            raise ValueError("new-count + repeat-count debe ser > 0")
        if total > args.max_events:
            raise ValueError(f"Total de eventos ({total}) supera max-events ({args.max_events}).")
        if args.delay < 0:
            raise ValueError("delay no puede ser negativo")

        stream, input_meta = build_stream(args)
        expected_records, expected_counts = compute_expected_changes(stream)
        print(
            f"[INFO] Simulación lista: source={args.source} mode={args.mode} total={len(stream)} new={args.new_count} repeat={args.repeat_count}"
        )
        LOGGER.info("Stream construido | eventos=%s", len(stream))

        if args.source == "replay":
            summary_hint = (
                f"encoding={input_meta.get('encoding')} "
                f"strategy={input_meta.get('strategy')} "
                f"read_attempts={input_meta.get('read_attempts')}"
            )
            print(f"[INFO] Replay parser: {summary_hint}")
            LOGGER.info("Replay parser | %s", summary_hint)

        if args.mode == "log":
            summary = run_log_mode(stream, args.delay, expected_records)
        else:
            summary = run_db_mode(stream, args.delay, args.reset_table, expected_records)

        summary["total_expected_appends"] = expected_counts["append"]
        summary["total_expected_updates"] = expected_counts["update"]
        summary["total_expected_noops"] = expected_counts["noop"]

        print("[QA] Resumen esperado: "
              f"Total simulados={len(stream)} | "
              f"Appends esperados={expected_counts['append']} | "
              f"Updates esperados={expected_counts['update']} | "
              f"Noops esperados={expected_counts['noop']}")

        if args.source == "replay":
            summary["replay_encoding"] = input_meta.get("encoding")
            summary["replay_strategy"] = input_meta.get("strategy")
            summary["replay_read_attempts"] = input_meta.get("read_attempts")
            summary["replay_path"] = input_meta.get("replay_path")

        run_id = now_stamp()
        expected_path = write_expected_changes_file(Path(args.output_dir), run_id, args, expected_records, expected_counts)
        qa_md_path = write_qa_markdown_summary(
            Path(args.output_dir),
            run_id,
            args,
            input_meta,
            expected_records,
            expected_counts,
        )
        history_path = write_run_history(Path(args.output_dir), run_id, args, stream, summary, input_meta, expected_records)
        print(f"[OK] Historial JSON guardado: {history_path}")
        print(f"[OK] Expected Change Log guardado: {expected_path}")
        print(f"[OK] QA Summary Markdown guardado: {qa_md_path}")
        LOGGER.info("Historial JSON guardado en %s", history_path)
        LOGGER.info("Expected Change Log guardado en %s", expected_path)
        LOGGER.info("QA Summary Markdown guardado en %s", qa_md_path)
    except Exception as exc:
        LOGGER.exception("Error ejecutando simulador: %s", exc)
        print(f"[ERROR] Simulador falló: {exc}")
        print(f"[ERROR] Revisar log: {log_file}")
        raise


if __name__ == "__main__":
    main()
