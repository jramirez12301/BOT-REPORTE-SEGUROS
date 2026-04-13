import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


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

CANONICAL_NORMALIZED = {"".join(c for c in unicodedata.normalize("NFKD", col.lower()) if not unicodedata.combining(c)): col for col in CANONICAL_COLUMNS}

ALIASES = {
    "ano": "Anio",
    "anio": "Anio",
    "año": "Anio",
    "precio": "Precioventa",
    "precio_venta": "Precioventa",
    "codigo_unidad": "Codigounidad",
    "codigo": "Codigounidad",
    "modelo": "Marcamodelo",
    "estado_avance": "Estadoavance",
}

STATUS_VALUES = {
    "asignado",
    "autorizado",
    "facturada",
    "turno entrega",
    "ingresado",
    "patentado",
    "entregado",
}

HEADER_TOKENS = set(CANONICAL_NORMALIZED.keys()) | set(ALIASES.keys())


@dataclass
class FileReport:
    file_name: str
    rows_in: int
    rows_out: int
    rows_rejected: int
    source_columns: List[str]
    column_mapping: Dict[str, str]
    warnings: List[str]


def normalize_text(value: str) -> str:
    value = "" if value is None else str(value)
    value = value.strip()
    if value.lower() in {"nan", "none", "null"}:
        return ""
    return value


def normalize_header(header: str) -> str:
    header = normalize_text(header)
    header = re.sub(r"\.\d+$", "", header)
    header = "".join(c for c in unicodedata.normalize("NFKD", header.lower()) if not unicodedata.combining(c))
    header = re.sub(r"[^a-z0-9]", "", header)
    return header


def canonical_from_header(header: str) -> Optional[str]:
    normalized = normalize_header(header)
    if normalized in CANONICAL_NORMALIZED:
        return CANONICAL_NORMALIZED[normalized]
    if normalized in ALIASES:
        return ALIASES[normalized]
    return None


def detect_files(input_dir: Path) -> List[Path]:
    candidates: List[Path] = []
    for pattern in ("*.csv", "*.txt", "*.xlsx", "*.xls"):
        candidates.extend(input_dir.glob(pattern))

    def sort_key(path: Path):
        match = re.search(r"(\d{8})_(\d{6})", path.stem)
        if match:
            return (0, match.group(1) + match.group(2), path.name)
        return (1, f"{int(path.stat().st_mtime):014d}", path.name)

    return sorted(candidates, key=sort_key)


def detect_header_row(raw_df: pd.DataFrame) -> int:
    max_rows = min(len(raw_df), 60)
    best_idx = 0
    best_score = -1

    for idx in range(max_rows):
        row_values = [normalize_text(v) for v in raw_df.iloc[idx].tolist()]
        normalized = [normalize_header(v) for v in row_values if v]
        if not normalized:
            continue

        token_list = [tok for tok in normalized if tok in HEADER_TOKENS]
        unique_tokens = set(token_list)
        prereserva_hit = 1 if "prereserva" in unique_tokens else 0

        # Priorizamos la fila con mayor cantidad de nombres de columnas canónicas.
        score = (len(unique_tokens) * 4) + len(token_list) + (prereserva_hit * 5)

        if score > best_score:
            best_score = score
            best_idx = idx

    return best_idx


def with_promoted_header(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    header_idx = detect_header_row(raw_df)
    raw_headers = [normalize_text(v) for v in raw_df.iloc[header_idx].tolist()]

    # Conserva columnas duplicadas de origen para que el mapeo heurístico pueda elegir.
    counts: Dict[str, int] = {}
    promoted_headers: List[str] = []
    for i, header in enumerate(raw_headers):
        base = header if header else f"__empty_col_{i}"
        n = counts.get(base, 0)
        promoted_headers.append(base if n == 0 else f"{base}.{n}")
        counts[base] = n + 1

    body = raw_df.iloc[header_idx + 1 :].copy().reset_index(drop=True)
    body.columns = promoted_headers

    # Elimina filas totalmente vacías para evitar ruido en el scoring.
    non_empty_mask = ~(body.apply(lambda row: all(normalize_text(v) == "" for v in row), axis=1))
    body = body[non_empty_mask].reset_index(drop=True)
    return body


def read_with_fallbacks(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        raw_df = pd.read_excel(path, dtype=str, keep_default_na=False, na_filter=False, header=None)
        return with_promoted_header(raw_df)

    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
    last_error = None
    for enc in encodings:
        try:
            raw_df = pd.read_csv(
                path,
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                sep=None,
                engine="python",
                encoding=enc,
                header=None,
            )
            return with_promoted_header(raw_df)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"No se pudo leer {path.name}: {last_error}")


def sample_values(series: pd.Series, limit: int = 120) -> List[str]:
    values = [normalize_text(v) for v in series.tolist()]
    values = [v for v in values if v != ""]
    return values[:limit]


def ratio(values: List[str], predicate) -> float:
    if not values:
        return 0.0
    valid = sum(1 for v in values if predicate(v))
    return valid / len(values)


def is_prereserva(v: str) -> bool:
    return bool(re.fullmatch(r"[0-9.\-]{5,20}", v))


def is_short_numeric(v: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{1,4}", v))


def is_money(v: str) -> bool:
    return bool(re.fullmatch(r"[0-9.,]+", v)) and any(ch in v for ch in {",", "."})


def is_cuit(v: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{2}-?[0-9]{8}-?[0-9]", v.replace(" ", "")))


def is_email(v: str) -> bool:
    return "@" in v and "." in v.split("@")[-1]


def is_phone(v: str) -> bool:
    cleaned = re.sub(r"[^0-9]", "", v)
    return 8 <= len(cleaned) <= 18


def is_date(v: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", v))


def is_vin(v: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9]{10,20}", v.replace(" ", "")))


def is_plate(v: str) -> bool:
    cleaned = v.replace(" ", "")
    return bool(re.fullmatch(r"[A-Za-z0-9]{6,8}", cleaned))


def score_for_target(values: List[str], target: str) -> float:
    non_empty_ratio = 0.0 if not values else 1.0
    if target == "Prereserva":
        return ratio(values, is_prereserva) * 1.1 + non_empty_ratio * 0.2
    if target == "Rubro":
        return ratio(values, is_short_numeric) * 1.1 + non_empty_ratio * 0.2
    if target == "Precioventa":
        return ratio(values, is_money) * 1.0 + non_empty_ratio * 0.2
    if target == "Cuitcliente":
        return ratio(values, is_cuit) * 1.1 + non_empty_ratio * 0.1
    if target == "Email":
        return ratio(values, is_email) * 1.2 + non_empty_ratio * 0.1
    if target == "Telefono":
        return ratio(values, is_phone) * 1.0 + non_empty_ratio * 0.1
    if target in {"Fechaprereserva", "Fechaventa", "Fechaentrega", "Fechapatentamiento", "Fechaproceso"}:
        return ratio(values, is_date) * 1.1 + non_empty_ratio * 0.1
    if target == "Estadoavance":
        status_ratio = ratio(values, lambda x: normalize_header(x) in {normalize_header(s) for s in STATUS_VALUES})
        return status_ratio * 1.3 + non_empty_ratio * 0.2
    if target == "Estado":
        return ratio(values, lambda x: len(x) <= 40 and re.search(r"[A-Za-z]", x) is not None) * 0.8 + non_empty_ratio * 0.2
    if target == "Vin":
        return ratio(values, is_vin) * 1.2 + non_empty_ratio * 0.1
    if target == "Patente":
        return ratio(values, is_plate) * 1.2 + non_empty_ratio * 0.1
    if target in {"Sucursal", "Origen", "Codigounidad", "Anio"}:
        return ratio(values, lambda x: bool(re.fullmatch(r"[0-9.\-]{1,15}", x))) * 1.0 + non_empty_ratio * 0.2
    return non_empty_ratio * 0.4 + ratio(values, lambda x: len(x) >= 2) * 0.8


def map_columns(df: pd.DataFrame) -> Tuple[Dict[str, str], List[str]]:
    source_cols = [str(c) for c in df.columns]
    used_sources = set()
    mapping: Dict[str, str] = {}
    warnings: List[str] = []

    source_samples = {src: sample_values(df[src]) for src in source_cols}

    for target in CANONICAL_COLUMNS:
        best_src = None
        best_score = -1.0
        for src in source_cols:
            if src in used_sources:
                continue
            values = source_samples[src]
            score = score_for_target(values, target)
            expected = canonical_from_header(src)
            if expected == target:
                score += 1.8
            elif expected is not None:
                score -= 0.4
            if score > best_score:
                best_score = score
                best_src = src

        threshold = 0.65
        if best_src is not None and best_score >= threshold:
            mapping[target] = best_src
            used_sources.add(best_src)
        else:
            mapping[target] = ""
            warnings.append(f"No se encontró origen confiable para columna '{target}'. Se completará vacío.")

    for src in source_cols:
        expected = canonical_from_header(src)
        if expected and mapping.get(expected) and mapping[expected] != src:
            warnings.append(f"Duplicada detectada para '{expected}': se ignoró columna '{src}' y se usó '{mapping[expected]}'.")

    return mapping, warnings


def build_normalized(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    normalized = pd.DataFrame(index=df.index)
    for target in CANONICAL_COLUMNS:
        src = mapping.get(target, "")
        if src:
            normalized[target] = df[src].map(normalize_text)
        else:
            normalized[target] = ""
    return normalized


def remove_embedded_header_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    canonical_norm = {col: normalize_header(col) for col in CANONICAL_COLUMNS}
    canonical_tokens = set(canonical_norm.values())

    def is_header_like(row: pd.Series) -> bool:
        compared = 0
        matches = 0
        token_hits = 0
        for col in CANONICAL_COLUMNS:
            value = normalize_text(row.get(col, ""))
            if not value:
                continue

            normalized_value = normalize_header(value)
            if normalized_value in canonical_tokens:
                token_hits += 1

            compared += 1
            if normalized_value == canonical_norm[col]:
                matches += 1

        if compared == 0:
            return False

        # Caso 1: encabezado repetido en la misma posicion de columna.
        aligned_header = matches >= 6 and (matches / compared) >= 0.45

        # Caso 2: encabezado desordenado/corrido (tokens de header en columnas mezcladas).
        shuffled_header = token_hits >= 8 and (token_hits / compared) >= 0.55

        return aligned_header or shuffled_header

    mask = df.apply(is_header_like, axis=1)
    clean_df = df[~mask].copy()
    embedded_headers = df[mask].copy()
    return clean_df, embedded_headers


def split_rejected(df: pd.DataFrame, source_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    all_empty = (df[CANONICAL_COLUMNS].replace("", pd.NA).isna()).all(axis=1)
    no_prereserva = df["Prereserva"].eq("")
    rejected_mask = all_empty | no_prereserva

    accepted = df[~rejected_mask].copy()
    rejected = df[rejected_mask].copy()
    rejected.insert(0, "source_file", source_name)
    rejected.insert(1, "reject_reason", "sin_prereserva_o_fila_vacia")
    return accepted, rejected


def process_file(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, FileReport]:
    raw_df = read_with_fallbacks(path)
    raw_df.columns = [normalize_text(c) for c in raw_df.columns]

    mapping, warnings = map_columns(raw_df)
    normalized_df = build_normalized(raw_df, mapping)
    normalized_df, embedded_headers = remove_embedded_header_rows(normalized_df)
    accepted, rejected = split_rejected(normalized_df, path.name)
    if not embedded_headers.empty:
        embedded_headers = embedded_headers.copy()
        embedded_headers.insert(0, "source_file", path.name)
        embedded_headers.insert(1, "reject_reason", "encabezado_repetido_en_filas")
        rejected = pd.concat([rejected, embedded_headers], ignore_index=True)
        warnings.append(f"Se eliminaron {len(embedded_headers)} filas de encabezado repetido.")

    report = FileReport(
        file_name=path.name,
        rows_in=len(raw_df),
        rows_out=len(accepted),
        rows_rejected=len(rejected),
        source_columns=[str(c) for c in raw_df.columns],
        column_mapping=mapping,
        warnings=warnings,
    )
    return accepted, rejected, report


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run(input_dir: Path, output_dir: Path, output_name: str, rejected_name: str, report_name: str) -> None:
    ensure_output_dir(output_dir)
    files = detect_files(input_dir)
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos en {input_dir}")

    all_accepted: List[pd.DataFrame] = []
    all_rejected: List[pd.DataFrame] = []
    reports: List[FileReport] = []

    for file_path in files:
        print(f"[INFO] Normalizando: {file_path.name}")
        accepted, rejected, report = process_file(file_path)
        all_accepted.append(accepted)
        if not rejected.empty:
            all_rejected.append(rejected)
        reports.append(report)
        print(f"[INFO] Filas entrada={report.rows_in} salida={report.rows_out} rechazadas={report.rows_rejected}")

    consolidated = pd.concat(all_accepted, ignore_index=True) if all_accepted else pd.DataFrame(columns=CANONICAL_COLUMNS)
    rejected_df = pd.concat(all_rejected, ignore_index=True) if all_rejected else pd.DataFrame(columns=["source_file", "reject_reason"] + CANONICAL_COLUMNS)

    output_path = output_dir / output_name
    rejected_path = output_dir / rejected_name
    report_path = output_dir / report_name

    consolidated.to_csv(output_path, index=False, encoding="utf-8-sig")
    rejected_df.to_csv(rejected_path, index=False, encoding="utf-8-sig")

    summary = {
        "files_processed": len(files),
        "rows_output": int(len(consolidated)),
        "rows_rejected": int(len(rejected_df)),
        "files": [report.__dict__ for report in reports],
    }
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Consolidado generado: {output_path}")
    print(f"[OK] Rechazados generado: {rejected_path}")
    print(f"[OK] Reporte generado: {report_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normaliza reportes legacy para el simulador de concesionaria.")
    parser.add_argument("--input-dir", default="dataset/SIN_NORMALIZAR", help="Carpeta de entrada con archivos crudos.")
    parser.add_argument("--output-dir", default="dataset/NORMALIZADO", help="Carpeta de salida para archivos normalizados.")
    parser.add_argument("--output-name", default="historial_completo_normalizado.csv", help="Nombre del CSV consolidado.")
    parser.add_argument("--rejected-name", default="rechazados_reporte.csv", help="Nombre del CSV de filas rechazadas.")
    parser.add_argument("--report-name", default="normalizacion_reporte.json", help="Nombre del reporte JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        output_name=args.output_name,
        rejected_name=args.rejected_name,
        report_name=args.report_name,
    )


if __name__ == "__main__":
    main()
