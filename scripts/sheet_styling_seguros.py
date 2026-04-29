import json
import os
import sys
import unicodedata
from pathlib import Path

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def norm(text: str) -> str:
    base = (text or "").strip().lower()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    return " ".join(base.split())


def a1_col_letter(col_idx_1_based: int) -> str:
    n = col_idx_1_based
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def find_col(header: list[str], candidates: list[str]) -> int:
    normalized = [norm(h) for h in header]
    candidate_set = {norm(c) for c in candidates}
    for idx, item in enumerate(normalized, start=1):
        if item in candidate_set:
            return idx
    return 0


def col_range(sheet_id: int, start_col_1: int, end_col_1_exclusive: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startColumnIndex": start_col_1 - 1,
        "endColumnIndex": end_col_1_exclusive - 1,
    }


def main() -> int:
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    sheet_name = os.getenv("SHEET_NAME", "Hoja 1").strip() or "Hoja 1"
    cred_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()

    if not spreadsheet_id:
        raise ValueError("Falta SPREADSHEET_ID en .env")

    if cred_file:
        env_candidate = Path(cred_file).expanduser().resolve()
        if env_candidate.exists():
            credentials_file = env_candidate
        else:
            cred_file = ""

    if not cred_file:
        local_root = ROOT / "credentials.json"
        local_etl = ROOT / "automatizaciones" / "etl_seguros" / "credentials.json"
        if local_root.exists():
            credentials_file = local_root
        elif local_etl.exists():
            credentials_file = local_etl
        else:
            raise FileNotFoundError("No se encontro credentials.json ni GOOGLE_CREDENTIALS_FILE")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(str(credentials_file), scopes=scopes)
    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(sheet_name)
    sheet_id = ws.id

    metadata = sh.fetch_sheet_metadata()
    sheet_meta = next(
        (s for s in metadata.get("sheets", []) if s.get("properties", {}).get("sheetId") == sheet_id),
        {},
    )

    values = ws.get_all_values()
    if not values:
        raise RuntimeError("La hoja esta vacia. Ejecuta primero el ETL para crear encabezado.")

    header = values[0]
    used_cols = max((i for i, val in enumerate(header, start=1) if str(val).strip()), default=1)
    total_rows = max(len(values), 2)

    idx_fecha_entrega = find_col(header, ["Fecha de Entrega", "FechaEntrega"])
    idx_fecha_pre = find_col(header, ["FechaPrereserva", "Fecha Prereserva"])
    idx_fecha_venta = find_col(header, ["FechaVenta", "Fecha Venta"])
    idx_precio = find_col(header, ["PrecioVenta", "Precio Venta"])

    idx_primer_contacto = find_col(header, ["Primer contacto"])
    idx_segundo_contacto = find_col(header, ["Segundo contacto"])
    idx_vendido = find_col(header, ["Vendido / No vendido", "Vendido/No vendido", "Vendido No vendido"])

    missing_user_headers = []
    if idx_primer_contacto == 0:
        missing_user_headers.append("Primer contacto")
    if idx_segundo_contacto == 0:
        missing_user_headers.append("Segundo contacto")
    if idx_vendido == 0:
        missing_user_headers.append("Vendido / No vendido")

    if missing_user_headers:
        start_col = used_cols + 1
        ws.update(
            range_name=f"{a1_col_letter(start_col)}1:{a1_col_letter(start_col + len(missing_user_headers) - 1)}1",
            values=[missing_user_headers],
            value_input_option="USER_ENTERED",
        )
        values = ws.get_all_values()
        header = values[0]
        used_cols = max((i for i, val in enumerate(header, start=1) if str(val).strip()), default=1)
        total_rows = max(len(values), 2)
        idx_primer_contacto = find_col(header, ["Primer contacto"])
        idx_segundo_contacto = find_col(header, ["Segundo contacto"])
        idx_vendido = find_col(
            header,
            ["Vendido / No vendido", "Vendido/No vendido", "Vendido No vendido"],
        )

    end_col_letter = a1_col_letter(used_cols)

    requests = []

    existing_rules = sheet_meta.get("conditionalFormats", [])
    for idx in range(len(existing_rules) - 1, -1, -1):
        requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": idx}})

    existing_bandings = sheet_meta.get("bandedRanges", [])
    for banded in existing_bandings:
        banded_id = banded.get("bandedRangeId")
        if banded_id is not None:
            requests.append({"deleteBanding": {"bandedRangeId": int(banded_id)}})

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": used_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "fontFamily": "Arial",
                            "fontSize": 10,
                            "foregroundColor": {"red": 0.13, "green": 0.13, "blue": 0.13},
                        },
                        "verticalAlignment": "MIDDLE",
                        "horizontalAlignment": "LEFT",
                        "wrapStrategy": "CLIP",
                    }
                },
                "fields": (
                    "userEnteredFormat.textFormat.fontFamily,"
                    "userEnteredFormat.textFormat.fontSize,"
                    "userEnteredFormat.textFormat.foregroundColor,"
                    "userEnteredFormat.verticalAlignment,"
                    "userEnteredFormat.horizontalAlignment,"
                    "userEnteredFormat.wrapStrategy"
                ),
            }
        }
    )

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": used_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.12, "green": 0.25, "blue": 0.42},
                        "textFormat": {
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                            "fontFamily": "Arial",
                            "fontSize": 10,
                            "bold": True,
                        },
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
            }
        }
    )

    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 36},
                "fields": "pixelSize",
            }
        }
    )

    for idx in [idx_fecha_entrega, idx_fecha_pre, idx_fecha_venta]:
        if idx > 0:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": idx - 1,
                            "endColumnIndex": idx,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )

    if idx_precio > 0:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": idx_precio - 1,
                        "endColumnIndex": idx_precio,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "NUMBER", "pattern": "$ #,##0"}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    width_map = {
        "Cliente - Razón Social": 230,
        "Email": 230,
        "Domicilio": 240,
        "MarcaModelo": 220,
        "Telefono": 140,
        "Vendedor": 140,
        "Primer contacto": 140,
        "Segundo contacto": 140,
        "Vendido / No vendido": 150,
        "NroSucursal": 95,
        "CodigoUnidad": 95,
        "Rubro": 95,
        "Tipo de Venta": 95,
    }

    for label, pixels in width_map.items():
        idx = find_col(header, [label])
        if idx > 0:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": idx - 1,
                            "endIndex": idx,
                        },
                        "properties": {"pixelSize": pixels},
                        "fields": "pixelSize",
                    }
                }
            )

    for wrap_label in ["Email", "Domicilio"]:
        idx_wrap = find_col(header, [wrap_label])
        if idx_wrap > 0:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": idx_wrap - 1,
                            "endColumnIndex": idx_wrap,
                        },
                        "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                        "fields": "userEnteredFormat.wrapStrategy",
                    }
                }
            )

    requests.append(
        {
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "startColumnIndex": 0,
                        "endColumnIndex": used_cols,
                    },
                    "rowProperties": {
                        "headerColor": {"red": 0.12, "green": 0.25, "blue": 0.42},
                        "firstBandColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                        "secondBandColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
                    },
                }
            }
        }
    )

    if idx_vendido > 0:
        formula_col = a1_col_letter(idx_vendido)
        full_range = {
            "sheetId": sheet_id,
            "startRowIndex": 1,
            "startColumnIndex": 0,
            "endColumnIndex": used_cols,
        }
        requests.extend(
            [
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [full_range],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": f'=UPPER(${formula_col}2)="VENDIDO"'}],
                                },
                                "format": {
                                    "backgroundColor": {"red": 0.89, "green": 0.97, "blue": 0.91}
                                },
                            },
                        },
                        "index": 0,
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [full_range],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": f'=UPPER(${formula_col}2)="NO VENDIDO"'}],
                                },
                                "format": {
                                    "backgroundColor": {"red": 0.99, "green": 0.90, "blue": 0.90}
                                },
                            },
                        },
                        "index": 0,
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [full_range],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": f'=UPPER(${formula_col}2)="PENDIENTE"'}],
                                },
                                "format": {
                                    "backgroundColor": {"red": 1.0, "green": 0.96, "blue": 0.80}
                                },
                            },
                        },
                        "index": 0,
                    }
                },
            ]
        )

    for idx_contact in [idx_primer_contacto, idx_segundo_contacto]:
        if idx_contact > 0:
            requests.append(
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": idx_contact - 1,
                            "endColumnIndex": idx_contact,
                        },
                        "rule": {
                            "condition": {"type": "DATE_IS_VALID"},
                            "strict": False,
                            "showCustomUi": True,
                        },
                    }
                }
            )

    if idx_vendido > 0:
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": idx_vendido - 1,
                        "endColumnIndex": idx_vendido,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": "Vendido"},
                                {"userEnteredValue": "No vendido"},
                                {"userEnteredValue": "Pendiente"},
                            ],
                        },
                        "strict": False,
                        "showCustomUi": True,
                    },
                }
            }
        )

    sh.batch_update({"requests": requests})

    ws.freeze(rows=1)
    ws.freeze(cols=6)
    ws.set_basic_filter(f"A1:{end_col_letter}{total_rows}")

    print("[OK] Estilos aplicados correctamente.")
    print(json.dumps({
        "sheet": sheet_name,
        "used_cols": used_cols,
        "used_rows": total_rows,
        "fecha_cols": [idx_fecha_entrega, idx_fecha_pre, idx_fecha_venta],
        "precio_col": idx_precio,
        "primer_contacto_col": idx_primer_contacto,
        "segundo_contacto_col": idx_segundo_contacto,
        "vendido_col": idx_vendido,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
