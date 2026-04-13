import os
import sys
import mysql.connector
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv


def _require_env_vars(var_names):
    missing = [name for name in var_names if not os.getenv(name)]
    if missing:
        print(f"[ERROR] Variables de entorno faltantes: {', '.join(missing)}")
        return False
    return True


def test_diagnostics():
    print("--- Iniciando Diagnostico de Conexiones ---")
    load_dotenv()
    
    # 1. Verificar Archivos Requeridos
    required_files = ['.env', 'credentials.json', 'watermark.json']
    for file in required_files:
        if os.path.exists(file):
            print(f"[OK] Archivo encontrado: {file}")
        else:
            print(f"[ERROR] No se encuentra el archivo: {file}")
            return

    # 2. Probar Conexión MySQL
    print("\n--- Probando Conexion MySQL ---")
    mysql_required = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    if not _require_env_vars(mysql_required):
        return

    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_port = int(os.getenv("DB_PORT", "3306"))

    print(f"Debug: Intentando conectar a {db_host}:{db_port} con usuario {db_user}...")
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            connection_timeout=10,
            use_pure=True,
        )
        print("Debug: Llamada a connect() finalizada.")
        if conn.is_connected():
            print("[OK] Conexión exitosa a MySQL.")
            cursor = conn.cursor()

            print("Debug: Verificando tabla 'Seguros'...")
            cursor.execute("SHOW TABLES LIKE 'Seguros'")
            table_exists = cursor.fetchone()
            if not table_exists:
                print("[ERROR] La tabla 'Seguros' no existe en la base configurada.")
                return

            print("Debug: Ejecutando SELECT COUNT(*)...")
            cursor.execute("SELECT COUNT(*) FROM Seguros")
            count = cursor.fetchone()[0]
            print(f"[OK] Acceso a tabla 'Seguros' verificado. Filas actuales: {count}")
        else:
            print("[ERROR] mysql.connector.connect() devolvió una conexión no activa.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Error específico de MySQL: {err}")
    except Exception as e:
        print(f"[ERROR] Fallo inesperado: {type(e).__name__}: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
            print("Debug: Conexión MySQL cerrada correctamente.")

    # 3. Probar Conexión Google Sheets
    print("\n--- Probando Conexion Google Sheets ---")
    try:
        if not _require_env_vars(["SPREADSHEET_ID"]):
            return

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_file('credentials.json', scopes=scopes)
        gc = gspread.authorize(credentials)
        print("[OK] Autenticación con Google Cloud exitosa.")
        
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        sheet_name = os.getenv('SHEET_NAME', 'Hoja 1')
        
        sh = gc.open_by_key(spreadsheet_id)
        print(f"[OK] Documento '{sh.title}' encontrado.")
        
        worksheet = sh.worksheet(sheet_name)
        print(f"[OK] Hoja '{sheet_name}' accesible.")
        
        # Probar lectura de la primera celda
        first_val = worksheet.acell('A1').value
        print(f"[OK] Lectura exitosa. Valor en A1: '{first_val}'")
        
        print("\n--- Diagnostico Finalizado con Exito ---")
        print("Todos los sistemas están listos para ejecutar 'etl.py'.")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"[ERROR] No se encontró el Spreadsheet con ID: {os.getenv('SPREADSHEET_ID')}")
        print("Tip: Verifica el ID en tu .env y asegúrate de haber compartido el archivo con el email de la cuenta de servicio.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"[ERROR] No se encontró la hoja llamada: '{os.getenv('SHEET_NAME')}'")
    except Exception as e:
        print(f"[ERROR] Fallo en Google Sheets: {e}")

if __name__ == "__main__":
    test_diagnostics()
