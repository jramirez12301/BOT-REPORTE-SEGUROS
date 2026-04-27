# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Dependencias del sistema para pyodbc + driver ODBC de SQL Server.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        unixodbc \
        unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/microsoft-prod.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- PARCHE DE SEGURIDAD PARA SQL SERVER LEGACY (FORD/HYUNDAI) ---
# Forzamos a OpenSSL 3 a bajar su nivel de seguridad a 0 (permite firmas SHA1/viejas).
RUN sed -i 's/\[openssl_init\]/\[openssl_init\]\nssl_conf = ssl_sect/g' /etc/ssl/openssl.cnf \
    && echo "\n[ssl_sect]\nsystem_default = system_default_sect\n\n[system_default_sect]\nCipherString = DEFAULT@SECLEVEL=0" >> /etc/ssl/openssl.cnf

# Dependencias Python.
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiamos solo el codigo necesario del proyecto.
COPY core ./core
COPY automatizaciones ./automatizaciones

# Entry point del ETL. Permite pasar flags directo en docker run/compose run.
ENTRYPOINT ["python", "automatizaciones/etl_seguros/etl.py"]
CMD []