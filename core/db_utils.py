"""Database connection helpers for shared audit infrastructure."""

from __future__ import annotations

import os
from typing import Any, Callable

import mysql.connector
import pyodbc
from dotenv import load_dotenv

load_dotenv()


def _resolve_sqlserver_driver(preferred_driver: str | None = None) -> str:
    """Resolve a usable SQL Server ODBC driver from installed drivers."""
    installed = set(pyodbc.drivers())

    if preferred_driver and preferred_driver in installed:
        return preferred_driver

    preferred_candidates = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for candidate in preferred_candidates:
        if candidate in installed:
            return candidate

    raise ValueError(
        "No se encontro un driver ODBC compatible para SQL Server. "
        f"Drivers instalados: {sorted(installed)}"
    )


def get_app_env(default: str = "TEST") -> str:
    """Return normalized runtime environment."""
    return os.getenv("APP_ENV", default).strip().upper()


def _get_env_value(base_key: str, env: str) -> str | None:
    env_key = f"{base_key}_{env}"
    value = os.getenv(env_key)
    if value is not None and value != "":
        return value
    return os.getenv(base_key)


def _require_env_value(base_key: str, env: str) -> str:
    value = _get_env_value(base_key, env)
    if value is None or value == "":
        raise ValueError(
            f"Missing required environment variable: {base_key}_{env} or {base_key}"
        )
    return value


def build_audit_db_config(env: str | None = None, prefix: str = "AUDIT_DB") -> dict[str, Any]:
    """Build audit DB config from .env with ENV suffix fallback."""
    selected_env = (env or get_app_env()).strip().upper()

    host = _require_env_value(f"{prefix}_HOST", selected_env)
    user = _require_env_value(f"{prefix}_USER", selected_env)
    password = _require_env_value(f"{prefix}_PASSWORD", selected_env)
    database = _require_env_value(f"{prefix}_NAME", selected_env)

    port = int(_get_env_value(f"{prefix}_PORT", selected_env) or "3306")
    timeout = int(_get_env_value(f"{prefix}_CONNECTION_TIMEOUT", selected_env) or "10")
    charset = _get_env_value(f"{prefix}_CHARSET", selected_env) or "utf8mb4"
    collation = _get_env_value(f"{prefix}_COLLATION", selected_env) or "utf8mb4_unicode_ci"

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "connection_timeout": timeout,
        "charset": charset,
        "collation": collation,
        "use_pure": True,
        "use_unicode": True,
    }


def create_audit_connection(
    env: str | None = None,
    prefix: str = "AUDIT_DB",
    extra_config: dict[str, Any] | None = None,
):
    """Create a MySQL connection for audit persistence."""
    config = build_audit_db_config(env=env, prefix=prefix)
    if extra_config:
        config.update(extra_config)

    conn = mysql.connector.connect(**config)
    conn.set_charset_collation(charset=config["charset"], collation=config["collation"])
    return conn


def get_audit_db_connection_factory(
    env: str | None = None,
    prefix: str = "AUDIT_DB",
    extra_config: dict[str, Any] | None = None,
) -> Callable[[], Any]:
    """Return deferred factory to create audit DB connections."""

    def _factory():
        return create_audit_connection(env=env, prefix=prefix, extra_config=extra_config)

    return _factory


def build_sqlserver_config(env: str | None = None, prefix: str = "SQLSERVER") -> dict[str, Any]:
    """Build SQL Server config from .env with ENV suffix fallback."""
    selected_env = (env or get_app_env(default="PROD")).strip().upper()

    host = _require_env_value(f"{prefix}_HOST", selected_env)
    user = _require_env_value(f"{prefix}_USER", selected_env)
    password = _require_env_value(f"{prefix}_PASSWORD", selected_env)

    port = int(_get_env_value(f"{prefix}_PORT", selected_env) or "1433")
    database = _get_env_value(f"{prefix}_DATABASE", selected_env) or "master"
    configured_driver = _get_env_value(f"{prefix}_DRIVER", selected_env)
    driver = _resolve_sqlserver_driver(configured_driver)
    timeout = int(_get_env_value(f"{prefix}_CONNECTION_TIMEOUT", selected_env) or "10")
    encrypt = _get_env_value(f"{prefix}_ENCRYPT", selected_env) or "no"
    trust_certificate = _get_env_value(f"{prefix}_TRUST_SERVER_CERTIFICATE", selected_env) or "yes"

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "driver": driver,
        "timeout": timeout,
        "encrypt": encrypt,
        "trust_server_certificate": trust_certificate,
    }


def create_sqlserver_connection(
    env: str | None = None,
    prefix: str = "SQLSERVER",
    extra_config: dict[str, Any] | None = None,
):
    """Create SQL Server connection with pyodbc."""
    config = build_sqlserver_config(env=env, prefix=prefix)
    if extra_config:
        config.update(extra_config)

    conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['host']},{config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
    )

    # Encrypt/TrustServerCertificate aplican para drivers modernos.
    if str(config["driver"]).startswith("ODBC Driver"):
        conn_str += (
            f"Encrypt={config['encrypt']};"
            f"TrustServerCertificate={config['trust_server_certificate']};"
        )

    return pyodbc.connect(conn_str, timeout=int(config["timeout"]))


def create_sqlserver_connection_from_config(config: dict[str, Any]):
    """Create SQL Server connection from explicit runtime config."""
    host = str(config.get("host", "")).strip()
    user = str(config.get("user", "")).strip()
    password = str(config.get("password", "")).strip()
    if not host or not user or not password:
        raise ValueError("SQL Server config invalida: host/user/password son obligatorios")

    port = int(config.get("port", 1433))
    database = str(config.get("database", "master")).strip() or "master"
    timeout = int(config.get("timeout", 10))
    query_timeout = int(config.get("query_timeout", 0) or 0)
    encrypt = str(config.get("encrypt", "no")).strip() or "no"
    trust_server_certificate = str(config.get("trust_server_certificate", "yes")).strip() or "yes"

    configured_driver = str(config.get("driver", "")).strip() or None
    driver = _resolve_sqlserver_driver(configured_driver)

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
    )

    if str(driver).startswith("ODBC Driver"):
        conn_str += (
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_server_certificate};"
        )

    conn = pyodbc.connect(conn_str, timeout=timeout)
    if query_timeout > 0:
        conn.timeout = query_timeout
    return conn


def get_sqlserver_connection_factory(
    env: str | None = None,
    prefix: str = "SQLSERVER",
    extra_config: dict[str, Any] | None = None,
) -> Callable[[], Any]:
    """Return deferred factory to create SQL Server connections."""

    def _factory():
        return create_sqlserver_connection(env=env, prefix=prefix, extra_config=extra_config)

    return _factory
