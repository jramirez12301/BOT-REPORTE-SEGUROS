"""Database connection helpers for shared audit infrastructure."""

from __future__ import annotations

import os
from typing import Any, Callable

import mysql.connector
from dotenv import load_dotenv

load_dotenv()


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
