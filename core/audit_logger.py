"""Logger de auditoria reutilizable para todas las automatizaciones."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo


VALID_STATES = {"EXITO", "ADVERTENCIA", "ERROR"}


@dataclass
class AuditLogger:
    """Registra la ejecucion del proceso y persiste la auditoria al finalizar.

    Ciclo de vida:
        __init__ -> start -> record_* / set_metric -> mark_* -> persist
    """

    db_conn_factory: Callable[[], Any]
    id_proceso: int | None = None
    process_name: str | None = None
    process_description: str | None = None
    create_process_if_missing: bool = False
    timezone_name: str = "America/Argentina/Buenos_Aires"
    max_chunk_chars: int = 10_000

    fecha_inicio: datetime | None = field(default=None, init=False)
    fecha_fin: datetime | None = field(default=None, init=False)
    estado: str | None = field(default=None, init=False)

    _metrics: dict[str, int] = field(default_factory=dict, init=False)
    _info_lines: list[str] = field(default_factory=list, init=False)
    _insert_ids: list[str] = field(default_factory=list, init=False)
    _update_headers: list[str] = field(default_factory=list, init=False)
    _update_details: list[str] = field(default_factory=list, init=False)
    _delete_ids: list[str] = field(default_factory=list, init=False)
    _warning_lines: list[str] = field(default_factory=list, init=False)
    _error_lines: list[str] = field(default_factory=list, init=False)

    _started: bool = field(default=False, init=False)
    _persisted: bool = field(default=False, init=False)
    _id_ejecucion: int | None = field(default=None, init=False)

    def start(self) -> None:
        if self._started:
            return
        self.fecha_inicio = self._now()
        self._started = True

    def set_metric(self, name: str, value: int) -> None:
        self._metrics[name] = int(value)

    def increment_metric(self, name: str, value: int = 1) -> None:
        self._metrics[name] = self._metrics.get(name, 0) + int(value)

    def record_info(self, message: str) -> None:
        self._info_lines.append(self._safe(message))

    def record_insert(self, entity_ids: list[str]) -> None:
        ids = [self._safe(x) for x in entity_ids if self._safe(x)]
        self._insert_ids.extend(ids)

    def record_update(self, entity_id: str, changes: list[str]) -> None:
        safe_id = self._safe(entity_id)
        clean_changes = [self._safe(c) for c in changes if self._safe(c)]

        if not safe_id:
            return

        self._update_headers.append(f"{safe_id} ({len(clean_changes)} cambios)")
        for change in clean_changes:
            self._update_details.append(f"- {safe_id}.{change}")

    def record_delete(self, entity_ids: list[str]) -> None:
        ids = [self._safe(x) for x in entity_ids if self._safe(x)]
        self._delete_ids.extend(ids)

    def record_warning(self, message: str) -> None:
        self._warning_lines.append(self._safe(message))

    def record_error(self, message: str) -> None:
        self._error_lines.append(self._safe(message))

    def mark_success(self) -> None:
        self.estado = "EXITO"

    def mark_warning(self) -> None:
        self.estado = "ADVERTENCIA"

    def mark_error(self, message: str, exc: Exception | None = None) -> None:
        full_message = self._safe(message)
        if exc is not None:
            full_message = f"{full_message} | {type(exc).__name__}: {self._safe(str(exc))}"
        self.record_error(full_message)
        self.estado = "ERROR"

    def build_summary(self) -> str:
        parts = [f"{k}={v}" for k, v in sorted(self._metrics.items())]
        parts.extend(
            [
                f"inserts={len(self._insert_ids)}",
                f"updates={len(self._update_headers)}",
                f"deletes={len(self._delete_ids)}",
                f"warnings={len(self._warning_lines)}",
                f"errors={len(self._error_lines)}",
            ]
        )
        return ", ".join(parts)

    def build_detail_chunks(self) -> list[str]:
        lines = self._build_detail_lines()
        return self._chunk_lines(lines, self.max_chunk_chars)

    def persist(self) -> int:
        if self._persisted:
            if self._id_ejecucion is None:
                raise RuntimeError("La auditoria ya fue persistida, pero falta el id de ejecucion")
            return self._id_ejecucion

        if not self._started:
            self.start()

        self.fecha_fin = self.fecha_fin or self._now()
        if not self.estado:
            self.estado = self._derive_state()

        if self.estado not in VALID_STATES:
            raise ValueError(f"Estado de auditoria invalido: {self.estado}")

        summary = self.build_summary()
        detail_chunks = self.build_detail_chunks()

        conn = self.db_conn_factory()
        try:
            conn.autocommit = False
            cursor = conn.cursor()
            try:
                resolved_id_proceso = self._resolve_process_id(cursor)

                cursor.execute(
                    (
                        "INSERT INTO EJECUCION "
                        "(id_proceso, fecha_inicio, fecha_fin, resumen, estado) "
                        "VALUES (%s, %s, %s, %s, %s)"
                    ),
                    (
                        resolved_id_proceso,
                        self._dt_as_sql(self.fecha_inicio),
                        self._dt_as_sql(self.fecha_fin),
                        summary,
                        self.estado,
                    ),
                )

                id_ejecucion = int(cursor.lastrowid)

                for chunk in detail_chunks:
                    cursor.execute(
                        "INSERT INTO LOG_PROCESOS (id_ejecucion, detalle) VALUES (%s, %s)",
                        (id_ejecucion, chunk),
                    )

                conn.commit()
                self._id_ejecucion = id_ejecucion
                self._persisted = True
                return id_ejecucion
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _resolve_process_id(self, cursor) -> int:
        if self.id_proceso is not None:
            return int(self.id_proceso)

        if not self.process_name:
            raise ValueError("Se requiere id_proceso o process_name para persistir la auditoria")

        cursor.execute(
            "SELECT id_proceso FROM PROCESOS WHERE nombre_proceso = %s LIMIT 1",
            (self.process_name,),
        )
        row = cursor.fetchone()
        if row:
            return int(row[0])

        if not self.create_process_if_missing:
            raise ValueError(
                "Proceso no encontrado en PROCESOS. "
                "Indica id_proceso, registra el proceso o activa create_process_if_missing=True"
            )

        cursor.execute(
            "INSERT INTO PROCESOS (nombre_proceso, descripcion_proceso) VALUES (%s, %s)",
            (self.process_name, self.process_description or ""),
        )
        return int(cursor.lastrowid)

    def _build_detail_lines(self) -> list[str]:
        if not self._started:
            self.start()

        final_state = self.estado or self._derive_state()
        end_dt = self.fecha_fin or self._now()

        lines: list[str] = [
            f"[INICIO] {self._fmt_dt(self.fecha_inicio)}",
            f"[RESUMEN] {self.build_summary()}",
        ]

        if self._info_lines:
            lines.append(f"[INFO] {self._join_compact(self._info_lines)}")

        if self._insert_ids:
            lines.append(f"[INSERT] {self._join_compact(self._insert_ids)}")

        if self._update_headers:
            lines.append(f"[UPDATE] {self._join_compact(self._update_headers)}")
            lines.extend(self._update_details)

        if self._delete_ids:
            lines.append(f"[DELETE] {self._join_compact(self._delete_ids)}")

        if self._warning_lines:
            lines.append(f"[WARNING] {self._join_compact(self._warning_lines)}")

        if self._error_lines:
            lines.append(f"[ERROR] {self._join_compact(self._error_lines)}")

        lines.append(f"[FIN] {final_state} | {self._fmt_dt(end_dt)}")
        return lines

    @staticmethod
    def _chunk_lines(lines: list[str], max_chunk_chars: int) -> list[str]:
        if max_chunk_chars < 500:
            raise ValueError("max_chunk_chars debe ser >= 500")

        chunks: list[str] = []
        current: list[str] = []
        current_len = 0

        for raw_line in lines:
            line = raw_line if raw_line is not None else ""
            line_len = len(line)
            sep_len = 1 if current else 0

            if current and current_len + sep_len + line_len > max_chunk_chars:
                chunks.append("\n".join(current))
                current = []
                current_len = 0
                sep_len = 0

            if line_len > max_chunk_chars:
                if current:
                    chunks.append("\n".join(current))
                    current = []
                    current_len = 0

                start = 0
                while start < line_len:
                    end = min(start + max_chunk_chars, line_len)
                    chunks.append(line[start:end])
                    start = end
                continue

            current.append(line)
            current_len += sep_len + line_len

        if current:
            chunks.append("\n".join(current))

        if not chunks:
            chunks = [""]

        return chunks

    def _derive_state(self) -> str:
        if self.estado in VALID_STATES:
            return self.estado
        if self._error_lines:
            return "ADVERTENCIA"
        if self._warning_lines:
            return "ADVERTENCIA"
        return "EXITO"

    def _now(self) -> datetime:
        return datetime.now(ZoneInfo(self.timezone_name))

    @staticmethod
    def _fmt_dt(dt: datetime | None) -> str:
        if dt is None:
            return "N/D"
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _dt_as_sql(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _safe(value: Any) -> str:
        text = "" if value is None else str(value)
        return " ".join(text.strip().split())

    @staticmethod
    def _join_compact(items: list[str]) -> str:
        return ", ".join(item for item in items if item)
