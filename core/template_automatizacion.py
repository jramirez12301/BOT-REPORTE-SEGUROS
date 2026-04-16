"""Template base para nuevas automatizaciones.

Copiar este archivo dentro de la carpeta de una nueva automatizacion y
reemplazar las secciones TODO segun la logica de negocio.
"""

from __future__ import annotations

import logging

from core.audit_logger import AuditLogger
from core.db_utils import get_audit_db_connection_factory


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def run_automatizacion() -> None:
    """Punto de entrada generico para una automatizacion."""
    audit = AuditLogger(
        db_conn_factory=get_audit_db_connection_factory(env="TEST"),
        process_name="NOMBRE_AUTOMATIZACION",
        process_description="Descripcion breve de la automatizacion",
        create_process_if_missing=True,
        timezone_name="America/Argentina/Buenos_Aires",
        max_chunk_chars=10_000,
    )

    try:
        audit.start()
        audit.record_info("Inicio de ciclo de automatizacion")

        # TODO: extraer datos
        # ejemplo: rows = fetch_source_data()
        rows = []
        audit.set_metric("extraidos", len(rows))

        # TODO: transformar datos
        processed_rows = rows
        audit.set_metric("procesados", len(processed_rows))

        # TODO: aplicar operaciones de negocio
        # Aqui identificas el ID principal de tu entidad y lo pasas al logger.
        # Ejemplo insert:
        inserted_ids = []
        if inserted_ids:
            audit.record_insert(inserted_ids)

        # Ejemplo update con detalle de cambios por campo:
        # changes debe ser lista de strings con formato sugerido:
        # "campo_x: 'valor_old' -> 'valor_new'"
        updates: list[tuple[str, list[str]]] = []
        for entity_id, changes in updates:
            audit.record_update(entity_id=entity_id, changes=changes)

        # Ejemplo delete opcional:
        deleted_ids = []
        if deleted_ids:
            audit.record_delete(deleted_ids)

        # Warning no fatal (si aplica):
        # audit.record_warning("Entidad ABC omitida por validacion")

        # Decide estado final de la automatizacion
        # Si hubo warnings de negocio, usar mark_warning().
        audit.mark_success()

    except Exception as exc:
        logging.exception("Fallo fatal en automatizacion")
        audit.mark_error("Fallo fatal en automatizacion", exc=exc)
        raise

    finally:
        try:
            id_ejecucion = audit.persist()
            logging.info("Auditoria persistida. id_ejecucion=%s", id_ejecucion)
        except Exception:
            logging.exception("No se pudo persistir la auditoria")


if __name__ == "__main__":
    run_automatizacion()
