"""Template base para automatizaciones con auditoria transversal.

Objetivos:
- Mantener flujo limpio via Strategy (Extractor/Loader).
- Estandarizar metricas obligatorias para auditoria.
- Forzar dry-run fuera de PROD como boton de panico.

Como extender:
1) Crear extractores concretos heredando BaseExtractor.
2) Reemplazar transformador y loader por implementaciones reales.
3) Mantener el contrato de ActionPlan y metricas obligatorias.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.audit_logger import AuditLogger
from core.db_utils import get_app_env, get_audit_db_connection_factory


MANDATORY_METRICS = (
    "extraidos",
    "procesados",
    "noop",
    "inserts",
    "updates",
    "deletes",
    "warnings",
    "errors",
)


@dataclass
class RuntimeConfig:
    """Configuracion transversal de runtime para cualquier automatizacion."""

    app_env: str
    dry_run_requested: bool = False
    dry_run: bool = False
    process_name: str = "TEMPLATE_AUTOMATIZACION"
    process_description: str = "Template base para automatizaciones"
    timezone_name: str = "America/Argentina/Buenos_Aires"
    max_chunk_chars: int = 10_000


@dataclass
class ExecutionContext:
    """Contexto de ejecucion compartido entre etapas."""

    config: RuntimeConfig
    audit: AuditLogger


@dataclass
class Record:
    """Registro canonico de ejemplo para dominio multi-sucursal."""

    entity_id: str
    sucursal_origen: str
    payload: dict[str, Any]


@dataclass
class UpdatePlan:
    """Entidad candidata a update con detalle de cambios."""

    entity_id: str
    changes: list[str]
    new_payload: dict[str, Any]


@dataclass
class ActionPlan:
    """Plan CRUD generado por la fase de clasificacion."""

    inserts: list[Record] = field(default_factory=list)
    updates: list[UpdatePlan] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)
    noop: int = 0


@dataclass
class ApplyResult:
    """Resultado de aplicar (o simular) acciones."""

    inserted_ids: list[str] = field(default_factory=list)
    updated_items: list[tuple[str, list[str]]] = field(default_factory=list)
    deleted_ids: list[str] = field(default_factory=list)


class BaseExtractor:
    """Strategy base de extraccion."""

    source_name: str = "BASE"

    def extract(self, ctx: ExecutionContext) -> list[Record]:
        raise NotImplementedError


class BaseTransformer:
    """Strategy base de transformacion."""

    def transform(self, rows: list[Record], ctx: ExecutionContext) -> list[Record]:
        raise NotImplementedError


class BaseLoader:
    """Strategy base de carga/aplicacion."""

    def load_current_state(self, ctx: ExecutionContext) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    def apply_actions(self, plan: ActionPlan, ctx: ExecutionContext) -> ApplyResult:
        raise NotImplementedError


class FordProspectExtractor(BaseExtractor):
    source_name = "FORD"

    def extract(self, ctx: ExecutionContext) -> list[Record]:
        # Dummy de ejemplo de dominio multi-sucursal.
        return [
            Record(
                entity_id="P-1001|FORD",
                sucursal_origen="FORD",
                payload={"prospecto_id": "P-1001", "nombre": "Juan Perez", "telefono": "1122334455"},
            ),
            Record(
                entity_id="P-1002|FORD",
                sucursal_origen="FORD",
                payload={"prospecto_id": "P-1002", "nombre": "Ana Gomez", "telefono": "1199887766"},
            ),
        ]


class PeugeotProspectExtractor(BaseExtractor):
    source_name = "PEUGEOT"

    def extract(self, ctx: ExecutionContext) -> list[Record]:
        return [
            Record(
                entity_id="P-2001|PEUGEOT",
                sucursal_origen="PEUGEOT",
                payload={"prospecto_id": "P-2001", "nombre": "Mario Lopez", "telefono": "1133344455"},
            ),
            Record(
                entity_id="P-2002|PEUGEOT",
                sucursal_origen="PEUGEOT",
                payload={"prospecto_id": "P-2002", "nombre": "Julia Diaz", "telefono": "1144455566"},
            ),
        ]


class ProspectTransformer(BaseTransformer):
    """Normaliza payload basico y limpia strings."""

    def transform(self, rows: list[Record], ctx: ExecutionContext) -> list[Record]:
        normalized: list[Record] = []
        for row in rows:
            clean_payload = {
                key: (str(value).strip() if value is not None else "")
                for key, value in row.payload.items()
            }
            normalized.append(
                Record(
                    entity_id=row.entity_id.strip(),
                    sucursal_origen=row.sucursal_origen.strip().upper(),
                    payload=clean_payload,
                )
            )
        return normalized


class DummyLoader(BaseLoader):
    """Loader de ejemplo: simula estado actual y aplica CRUD dummy."""

    def load_current_state(self, ctx: ExecutionContext) -> dict[str, dict[str, Any]]:
        # Estado simulado para mostrar INSERT/UPDATE/NOOP.
        return {
            "P-1001|FORD": {
                "prospecto_id": "P-1001",
                "nombre": "Juan Perez",
                "telefono": "1100000000",
            },
            "P-2002|PEUGEOT": {
                "prospecto_id": "P-2002",
                "nombre": "Julia Diaz",
                "telefono": "1144455566",
            },
        }

    def apply_actions(self, plan: ActionPlan, ctx: ExecutionContext) -> ApplyResult:
        if ctx.config.dry_run:
            return ApplyResult(
                inserted_ids=[item.entity_id for item in plan.inserts],
                updated_items=[(item.entity_id, item.changes) for item in plan.updates],
                deleted_ids=plan.deletes[:],
            )

        # En implementaciones reales: ejecutar escrituras (DB/API/Sheets).
        return ApplyResult(
            inserted_ids=[item.entity_id for item in plan.inserts],
            updated_items=[(item.entity_id, item.changes) for item in plan.updates],
            deleted_ids=plan.deletes[:],
        )


def build_runtime_config(dry_run_requested: bool = False) -> RuntimeConfig:
    app_env = get_app_env(default="TEST")
    force_dry_run = app_env != "PROD"
    effective_dry_run = True if force_dry_run else bool(dry_run_requested)
    return RuntimeConfig(
        app_env=app_env,
        dry_run_requested=bool(dry_run_requested),
        dry_run=effective_dry_run,
    )


def merge_sources(records_by_source: dict[str, list[Record]], ctx: ExecutionContext) -> list[Record]:
    merged: list[Record] = []
    for source_name, rows in records_by_source.items():
        ctx.audit.record_detail_line(f"[SOURCE_SUMMARY] source={source_name} extraidos={len(rows)}")
        merged.extend(rows)
    return merged


def plan_actions(
    processed_rows: list[Record],
    current_state: dict[str, dict[str, Any]],
) -> ActionPlan:
    plan = ActionPlan()

    for row in processed_rows:
        existing = current_state.get(row.entity_id)
        if existing is None:
            plan.inserts.append(row)
            continue

        changes: list[str] = []
        for key, new_value in row.payload.items():
            old_value = str(existing.get(key, "")).strip()
            if old_value != new_value:
                changes.append(f"{key}: '{old_value}' -> '{new_value}'")

        if changes:
            plan.updates.append(UpdatePlan(entity_id=row.entity_id, changes=changes, new_payload=row.payload))
        else:
            plan.noop += 1

    return plan


def apply_metrics(plan: ActionPlan, extracted_count: int, processed_count: int, ctx: ExecutionContext) -> None:
    ctx.audit.set_metric("extraidos", extracted_count)
    ctx.audit.set_metric("procesados", processed_count)
    ctx.audit.set_metric("noop", plan.noop)


def ensure_mandatory_metrics(ctx: ExecutionContext) -> None:
    for metric in MANDATORY_METRICS:
        if metric not in ctx.audit._metrics:  # acceso controlado para forzar contrato en template
            ctx.audit.set_metric(metric, 0)


def run_automatizacion(dry_run_requested: bool = False) -> None:
    config = build_runtime_config(dry_run_requested=dry_run_requested)
    audit_env = "PROD" if config.app_env == "PROD" else "TEST"

    audit = AuditLogger(
        db_conn_factory=get_audit_db_connection_factory(env=audit_env),
        process_name=config.process_name,
        process_description=config.process_description,
        create_process_if_missing=True,
        timezone_name=config.timezone_name,
        max_chunk_chars=config.max_chunk_chars,
    )
    ctx = ExecutionContext(config=config, audit=audit)

    extractors: list[BaseExtractor] = [FordProspectExtractor(), PeugeotProspectExtractor()]
    transformer: BaseTransformer = ProspectTransformer()
    loader: BaseLoader = DummyLoader()

    try:
        audit.start()
        audit.record_info(
            f"APP_ENV={config.app_env}, dry_run_requested={config.dry_run_requested}, dry_run_effective={config.dry_run}"
        )

        records_by_source: dict[str, list[Record]] = {}
        for extractor in extractors:
            audit.record_detail_line(f"[SOURCE_START] source={extractor.source_name}")
            rows = extractor.extract(ctx)
            records_by_source[extractor.source_name] = rows

        extracted_rows = merge_sources(records_by_source, ctx)
        processed_rows = transformer.transform(extracted_rows, ctx)

        current_state = loader.load_current_state(ctx)
        plan = plan_actions(processed_rows=processed_rows, current_state=current_state)
        apply_metrics(plan=plan, extracted_count=len(extracted_rows), processed_count=len(processed_rows), ctx=ctx)

        result = loader.apply_actions(plan=plan, ctx=ctx)

        if result.inserted_ids:
            audit.record_insert(result.inserted_ids)
        for entity_id, changes in result.updated_items:
            audit.record_update(entity_id=entity_id, changes=changes)
        if result.deleted_ids:
            audit.record_delete(result.deleted_ids)

        if config.dry_run:
            audit.record_warning(
                "Dry-run activo por politica de entorno o parametro. No se aplicaron escrituras reales."
            )

        # Metricas estructurales obligatorias.
        audit.set_metric("inserts", len(result.inserted_ids))
        audit.set_metric("updates", len(result.updated_items))
        audit.set_metric("deletes", len(result.deleted_ids))
        audit.set_metric("warnings", 1 if config.dry_run else 0)
        audit.set_metric("errors", 0)

        if config.dry_run:
            audit.mark_warning()
        else:
            audit.mark_success()

    except Exception as exc:
        audit.record_error(f"Fallo fatal: {type(exc).__name__}: {exc}")
        audit.set_metric("errors", 1)
        audit.mark_error("Fallo fatal en automatizacion", exc=exc)
        raise

    finally:
        ensure_mandatory_metrics(ctx)
        id_ejecucion = audit.persist()
        print(
            f"[INFO] Template ejecutado. APP_ENV={config.app_env} dry_run={config.dry_run} "
            f"id_ejecucion={id_ejecucion}"
        )


if __name__ == "__main__":
    run_automatizacion(dry_run_requested=False)
