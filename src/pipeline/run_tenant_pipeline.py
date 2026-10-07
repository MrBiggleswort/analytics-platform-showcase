from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy.engine import Engine

from src.common.control import (
    record_chunk_checkpoint,
    record_entity_stats,
    start_load_run,
)
from src.common.data_quality import (
    STAGE_CHECKS,
    run_checks,
)
from src.load.warehouse_writer import (
    WarehouseWriter,
)
from src.pipeline.semantic_publication import (
    build_snapshot,
    publish_snapshot,
)
from src.sources.base import SourceClient
from src.transform.raw_to_stage.visits import (
    transform_visits,
)
from src.transform.stage_to_mart.fulfillment import (
    rebuild_fulfillment_window,
)
from src.transform.stage_to_mart.revenue import (
    refresh_revenue_window,
)


DEFAULT_ENTITIES = (
    "doctors",
    "patients",
    "services",
    "visits",
    "sales",
    "sale_services",
    "visit_appointed_services",
    "visit_rendered_services",
)


def acquire_raw(
    engine: Engine,
    *,
    tenant_id: int,
    source_system: str,
    source: SourceClient,
    until: datetime | None = None,
    entities:
        Iterable[str] = DEFAULT_ENTITIES,
) -> str:
    """Выполняет только чтение источника и запись в `raw`."""

    entities = tuple(entities)

    run_id, since_map = start_load_run(
        engine,
        tenant_id=tenant_id,
        source_system=source_system,
        entities=entities,
    )

    writer = WarehouseWriter(
        engine
    )

    for entity_name in entities:

        def checkpoint(
            chunk_no: int,
            chunk_rows: int,
            total_rows: int,
            *,
            entity: str = entity_name,
        ) -> None:
            record_chunk_checkpoint(
                engine,
                run_id=run_id,
                tenant_id=tenant_id,
                entity_name=entity,
                chunk_no=chunk_no,
                chunk_rows=chunk_rows,
                total_rows=total_rows,
            )

        records = source.fetch_entity(
            entity_name,
            since=since_map.get(
                entity_name
            ),
            until=until,
        )

        extracted = (
            writer.write_raw_records(
                tenant_id=tenant_id,
                source_system=
                    source_system,
                entity_name=
                    entity_name,
                records=records,
                load_run_id=run_id,
                on_chunk_written=
                    checkpoint,
            )
        )

        record_entity_stats(
            engine,
            run_id=run_id,
            tenant_id=tenant_id,
            entity_name=entity_name,
            extracted=extracted,
        )

    return run_id


def continue_pipeline(
    engine: Engine,
    *,
    tenant_id: int,
    source_system: str,
    load_run_id: str,
    date_from,
    date_to,
) -> str:
    """Продолжает обработку после общего барьера получения данных."""

    staged_visits = (
        transform_visits(
            engine,
            tenant_id=tenant_id,
            load_run_id=load_run_id,
        )
    )

    record_entity_stats(
        engine,
        run_id=load_run_id,
        tenant_id=tenant_id,
        entity_name="visits",
        staged=staged_visits,
    )

    run_checks(
        engine,
        tenant_id=tenant_id,
        load_run_id=load_run_id,
        checks=STAGE_CHECKS,
        fail_on_error=True,
    )

    rebuild_fulfillment_window(
        engine,
        tenant_id=tenant_id,
        date_from=date_from,
        date_to=date_to,
    )

    refresh_revenue_window(
        engine,
        tenant_id=tenant_id,
        date_from=date_from,
        date_to=date_to,
    )

    publication = build_snapshot(
        engine,
        tenant_id=tenant_id,
        load_run_id=load_run_id,
        dataset_names=(
            "visit_activity",
            "revenue",
            "fulfillment",
        ),
    )

    publish_snapshot(
        engine,
        snapshot_id=
            publication.snapshot_id,
        tenant_id=tenant_id,
        load_run_id=load_run_id,
        source_system=source_system,
        watermark_entities=
            DEFAULT_ENTITIES,
    )

    return (
        publication.snapshot_id
    )
