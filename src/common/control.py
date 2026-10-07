from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


DEFAULT_OVERLAP_DAYS = {
    "doctors": 3,
    "patients": 3,
    "services": 7,
    "visits": 7,
    "sales": 7,
    "sale_services": 7,
    "visit_appointed_services": 14,
    "visit_rendered_services": 14,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_entity_since_map(
    engine: Engine,
    *,
    tenant_id: int,
    source_system: str,
    entities: Iterable[str],
) -> dict[str, datetime | None]:
    """Возвращает отдельную отметку прогресса для каждой сущности."""

    names = list(dict.fromkeys(entities))

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                select
                    e.entity_name,
                    w.watermark,
                    coalesce(c.overlap_days, 3)
                        as overlap_days
                from unnest(
                    cast(:entities as text[])
                ) e(entity_name)
                left join control.entity_watermarks w
                  on w.tenant_id=:tenant_id
                 and w.source_system=:source_system
                 and w.entity_name=e.entity_name
                left join control.entity_load_config c
                  on c.entity_name=e.entity_name
                 and c.enabled
            """),
            {
                "entities": names,
                "tenant_id": tenant_id,
                "source_system": source_system,
            },
        ).mappings().all()

    result: dict[str, datetime | None] = {}

    for row in rows:
        entity = str(row["entity_name"])
        watermark = row["watermark"]

        overlap = int(
            row["overlap_days"]
            if row["overlap_days"] is not None
            else DEFAULT_OVERLAP_DAYS.get(
                entity,
                3,
            )
        )

        result[entity] = (
            watermark - timedelta(days=overlap)
            if watermark is not None
            else None
        )

    for entity in names:
        result.setdefault(entity, None)

    return result


def start_load_run(
    engine: Engine,
    *,
    tenant_id: int,
    source_system: str,
    entities: Iterable[str],
) -> tuple[str, dict[str, datetime | None]]:
    """Создает запуск загрузки и фиксирует стартовые отметки прогресса."""

    since_map = get_entity_since_map(
        engine,
        tenant_id=tenant_id,
        source_system=source_system,
        entities=entities,
    )

    run_id = str(uuid.uuid4())

    serializable = {
        entity:
            value.isoformat()
            if value is not None
            else None
        for entity, value in since_map.items()
    }

    non_null = [
        value
        for value in since_map.values()
        if value is not None
    ]

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into control.load_runs(
                    load_run_id,
                    tenant_id,
                    source_system,
                    status,
                    since_watermark,
                    entity_since_watermarks
                )
                values(
                    cast(:run_id as uuid),
                    :tenant_id,
                    :source_system,
                    'running',
                    :minimum_since,
                    cast(:entity_since as jsonb)
                )
            """),
            {
                "run_id": run_id,
                "tenant_id": tenant_id,
                "source_system": source_system,
                "minimum_since":
                    min(non_null)
                    if non_null
                    else None,
                "entity_since":
                    json.dumps(serializable),
            },
        )

    return run_id, since_map


def record_entity_stats(
    engine: Engine,
    *,
    run_id: str,
    tenant_id: int,
    entity_name: str,
    extracted: int = 0,
    staged: int = 0,
    rejected: int = 0,
) -> None:
    """Накапливает статистику сущности внутри запуска загрузки."""

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into control.entity_load_stats(
                    load_run_id,
                    tenant_id,
                    entity_name,
                    extracted_rows,
                    staged_rows,
                    rejected_rows
                )
                values(
                    cast(:run_id as uuid),
                    :tenant_id,
                    :entity_name,
                    :extracted,
                    :staged,
                    :rejected
                )
                on conflict(
                    load_run_id,
                    entity_name
                )
                do update set
                    extracted_rows=
                        control.entity_load_stats
                            .extracted_rows
                        + excluded.extracted_rows,
                    staged_rows=
                        control.entity_load_stats
                            .staged_rows
                        + excluded.staged_rows,
                    rejected_rows=
                        control.entity_load_stats
                            .rejected_rows
                        + excluded.rejected_rows
            """),
            {
                "run_id": run_id,
                "tenant_id": tenant_id,
                "entity_name": entity_name,
                "extracted": extracted,
                "staged": staged,
                "rejected": rejected,
            },
        )


def record_chunk_checkpoint(
    engine: Engine,
    *,
    run_id: str,
    tenant_id: int,
    entity_name: str,
    chunk_no: int,
    chunk_rows: int,
    total_rows: int,
) -> None:
    """Фиксирует прогресс порционной загрузки в `raw`."""

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into
                control.entity_chunk_checkpoints(
                    load_run_id,
                    tenant_id,
                    entity_name,
                    chunk_no,
                    chunk_rows,
                    total_rows,
                    status
                )
                values(
                    cast(:run_id as uuid),
                    :tenant_id,
                    :entity_name,
                    :chunk_no,
                    :chunk_rows,
                    :total_rows,
                    'written'
                )
                on conflict(
                    load_run_id,
                    entity_name,
                    chunk_no
                )
                do update set
                    chunk_rows=excluded.chunk_rows,
                    total_rows=excluded.total_rows,
                    status=excluded.status,
                    updated_at=now()
            """),
            {
                "run_id": run_id,
                "tenant_id": tenant_id,
                "entity_name": entity_name,
                "chunk_no": chunk_no,
                "chunk_rows": chunk_rows,
                "total_rows": total_rows,
            },
        )


def advance_watermarks_in_connection(
    conn: Connection,
    *,
    tenant_id: int,
    source_system: str,
    run_id: str,
    entities: Iterable[str],
) -> None:
    """Продвигает отметки прогресса только в транзакции успешной публикации."""

    for entity in entities:
        attempted = conn.execute(
            text("""
                select exists(
                    select 1
                    from control.entity_load_stats
                    where
                        load_run_id=
                            cast(:run_id as uuid)
                        and entity_name=:entity
                )
            """),
            {
                "run_id": run_id,
                "entity": entity,
            },
        ).scalar()

        if not attempted:
            continue

        watermark = conn.execute(
            text("""
                select max(
                    coalesce(
                        least(
                            source_updated_at,
                            extracted_at
                                + interval '1 day'
                        ),
                        extracted_at
                    )
                )
                from raw.raw_records
                where
                    load_run_id=
                        cast(:run_id as uuid)
                    and entity_name=:entity
            """),
            {
                "run_id": run_id,
                "entity": entity,
            },
        ).scalar()

        conn.execute(
            text("""
                insert into control.entity_watermarks(
                    tenant_id,
                    source_system,
                    entity_name,
                    watermark,
                    last_successful_run_id,
                    last_checked_at
                )
                values(
                    :tenant_id,
                    :source_system,
                    :entity,
                    :watermark,
                    cast(:run_id as uuid),
                    now()
                )
                on conflict(
                    tenant_id,
                    source_system,
                    entity_name
                )
                do update set
                    watermark=case
                        when excluded.watermark is null
                            then control
                                .entity_watermarks
                                .watermark
                        when control
                            .entity_watermarks
                            .watermark is null
                            then excluded.watermark
                        else greatest(
                            control
                                .entity_watermarks
                                .watermark,
                            excluded.watermark
                        )
                    end,
                    last_successful_run_id=
                        excluded
                            .last_successful_run_id,
                    last_checked_at=
                        excluded.last_checked_at,
                    updated_at=now()
            """),
            {
                "tenant_id": tenant_id,
                "source_system": source_system,
                "entity": entity,
                "watermark": watermark,
                "run_id": run_id,
            },
        )
