from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.source_keys import (
    normalize_source_key,
    resolve_source_keys,
)
from src.load.warehouse_writer import (
    WarehouseWriter,
)


def payload_rows(
    engine: Engine,
    *,
    tenant_id: int,
    load_run_id: str,
) -> list[dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                select payload
                from raw.raw_records
                where
                    tenant_id=:tenant_id
                    and load_run_id=
                        cast(:run_id as uuid)
                    and entity_name='visits'
                order by id
            """),
            {
                "tenant_id": tenant_id,
                "run_id": load_run_id,
            },
        ).scalars().all()

    return [
        dict(row)
        for row in rows
        if isinstance(row, dict)
    ]


def transform_visits(
    engine: Engine,
    *,
    tenant_id: int,
    load_run_id: str,
) -> int:
    """Нормализует визиты и разрешает ссылки через реестр."""

    source_rows = payload_rows(
        engine,
        tenant_id=tenant_id,
        load_run_id=load_run_id,
    )

    if not source_rows:
        return 0

    visit_keys = [
        normalize_source_key(
            row.get("source_key")
            or row.get("visit_id")
            or row.get("id")
        )
        for row in source_rows
    ]

    patient_keys = [
        normalize_source_key(
            row.get(
                "patient_source_key"
            )
            or row.get("patient_id")
        )
        for row in source_rows
    ]

    doctor_keys = [
        normalize_source_key(
            row.get(
                "doctor_source_key"
            )
            or row.get("doctor_id")
        )
        for row in source_rows
    ]

    visit_map = resolve_source_keys(
        engine,
        tenant_id=tenant_id,
        entity_type="visit",
        values=visit_keys,
        load_run_id=load_run_id,
    )

    patient_map = resolve_source_keys(
        engine,
        tenant_id=tenant_id,
        entity_type="patient",
        values=patient_keys,
        load_run_id=load_run_id,
    )

    doctor_map = resolve_source_keys(
        engine,
        tenant_id=tenant_id,
        entity_type="doctor",
        values=doctor_keys,
        load_run_id=load_run_id,
    )

    normalized: list[
        dict[str, Any]
    ] = []

    for source in source_rows:
        visit_key = normalize_source_key(
            source.get("source_key")
            or source.get("visit_id")
            or source.get("id")
        )

        patient_key = (
            normalize_source_key(
                source.get(
                    "patient_source_key"
                )
                or source.get(
                    "patient_id"
                )
            )
        )

        doctor_key = (
            normalize_source_key(
                source.get(
                    "doctor_source_key"
                )
                or source.get(
                    "doctor_id"
                )
            )
        )

        if visit_key is None:
            continue

        normalized.append(
            {
                "tenant_id": tenant_id,
                "id":
                    visit_map[visit_key],
                "source_key":
                    visit_key,
                "patient_id":
                    patient_map.get(
                        patient_key
                    )
                    if patient_key
                    else None,
                "doctor_id":
                    doctor_map.get(
                        doctor_key
                    )
                    if doctor_key
                    else None,
                "visit_datetime":
                    source.get(
                        "visit_datetime"
                    )
                    or source.get(
                        "event_datetime"
                    ),
                "source_load_run_id":
                    load_run_id,
            }
        )

    frame = pd.DataFrame(
        normalized
    )

    return WarehouseWriter(
        engine
    ).upsert_dataframe(
        schema="stage",
        table="visits",
        df=frame,
        conflict_cols=(
            "tenant_id",
            "id",
        ),
        update_cols=(
            "source_key",
            "patient_id",
            "doctor_id",
            "visit_datetime",
            "source_load_run_id",
        ),
        compare_cols=(
            "source_key",
            "patient_id",
            "doctor_id",
            "visit_datetime",
        ),
    )
