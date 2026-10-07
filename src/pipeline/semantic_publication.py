from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.control import (
    advance_watermarks_in_connection,
)


@dataclass(frozen=True)
class PublicationResult:
    snapshot_id: str
    dataset_rows: dict[str, int]


def build_snapshot(
    engine: Engine,
    *,
    tenant_id: int,
    load_run_id: str,
    dataset_names: Iterable[str],
) -> PublicationResult:
    """Собирает снимок тенанта, не меняя опубликованное состояние BI."""

    snapshot_id = str(uuid.uuid4())

    datasets = tuple(
        dict.fromkeys(dataset_names)
    )

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into
                control.semantic_snapshots(
                    snapshot_id,
                    tenant_id,
                    load_run_id,
                    status
                )
                values(
                    cast(:snapshot_id as uuid),
                    :tenant_id,
                    cast(:run_id as uuid),
                    'building'
                )
            """),
            {
                "snapshot_id": snapshot_id,
                "tenant_id": tenant_id,
                "run_id": load_run_id,
            },
        )

    row_counts: dict[str, int] = {}

    try:
        for dataset_name in datasets:
            safe_name = (
                dataset_name
                .replace("_", "")
                .isalnum()
            )

            if not safe_name:
                raise ValueError(
                    "Некорректное имя набора данных: "
                    f"{dataset_name!r}"
                )

            with engine.begin() as conn:
                conn.execute(
                    text("""
                        insert into
                        control
                            .semantic_snapshot_datasets(
                                snapshot_id,
                                tenant_id,
                                dataset_name,
                                status
                            )
                        values(
                            cast(
                                :snapshot_id
                                as uuid
                            ),
                            :tenant_id,
                            :dataset_name,
                            'building'
                        )
                    """),
                    {
                        "snapshot_id":
                            snapshot_id,
                        "tenant_id":
                            tenant_id,
                        "dataset_name":
                            dataset_name,
                    },
                )

                result = conn.execute(
                    text(f"""
                        insert into
                            published
                                .{dataset_name}
                        select
                            cast(
                                :snapshot_id
                                as uuid
                            ),
                            semantic.*
                        from
                            semantic
                                .{dataset_name}
                                semantic
                        where
                            semantic.tenant_id=
                                :tenant_id
                    """),
                    {
                        "snapshot_id":
                            snapshot_id,
                        "tenant_id":
                            tenant_id,
                    },
                )

                count = max(
                    int(result.rowcount or 0),
                    0,
                )

                row_counts[
                    dataset_name
                ] = count

                conn.execute(
                    text("""
                        update control
                            .semantic_snapshot_datasets
                        set
                            status='success',
                            row_count=:row_count,
                            build_finished_at=now()
                        where
                            snapshot_id=
                                cast(
                                    :snapshot_id
                                    as uuid
                                )
                            and
                            dataset_name=
                                :dataset_name
                    """),
                    {
                        "snapshot_id":
                            snapshot_id,
                        "dataset_name":
                            dataset_name,
                        "row_count":
                            count,
                    },
                )

        with engine.begin() as conn:
            conn.execute(
                text("""
                    update
                        control.semantic_snapshots
                    set
                        status='validated',
                        validated_at=now(),
                        row_count=:row_count,
                        details=
                            cast(
                                :details
                                as jsonb
                            )
                    where
                        snapshot_id=
                            cast(
                                :snapshot_id
                                as uuid
                            )
                """),
                {
                    "snapshot_id":
                        snapshot_id,
                    "row_count":
                        sum(
                            row_counts.values()
                        ),
                    "details":
                        json.dumps(
                            {
                                "datasets":
                                    row_counts,
                            },
                            ensure_ascii=False,
                        ),
                },
            )

    except Exception as exc:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    update
                        control.semantic_snapshots
                    set
                        status='failed',
                        failed_at=now(),
                        error_message=:error
                    where
                        snapshot_id=
                            cast(
                                :snapshot_id
                                as uuid
                            )
                """),
                {
                    "snapshot_id":
                        snapshot_id,
                    "error":
                        str(exc)[:4000],
                },
            )

        raise

    return PublicationResult(
        snapshot_id=snapshot_id,
        dataset_rows=row_counts,
    )


def publish_snapshot(
    engine: Engine,
    *,
    snapshot_id: str,
    tenant_id: int,
    load_run_id: str,
    source_system: str,
    watermark_entities:
        Iterable[str],
) -> None:
    """Атомарно переключает снимок BI и продвигает контрольные отметки."""

    with engine.begin() as conn:
        status = conn.execute(
            text("""
                select status
                from
                    control.semantic_snapshots
                where
                    snapshot_id=
                        cast(
                            :snapshot_id
                            as uuid
                        )
                    and
                    tenant_id=:tenant_id
                for update
            """),
            {
                "snapshot_id":
                    snapshot_id,
                "tenant_id":
                    tenant_id,
            },
        ).scalar_one_or_none()

        if status != "validated":
            raise ValueError(
                "Снимок нельзя "
                "публиковать из состояния "
                f"{status!r}"
            )

        conn.execute(
            text("""
                update
                    control.semantic_snapshots
                set
                    status='retired',
                    retired_at=now()
                where
                    tenant_id=:tenant_id
                    and status='published'
            """),
            {
                "tenant_id": tenant_id,
            },
        )

        conn.execute(
            text("""
                update
                    control.semantic_snapshots
                set
                    status='published',
                    published_at=now()
                where
                    snapshot_id=
                        cast(
                            :snapshot_id
                            as uuid
                        )
            """),
            {
                "snapshot_id":
                    snapshot_id,
            },
        )

        advance_watermarks_in_connection(
            conn,
            tenant_id=tenant_id,
            source_system=source_system,
            run_id=load_run_id,
            entities=watermark_entities,
        )

        conn.execute(
            text("""
                update control.load_runs
                set
                    status='success',
                    finished_at=now(),
                    published_snapshot_id=
                        cast(
                            :snapshot_id
                            as uuid
                        )
                where
                    load_run_id=
                        cast(
                            :run_id
                            as uuid
                        )
            """),
            {
                "run_id": load_run_id,
                "snapshot_id": snapshot_id,
            },
        )
