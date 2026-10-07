from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from socket import gethostname
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class RecalculationWindow:
    queue_ids: tuple[int, ...]
    tenant_id: int
    date_from: date
    date_to: date
    scope: str
    run_id: str


def enqueue_window(
    engine: Engine,
    *,
    tenant_id: int,
    date_from: date,
    date_to: date,
    reason: str,
    priority: int = 100,
) -> int:
    """Добавляет окно бизнес-дат для точечного пересчета."""

    if date_from > date_to:
        raise ValueError(
            "date_from позже date_to"
        )

    with engine.begin() as conn:
        return int(
            conn.execute(
                text("""
                    insert into
                    control.recalculation_queue(
                        tenant_id,
                        visit_date_from,
                        visit_date_to,
                        reason,
                        priority,
                        status
                    )
                    values(
                        :tenant_id,
                        :date_from,
                        :date_to,
                        :reason,
                        :priority,
                        'pending'
                    )
                    returning id
                """),
                {
                    "tenant_id": tenant_id,
                    "date_from": date_from,
                    "date_to": date_to,
                    "reason": reason,
                    "priority": priority,
                },
            ).scalar_one()
        )


def claim_window(
    engine: Engine,
    *,
    tenant_id: int,
    max_items: int = 1000,
    worker_id: str | None = None,
) -> RecalculationWindow | None:
    """Объединяет пересекающиеся окна и забирает их обработчиком."""

    worker_id = worker_id or gethostname()
    run_id = str(uuid4())

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                with anchor as (
                    select
                        id,
                        visit_date_from,
                        visit_date_to,
                        scope
                    from control.recalculation_queue
                    where
                        tenant_id=:tenant_id
                        and status='pending'
                        and available_at <= now()
                    order by priority, id
                    for update skip locked
                    limit 1
                ),
                selected as (
                    select q.id
                    from control.recalculation_queue q
                    join anchor a
                      on q.visit_date_from
                            <= a.visit_date_to
                     and q.visit_date_to
                            >= a.visit_date_from
                    where
                        q.tenant_id=:tenant_id
                        and q.status='pending'
                    order by q.priority, q.id
                    limit :max_items
                    for update of q skip locked
                )
                update control.recalculation_queue q
                set
                    status='running',
                    claimed_at=now(),
                    worker_id=:worker_id,
                    recalculation_run_id=
                        cast(:run_id as uuid)
                from selected s
                where q.id=s.id
                returning
                    q.id,
                    q.visit_date_from,
                    q.visit_date_to,
                    q.scope
            """),
            {
                "tenant_id": tenant_id,
                "max_items": int(max_items),
                "worker_id": worker_id,
                "run_id": run_id,
            },
        ).mappings().all()

    if not rows:
        return None

    return RecalculationWindow(
        queue_ids=tuple(
            int(row["id"])
            for row in rows
        ),
        tenant_id=tenant_id,
        date_from=min(
            row["visit_date_from"]
            for row in rows
        ),
        date_to=max(
            row["visit_date_to"]
            for row in rows
        ),
        scope="all",
        run_id=run_id,
    )


def finish_window(
    engine: Engine,
    *,
    window: RecalculationWindow,
    success: bool,
    error_message: str | None = None,
) -> None:
    """Завершает строки очереди одного запуска пересчета."""

    status = (
        "success"
        if success
        else "failed"
    )

    with engine.begin() as conn:
        conn.execute(
            text("""
                update control.recalculation_queue
                set
                    status=:status,
                    finished_at=now(),
                    error_message=:error
                where
                    id=any(
                        cast(:ids as bigint[])
                    )
                    and recalculation_run_id=
                        cast(:run_id as uuid)
            """),
            {
                "status": status,
                "error": error_message,
                "ids": list(window.queue_ids),
                "run_id": window.run_id,
            },
        )
