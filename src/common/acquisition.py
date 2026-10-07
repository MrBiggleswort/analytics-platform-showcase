from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine


TERMINAL_STATUSES = {
    "success",
    "failed",
    "no_fresh_data",
    "timeout",
}


def start_wave(
    engine: Engine,
    *,
    tenants: Iterable[dict[str, Any]],
    timeout_minutes: int,
    airflow_dag_id: str,
    airflow_run_id: str,
) -> str:
    """Создает волну и заранее регистрирует всех активных тенантов."""

    tenants = list(tenants)

    if not tenants:
        raise ValueError(
            "Нельзя создать волну получения данных без тенантов"
        )

    wave_id = str(uuid.uuid4())
    deadline = (
        datetime.now(timezone.utc)
        + timedelta(
            minutes=int(timeout_minutes)
        )
    )

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into control.acquisition_waves(
                    wave_id,
                    status,
                    deadline_at,
                    airflow_dag_id,
                    airflow_run_id,
                    tenant_count
                )
                values(
                    cast(:wave_id as uuid),
                    'running',
                    :deadline,
                    :dag_id,
                    :run_id,
                    :tenant_count
                )
            """),
            {
                "wave_id": wave_id,
                "deadline": deadline,
                "dag_id": airflow_dag_id,
                "run_id": airflow_run_id,
                "tenant_count": len(tenants),
            },
        )

        for tenant in tenants:
            conn.execute(
                text("""
                    insert into control.acquisition_runs(
                        acquisition_run_id,
                        wave_id,
                        tenant_id,
                        tenant_code,
                        source_type,
                        acquisition_mode,
                        status,
                        deadline_at,
                        airflow_task_id
                    )
                    values(
                        gen_random_uuid(),
                        cast(:wave_id as uuid),
                        :tenant_id,
                        :tenant_code,
                        :source_type,
                        :acquisition_mode,
                        'pending',
                        :deadline,
                        :airflow_task_id
                    )
                """),
                {
                    "wave_id": wave_id,
                    "tenant_id":
                        int(tenant["tenant_id"]),
                    "tenant_code":
                        str(tenant["tenant_code"]),
                    "source_type":
                        str(tenant["source_type"]),
                    "acquisition_mode":
                        str(tenant["acquisition_mode"]),
                    "deadline": deadline,
                    "airflow_task_id":
                        tenant.get(
                            "airflow_task_id"
                        ),
                },
            )

    return wave_id


def get_wave_deadline(
    engine: Engine,
    *,
    wave_id: str,
) -> datetime:
    with engine.begin() as conn:
        deadline = conn.execute(
            text("""
                select deadline_at
                from control.acquisition_waves
                where wave_id=
                    cast(:wave_id as uuid)
            """),
            {"wave_id": wave_id},
        ).scalar_one_or_none()

    if deadline is None:
        raise KeyError(
            f"Волна получения данных не найдена: {wave_id}"
        )

    return deadline


def claim_tenant(
    engine: Engine,
    *,
    wave_id: str,
    tenant_id: int,
    airflow_task_id: str,
) -> str:
    """Атомарно переводит тенант из `pending` в `running`."""

    with engine.begin() as conn:
        run_id = conn.execute(
            text("""
                update control.acquisition_runs
                set
                    status='running',
                    started_at=
                        coalesce(
                            started_at,
                            now()
                        ),
                    airflow_task_id=:task_id
                where
                    wave_id=
                        cast(:wave_id as uuid)
                    and tenant_id=:tenant_id
                    and status='pending'
                    and deadline_at > now()
                returning
                    acquisition_run_id::text
            """),
            {
                "wave_id": wave_id,
                "tenant_id": tenant_id,
                "task_id": airflow_task_id,
            },
        ).scalar_one_or_none()

    if run_id is None:
        raise RuntimeError(
            f"Tenant {tenant_id} "
            "не может быть взят в обработку получения данных"
        )

    return str(run_id)


def finish_tenant(
    engine: Engine,
    *,
    acquisition_run_id: str,
    status: str,
    load_run_id: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> str:
    """Фиксирует конечное состояние тенанта."""

    normalized = status.strip().lower()

    if normalized not in TERMINAL_STATUSES:
        raise ValueError(
            f"Некорректный конечный статус: {status!r}"
        )

    with engine.begin() as conn:
        result = conn.execute(
            text("""
                update control.acquisition_runs
                set
                    status=:status,
                    finished_at=now(),
                    load_run_id=case
                        when :load_run_id is null
                            then load_run_id
                        else
                            cast(
                                :load_run_id
                                as uuid
                            )
                    end,
                    error_type=:error_type,
                    error_message=:error_message,
                    details=
                        details
                        || cast(:details as jsonb)
                where
                    acquisition_run_id=
                        cast(:run_id as uuid)
                    and status='running'
                returning status
            """),
            {
                "run_id": acquisition_run_id,
                "status": normalized,
                "load_run_id": load_run_id,
                "error_type": error_type,
                "error_message":
                    error_message[:4000]
                    if error_message
                    else None,
                "details":
                    json.dumps(
                        details or {},
                        default=str,
                    ),
            },
        ).scalar_one_or_none()

    if result is None:
        raise RuntimeError(
            "Запуск получения данных уже завершен: "
            f"{acquisition_run_id}"
        )

    return str(result)


def expire_deadline(
    engine: Engine,
    *,
    wave_id: str,
) -> int:
    """Закрывает незавершенные записи после истечения общего срока."""

    with engine.begin() as conn:
        result = conn.execute(
            text("""
                update control.acquisition_runs r
                set
                    status='timeout',
                    finished_at=now(),
                    error_type=
                        'ACQUISITION_TIMEOUT',
                    error_message=
                        'Общий срок ожидания получения данных истек'
                from control.acquisition_waves w
                where
                    r.wave_id=w.wave_id
                    and w.wave_id=
                        cast(:wave_id as uuid)
                    and w.deadline_at <= now()
                    and r.status in (
                        'pending',
                        'running'
                    )
            """),
            {"wave_id": wave_id},
        )

    return max(
        int(result.rowcount or 0),
        0,
    )


def wave_ready(
    engine: Engine,
    *,
    wave_id: str,
) -> bool:
    """Возвращает True только когда каждый тенант достиг конечного состояния."""

    expire_deadline(
        engine,
        wave_id=wave_id,
    )

    with engine.begin() as conn:
        pending = conn.execute(
            text("""
                select count(*)
                from control.acquisition_runs
                where
                    wave_id=
                        cast(:wave_id as uuid)
                    and status not in (
                        'success',
                        'failed',
                        'no_fresh_data',
                        'timeout'
                    )
            """),
            {"wave_id": wave_id},
        ).scalar_one()

    return int(pending) == 0


def acquisition_for_tenant(
    engine: Engine,
    *,
    wave_id: str,
    tenant_id: int,
) -> dict[str, Any] | None:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                select *
                from control.acquisition_runs
                where
                    wave_id=
                        cast(:wave_id as uuid)
                    and tenant_id=:tenant_id
            """),
            {
                "wave_id": wave_id,
                "tenant_id": tenant_id,
            },
        ).mappings().one_or_none()

    return dict(row) if row else None


def finalize_wave(
    engine: Engine,
    *,
    wave_id: str,
) -> dict[str, Any]:
    """Фиксирует итог волны и возвращает сводку для оператора."""

    expire_deadline(
        engine,
        wave_id=wave_id,
    )

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                select
                    tenant_id,
                    tenant_code,
                    status,
                    error_type,
                    error_message,
                    extract(
                        epoch from (
                            coalesce(
                                finished_at,
                                now()
                            )
                            - coalesce(
                                started_at,
                                created_at
                            )
                        )
                    )::bigint
                        as duration_seconds
                from control.acquisition_runs
                where
                    wave_id=
                        cast(:wave_id as uuid)
                order by tenant_id
            """),
            {"wave_id": wave_id},
        ).mappings().all()

        non_terminal = [
            row
            for row in rows
            if row["status"]
            not in TERMINAL_STATUSES
        ]

        if non_terminal:
            raise RuntimeError(
                "Волну нельзя завершить до "
                "получения конечного ответа от всех тенантов"
            )

        failures = sum(
            1
            for row in rows
            if row["status"] != "success"
        )

        wave_status = (
            "success"
            if failures == 0
            else "completed_with_failures"
        )

        conn.execute(
            text("""
                update control.acquisition_waves
                set
                    status=:status,
                    finished_at=now(),
                    success_count=:success_count,
                    failure_count=:failure_count
                where
                    wave_id=
                        cast(:wave_id as uuid)
            """),
            {
                "wave_id": wave_id,
                "status": wave_status,
                "success_count":
                    len(rows) - failures,
                "failure_count": failures,
            },
        )

    return {
        "wave_id": wave_id,
        "status": wave_status,
        "tenants": [
            dict(row)
            for row in rows
        ],
    }
