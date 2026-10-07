from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    expected: str
    severity: str = "fail"


@dataclass(frozen=True)
class QualitySummary:
    passed: int
    warned: int
    failed: int

    @property
    def ok(self) -> bool:
        return self.failed == 0


STAGE_CHECKS = (
    Check(
        name="visit_without_patient",
        sql="""
            select count(*)
            from stage.visits
            where tenant_id=:tenant_id
              and patient_id is null
        """,
        expected="0",
    ),
    Check(
        name="sale_without_service",
        sql="""
            select count(*)
            from stage.sale_services
            where tenant_id=:tenant_id
              and service_id is null
        """,
        expected="0",
    ),
    Check(
        name="negative_revenue",
        sql="""
            select count(*)
            from stage.sale_services
            where tenant_id=:tenant_id
              and coalesce(revenue, 0) < 0
        """,
        expected="0",
        severity="warn",
    ),
)


def _status(
    measured: int,
    *,
    expected: str,
    severity: str,
) -> str:
    if expected == "0" and measured == 0:
        return "pass"

    if severity == "warn":
        return "warn"

    return "fail"


def run_checks(
    engine: Engine,
    *,
    tenant_id: int,
    load_run_id: str,
    checks: Iterable[Check],
    fail_on_error: bool = True,
) -> QualitySummary:
    """Выполняет проверки качества данных и сохраняет результаты для оператора."""

    results: list[str] = []

    with engine.begin() as conn:
        for check in checks:
            measured = int(
                conn.execute(
                    text(check.sql),
                    {"tenant_id": tenant_id},
                ).scalar()
                or 0
            )

            status = _status(
                measured,
                expected=check.expected,
                severity=check.severity,
            )

            results.append(status)

            conn.execute(
                text("""
                    insert into
                    control.data_quality_results(
                        load_run_id,
                        tenant_id,
                        check_name,
                        status,
                        measured_value,
                        expected_value
                    )
                    values(
                        cast(:run_id as uuid),
                        :tenant_id,
                        :check_name,
                        :status,
                        :measured,
                        :expected
                    )
                """),
                {
                    "run_id": load_run_id,
                    "tenant_id": tenant_id,
                    "check_name": check.name,
                    "status": status,
                    "measured": measured,
                    "expected": check.expected,
                },
            )

    summary = QualitySummary(
        passed=results.count("pass"),
        warned=results.count("warn"),
        failed=results.count("fail"),
    )

    if fail_on_error and not summary.ok:
        raise RuntimeError(
            "DQ failed: "
            f"tenant_id={tenant_id}, "
            f"failed={summary.failed}"
        )

    return summary
