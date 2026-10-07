from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine


def refresh_revenue_window(
    engine: Engine,
    *,
    tenant_id: int,
    date_from: date,
    date_to: date,
) -> int:
    """Строит витрину выручки по затронутому периоду конкретного тенанта."""

    with engine.begin() as conn:
        conn.execute(
            text("""
                delete from mart.revenue
                where
                    tenant_id=:tenant_id
                    and sale_date
                        between
                        :date_from
                        and :date_to
            """),
            {
                "tenant_id": tenant_id,
                "date_from": date_from,
                "date_to": date_to,
            },
        )

        result = conn.execute(
            text("""
                insert into mart.revenue(
                    tenant_id,
                    sale_id,
                    sale_service_id,
                    sale_date,
                    visit_id,
                    patient_id,
                    service_id,
                    doctor_id,
                    quantity,
                    revenue
                )
                select
                    s.tenant_id,
                    s.id,
                    ss.id,
                    s.sale_datetime::date,
                    s.visit_id,
                    s.patient_id,
                    ss.service_id,
                    ss.doctor_id,
                    ss.quantity,
                    ss.revenue
                from stage.sales s
                join stage.sale_services ss
                  on ss.tenant_id=
                        s.tenant_id
                 and ss.sale_id=s.id
                where
                    s.tenant_id=
                        :tenant_id
                    and
                    s.sale_datetime::date
                        between
                        :date_from
                        and :date_to
            """),
            {
                "tenant_id": tenant_id,
                "date_from": date_from,
                "date_to": date_to,
            },
        )

    return max(
        int(result.rowcount or 0),
        0,
    )
