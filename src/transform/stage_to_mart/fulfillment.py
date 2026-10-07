from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine


def rebuild_fulfillment_window(
    engine: Engine,
    *,
    tenant_id: int,
    date_from: date,
    date_to: date,
) -> int:
    """Пересчитывает упрощенную модель выполнения назначений.

    Полная рабочая матрица приоритетов, альтернатив и причин
    отмены в публичной витрине не публикуется.
    """

    with engine.begin() as conn:
        conn.execute(
            text("""
                delete from
                    mart.appointment_fulfillment
                where
                    tenant_id=:tenant_id
                    and visit_date
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
                with appointments as (
                    select
                        a.tenant_id,
                        a.visit_id,
                        v.visit_datetime::date
                            as visit_date,
                        a.service_id,
                        min(a.id)
                            as appointment_id
                    from
                        stage
                            .visit_appointed_services a
                    join stage.visits v
                      on v.tenant_id=
                            a.tenant_id
                     and v.id=a.visit_id
                    where
                        a.tenant_id=
                            :tenant_id
                        and
                        v.visit_datetime::date
                            between
                            :date_from
                            and :date_to
                    group by
                        a.tenant_id,
                        a.visit_id,
                        v.visit_datetime::date,
                        a.service_id
                ),
                rendered as (
                    select
                        r.tenant_id,
                        r.visit_id,
                        r.service_id,
                        min(r.id)
                            as rendered_id
                    from
                        stage
                            .visit_rendered_services r
                    where
                        r.tenant_id=
                            :tenant_id
                    group by
                        r.tenant_id,
                        r.visit_id,
                        r.service_id
                ),
                sales as (
                    select
                        s.tenant_id,
                        s.visit_id,
                        ss.service_id,
                        min(ss.id)
                            as sale_service_id,
                        sum(ss.revenue)
                            as revenue
                    from stage.sales s
                    join stage.sale_services ss
                      on ss.tenant_id=
                            s.tenant_id
                     and ss.sale_id=s.id
                    where
                        s.tenant_id=
                            :tenant_id
                    group by
                        s.tenant_id,
                        s.visit_id,
                        ss.service_id
                )
                insert into
                    mart.appointment_fulfillment(
                        tenant_id,
                        visit_id,
                        visit_date,
                        service_id,
                        appointment_id,
                        rendered_id,
                        sale_service_id,
                        is_appointed,
                        is_rendered,
                        revenue
                    )
                select
                    a.tenant_id,
                    a.visit_id,
                    a.visit_date,
                    a.service_id,
                    a.appointment_id,
                    r.rendered_id,
                    s.sale_service_id,
                    true,
                    r.rendered_id
                        is not null
                        or
                    s.sale_service_id
                        is not null,
                    coalesce(
                        s.revenue,
                        0
                    )
                from appointments a
                left join rendered r
                  on r.tenant_id=
                        a.tenant_id
                 and r.visit_id=
                        a.visit_id
                 and r.service_id=
                        a.service_id
                left join sales s
                  on s.tenant_id=
                        a.tenant_id
                 and s.visit_id=
                        a.visit_id
                 and s.service_id=
                        a.service_id
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
