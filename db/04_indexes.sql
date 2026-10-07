create unique index
    raw_record_idempotency_idx
    on raw.raw_records(
        tenant_id,
        source_system,
        entity_name,
        record_key,
        load_run_id
    );

create index
    raw_record_source_time_idx
    on raw.raw_records(
        tenant_id,
        entity_name,
        source_updated_at
    );

create index
    stage_visits_date_idx
    on stage.visits(
        tenant_id,
        visit_datetime
    );

create index
    stage_sales_date_idx
    on stage.sales(
        tenant_id,
        sale_datetime
    );

create index
    fulfillment_visit_date_idx
    on mart.appointment_fulfillment(
        tenant_id,
        visit_date
    );

create index
    revenue_sale_date_idx
    on mart.revenue(
        tenant_id,
        sale_date
    );

create index
    recalculation_pending_idx
    on control.recalculation_queue(
        tenant_id,
        priority,
        id
    )
    where status='pending';

create unique index
    semantic_one_published_per_tenant_idx
    on control.semantic_snapshots(
        tenant_id
    )
    where status='published';
