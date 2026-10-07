create table stage.visits (
    tenant_id bigint not null,
    id bigint not null,
    source_key text not null,
    patient_id bigint,
    doctor_id bigint,
    visit_datetime timestamptz not null,
    source_load_run_id uuid,
    updated_at timestamptz
        not null default now(),

    primary key (
        tenant_id,
        id
    ),

    unique (
        tenant_id,
        source_key
    )
);

create table stage.sales (
    tenant_id bigint not null,
    id bigint not null,
    source_key text not null,
    visit_id bigint,
    patient_id bigint,
    sale_datetime timestamptz not null,
    source_load_run_id uuid,
    updated_at timestamptz
        not null default now(),

    primary key (
        tenant_id,
        id
    ),

    unique (
        tenant_id,
        source_key
    )
);

create table stage.sale_services (
    tenant_id bigint not null,
    id bigint not null,
    sale_id bigint not null,
    service_id bigint not null,
    doctor_id bigint,
    quantity numeric(18, 4)
        not null default 1,
    revenue numeric(18, 2)
        not null default 0,

    primary key (
        tenant_id,
        id
    )
);

create table
stage.visit_appointed_services (
    tenant_id bigint not null,
    id bigint not null,
    visit_id bigint not null,
    service_id bigint not null,

    primary key (
        tenant_id,
        id
    )
);

create table
stage.visit_rendered_services (
    tenant_id bigint not null,
    id bigint not null,
    visit_id bigint not null,
    service_id bigint not null,

    primary key (
        tenant_id,
        id
    )
);

create table mart.appointment_fulfillment (
    tenant_id bigint not null,
    visit_id bigint not null,
    visit_date date not null,
    service_id bigint not null,
    appointment_id bigint,
    rendered_id bigint,
    sale_service_id bigint,
    is_appointed boolean not null,
    is_rendered boolean not null,
    revenue numeric(18, 2)
        not null default 0,

    primary key (
        tenant_id,
        visit_id,
        service_id
    )
);

create table mart.revenue (
    tenant_id bigint not null,
    sale_id bigint not null,
    sale_service_id bigint not null,
    sale_date date not null,
    visit_id bigint,
    patient_id bigint,
    service_id bigint not null,
    doctor_id bigint,
    quantity numeric(18, 4)
        not null,
    revenue numeric(18, 2)
        not null,

    primary key (
        tenant_id,
        sale_service_id
    )
);

create table control.recalculation_queue (
    id bigint generated always
        as identity primary key,
    tenant_id bigint not null,
    visit_date_from date not null,
    visit_date_to date not null,
    scope text
        not null default 'all',
    reason text not null,
    priority smallint
        not null default 100,
    status text
        not null default 'pending'
        check (
            status in (
                'pending',
                'running',
                'success',
                'failed'
            )
        ),
    available_at timestamptz
        not null default now(),
    claimed_at timestamptz,
    finished_at timestamptz,
    worker_id text,
    recalculation_run_id uuid,
    error_message text,

    check (
        visit_date_from
        <= visit_date_to
    )
);

create table control.semantic_snapshots (
    snapshot_id uuid primary key,
    tenant_id bigint not null,
    load_run_id uuid,
    status text not null
        check (
            status in (
                'building',
                'validated',
                'published',
                'retired',
                'failed'
            )
        ),
    row_count bigint,
    details jsonb
        not null default '{}'::jsonb,
    created_at timestamptz
        not null default now(),
    validated_at timestamptz,
    published_at timestamptz,
    retired_at timestamptz,
    failed_at timestamptz,
    error_message text
);

create table
control.semantic_snapshot_datasets (
    snapshot_id uuid not null
        references
            control.semantic_snapshots(
                snapshot_id
            )
        on delete cascade,
    tenant_id bigint not null,
    dataset_name text not null,
    status text not null,
    row_count bigint,
    build_finished_at timestamptz,

    primary key (
        snapshot_id,
        dataset_name
    )
);
