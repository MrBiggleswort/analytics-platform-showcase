create schema if not exists raw;
create schema if not exists stage;
create schema if not exists mart;
create schema if not exists semantic;
create schema if not exists published;
create schema if not exists control;

create table control.load_runs (
    load_run_id uuid primary key,
    tenant_id bigint not null,
    source_system text not null,
    status text not null
        check (
            status in (
                'running',
                'success',
                'failed'
            )
        ),
    started_at timestamptz
        not null default now(),
    finished_at timestamptz,
    since_watermark timestamptz,
    entity_since_watermarks jsonb
        not null default '{}'::jsonb,
    published_snapshot_id uuid,
    error_message text
);

create table control.entity_load_config (
    entity_name text primary key,
    overlap_days integer
        not null default 3
        check (overlap_days >= 0),
    enabled boolean
        not null default true
);

create table control.entity_watermarks (
    tenant_id bigint not null,
    source_system text not null,
    entity_name text not null,
    watermark timestamptz,
    last_successful_run_id uuid,
    last_checked_at timestamptz,
    updated_at timestamptz
        not null default now(),
    primary key (
        tenant_id,
        source_system,
        entity_name
    )
);

create table control.entity_load_stats (
    load_run_id uuid not null,
    tenant_id bigint not null,
    entity_name text not null,
    extracted_rows bigint
        not null default 0,
    staged_rows bigint
        not null default 0,
    rejected_rows bigint
        not null default 0,
    primary key (
        load_run_id,
        entity_name
    )
);

create table control.entity_chunk_checkpoints (
    load_run_id uuid not null,
    tenant_id bigint not null,
    entity_name text not null,
    chunk_no integer not null,
    chunk_rows integer not null,
    total_rows bigint not null,
    status text not null,
    updated_at timestamptz
        not null default now(),
    primary key (
        load_run_id,
        entity_name,
        chunk_no
    )
);

create table control.data_quality_results (
    id bigint generated always
        as identity primary key,
    load_run_id uuid,
    tenant_id bigint not null,
    check_name text not null,
    status text not null
        check (
            status in (
                'pass',
                'warn',
                'fail'
            )
        ),
    measured_value numeric,
    expected_value text,
    details jsonb
        not null default '{}'::jsonb,
    checked_at timestamptz
        not null default now()
);

create table raw.raw_records (
    id bigint generated always
        as identity primary key,
    tenant_id bigint not null,
    source_system text not null,
    entity_name text not null,
    extracted_at timestamptz not null,
    payload jsonb not null,
    record_key text not null,
    load_run_id uuid not null,
    source_updated_at timestamptz
);
