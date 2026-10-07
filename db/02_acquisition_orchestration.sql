create table control.acquisition_waves (
    wave_id uuid primary key,
    status text not null
        check (
            status in (
                'running',
                'success',
                'completed_with_failures'
            )
        ),
    started_at timestamptz
        not null default now(),
    finished_at timestamptz,
    deadline_at timestamptz not null,
    airflow_dag_id text,
    airflow_run_id text,
    tenant_count integer not null,
    success_count integer
        not null default 0,
    failure_count integer
        not null default 0
);

create table control.acquisition_runs (
    acquisition_run_id uuid primary key,
    wave_id uuid not null
        references
            control.acquisition_waves(
                wave_id
            )
        on delete cascade,
    tenant_id bigint not null,
    tenant_code text not null,
    source_type text not null,
    acquisition_mode text not null,
    status text not null
        check (
            status in (
                'pending',
                'running',
                'success',
                'failed',
                'no_fresh_data',
                'timeout'
            )
        ),
    deadline_at timestamptz not null,
    started_at timestamptz,
    finished_at timestamptz,
    load_run_id uuid,
    airflow_task_id text,
    error_type text,
    error_message text,
    details jsonb
        not null default '{}'::jsonb,
    created_at timestamptz
        not null default now(),

    unique (
        wave_id,
        tenant_id
    )
);

create index
    acquisition_runs_wave_status_idx
    on control.acquisition_runs(
        wave_id,
        status
    );

create index
    acquisition_runs_active_tenant_idx
    on control.acquisition_runs(
        tenant_id,
        created_at desc
    )
    where status in (
        'pending',
        'running'
    );

comment on table
    control.acquisition_waves is
'Общий барьер получения данных с единым жестким сроком для пакетного запуска.';

comment on table
    control.acquisition_runs is
'Конечное состояние получения данных каждого тенанта внутри волны.';
