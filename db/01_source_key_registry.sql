create table control.source_key_registry (
    internal_id bigint generated always
        as identity,
    tenant_id bigint not null,
    entity_type text not null,
    source_key text not null,
    first_seen_load_run_id uuid,
    last_seen_load_run_id uuid,
    created_at timestamptz
        not null default now(),
    updated_at timestamptz
        not null default now(),

    primary key (internal_id),

    constraint
        source_key_registry_identity_uq
        unique (
            tenant_id,
            entity_type,
            source_key
        ),

    constraint
        source_key_registry_positive_id
        check (internal_id > 0),

    constraint
        source_key_registry_nonempty_key
        check (btrim(source_key) <> '')
);

create index
    source_key_registry_lookup_idx
    on control.source_key_registry(
        tenant_id,
        entity_type,
        source_key
    );

comment on table
    control.source_key_registry is
'Связывает внешний source_key с внутренним числовым ID тенанта.';
