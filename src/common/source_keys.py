from __future__ import annotations

import math
from numbers import Integral, Real
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine


def normalize_source_key(value: Any) -> str | None:
    """Приводит внешний ключ к стабильному текстовому виду без изменения его смысла."""

    if value is None:
        return None

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, Integral):
        return str(int(value))

    if isinstance(value, Real):
        number = float(value)

        if not math.isfinite(number):
            return None

        if number.is_integer():
            return str(int(number))

    result = str(value).strip()

    if not result:
        return None

    if result.casefold() in {"nan", "nat", "<na>"}:
        return None

    return result


def resolve_source_keys(
    engine: Engine,
    *,
    tenant_id: int,
    entity_type: str,
    values: Iterable[Any],
    load_run_id: str | None = None,
) -> dict[str, int]:
    """Регистрирует ключи источника и возвращает внутренние bigint ID."""

    normalized = sorted(
        {
            key
            for value in values
            if (key := normalize_source_key(value))
            is not None
        }
    )

    if not normalized:
        return {}

    entity_type = entity_type.strip()

    if not entity_type:
        raise ValueError(
            "entity_type не может быть пустым"
        )

    sql = text("""
        insert into control.source_key_registry(
            tenant_id,
            entity_type,
            source_key,
            first_seen_load_run_id,
            last_seen_load_run_id
        )
        select
            :tenant_id,
            :entity_type,
            source_key,
            cast(:load_run_id as uuid),
            cast(:load_run_id as uuid)
        from unnest(
            cast(:source_keys as text[])
        ) as src(source_key)
        on conflict(
            tenant_id,
            entity_type,
            source_key
        )
        do update set
            last_seen_load_run_id=coalesce(
                excluded.last_seen_load_run_id,
                control.source_key_registry
                    .last_seen_load_run_id
            ),
            updated_at=case
                when excluded.last_seen_load_run_id
                    is not null
                    then now()
                else control.source_key_registry.updated_at
            end
        returning source_key, internal_id
    """)

    with engine.begin() as conn:
        rows = conn.execute(
            sql,
            {
                "tenant_id": int(tenant_id),
                "entity_type": entity_type,
                "source_keys": normalized,
                "load_run_id": load_run_id,
            },
        ).mappings().all()

    result = {
        str(row["source_key"]):
            int(row["internal_id"])
        for row in rows
    }

    missing = set(normalized) - set(result)

    if missing:
        sample = ", ".join(
            sorted(missing)[:5]
        )
        raise RuntimeError(
            "Реестр не вернул часть ключей источника: "
            f"entity={entity_type}, sample={sample}"
        )

    if any(
        value <= 0
        for value in result.values()
    ):
        raise RuntimeError(
            "Реестр вернул неположительный ID: "
            f"entity={entity_type}"
        )

    return result
