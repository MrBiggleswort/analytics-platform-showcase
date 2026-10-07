from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy.engine import Engine

from src.common.source_keys import normalize_source_key


def stable_record_key(
    entity_name: str,
    record: Mapping[str, Any],
) -> str:
    """Возвращает ключ записи, построенный с учетом источника."""

    explicit = normalize_source_key(
        record.get("source_key")
        or record.get("id")
        or record.get("guid")
    )

    if explicit is not None:
        return explicit

    parts = [
        entity_name,
        str(
            record.get("parent_source_key")
            or ""
        ),
        str(
            record.get("child_source_key")
            or ""
        ),
        str(
            record.get("event_date")
            or ""
        ),
    ]

    return "|".join(parts)


class WarehouseWriter:
    """Общий механизм пакетной записи в слои `raw` и `stage`."""

    def __init__(
        self,
        engine: Engine,
    ):
        self.engine = engine

    def _execute_values(
        self,
        sql: str,
        values: Sequence[Sequence[Any]],
        *,
        chunk_size: int,
    ) -> int:
        """В рабочей версии используется `psycopg2.execute_values`."""

        if not values:
            return 0

        raw = self.engine.raw_connection()

        try:
            from psycopg2.extras import (
                execute_values,
            )

            with raw.cursor() as cursor:
                execute_values(
                    cursor,
                    sql,
                    values,
                    page_size=chunk_size,
                )

            raw.commit()
            return len(values)

        except Exception:
            raw.rollback()
            raise

        finally:
            raw.close()

    def write_raw_records(
        self,
        *,
        tenant_id: int,
        source_system: str,
        entity_name: str,
        records: Iterable[
            Mapping[str, Any]
        ],
        load_run_id: str,
        chunk_size: int = 20_000,
        on_chunk_written:
            Callable[
                [int, int, int],
                None,
            ]
            | None = None,
    ) -> int:
        """Пишет `raw` порциями, не собирая сущность целиком в памяти."""

        extracted_at = (
            datetime.now(timezone.utc)
        )

        sql = """
            insert into raw.raw_records(
                tenant_id,
                source_system,
                entity_name,
                extracted_at,
                payload,
                record_key,
                load_run_id,
                source_updated_at
            )
            values %s
            on conflict do nothing
        """

        total = 0
        chunk_no = 0
        values: list[list[Any]] = []

        for source_record in records:
            record = dict(
                source_record
            )

            source_updated_at = (
                record.get(
                    "source_updated_at"
                )
                or record.get(
                    "updated_at"
                )
                or record.get(
                    "modified_at"
                )
            )

            values.append(
                [
                    tenant_id,
                    source_system,
                    entity_name,
                    extracted_at,
                    record,
                    stable_record_key(
                        entity_name,
                        record,
                    ),
                    load_run_id,
                    source_updated_at,
                ]
            )

            if len(values) >= chunk_size:
                written = (
                    self._execute_values(
                        sql,
                        values,
                        chunk_size=
                            chunk_size,
                    )
                )

                total += written
                chunk_no += 1

                if (
                    on_chunk_written
                    is not None
                ):
                    on_chunk_written(
                        chunk_no,
                        written,
                        total,
                    )

                values = []

        if values:
            written = (
                self._execute_values(
                    sql,
                    values,
                    chunk_size=chunk_size,
                )
            )

            total += written
            chunk_no += 1

            if (
                on_chunk_written
                is not None
            ):
                on_chunk_written(
                    chunk_no,
                    written,
                    total,
                )

        return total

    def upsert_dataframe(
        self,
        *,
        schema: str,
        table: str,
        df: pd.DataFrame,
        conflict_cols:
            Sequence[str],
        update_cols:
            Sequence[str] | None = None,
        compare_cols:
            Sequence[str] | None = None,
        chunk_size: int = 2000,
    ) -> int:
        """Выполняет условную вставку или обновление DataFrame."""

        if (
            df is None
            or df.empty
        ):
            return 0

        frame = df.copy()

        frame.columns = [
            str(column).lower()
            for column in frame.columns
        ]

        columns = list(
            frame.columns
        )

        conflict_cols = [
            column.lower()
            for column in conflict_cols
        ]

        if update_cols is None:
            update_cols = [
                column
                for column in columns
                if column
                not in set(conflict_cols)
            ]
        else:
            update_cols = [
                column.lower()
                for column in update_cols
                if column.lower()
                in columns
            ]

        if compare_cols is None:
            compare_cols = list(
                update_cols
            )
        else:
            compare_cols = [
                column.lower()
                for column in compare_cols
                if column.lower()
                in columns
            ]

        values = [
            [
                None
                if pd.isna(value)
                else value
                for value in row
            ]
            for row in frame.itertuples(
                index=False,
                name=None,
            )
        ]

        columns_sql = ", ".join(
            columns
        )

        conflict_sql = ", ".join(
            conflict_cols
        )

        if not update_cols:
            sql = (
                f"insert into "
                f"{schema}.{table} "
                f"({columns_sql}) "
                f"values %s "
                f"on conflict "
                f"({conflict_sql}) "
                f"do nothing"
            )

        else:
            set_sql = ", ".join(
                f"{column}="
                f"excluded.{column}"
                for column
                in update_cols
            )

            compare_sql = " or ".join(
                f"{table}.{column} "
                f"is distinct from "
                f"excluded.{column}"
                for column
                in compare_cols
            )

            where_sql = (
                f" where {compare_sql}"
                if compare_sql
                else ""
            )

            sql = (
                f"insert into "
                f"{schema}.{table} "
                f"({columns_sql}) "
                f"values %s "
                f"on conflict "
                f"({conflict_sql}) "
                f"do update set "
                f"{set_sql}"
                f"{where_sql}"
            )

        return self._execute_values(
            sql,
            values,
            chunk_size=chunk_size,
        )
