from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Migration:
    version: str
    path: Path
    checksum: str
    sql: str


def discover_migrations(
    directory: str | Path,
) -> list[Migration]:
    """Читает неизменяемые файлы миграций в порядке версий."""

    root = Path(directory)
    migrations: list[Migration] = []

    for path in sorted(
        root.glob("*.sql")
    ):
        sql = path.read_text(
            encoding="utf-8"
        )

        migrations.append(
            Migration(
                version=path.stem,
                path=path,
                checksum=
                    hashlib.sha256(
                        sql.encode(
                            "utf-8"
                        )
                    ).hexdigest(),
                sql=sql,
            )
        )

    versions = [
        migration.version
        for migration in migrations
    ]

    if (
        len(versions)
        != len(set(versions))
    ):
        raise ValueError(
            "Найдены дублирующиеся "
            "версии миграций"
        )

    return migrations


def apply_migrations(
    dsn: str,
    migrations: Iterable[Migration],
    *,
    dry_run: bool = False,
) -> list[str]:
    """Применяет журнал миграций под сеансовой блокировкой PostgreSQL."""

    import psycopg2

    applied: list[str] = []

    with psycopg2.connect(
        dsn
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "select "
                "pg_advisory_lock("
                "hashtext("
                "'analytics_showcase_migrations'"
                "))"
            )

            try:
                cursor.execute(
                    "create schema "
                    "if not exists control"
                )

                cursor.execute("""
                    create table
                    if not exists
                    control.schema_migrations(
                        version text
                            primary key,
                        checksum text
                            not null,
                        applied_at
                            timestamptz
                            not null
                            default now()
                    )
                """)

                for migration in migrations:
                    cursor.execute(
                        """
                        select checksum
                        from
                            control
                                .schema_migrations
                        where version=%s
                        """,
                        (
                            migration.version,
                        ),
                    )

                    row = cursor.fetchone()

                    if row is not None:
                        existing = str(
                            row[0]
                        )

                        if (
                            existing
                            != migration.checksum
                        ):
                            raise RuntimeError(
                                "Checksum mismatch: "
                                f"{migration.version}"
                            )

                        continue

                    if dry_run:
                        applied.append(
                            migration.version
                            + ":pending"
                        )
                        continue

                    cursor.execute(
                        migration.sql
                    )

                    cursor.execute(
                        """
                        insert into
                            control
                                .schema_migrations(
                                    version,
                                    checksum
                                )
                        values(%s, %s)
                        """,
                        (
                            migration.version,
                            migration.checksum,
                        ),
                    )

                    conn.commit()
                    applied.append(
                        migration.version
                    )

            except Exception:
                conn.rollback()

                try:
                    cursor.execute(
                        "select "
                        "pg_advisory_unlock("
                        "hashtext("
                        "'analytics_showcase_migrations'"
                        "))"
                    )
                finally:
                    conn.rollback()

                raise

            else:
                cursor.execute(
                    "select "
                    "pg_advisory_unlock("
                    "hashtext("
                    "'analytics_showcase_migrations'"
                    "))"
                )

        if dry_run:
            conn.rollback()

    return applied
