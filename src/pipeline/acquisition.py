from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from sqlalchemy.engine import Engine

from src.common.acquisition import (
    claim_tenant,
    finish_tenant,
    get_wave_deadline,
)


class NoFreshDataError(RuntimeError):
    """До общего срока не появился новый полный набор данных источника."""


class AcquisitionDeadlineExceeded(TimeoutError):
    """Общий срок волны истек."""


def classify_exception(
    exc: BaseException,
) -> str:
    """Классифицирует ошибку источника для служебного состояния."""

    if isinstance(
        exc,
        AcquisitionDeadlineExceeded,
    ):
        return "ACQUISITION_TIMEOUT"

    if isinstance(
        exc,
        NoFreshDataError,
    ):
        return "NO_FRESH_DATA"

    if (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
    ):
        return (
            f"HTTP_{exc.response.status_code}"
        )

    if isinstance(
        exc,
        requests.ConnectTimeout,
    ):
        return "CONNECT_TIMEOUT"

    if isinstance(
        exc,
        requests.ReadTimeout,
    ):
        return "READ_TIMEOUT"

    if isinstance(
        exc,
        requests.ConnectionError,
    ):
        return "CONNECTION_ERROR"

    if isinstance(
        exc,
        FileNotFoundError,
    ):
        return "SOURCE_FILE_NOT_FOUND"

    return type(exc).__name__.upper()


def stat_signature(
    paths: Iterable[Path],
) -> list[dict[str, Any]] | None:
    """Снимает легковесный манифест стабильности файлов."""

    result: list[dict[str, Any]] = []

    for path in paths:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None

        if not path.is_file():
            return None

        result.append(
            {
                "path": str(path),
                "size": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )

    return result


def hash_manifest(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Добавляет SHA-256 после стабилизации набора файлов."""

    result: list[dict[str, Any]] = []

    for row in rows:
        path = Path(
            str(row["path"])
        )
        digest = hashlib.sha256()

        with path.open("rb") as handle:
            while True:
                block = handle.read(
                    1024 * 1024
                )

                if not block:
                    break

                digest.update(block)

        result.append(
            {
                **row,
                "sha256":
                    digest.hexdigest(),
            }
        )

    return result


def complete_manifest_is_fresh(
    manifest: list[dict[str, Any]],
    *,
    previous_manifest:
        list[dict[str, Any]] | None,
    previous_finished_at:
        datetime | None,
) -> bool:
    """Требует свежести всего обязательного набора файлов."""

    if previous_manifest:
        previous = {
            str(row["path"]): row
            for row in previous_manifest
        }

        if (
            len(previous)
            != len(manifest)
        ):
            return False

        for row in manifest:
            old = previous.get(
                str(row["path"])
            )

            if old is None:
                return False

            changed = (
                int(row["mtime_ns"])
                > int(
                    old.get("mtime_ns")
                    or 0
                )
                or str(row["sha256"])
                != str(
                    old.get("sha256")
                    or ""
                )
            )

            if not changed:
                return False

        return True

    if previous_finished_at is not None:
        baseline = int(
            previous_finished_at.timestamp()
            * 1_000_000_000
        )

        return all(
            int(row["mtime_ns"])
            > baseline
            for row in manifest
        )

    return True


def wait_for_fresh_csv(
    paths: list[Path],
    *,
    deadline: datetime,
    previous_manifest:
        list[dict[str, Any]] | None,
    previous_finished_at:
        datetime | None,
    poll_seconds: int = 15,
    stable_seconds: int = 30,
) -> list[dict[str, Any]]:
    """Ждет полный стабильный набор CSV до общего срока волны."""

    last_stat: list[dict[str, Any]] | None = None
    stable_since: float | None = None

    while (
        datetime.now(timezone.utc)
        < deadline
    ):
        current = stat_signature(
            paths
        )

        if current is None:
            last_stat = None
            stable_since = None

        elif current == last_stat:
            if stable_since is None:
                stable_since = (
                    time.monotonic()
                )

            if (
                time.monotonic()
                - stable_since
                >= stable_seconds
            ):
                manifest = hash_manifest(
                    current
                )

                if complete_manifest_is_fresh(
                    manifest,
                    previous_manifest=
                        previous_manifest,
                    previous_finished_at=
                        previous_finished_at,
                ):
                    return manifest

        else:
            last_stat = current
            stable_since = (
                time.monotonic()
            )

        remaining = (
            deadline
            - datetime.now(timezone.utc)
        ).total_seconds()

        if remaining <= 0:
            break

        time.sleep(
            min(
                float(poll_seconds),
                remaining,
            )
        )

    raise NoFreshDataError(
        "Новый полный набор CSV не получен "
        "до общего срока"
    )


def acquire_tenant_for_wave(
    engine: Engine,
    *,
    wave_id: str,
    tenant_id: int,
    airflow_task_id: str,
    acquire_raw,
) -> str:
    """Получает данные и фиксирует конечное состояние."""

    acquisition_run_id = claim_tenant(
        engine,
        wave_id=wave_id,
        tenant_id=tenant_id,
        airflow_task_id=airflow_task_id,
    )

    deadline = get_wave_deadline(
        engine,
        wave_id=wave_id,
    )

    try:
        if (
            datetime.now(timezone.utc)
            >= deadline
        ):
            raise AcquisitionDeadlineExceeded(
                "Общий срок получения данных "
                "уже истек"
            )

        load_run_id, details = acquire_raw(
            tenant_id=tenant_id,
            deadline=deadline,
        )

        finish_tenant(
            engine,
            acquisition_run_id=
                acquisition_run_id,
            status="success",
            load_run_id=load_run_id,
            details=details,
        )

        return load_run_id

    except NoFreshDataError as exc:
        finish_tenant(
            engine,
            acquisition_run_id=
                acquisition_run_id,
            status="no_fresh_data",
            error_type="NO_FRESH_DATA",
            error_message=str(exc),
        )
        raise

    except AcquisitionDeadlineExceeded as exc:
        finish_tenant(
            engine,
            acquisition_run_id=
                acquisition_run_id,
            status="timeout",
            error_type=
                "ACQUISITION_TIMEOUT",
            error_message=str(exc),
        )
        raise

    except Exception as exc:
        finish_tenant(
            engine,
            acquisition_run_id=
                acquisition_run_id,
            status="failed",
            error_type=
                classify_exception(exc),
            error_message=str(exc),
        )
        raise
