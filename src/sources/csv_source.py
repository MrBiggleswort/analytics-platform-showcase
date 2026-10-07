from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .base import SourceClient


class CsvSourceClient(SourceClient):
    """Адаптер файловой выгрузки с привязкой по заголовкам.

    В рабочей версии профили определяют варианты названий колонок конкретных МИС.
    В публичной витрине оставлен общий механизм без реальных сопоставлений.
    """

    source_name = "csv"

    def __init__(
        self,
        *,
        folder: str | Path,
        file_map: Mapping[str, str],
        delimiter: str = ";",
        encoding: str = "utf-8",
    ):
        self.folder = Path(folder)
        self.file_map = dict(file_map)
        self.delimiter = delimiter
        self.encoding = encoding

    def _path(self, entity_name: str) -> Path:
        try:
            filename = self.file_map[entity_name]
        except KeyError as exc:
            raise KeyError(
                f"Для сущности {entity_name!r} не задан CSV файл"
            ) from exc

        path = self.folder / filename
        if not path.is_file():
            raise FileNotFoundError(path)

        return path

    @staticmethod
    def _clean(value: Any) -> Any:
        if value is None:
            return None

        text = str(value).strip()
        return text if text else None

    def _iter_rows(self, path: Path) -> Iterable[dict[str, Any]]:
        with path.open(
            "r",
            encoding=self.encoding,
            newline="",
        ) as handle:
            reader = csv.DictReader(
                handle,
                delimiter=self.delimiter,
            )

            if not reader.fieldnames:
                raise ValueError(
                    f"CSV {path.name} не содержит заголовка"
                )

            headers = [
                str(name).strip().lower()
                for name in reader.fieldnames
            ]

            if len(headers) != len(set(headers)):
                raise ValueError(
                    f"CSV {path.name} содержит дублирующиеся заголовки"
                )

            for raw in reader:
                yield {
                    str(key).strip().lower():
                        self._clean(value)
                    for key, value in raw.items()
                }

    @staticmethod
    def _in_window(
        record: Mapping[str, Any],
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> bool:
        """Рабочий профиль знает точное поле обновления сущности в источнике."""

        raw = record.get("source_updated_at")

        if raw in (None, ""):
            return True

        try:
            value = datetime.fromisoformat(str(raw))
        except ValueError:
            return True

        if since is not None and value < since:
            return False

        if until is not None and value >= until:
            return False

        return True

    def fetch_entity(
        self,
        entity_name: str,
        *,
        since: datetime | None,
        until: datetime | None,
        bootstrap: bool = False,
    ):
        path = self._path(entity_name)

        for row in self._iter_rows(path):
            if bootstrap or self._in_window(
                row,
                since=since,
                until=until,
            ):
                yield row
