from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterable, Mapping


class SourceClient(ABC):
    """Единый контракт чтения сущностей из внешней системы."""

    source_name: str

    @abstractmethod
    def fetch_entity(
        self,
        entity_name: str,
        *,
        since: datetime | None,
        until: datetime | None,
        bootstrap: bool = False,
    ) -> Iterable[Mapping[str, Any]]:
        """Возвращает записи одной сущности из источника."""

    def fetch_doctors(self, since: datetime | None):
        return self.fetch_entity("doctors", since=since, until=None)

    def fetch_patients(self, since: datetime | None):
        return self.fetch_entity("patients", since=since, until=None)

    def fetch_services(self, since: datetime | None):
        return self.fetch_entity("services", since=since, until=None)

    def fetch_visits(self, since: datetime | None):
        return self.fetch_entity("visits", since=since, until=None)

    def fetch_sales(self, since: datetime | None):
        return self.fetch_entity("sales", since=since, until=None)


class CompositeSourceClient(SourceClient):
    """Объединяет основной и дополнительный источник без изменения общего процесса."""

    def __init__(
        self,
        primary: SourceClient,
        secondary: SourceClient,
        *,
        secondary_entities: set[str],
    ):
        self.primary = primary
        self.secondary = secondary
        self.secondary_entities = set(secondary_entities)
        self.source_name = f"{primary.source_name}+{secondary.source_name}"

    def fetch_entity(
        self,
        entity_name: str,
        *,
        since: datetime | None,
        until: datetime | None,
        bootstrap: bool = False,
    ):
        yield from self.primary.fetch_entity(
            entity_name,
            since=since,
            until=until,
            bootstrap=bootstrap,
        )

        if entity_name in self.secondary_entities:
            yield from self.secondary.fetch_entity(
                entity_name,
                since=since,
                until=until,
                bootstrap=bootstrap,
            )
