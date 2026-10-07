from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping

import requests

from .base import SourceClient


class RestSourceClient(SourceClient):
    """Постраничный REST-адаптер с ограниченным контрактом ответа."""

    source_name = "rest"

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        timeout_seconds: int = 60,
        page_size: int = 1000,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = int(timeout_seconds)
        self.page_size = int(page_size)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    def fetch_entity(
        self,
        entity_name: str,
        *,
        since: datetime | None,
        until: datetime | None,
        bootstrap: bool = False,
    ) -> Iterable[Mapping[str, Any]]:
        page = 1

        while True:
            params: dict[str, Any] = {
                "page": page,
                "page_size": self.page_size,
            }

            if not bootstrap and since is not None:
                params["updated_from"] = since.isoformat()

            if until is not None:
                params["updated_to"] = until.isoformat()

            response = requests.get(
                f"{self.base_url}/{entity_name}",
                headers=self._headers(),
                params=params,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()

            payload = response.json()
            rows = payload.get("items")

            if not isinstance(rows, list):
                raise ValueError(
                    f"Источник {entity_name!r} вернул некорректный список items"
                )

            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError(
                        f"Источник {entity_name!r} вернул значение, которое не является объектом"
                    )
                yield row

            if not payload.get("has_more"):
                break

            page += 1
