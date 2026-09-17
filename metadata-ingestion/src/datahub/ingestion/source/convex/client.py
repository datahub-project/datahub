"""Thin client for Convex's streaming export REST API.

See https://docs.convex.dev/http-api/#streaming-export. Two endpoints are used:

- ``GET /api/json_schemas`` returns ``{table_name: JSON Schema}`` for every table.
- ``GET /api/list_snapshot`` pages through a consistent snapshot of one table.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

# Server-side snapshot page size. Informational only - the server decides how many
# documents a page holds; we just follow the cursor until it says there is no more.
PAGE_SIZE_HINT = 1024


@dataclass(frozen=True)
class RowCount:
    count: int
    # False when the page cap was reached before the snapshot was exhausted, so
    # `count` is a lower bound rather than the true row count.
    exact: bool


class ConvexStreamingExportClient:
    def __init__(self, url: str, deploy_key: str, timeout: int = 60) -> None:
        self.base_url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Convex {deploy_key}"
        self.timeout = timeout

    def _get(self, path: str, params: Optional[Dict[str, str]] = None) -> Any:
        response = self.session.get(
            f"{self.base_url}{path}", params=params, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def list_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Return the draft-07 JSON Schema of every table, keyed by table name."""
        return self._get(
            "/api/json_schemas", params={"deltaSchema": "true", "format": "json"}
        )

    def count_rows(self, table: str, max_pages: int) -> RowCount:
        """Count the rows of a table by paging through a snapshot of it."""
        count = 0
        cursor: Optional[str] = None
        for _ in range(max_pages):
            params: Dict[str, str] = {"tableName": table}
            if cursor:
                params["cursor"] = cursor
            page = self._get("/api/list_snapshot", params=params)
            count += len(page.get("values", []))
            if not page.get("hasMore"):
                return RowCount(count=count, exact=True)
            cursor = page.get("cursor")
        return RowCount(count=count, exact=False)
