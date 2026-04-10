import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import pydantic
import requests

logger = logging.getLogger(__name__)

# Safety cap: avoids unbounded requests if cursor pagination misbehaves.
_MAX_PAGINATION_PAGES = 1000


class OmniClient:
    """HTTP client for the Omni REST API.

    Handles authentication, client-side rate limiting, exponential-backoff
    retries on 429 / 5xx responses, and cursor-based pagination.
    """

    def __init__(
        self,
        base_url: str,
        api_key: pydantic.SecretStr,
        timeout_seconds: int = 30,
        max_requests_per_minute: int = 50,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._last_request_ts = 0.0
        self._min_interval = 60.0 / max_requests_per_minute
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key.get_secret_value()}",
                "Content-Type": "application/json",
            }
        )

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    def _request(
        self, method: str, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        retries = 8
        for attempt in range(retries):
            self._throttle()
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                timeout=self._timeout,
            )
            if (
                response.status_code in (429, 500, 502, 503, 504)
                and attempt < retries - 1
            ):
                retry_after = response.headers.get("Retry-After")
                backoff = float(retry_after) if retry_after else min(2**attempt, 30)
                time.sleep(backoff)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Request failed after retries: {method} {path}")

    def paginate_records(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Iterator[Dict[str, Any]]:
        """Yield records from cursor-paged GET responses.

        Stops after at most ``_MAX_PAGINATION_PAGES`` pages as a safety limit
        if the API keeps returning ``hasNextPage`` (e.g. misbehaving cursor).
        """
        query = dict(params or {})
        seen_cursors: set = set()
        page_count = 0
        while True:
            page_count += 1
            payload = self._request("GET", path, params=query)
            for row in payload.get("records", []):
                yield row
            page_info = payload.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            next_cursor = page_info.get("nextCursor")
            if not next_cursor:
                break
            if next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            query["cursor"] = next_cursor
            if page_count >= _MAX_PAGINATION_PAGES:
                logger.warning(
                    "Stopping pagination after %s pages (safety cap) for %s; "
                    "more pages may exist.",
                    _MAX_PAGINATION_PAGES,
                    path,
                )
                break

    def test_connection(self) -> bool:
        """Verify credentials are accepted by the Omni API."""
        try:
            self._request("GET", "/v1/models")
            return True
        except Exception:
            return False

    def list_connections(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        params = {"includeDeleted": str(include_deleted).lower()}
        payload = self._request("GET", "/v1/connections", params=params)
        return payload.get("connections", [])

    def list_models(self, page_size: int = 50) -> Iterator[Dict[str, Any]]:
        yield from self.paginate_records("/v1/models", params={"pageSize": page_size})

    def get_model_yaml(self, model_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/v1/models/{model_id}/yaml")

    def get_topic(self, model_id: str, topic_name: str) -> Dict[str, Any]:
        payload = self._request("GET", f"/v1/models/{model_id}/topic/{topic_name}")
        return payload.get("topic", {})

    def list_documents(
        self, page_size: int = 50, include_deleted: bool = False
    ) -> Iterator[Dict[str, Any]]:
        include = "includeDeleted" if include_deleted else ""
        params: Dict[str, Any] = {"pageSize": page_size}
        if include:
            params["include"] = include
        yield from self.paginate_records("/v1/documents", params=params)

    def get_dashboard_document(self, document_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/v1/documents/{document_id}")

    def get_document_queries(self, document_id: str) -> List[Dict[str, Any]]:
        payload = self._request("GET", f"/v1/documents/{document_id}/queries")
        return payload.get("queries", [])

    def list_folders(self, page_size: int = 50) -> Iterator[Dict[str, Any]]:
        yield from self.paginate_records("/v1/folders", params={"pageSize": page_size})


# Backwards-compatibility alias used in some tests
OmniAPIClient = OmniClient
