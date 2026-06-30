import logging
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

import pydantic
import requests

if TYPE_CHECKING:
    from datahub.ingestion.source.omni.omni_report import OmniClientReport

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
        report: Optional["OmniClientReport"] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._last_request_ts = 0.0
        self._min_interval = 60.0 / max_requests_per_minute
        self._throttle_lock = threading.Lock()
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key.get_secret_value()}",
                "Content-Type": "application/json",
            }
        )
        self._report = report

    @contextmanager
    def _track_call(self, method_name: str) -> Iterator[None]:
        """Context manager to track API call timing."""
        start_time = time.monotonic()
        try:
            yield
        finally:
            duration = time.monotonic() - start_time
            if self._report:
                self._report.record_call(method_name, duration)

    def _throttle(self) -> None:
        """Thread-safe rate limiting with sleep outside lock.

        Each thread reserves its time slot by updating _last_request_ts,
        then sleeps outside the lock so other threads can proceed in parallel.
        """
        # Calculate sleep time and reserve next slot while holding lock
        with self._throttle_lock:
            now = time.monotonic()
            target_time = self._last_request_ts + self._min_interval
            sleep_seconds = max(0.0, target_time - now)
            # Reserve the next time slot by updating _last_request_ts
            # This allows the next thread to calculate its slot while we sleep
            self._last_request_ts = max(now, target_time)

        # Sleep OUTSIDE the lock so other threads can proceed
        if sleep_seconds > 0:
            logger.warning(
                "Throttling request: sleeping for %.3f seconds to respect rate limit",
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

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
        with self._track_call("test_connection"):
            try:
                self._request("GET", "/v1/models")
                return True
            except Exception:
                return False

    def list_connections(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        with self._track_call("list_connections"):
            params = {"includeDeleted": str(include_deleted).lower()}
            payload = self._request("GET", "/v1/connections", params=params)
            connections = payload.get("connections", [])
            logger.debug(
                "API response list_connections: count=%d connections=%s",
                len(connections),
                connections,
            )
            return connections

    def list_models(self, page_size: int = 50) -> Iterator[Dict[str, Any]]:
        with self._track_call("list_models"):
            for model in self.paginate_records(
                "/v1/models", params={"pageSize": page_size}
            ):
                logger.debug("API response list_models: model=%s", model)
                yield model

    def get_model_yaml(self, model_id: str) -> Dict[str, Any]:
        with self._track_call("get_model_yaml"):
            response = self._request("GET", f"/v1/models/{model_id}/yaml")
            logger.debug(
                "API response get_model_yaml: model_id=%s payload=%s",
                model_id,
                response,
            )
            return response

    def get_topic(self, model_id: str, topic_name: str) -> Dict[str, Any]:
        with self._track_call("get_topic"):
            payload = self._request("GET", f"/v1/models/{model_id}/topic/{topic_name}")
            topic = payload.get("topic", {})
            logger.debug(
                "API response get_topic: model_id=%s topic_name=%s payload=%s",
                model_id,
                topic_name,
                topic,
            )
            return topic

    def list_documents(
        self, page_size: int = 50, include_deleted: bool = False
    ) -> Iterator[Dict[str, Any]]:
        with self._track_call("list_documents"):
            include = "includeDeleted" if include_deleted else ""
            params: Dict[str, Any] = {"pageSize": page_size}
            if include:
                params["include"] = include
            for document in self.paginate_records("/v1/documents", params=params):
                logger.debug("API response list_documents: document=%s", document)
                yield document

    def get_dashboard_document(self, document_id: str) -> Dict[str, Any]:
        with self._track_call("get_dashboard_document"):
            response = self._request("GET", f"/v1/documents/{document_id}")
            logger.debug(
                "API response get_dashboard_document: doc_id=%s payload=%s",
                document_id,
                response,
            )
            return response

    def get_document_queries(self, document_id: str) -> List[Dict[str, Any]]:
        with self._track_call("get_document_queries"):
            payload = self._request("GET", f"/v1/documents/{document_id}/queries")
            queries = payload.get("queries", [])
            logger.debug(
                "API response get_document_queries: doc_id=%s queries=%s",
                document_id,
                queries,
            )
            return queries

    def list_folders(self, page_size: int = 50) -> Iterator[Dict[str, Any]]:
        with self._track_call("list_folders"):
            for folder in self.paginate_records(
                "/v1/folders", params={"pageSize": page_size}
            ):
                logger.debug("API response list_folders: folder=%s", folder)
                yield folder
