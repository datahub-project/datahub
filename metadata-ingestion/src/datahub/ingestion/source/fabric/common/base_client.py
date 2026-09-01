"""Base REST client for Microsoft Fabric workloads."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Iterator, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.report import FabricClientReport

logger = logging.getLogger(__name__)

# Fabric REST API base URL
FABRIC_API_BASE_URL = "https://api.fabric.microsoft.com/v1"

# Fabric REST API pagination token. The same name is used as the response
# field and the request parameter; it is read and written in several places,
# and a typo in any one of them silently degrades pagination to a single
# page, so every site goes through this constant.
CONTINUATION_TOKEN_KEY = "continuationToken"

# Response keys that belong to the pagination envelope rather than the
# payload. A terminal page may legitimately omit the items array and carry
# only these (e.g. {"next_page_token": null} from the Unity Catalog-style
# OneLake Table API, or {"continuationToken": null, "continuationUri": null}
# from the Fabric REST API) — such a page is empty, not malformed.
# "next_page_token" is the OneLake Table API field (see onelake/client.py's
# NEXT_PAGE_TOKEN_FIELD); duplicated here as a literal to avoid a circular
# import.
PAGINATION_ENVELOPE_KEYS = frozenset(
    {CONTINUATION_TOKEN_KEY, "continuationUri", "next_page_token"}
)

# Retry configuration
RETRY_MAX_TIMES = 3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
RETRY_BACKOFF_FACTOR = 1
RETRY_ALLOWED_METHODS = ["GET", "POST"]


class BaseFabricClient(ABC):
    """Base class for all Microsoft Fabric REST API clients.

    Provides common functionality:
    - Authentication via Azure TokenCredential
    - HTTP session management with retry logic
    - Error handling and logging
    """

    @abstractmethod
    def get_base_endpoint(self) -> str:
        """Get the base API endpoint for this client.

        Returns:
            Base endpoint path (e.g., 'workspaces' for OneLake)
        """
        pass

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        timeout: int = 30,
        report: Optional[FabricClientReport] = None,
    ):
        """Initialize the base client.

        Args:
            auth_helper: Authentication helper for getting Bearer tokens
            timeout: Request timeout in seconds
            report: Optional client report for tracking metrics
        """
        self.auth_helper = auth_helper
        self.timeout = timeout
        self._session = self._create_session()
        self.report = report or FabricClientReport()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic.

        Returns:
            Configured requests.Session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=RETRY_MAX_TIMES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=RETRY_ALLOWED_METHODS,
            raise_on_status=False,  # We'll handle status codes ourselves
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        return session

    def _get_headers(self) -> dict:
        """Get headers for API requests, including authorization.

        Returns:
            Dictionary of headers
        """
        headers = {}
        try:
            headers["Authorization"] = self.auth_helper.get_authorization_header()
        except Exception as e:
            logger.error(f"Failed to get authorization header: {e}")
            raise
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """Make an authenticated HTTP request.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            json: JSON body for POST requests

        Returns:
            Response object

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{FABRIC_API_BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._get_headers()
        self.report.report_request()

        logger.debug(f"Making {method} request to {endpoint}")
        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=self.timeout,
            )

            # Raise for status codes
            response.raise_for_status()

            logger.debug(f"Successfully completed {method} request to {endpoint}")
            return response

        except requests.exceptions.HTTPError as e:
            self.report.report_error()
            logger.error(
                f"HTTP error {e.response.status_code} for {method} {url}: {e.response.text}"
            )
            raise
        except requests.exceptions.RequestException as e:
            self.report.report_error()
            logger.error(f"Request error for {method} {url}: {e}")
            raise

    def _is_repeated_pagination_token(
        self, token: Optional[str], seen_tokens: Set[str], context: str
    ) -> bool:
        """Return True if a pagination token has already been followed.

        A well-behaved endpoint issues a fresh token per page and finally omits
        it. An endpoint that ignores the page/continuation-token request
        parameter echoes the same token on every call; detecting a repeat lets
        the caller stop instead of looping forever. A repeat is recorded on the
        client report as a pagination truncation (results may be incomplete)
        in addition to the log warning, so it is visible in the run summary
        and the JSON report, not only in the console log.

        Args:
            token: The next-page token from the current response (may be falsy)
            seen_tokens: Tokens already followed on this pagination run
            context: Human-readable endpoint/url for the warning message

        Returns:
            True if ``token`` is truthy and already in ``seen_tokens``.
        """
        if token and token in seen_tokens:
            self.report.report_pagination_truncated(
                f"{context}: repeated pagination token; stopped after "
                f"{len(seen_tokens) + 1} page(s), results may be incomplete"
            )
            logger.warning(
                f"{context} returned a repeated pagination token; stopping "
                "pagination to avoid an infinite loop. Results may be incomplete — "
                "the endpoint may not honor the pagination-token parameter."
            )
            return True
        return False

    def _extract_page_items(
        self,
        data: dict,
        items_key: str,
        fallback_items_keys: Tuple[str, ...],
        context: str,
        warned_fallback_keys: Optional[Set[str]] = None,
    ) -> list:
        """Return one page's items array from a response body.

        Reads ``items_key`` first, then each of ``fallback_items_keys``; the
        first key present in the response wins. An explicit ``null`` value is
        treated as an empty page rather than raising. Two shapes are surfaced
        instead of silently returning nothing:

        - the items came from a fallback key: warn (once per pagination run,
          when ``warned_fallback_keys`` is passed), so we learn which key the
          endpoint actually serves rather than relying on the fallback forever;
        - none of the expected keys are present in a response that carries
          more than the pagination envelope: the page doesn't match the
          documented contract at all, which would otherwise ingest zero items
          with no signal — recorded as an API parse failure on the client
          report. A page carrying only envelope keys (e.g.
          ``{"next_page_token": null}``) is a legitimate empty page, not a
          parse failure.
        """
        for key in (items_key, *fallback_items_keys):
            if key in data:
                if key != items_key and (
                    warned_fallback_keys is None or key not in warned_fallback_keys
                ):
                    logger.warning(
                        f"{context}: response served items under fallback key "
                        f"'{key}' rather than expected key '{items_key}'."
                    )
                    if warned_fallback_keys is not None:
                        warned_fallback_keys.add(key)
                # Tolerate an explicit null (e.g. {"data": null}) as an
                # empty page.
                return data.get(key) or []
        if data and not set(data).issubset(PAGINATION_ENVELOPE_KEYS):
            self.report.report_parse_failure(
                f"{context}: response has none of the expected items keys "
                f"{[items_key, *fallback_items_keys]}; keys present: {sorted(data)}"
            )
        return []

    def _paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, str]] = None,
        items_key: str = "value",
        fallback_items_keys: Tuple[str, ...] = (),
    ) -> Iterator[dict]:
        """Yield all items from a paginated Fabric API endpoint.

        Handles continuation token pagination transparently — callers just
        iterate and get all items across all pages. The caller's ``params``
        dict is never mutated; each page's request gets its own dict.

        A page that has been fetched is always yielded, even if the endpoint
        then turns out to repeat a pagination token: re-emitting a page at
        worst duplicates idempotent-per-URN metadata, while dropping one
        loses entities. The repeated-token guard stops the loop *after* the
        yield and records the truncation on the client report.

        Args:
            endpoint: API endpoint (relative to base URL)
            params: Initial query parameters
            items_key: Response JSON key holding the items array (default "value")
            fallback_items_keys: Alternative items keys to read (with a warning)
                when ``items_key`` is absent from the response

        Yields:
            Item dictionaries from each page's items array
        """
        context = f"Fabric API endpoint '{endpoint}'"
        base_params: Dict[str, str] = dict(params or {})
        seen_tokens: Set[str] = set()
        warned_fallback_keys: Set[str] = set()
        next_token: Optional[str] = None
        page = 0
        total = 0
        while True:
            # A fresh dict per page: base_params (and the caller's dict) are
            # never written to.
            request_params = (
                base_params
                if next_token is None
                else {**base_params, CONTINUATION_TOKEN_KEY: next_token}
            )
            response = self.get(endpoint, params=request_params)
            data = response.json()
            page += 1

            items = self._extract_page_items(
                data, items_key, fallback_items_keys, context, warned_fallback_keys
            )
            logger.debug(f"Page {page}: got {len(items)} item(s) from {endpoint}")
            total += len(items)
            yield from items

            next_token = data.get(CONTINUATION_TOKEN_KEY)
            if not next_token:
                break
            if self._is_repeated_pagination_token(next_token, seen_tokens, context):
                break
            seen_tokens.add(next_token)

        # Not reached when the consumer stops iterating early or a request
        # raises — the absence of this line in a log is not itself a bug.
        logger.info(f"{endpoint}: fetched {total} item(s) across {page} page(s)")

    def _paginate_post(
        self,
        endpoint: str,
        body: dict,
        items_key: str = "value",
        fallback_items_keys: Tuple[str, ...] = (),
    ) -> Iterator[dict]:
        """Yield all items from a paginated Fabric POST API endpoint.

        Some Fabric APIs (e.g. queryActivityRuns) use POST with a
        continuationToken in both the request and response body.

        The caller's ``body`` dict is never mutated; each page's request gets
        its own dict. (The per-page copy is shallow — nested values are shared
        with the caller but never written to.) As in :meth:`_paginate`, a
        fetched page is always yielded and the repeated-token guard stops the
        loop after the yield, recording the truncation on the client report.

        Args:
            endpoint: API endpoint (relative to base URL)
            body: JSON request body
            items_key: Response JSON key holding the items array (default "value")
            fallback_items_keys: Alternative items keys to read (with a warning)
                when ``items_key`` is absent from the response

        Yields:
            Item dictionaries from each page's items array
        """
        context = f"Fabric API endpoint '{endpoint}'"
        base_body = dict(body)
        seen_tokens: Set[str] = set()
        warned_fallback_keys: Set[str] = set()
        next_token: Optional[str] = None
        page = 0
        total = 0
        while True:
            request_body = (
                base_body
                if next_token is None
                else {**base_body, CONTINUATION_TOKEN_KEY: next_token}
            )
            response = self.post(endpoint, json=request_body)
            data = response.json()
            page += 1

            if isinstance(data, list):
                # Some endpoints return a bare array with no pagination
                # envelope; yield it and stop.
                items = data
            else:
                items = self._extract_page_items(
                    data, items_key, fallback_items_keys, context, warned_fallback_keys
                )
            logger.debug(f"Page {page}: got {len(items)} item(s) from {endpoint}")
            total += len(items)
            yield from items

            next_token = (
                data.get(CONTINUATION_TOKEN_KEY) if isinstance(data, dict) else None
            )
            if not next_token:
                break
            if self._is_repeated_pagination_token(next_token, seen_tokens, context):
                break
            seen_tokens.add(next_token)

        # Not reached when the consumer stops iterating early or a request
        # raises — the absence of this line in a log is not itself a bug.
        logger.info(f"{endpoint}: fetched {total} item(s) across {page} page(s)")

    def _list_workspaces_raw(self) -> Iterator[dict]:
        """List all accessible Fabric workspaces (raw data).

        This method is common to all Fabric workloads.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/workspaces/list

        Yields:
            Workspace data dictionaries
        """
        logger.info("Listing Fabric workspaces")
        yield from self._paginate("workspaces")

    def get(self, endpoint: str, params: Optional[dict] = None) -> requests.Response:
        """Make a GET request.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response object
        """
        return self._request("GET", endpoint, params=params)

    def post(
        self, endpoint: str, json: Optional[dict] = None, params: Optional[dict] = None
    ) -> requests.Response:
        """Make a POST request.

        Args:
            endpoint: API endpoint
            json: JSON body
            params: Query parameters

        Returns:
            Response object
        """
        return self._request("POST", endpoint, json=json, params=params)

    def close(self) -> None:
        """Close the session and release resources."""
        if self._session:
            self._session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
