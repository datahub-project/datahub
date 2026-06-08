"""
BigID REST API client.

Handles authentication, pagination, retry logic, and response parsing
for all six APIs used by the BigID DataHub connector.

Auth note: BigID rejects the "Bearer " prefix — the access token must
be sent raw in the Authorization header.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.bigid.bigid_utils import IDSoRAttributeInfo

logger = logging.getLogger(__name__)

PAGE_SIZE = 500  # catalog API page size; verified against live instance


class BigIDAPIError(Exception):
    """Raised when a BigID API call fails in a non-retryable way."""


class BigIDClient:
    """
    Thin wrapper around the BigID REST APIs used by the DataHub connector.

    Six APIs are consumed:
    A. GET /api/v1/business_glossary_items                       — GlossaryTerm entities
    B. GET /api/v1/data-catalog/                                 — Dataset enumeration + tags
    C. GET /api/v1/data-catalog/columns                          — Column schema + classification findings
    D. GET /api/v1/ds-connections                                — Platform auto-detection
    E. GET /api/v1/all-classifications                           — Classifier -> glossary_id mapping
    F. GET /api/v1/data-catalog/results-tuning/attributes        — IDSoR attribute friendly-name + glossaryId map

    Authentication:
    - Supply either ``user_token`` (long-lived; exchanged for a short-lived
      access token at startup) or ``access_token`` (short-lived; used directly,
      primarily for testing).
    - Tokens are sent raw — no "Bearer " prefix.
    """

    def __init__(
        self,
        bigid_url: str,
        user_token: Optional[str] = None,
        access_token: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
    ) -> None:
        if not user_token and not access_token:
            raise BigIDAPIError(
                "Either user_token or access_token must be supplied."
            )

        self.bigid_url = bigid_url.rstrip("/")
        self.user_token = user_token
        self._access_token = access_token
        self.timeout = timeout

        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str:
        """Return a valid access token, refreshing from user_token if needed."""
        if self._access_token:
            return self._access_token

        # Exchange long-lived user_token for a short-lived access token.
        # GET is the correct method; POST /api/v1/refresh-access-token returns 404.
        # Bearer prefix behaviour for this step is unverified; send raw token.
        try:
            resp = self.session.get(
                f"{self.bigid_url}/api/v1/refresh-access-token",
                headers={
                    "Authorization": self.user_token,
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise BigIDAPIError(f"Token refresh failed: {exc}") from exc
        data: dict[str, Any] = resp.json()
        if not isinstance(data, dict):
            raise BigIDAPIError(
                f"Unexpected token response shape: expected dict, got {type(data).__name__}"
            )
        token = data.get("systemToken") or data.get("access_token") or data.get("token")
        if not token:
            raise BigIDAPIError(
                f"Could not extract access token from refresh response: {list(data.keys())}"
            )
        self._access_token = token
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": self._get_access_token(),
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Core request helper
    # ------------------------------------------------------------------

    def _request(
        self,
        endpoint: str,
        params: Optional[dict[str, str | int]] = None,
    ) -> Union[dict[str, Any], list[Any]]:
        """Authenticated GET with error handling."""
        url = f"{self.bigid_url}{endpoint}"
        try:
            resp = self.session.get(
                url,
                headers=self._auth_headers(),
                params=params,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            if status == 401 and self.user_token:
                # Cached token may have expired; clear it and retry once with a fresh one
                self._access_token = None
                try:
                    resp = self.session.get(
                        url, headers=self._auth_headers(), params=params, timeout=self.timeout
                    )
                    resp.raise_for_status()
                    return resp.json()
                except requests.exceptions.RequestException as retry_exc:
                    raise BigIDAPIError(
                        f"HTTP 401 and token refresh retry failed for {url}: {retry_exc}"
                    ) from retry_exc
            raise BigIDAPIError(f"HTTP {status} from {url}: {exc}") from exc
        except requests.exceptions.Timeout as exc:
            raise BigIDAPIError(f"Timeout calling {url}") from exc
        except requests.exceptions.RequestException as exc:
            raise BigIDAPIError(f"Request error calling {url}: {exc}") from exc

    # ------------------------------------------------------------------
    # API A — Business Glossary Items
    # ------------------------------------------------------------------

    def get_glossary_items(self) -> list[dict[str, Any]]:
        """
        Return all business glossary items.

        Response is a plain JSON array (no status/data wrapper).
        No pagination observed on test instance (~507 items).
        """
        result = self._request("/api/v1/business_glossary_items")
        if not isinstance(result, list):
            raise BigIDAPIError(
                f"Unexpected response shape for business_glossary_items: {type(result)}"
            )
        return result

    # ------------------------------------------------------------------
    # API B — Data Catalog (paginated)
    # ------------------------------------------------------------------

    def get_catalog_objects(self) -> Iterator[dict[str, Any]]:
        """
        Yield all catalog objects with limit/skip pagination.

        BigID's ``totalRowsCounter`` echoes the ``limit`` param, not the actual
        total — stop when ``len(results) < limit``.
        """
        skip = 0
        while True:
            data = self._request(
                "/api/v1/data-catalog/",
                params={"limit": PAGE_SIZE, "skip": skip},
            )
            if not isinstance(data, dict):
                raise BigIDAPIError(
                    f"Unexpected response shape from /api/v1/data-catalog/: {type(data)}"
                )
            results: list[dict[str, Any]] = data.get("results", [])
            for obj in results:
                yield obj

            if len(results) < PAGE_SIZE:
                break
            skip += PAGE_SIZE

    # ------------------------------------------------------------------
    # API C — Columns
    # ------------------------------------------------------------------

    def get_columns(
        self, object_name: str, source_name: str, fqn: str = ""
    ) -> list[dict[str, Any]]:
        """
        Return all columns for a single table/view.

        Filter: ``objectName = "{object_name}" AND source = "{source_name}"``
        Returns ALL columns, not just classified ones.

        BigID's filter uses substring matching (e.g. "info" matches "info2") and
        objectName is not schema-qualified, so two tables with the same name in
        different schemas return combined results. Each column row carries a
        fullyQualifiedName field; we filter by that client-side when available,
        falling back to exact objectName match otherwise.
        """
        # Double-quotes in object_name/source_name cause a 422 from BigID's filter parser.
        # In practice database identifiers never contain quotes, so this is not sanitised.
        # A 422 here raises BigIDAPIError, which _process_catalog_object catches and warns.
        filter_expr = f'objectName = "{object_name}" AND source = "{source_name}"'
        result = self._request(
            "/api/v1/data-catalog/columns",
            params={"filter": filter_expr},
        )
        # Response can be a list or wrapped in a dict; live API uses "results"
        if isinstance(result, list):
            cols = result
        else:
            # Use key presence (not truthiness) so an empty-list "results" key
            # is preferred over a non-empty "data" key from the same response.
            cols = next(
                (result[k] for k in ("results", "data", "columns") if k in result),
                None,
            )
            if cols is None:
                logger.warning(
                    "get_columns: unrecognised response shape for %s/%s — keys: %s",
                    source_name,
                    object_name,
                    list(result.keys()),
                )
                cols = []
        # Prefer FQN match (handles same-named tables in different schemas).
        # Fall back to objectName exact match (handles substring ambiguity).
        if fqn:
            return [c for c in cols if c.get("fullyQualifiedName") == fqn]
        return [c for c in cols if c.get("objectName") == object_name]

    # ------------------------------------------------------------------
    # API D — DS Connections
    # ------------------------------------------------------------------

    def get_connections(self) -> list[dict[str, Any]]:
        """
        Return all datasource connections.

        Response envelope: {status, statusCode, data: {ds_connections: [...]}, message}
        """
        data = self._request("/api/v1/ds-connections")
        if not isinstance(data, dict):
            raise BigIDAPIError(
                f"Unexpected response shape from /api/v1/ds-connections: {type(data)}"
            )
        return data.get("data", {}).get("ds_connections", [])

    # ------------------------------------------------------------------
    # API E — All Classifications
    # ------------------------------------------------------------------

    def get_all_classifications(self) -> list[dict[str, Any]]:
        """
        Return all classifiers.

        Response envelope: {status, statusCode, data: {classifications: [...]}, message}
        Returns ~1,600 classifiers. ``original_name`` is the lookup key.
        """
        data = self._request("/api/v1/all-classifications")
        if not isinstance(data, dict):
            raise BigIDAPIError(
                f"Unexpected response shape from /api/v1/all-classifications: {type(data)}"
            )
        return data.get("data", {}).get("classifications", [])

    # ------------------------------------------------------------------
    # API F — IDSoR Attribute Map
    # ------------------------------------------------------------------

    def get_idsor_attribute_map(self) -> dict[str, IDSoRAttributeInfo]:
        """
        Return a mapping of raw IDSoR attribute name → (friendly_name, glossary_id).

        Calls GET /api/v1/data-catalog/results-tuning/attributes.

        Verified response shape (BigID 6.x, 2026-04-29):
        ::

            {
              "status": "success",
              "statusCode": 200,
              "data": {
                "totalCount": 63,
                "attributes": [
                  {
                    "attributeId": "a837f938d48d839af721d49f99a35a7a",
                    "attributeType": "IDSoR Attribute",   # also "Classification" / "ClassificationMd"
                    "attributeName": "full_name",         # raw name; matches attributeDetails[].name
                    "displayName":   "Full Name",         # human-readable fallback
                    "totalFields":   128,
                    "totalFindings": 72231,
                    "categories": [ { "displayName": "Personal sensitive", ... } ],
                    "friendlyName": {                     # can be {} when uncurated
                      "originalName": "full_name",
                      "description":  null,
                      "friendlyName": "Full Name",        # canonical display name; may be null
                      "glossaryId":   "fn_item_hqp1"     # links to business glossary; may be null
                    }
                  }
                ]
              }
            }

        The endpoint covers all 63 attribute types (18 IDSoR, 33 classifier, 12 ClassificationMd
        on the test instance). Only ``attributeType == "IDSoR Attribute"`` entries are included
        in the returned map. Each entry represents exactly one raw attribute name; deduplication
        to a canonical friendly name (e.g. email_addr / employee_email / customer_email →
        "Email") is expressed through multiple entries sharing the same ``friendlyName`` object.

        ``glossary_id`` is non-None when the attribute links to an existing BigID Business
        Glossary item (path 1 resolution); None when an auto-generated term is needed
        (paths 2 and 3).
        """
        result = self._request("/api/v1/data-catalog/results-tuning/attributes")
        if not isinstance(result, dict):
            raise BigIDAPIError(
                f"Unexpected response shape from results-tuning/attributes: {type(result)}"
            )
        attributes: list[dict[str, Any]] = result.get("data", {}).get("attributes", [])

        attr_map: dict[str, IDSoRAttributeInfo] = {}
        for entry in attributes:
            if entry.get("attributeType") != "IDSoR Attribute":
                continue

            raw_name: str = entry.get("attributeName", "")
            if not raw_name:
                continue

            # friendlyName is a nested object; can be {} when no curation has been applied.
            friendly_obj: dict[str, Any] = entry.get("friendlyName") or {}
            friendly_name: str = (
                friendly_obj.get("friendlyName")
                or entry.get("displayName")
                or raw_name
            )
            glossary_id: Optional[str] = friendly_obj.get("glossaryId") or None

            attr_map[raw_name] = IDSoRAttributeInfo(
                friendly_name=friendly_name, glossary_id=glossary_id
            )

        logger.debug(
            "Loaded %d IDSoR attribute name mappings from results-tuning/attributes",
            len(attr_map),
        )
        return attr_map

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    def test_connection(self) -> None:
        """Attempt a lightweight call to verify credentials and connectivity.

        Raises BigIDAPIError on failure so callers can use str(exc) as failure_reason.
        """
        self._request("/api/v1/ds-connections")

    def close(self) -> None:
        self.session.close()
