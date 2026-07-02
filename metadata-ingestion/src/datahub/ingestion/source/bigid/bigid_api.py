import logging
from collections.abc import Iterator
from typing import Any, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.bigid.constants import IDSOR_ATTRIBUTE_TYPE
from datahub.ingestion.source.bigid.models import (
    BigIDCatalogObject,
    BigIDClassification,
    BigIDColumn,
    BigIDConnection,
    BigIDGlossaryItem,
    BigIDResultsTuningAttribute,
    IDSoRAttributeInfo,
)

logger = logging.getLogger(__name__)

PAGE_SIZE = 500  # catalog API page size; verified against live instance


class BigIDAPIError(Exception):
    """Raised when a BigID API call fails in a non-retryable way."""


class BigIDClient:
    # Auth: supply either user_token (long-lived, exchanged for a short-lived access
    # token at startup) or access_token (short-lived, used directly). BigID rejects the
    # "Bearer " prefix, so tokens are sent raw in the Authorization header.

    def __init__(
        self,
        bigid_url: str,
        user_token: Optional[str] = None,
        access_token: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
    ) -> None:
        if not user_token and not access_token:
            raise BigIDAPIError("Either user_token or access_token must be supplied.")

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

    def _get_access_token(self) -> str:
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

    def _request(
        self,
        endpoint: str,
        params: Optional[dict[str, Union[str, int]]] = None,
    ) -> Union[dict[str, Any], list[Any]]:
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
                        url,
                        headers=self._auth_headers(),
                        params=params,
                        timeout=self.timeout,
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

    def get_glossary_items(self) -> list[BigIDGlossaryItem]:
        # GET /api/v1/business_glossary_items returns a plain JSON array (no wrapper).
        result = self._request("/api/v1/business_glossary_items")
        if not isinstance(result, list):
            raise BigIDAPIError(
                f"Unexpected response shape for business_glossary_items: {type(result)}"
            )
        return [BigIDGlossaryItem.model_validate(item) for item in result]

    def get_catalog_objects(self) -> Iterator[BigIDCatalogObject]:
        # BigID's totalRowsCounter echoes the limit param, not the true total, so we
        # paginate until a short page (len(results) < limit) instead.
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
                yield BigIDCatalogObject.model_validate(obj)

            if len(results) < PAGE_SIZE:
                break
            skip += PAGE_SIZE

    def get_columns(
        self, object_name: str, source_name: str, fqn: str = ""
    ) -> list[BigIDColumn]:
        # BigID's filter uses substring matching and objectName is not schema-qualified,
        # so two tables with the same name in different schemas return combined results.
        # We narrow client-side by fullyQualifiedName, falling back to exact objectName.
        # Double-quotes in the identifiers 422 from BigID's filter parser; database
        # identifiers never contain quotes in practice, so they are not sanitised — a 422
        # raises BigIDAPIError, which _process_catalog_object catches and warns on.
        filter_expr = f'objectName = "{object_name}" AND source = "{source_name}"'
        result = self._request(
            "/api/v1/data-catalog/columns",
            params={"filter": filter_expr},
        )
        if isinstance(result, list):
            raw_columns = result
        else:
            # Key presence (not truthiness) so an empty-list "results" key is preferred
            # over a non-empty "data" key in the same response.
            for key in ("results", "data", "columns"):
                if key in result:
                    raw_columns = result[key]
                    break
            else:
                logger.warning(
                    "get_columns: unrecognised response shape for %s/%s — keys: %s",
                    source_name,
                    object_name,
                    list(result.keys()),
                )
                raw_columns = []
        columns = [BigIDColumn.model_validate(column) for column in raw_columns]
        if fqn:
            return [column for column in columns if column.fully_qualified_name == fqn]
        return [column for column in columns if column.object_name == object_name]

    def get_connections(self) -> list[BigIDConnection]:
        # Envelope: {status, statusCode, data: {ds_connections: [...]}, message}
        data = self._request("/api/v1/ds-connections")
        if not isinstance(data, dict):
            raise BigIDAPIError(
                f"Unexpected response shape from /api/v1/ds-connections: {type(data)}"
            )
        raw = data.get("data", {}).get("ds_connections", [])
        return [BigIDConnection.model_validate(conn) for conn in raw]

    def get_all_classifications(self) -> list[BigIDClassification]:
        # Envelope: {status, statusCode, data: {classifications: [...]}, message}.
        # original_name is the lookup key.
        data = self._request("/api/v1/all-classifications")
        if not isinstance(data, dict):
            raise BigIDAPIError(
                f"Unexpected response shape from /api/v1/all-classifications: {type(data)}"
            )
        raw = data.get("data", {}).get("classifications", [])
        return [BigIDClassification.model_validate(item) for item in raw]

    def get_idsor_attribute_map(self) -> dict[str, IDSoRAttributeInfo]:
        # Maps raw IDSoR attribute name → friendly name + glossary id. Each attributes[]
        # entry carries a nested friendlyName object (which can be {} when uncurated) whose
        # glossaryId links to an existing Business Glossary item (path 1) or is null when an
        # auto-generated term is needed (paths 2 and 3). Only "IDSoR Attribute" entries are
        # kept; the endpoint also returns Classification / ClassificationMd types.
        result = self._request("/api/v1/data-catalog/results-tuning/attributes")
        if not isinstance(result, dict):
            raise BigIDAPIError(
                f"Unexpected response shape from results-tuning/attributes: {type(result)}"
            )
        raw_attributes: list[dict[str, Any]] = result.get("data", {}).get(
            "attributes", []
        )

        attr_map: dict[str, IDSoRAttributeInfo] = {}
        for raw in raw_attributes:
            entry = BigIDResultsTuningAttribute.model_validate(raw)
            if entry.attribute_type != IDSOR_ATTRIBUTE_TYPE:
                continue
            if not entry.attribute_name:
                continue

            friendly = entry.friendly_name_obj
            friendly_name = (
                (friendly.friendly_name if friendly else "")
                or entry.display_name
                or entry.attribute_name
            )
            glossary_id = (friendly.glossary_id if friendly else None) or None

            attr_map[entry.attribute_name] = IDSoRAttributeInfo(
                friendly_name=friendly_name, glossary_id=glossary_id
            )

        logger.debug(
            "Loaded %d IDSoR attribute name mappings from results-tuning/attributes",
            len(attr_map),
        )
        return attr_map

    def test_connection(self) -> None:
        # Raises BigIDAPIError on failure so callers can use str(exc) as failure_reason.
        self._request("/api/v1/ds-connections")

    def close(self) -> None:
        self.session.close()
