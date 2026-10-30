import base64
import binascii
import logging
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional

import msal
import requests

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.powerbi.config import Constant

logger = logging.getLogger(__name__)

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"

# Fabric item-type names as returned by GET /v1/workspaces/{id}/items.
FABRIC_ITEM_LAKEHOUSE = "Lakehouse"
FABRIC_ITEM_WAREHOUSE = "Warehouse"
FABRIC_ITEM_SQL_ENDPOINT = "SQLEndpoint"
FABRIC_ITEM_SEMANTIC_MODEL = "SemanticModel"
FABRIC_ITEM_REPORT = "Report"

# Path segments of the item-definition endpoints.
FABRIC_KIND_SEMANTIC_MODELS = "semanticModels"
FABRIC_KIND_REPORTS = "reports"
FABRIC_KIND_PAGINATED_REPORTS = "paginatedReports"

FABRIC_FORMAT_TMDL = "TMDL"

# Definition parts with these suffixes carry metadata; anything else (images,
# custom-visual bundles, fonts) is binary and skipped without being reported.
TEXT_DEFINITION_SUFFIXES = (".tmdl", ".json", ".pbir", ".pbism", ".rdl", ".platform")

LRO_STATUS_SUCCEEDED = "Succeeded"
LRO_STATUS_FAILED = "Failed"
LRO_DEFAULT_RETRY_AFTER_SECONDS = 2
# Refresh the Fabric token this long before expiry so an in-flight LRO poll
# does not straddle the boundary.
FABRIC_TOKEN_REFRESH_MARGIN_SECONDS = 300
# Bounds on the poll interval: the floor keeps a `Retry-After: 0` server from
# turning the time-budgeted loop into a busy loop, the ceiling keeps a large
# hint from overshooting the budget.
LRO_MIN_SLEEP_SECONDS = 0.5
LRO_MAX_SLEEP_SECONDS = 10

HEADER_LOCATION = "Location"
HEADER_RETRY_AFTER = "Retry-After"

KEY_CONTINUATION_TOKEN = "continuationToken"
KEY_DEFINITION = "definition"
KEY_PARTS = "parts"
KEY_PATH = "path"
KEY_PAYLOAD = "payload"
KEY_FORMAT = "format"
KEY_PROPERTIES = "properties"
KEY_SQL_ENDPOINT_PROPERTIES = "sqlEndpointProperties"
KEY_WORKSPACE_ID = "workspaceId"
KEY_ERROR = "error"
KEY_MESSAGE = "message"
MSAL_ERROR = "error"
MSAL_ERROR_DESCRIPTION = "error_description"


class FabricDefinitionError(Exception):
    """A Fabric item definition could not be fetched for a non-transport reason."""


class FabricLroTimeoutError(FabricDefinitionError):
    """The long-running getDefinition operation did not finish in time."""


@dataclass
class FabricItem:
    id: str
    display_name: str
    type: str
    workspace_id: str
    description: Optional[str] = None


@dataclass
class DefinitionPart:
    path: str
    text: str


@dataclass
class ItemDefinition:
    format: Optional[str]
    parts: List[DefinitionPart] = field(default_factory=list)
    # Paths of parts whose payload could not be base64/UTF-8 decoded.
    failed_parts: List[str] = field(default_factory=list)


def item_definition_from_response(body: Dict[str, Any]) -> ItemDefinition:
    definition = body.get(KEY_DEFINITION) or {}
    item = ItemDefinition(format=definition.get(KEY_FORMAT))
    for raw_part in definition.get(KEY_PARTS) or []:
        path = raw_part.get(KEY_PATH)
        payload = raw_part.get(KEY_PAYLOAD)
        if not path or payload is None:
            item.failed_parts.append(str(path or "<missing path>"))
            continue
        try:
            raw_bytes = base64.b64decode(payload)
        except (binascii.Error, ValueError) as e:
            logger.debug("Undecodable definition part %s: %s", path, e)
            item.failed_parts.append(path)
            continue
        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            if path.lower().endswith(TEXT_DEFINITION_SUFFIXES):
                # A metadata part we cannot read is a real loss (a table or
                # visual goes missing), unlike an image or visual bundle.
                item.failed_parts.append(path)
            else:
                logger.debug("Skipping binary definition part %s", path)
            continue
        item.parts.append(DefinitionPart(path=path, text=text))
    return item


class FabricDefinitionResolver:
    """Client for the Microsoft Fabric REST API used by the ``workspace_apis``
    metadata mode.

    It shares the connector's MSAL application but requests a second token
    audience (``FABRIC_SCOPE``), because Power BI tokens are rejected by
    ``api.fabric.microsoft.com``. ``getDefinition`` is a long-running
    operation in practice: the first call answers 202 with a ``Location`` to
    poll, and the result lives at ``{Location}/result`` once the status is
    ``Succeeded``.
    """

    def __init__(
        self,
        msal_client: msal.ConfidentialClientApplication,
        request_session: requests.Session,
        lro_timeout_seconds: int,
        base_url: str = FABRIC_BASE_URL,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._msal_client = msal_client
        self._request_session = request_session
        self._lro_timeout_seconds = lro_timeout_seconds
        self._base_url = base_url
        self._sleep = sleep_fn
        self._access_token: Optional[str] = None
        self._access_token_expiry: float = 0.0

    def get_access_token(self) -> str:
        if self._access_token is not None and time.monotonic() < (
            self._access_token_expiry
        ):
            return self._access_token

        logger.info("Generating Fabric API access token")
        auth_response = self._msal_client.acquire_token_for_client(
            scopes=[FABRIC_SCOPE]
        )
        token = auth_response.get(Constant.ACCESS_TOKEN)
        if not token:
            raise ConfigurationError(
                "Failed to retrieve a Fabric API access token for the service "
                "principal. Ensure the tenant setting 'Service principals can "
                "use Fabric APIs' is enabled for this principal. MSAL error: "
                f"{auth_response.get(MSAL_ERROR)}: "
                f"{auth_response.get(MSAL_ERROR_DESCRIPTION)}"
            )
        expires_in = int(auth_response.get(Constant.ACCESS_TOKEN_EXPIRY, 0))
        self._access_token_expiry = time.monotonic() + max(
            expires_in - FABRIC_TOKEN_REFRESH_MARGIN_SECONDS, 0
        )
        self._access_token = f"Bearer {token}"
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        return {Constant.Authorization: self.get_access_token()}

    def _get(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        response = self._request_session.get(
            url, headers=self._headers(), params=params
        )
        response.raise_for_status()
        return response.json()

    def _iter_pages(
        self, url: str, params: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        request_params: Dict[str, str] = dict(params or {})
        while True:
            body = self._get(url, params=request_params)
            rows.extend(body.get(Constant.VALUE) or [])
            token = body.get(KEY_CONTINUATION_TOKEN)
            if not token:
                return rows
            request_params[KEY_CONTINUATION_TOKEN] = token

    def list_items(
        self, workspace_id: str, item_type: Optional[str] = None
    ) -> List[FabricItem]:
        params: Dict[str, str] = {}
        if item_type:
            params[Constant.TYPE] = item_type
        items: List[FabricItem] = []
        for row in self._iter_pages(
            f"{self._base_url}/workspaces/{workspace_id}/items", params=params
        ):
            item_id = row.get(Constant.ID)
            item_type_value = row.get(Constant.TYPE)
            if not item_id or not item_type_value:
                continue
            items.append(
                FabricItem(
                    id=item_id,
                    display_name=row.get(Constant.DISPLAY_NAME) or "",
                    type=item_type_value,
                    workspace_id=row.get(KEY_WORKSPACE_ID) or workspace_id,
                    description=row.get(Constant.DESCRIPTION),
                )
            )
        return items

    def get_lakehouse_sql_endpoint_id(
        self, workspace_id: str, lakehouse_id: str
    ) -> Optional[str]:
        """The SQL analytics endpoint id a DirectLake model's shared expression
        points at (``Sql.Database(server, <endpoint id>)``)."""
        body = self._get(
            f"{self._base_url}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        )
        properties = body.get(KEY_PROPERTIES) or {}
        endpoint = properties.get(KEY_SQL_ENDPOINT_PROPERTIES) or {}
        endpoint_id = endpoint.get(Constant.ID)
        return endpoint_id if isinstance(endpoint_id, str) and endpoint_id else None

    def get_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        item_kind: str,
        definition_format: Optional[str] = None,
    ) -> ItemDefinition:
        url = f"{self._base_url}/workspaces/{workspace_id}/{item_kind}/{item_id}/getDefinition"
        params: Dict[str, str] = {}
        if definition_format:
            params[KEY_FORMAT] = definition_format

        response = self._request_session.post(
            url, headers=self._headers(), params=params
        )
        response.raise_for_status()

        if response.status_code == HTTPStatus.ACCEPTED:
            body = self._wait_for_lro_result(response)
        else:
            body = response.json()
        return item_definition_from_response(body)

    def _wait_for_lro_result(self, accepted: requests.Response) -> Dict[str, Any]:
        location = accepted.headers.get(HEADER_LOCATION)
        if not location:
            raise FabricDefinitionError(
                "getDefinition returned 202 without a Location header to poll"
            )
        delay = self._retry_after_seconds(accepted)
        started = time.monotonic()
        waited = 0.0
        while True:
            # Budget counts both the sleeps and the time the poll requests
            # themselves took (including the session's 429/5xx retries).
            waited = max(waited, time.monotonic() - started)
            if waited + delay > self._lro_timeout_seconds:
                raise FabricLroTimeoutError(
                    f"getDefinition did not complete within {self._lro_timeout_seconds}s; "
                    "increase scan_timeout to wait longer"
                )
            self._sleep(delay)
            waited += delay

            poll = self._request_session.get(location, headers=self._headers())
            poll.raise_for_status()
            operation = poll.json()
            status = operation.get(Constant.STATUS)
            if status == LRO_STATUS_SUCCEEDED:
                break
            if status == LRO_STATUS_FAILED:
                error = operation.get(KEY_ERROR) or {}
                raise FabricDefinitionError(
                    f"getDefinition operation failed: {error.get(KEY_MESSAGE) or error}"
                )
            delay = self._retry_after_seconds(poll)

        result = self._request_session.get(
            f"{location}/result", headers=self._headers()
        )
        result.raise_for_status()
        return result.json()

    @staticmethod
    def _retry_after_seconds(response: requests.Response) -> float:
        raw = response.headers.get(HEADER_RETRY_AFTER)
        try:
            seconds = float(raw) if raw is not None else LRO_DEFAULT_RETRY_AFTER_SECONDS
        except ValueError:
            seconds = LRO_DEFAULT_RETRY_AFTER_SECONDS
        return max(LRO_MIN_SLEEP_SECONDS, min(seconds, LRO_MAX_SLEEP_SECONDS))
