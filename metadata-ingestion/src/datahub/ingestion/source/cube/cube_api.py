import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.cube.config import CubeDeploymentType, CubeSourceConfig
from datahub.ingestion.source.cube.constants import (
    API_ENDPOINT_DATA_SOURCES,
    API_ENDPOINT_ENTITIES_ALL,
    API_ENDPOINT_META,
    API_ENDPOINT_REPORTS,
    API_ENDPOINT_WORKBOOKS,
    CONTROL_PLANE_META_SYNC_TOKEN_PATH,
    HTTP_CONTENT_TYPE_JSON,
    HTTP_HEADER_AUTHORIZATION,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_PROTOCOL_HTTP,
    HTTP_PROTOCOL_HTTPS,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
    METADATA_API_PAGE_SIZE,
    PLATFORM_API_PAGE_SIZE,
)
from datahub.ingestion.source.cube.models import (
    CloudDataSource,
    CloudDataSourcesResponse,
    CloudEntitiesResponse,
    CloudReportsResponse,
    CloudWorkbooksResponse,
    CoreMetaResponse,
    CubeEntity,
    CubeReport,
    CubeWorkbook,
    merge_entities,
)

logger = logging.getLogger(__name__)


class CubeAPIClient:
    def __init__(self, config: CubeSourceConfig):
        self.config = config
        self.session = self._create_session()
        self._meta_sync_token: Optional[str] = None

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=HTTP_RETRY_MAX_ATTEMPTS,
            backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
            status_forcelist=HTTP_RETRY_STATUS_CODES,
            allowed_methods=[HTTP_METHOD_GET, HTTP_METHOD_POST],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount(HTTP_PROTOCOL_HTTP, adapter)
        session.mount(HTTP_PROTOCOL_HTTPS, adapter)
        session.headers.update({HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_JSON})
        return session

    def _uses_control_plane(self) -> bool:
        return bool(
            self.config.cloud_api_key
            and self.config.deployment_id
            and self.config.environment_id
        )

    def _metadata_api_token(self) -> str:
        # The Metadata API uses either a pre-supplied token (api_token) or a
        # short-lived token minted via the Control Plane API.
        if not self._uses_control_plane():
            return self.config.api_token.get_secret_value()
        if self._meta_sync_token is None:
            self._meta_sync_token = self._mint_meta_sync_token()
        return self._meta_sync_token

    def _control_plane_base(self) -> str:
        if self.config.cloud_api_url:
            return self.config.cloud_api_url.rstrip("/")
        parsed = urlparse(self.config.api_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _mint_meta_sync_token(self) -> str:
        assert self.config.cloud_api_key is not None
        path = CONTROL_PLANE_META_SYNC_TOKEN_PATH.format(
            deployment_id=self.config.deployment_id,
            environment_id=self.config.environment_id,
        )
        url = f"{self._control_plane_base()}/{path}"
        response = self.session.post(
            url,
            json={
                "security_context": self.config.security_context,
                "expires_in": self.config.meta_sync_token_expires_in,
            },
            headers={
                HTTP_HEADER_AUTHORIZATION: f"Bearer {self.config.cloud_api_key.get_secret_value()}"
            },
            timeout=self.config.request_timeout_sec,
        )
        response.raise_for_status()
        token = response.json().get("data", {}).get("token")
        if not token:
            raise ValueError(
                "Control Plane tokens-for-meta-sync response did not contain a token"
            )
        logger.info("Minted a Cube Metadata API token via the Control Plane API")
        return str(token)

    def _auth_header_value(self, bearer: bool) -> str:
        # Cube's REST API (/v1/meta, /v1/load) expects the raw data JWT, while the
        # Cube Cloud Metadata API expects a metadata-scoped token prefixed with
        # "Bearer " (minted via the Control Plane API when configured).
        if bearer:
            return f"Bearer {self._metadata_api_token()}"
        return self.config.api_token.get_secret_value()

    def _url(self, endpoint: str) -> str:
        return f"{self.config.api_url}/{endpoint}"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        bearer: bool = False,
    ) -> Dict[str, Any]:
        response = self.session.request(
            method,
            self._url(endpoint),
            params=params,
            json=json_body,
            headers={HTTP_HEADER_AUTHORIZATION: self._auth_header_value(bearer)},
            timeout=self.config.request_timeout_sec,
        )
        response.raise_for_status()
        return response.json()

    def get_entities(self) -> List[CubeEntity]:
        # /v1/meta carries the structural/presentation metadata (folders,
        # hierarchies, joins, formats, visibility) and is available on both Core
        # and Cloud, so it is always the base.
        core_entities = self._get_core_entities()

        use_metadata_api = (
            self.config.deployment_type == CubeDeploymentType.CLOUD
            and self.config.use_metadata_api
        )
        if not use_metadata_api:
            return core_entities

        # On Cloud, enrich the structural metadata with the Metadata API's
        # warehouse/column lineage. Fall back gracefully when the token cannot
        # reach the Metadata API.
        try:
            cloud_entities = self._get_cloud_entities()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (401, 403, 404):
                logger.warning(
                    "Cube Metadata API is not accessible "
                    f"(HTTP {status}); using /v1/meta only. Lineage to warehouse "
                    "tables will be unavailable. Provide cloud_api_key + "
                    "deployment_id + environment_id (or a Control Plane token) to "
                    "enable the Metadata API."
                )
                return core_entities
            raise
        return merge_entities(core_entities, cloud_entities)

    def get_data_sources(self) -> List[CloudDataSource]:
        if self.config.deployment_type != CubeDeploymentType.CLOUD:
            return []
        try:
            raw = self._request(HTTP_METHOD_GET, API_ENDPOINT_DATA_SOURCES, bearer=True)
        except requests.HTTPError as e:
            logger.info(f"Could not list Cube data sources: {e}")
            return []
        return CloudDataSourcesResponse.model_validate(raw).data.data_sources

    def _can_use_platform_api(self) -> bool:
        # The Platform API (reports/workbooks) is Cube Cloud only and is
        # authenticated with the Cube Cloud API key against the Control Plane host.
        return bool(
            self.config.deployment_type == CubeDeploymentType.CLOUD
            and self.config.cloud_api_key
            and self.config.deployment_id
        )

    def _platform_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        assert self.config.cloud_api_key is not None
        response = self.session.get(
            f"{self._control_plane_base()}/{endpoint}",
            params=params,
            headers={
                HTTP_HEADER_AUTHORIZATION: f"Bearer {self.config.cloud_api_key.get_secret_value()}"
            },
            timeout=self.config.request_timeout_sec,
        )
        response.raise_for_status()
        return response.json()

    def get_reports(self) -> List[CubeReport]:
        if not self._can_use_platform_api():
            return []
        endpoint = API_ENDPOINT_REPORTS.format(deployment_id=self.config.deployment_id)
        reports: List[CubeReport] = []
        cursor: Optional[str] = None
        try:
            while True:
                params: Dict[str, Any] = {"first": PLATFORM_API_PAGE_SIZE}
                if cursor:
                    params["after"] = cursor
                raw = self._platform_request(endpoint, params=params)
                response = CloudReportsResponse.model_validate(raw)
                reports.extend(CubeReport.from_cloud(r) for r in response.items)
                page_info = response.page_info
                if not page_info or not page_info.has_next_page:
                    break
                cursor = page_info.end_cursor
                if not cursor:
                    break
        except requests.HTTPError as e:
            logger.warning(f"Could not list Cube reports: {e}")
            return []
        return reports

    def get_workbooks(self) -> List[CubeWorkbook]:
        if not self._can_use_platform_api():
            return []
        endpoint = API_ENDPOINT_WORKBOOKS.format(
            deployment_id=self.config.deployment_id
        )
        workbooks: List[CubeWorkbook] = []
        cursor: Optional[str] = None
        try:
            while True:
                params: Dict[str, Any] = {"first": PLATFORM_API_PAGE_SIZE}
                if cursor:
                    params["after"] = cursor
                raw = self._platform_request(endpoint, params=params)
                response = CloudWorkbooksResponse.model_validate(raw)
                workbooks.extend(CubeWorkbook.from_cloud(w) for w in response.items)
                page_info = response.page_info
                if not page_info or not page_info.has_next_page:
                    break
                cursor = page_info.end_cursor
                if not cursor:
                    break
        except requests.HTTPError as e:
            logger.warning(f"Could not list Cube workbooks: {e}")
            return []
        return workbooks

    def test_connection(self) -> None:
        # /v1/meta is the universal connectivity check (available on Core and
        # Cloud). The Metadata API is additive and verified at ingestion time.
        self._request(HTTP_METHOD_GET, API_ENDPOINT_META)

    def _get_core_entities(self) -> List[CubeEntity]:
        # `extended` returns the SQL definitions we use for Core lineage parsing.
        raw = self._request(
            HTTP_METHOD_GET, API_ENDPOINT_META, params={"extended": "true"}
        )
        response = CoreMetaResponse.model_validate(raw)
        return [CubeEntity.from_core_cube(cube) for cube in response.cubes]

    def _get_cloud_entities(self) -> List[CubeEntity]:
        entities: List[CubeEntity] = []
        offset = 0
        while True:
            raw = self._request(
                HTTP_METHOD_POST,
                API_ENDPOINT_ENTITIES_ALL,
                params={"offset": offset, "limit": METADATA_API_PAGE_SIZE},
                json_body={},
                bearer=True,
            )
            response = CloudEntitiesResponse.model_validate(raw)
            page = response.data.entities
            entities.extend(CubeEntity.from_cloud_entity(entity) for entity in page)

            pagination = response.pagination
            if pagination is None:
                break
            offset += pagination.limit or METADATA_API_PAGE_SIZE
            if offset >= pagination.total or not page:
                break
        return entities
