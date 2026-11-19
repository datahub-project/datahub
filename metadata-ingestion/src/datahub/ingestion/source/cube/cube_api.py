import logging
from dataclasses import dataclass
from http import HTTPStatus
from typing import Dict, Iterable, List, Mapping, Optional, Type, TypeVar, Union
from urllib.parse import urlparse

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.cube.config import (
    CubeDeploymentType,
    CubeSourceConfig,
    CubeSourceReport,
)
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
    META_PRIVATE_SCOPE,
    METADATA_API_PAGE_SIZE,
    PLATFORM_API_PAGE_SIZE,
)
from datahub.ingestion.source.cube.models import (
    CloudDataSource,
    CloudDataSourcesResponse,
    CloudEntitiesResponse,
    CloudEntity,
    CloudReportsResponse,
    CloudWorkbooksResponse,
    CoreMetaResponse,
    CubeEntity,
    CubeReport,
    CubeWorkbook,
    merge_entities,
)

logger = logging.getLogger(__name__)

# Cloud Platform API list responses share a common cursor-paginated shape
# (`items` + `pageInfo`); this bounds the generic pagination helper to them.
_PlatformResponse = TypeVar(
    "_PlatformResponse", CloudReportsResponse, CloudWorkbooksResponse
)


class _UnexpectedMetadataApiResponseError(ValueError):
    """A /v1/entities/all page did not match the documented envelope."""


class _PrivateEntitiesRefusedError(Exception):
    """Cube Cloud answered the first include_private page with HTTP 403."""

    def __init__(self, http_error: requests.HTTPError) -> None:
        super().__init__(str(http_error))
        self.http_error = http_error


@dataclass
class _SkippedCloudEntity:
    name: Optional[str]
    context: str
    error: ValidationError


class CubeAPIClient:
    def __init__(self, config: CubeSourceConfig, report: CubeSourceReport):
        self.config = config
        self.report = report
        self.session = self._create_session()
        self._meta_sync_token: Optional[str] = None
        self._skipped_cloud_entities: List[_SkippedCloudEntity] = []

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
        body: Dict[str, object] = {
            "security_context": self.config.security_context,
            "expires_in": self.config.meta_sync_token_expires_in,
        }
        if self.config.include_private:
            body["scopes"] = [META_PRIVATE_SCOPE]
        response = self.session.post(
            url,
            json=body,
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
        params: Optional[Mapping[str, Union[str, int]]] = None,
        json_body: Optional[Mapping[str, object]] = None,
        bearer: bool = False,
    ) -> Dict[str, object]:
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
        # warehouse/column lineage. Fall back gracefully (to /v1/meta only) when
        # the Metadata API or the Control Plane token-mint cannot be reached, so
        # a lineage-only failure never aborts the whole ingestion.
        try:
            cloud_entities = self._get_cloud_entities_with_private_fallback()
        except (requests.RequestException, ValueError) as e:
            if self.config.include_private:
                # Private entities only come from the Metadata API, so a
                # failure is needed to stop stale removal from deleting the
                # ones earlier runs ingested.
                self.report.failure(
                    title="Cube Metadata API unavailable",
                    message=(
                        "Using /v1/meta only, so private cubes, views, and "
                        "members are missing from this run and stale removal "
                        "is skipped. See the error for the cause, or set "
                        "include_private: false."
                    ),
                    exc=e,
                )
                return core_entities
            unexpected = isinstance(e, _UnexpectedMetadataApiResponseError)
            self.report.warning(
                title=(
                    "Unexpected Cube Metadata API response"
                    if unexpected
                    else "Cube Metadata API unavailable"
                ),
                message=(
                    "The Metadata API response did not have the expected "
                    "shape. Using /v1/meta only; lineage to warehouse tables "
                    "will be unavailable."
                    if unexpected
                    else "Using /v1/meta only; lineage to warehouse tables will "
                    "be unavailable. Provide cloud_api_key + deployment_id + "
                    "environment_id (or a Control Plane token) to enable the "
                    "Metadata API."
                ),
                exc=e,
            )
            return core_entities
        core_names = {entity.name for entity in core_entities}
        for skipped in self._skipped_cloud_entities:
            self._report_skipped_entity(skipped, in_meta=skipped.name in core_names)
        return merge_entities(
            core_entities,
            cloud_entities,
            include_private=self.config.include_private,
        )

    def _get_cloud_entities_with_private_fallback(self) -> List[CubeEntity]:
        # Minting runs inside _request, so mint (and cache) the token first:
        # otherwise a Control Plane error would be read as a problem with the
        # /v1/entities/all response (a refused scope, or an unexpected body).
        self._metadata_api_token()
        if not self.config.include_private:
            return self._get_cloud_entities(include_private=False)
        try:
            return self._get_cloud_entities(include_private=True)
        except _PrivateEntitiesRefusedError as e:
            # Cube Cloud can drop the meta-private scope for callers who
            # may not read the data model, then rejects include_private.
            # Reported before the retry, so a failing retry cannot skip it.
            self.report.failure(
                title="Cube private entities unavailable",
                message=(
                    "Cube Cloud refused include_private (HTTP 403), usually "
                    "because the token lacks the meta-private scope. Ingesting "
                    "public entities only. Use a Cube Cloud API key or token for "
                    "an admin or a user who can read the data model, or set "
                    "include_private: false. If the retry without it also "
                    "fails, the token cannot read the Metadata API at all."
                ),
                exc=e.http_error,
            )
            return self._get_cloud_entities(include_private=False)

    def get_data_sources(self) -> List[CloudDataSource]:
        if self.config.deployment_type != CubeDeploymentType.CLOUD:
            return []
        try:
            raw = self._request(HTTP_METHOD_GET, API_ENDPOINT_DATA_SOURCES, bearer=True)
            return CloudDataSourcesResponse.model_validate(raw).data.data_sources
        except (requests.RequestException, ValueError) as e:
            self.report.warning(
                title="Could not list Cube data sources",
                message=(
                    "Warehouse platform/database auto-detection was skipped; set "
                    "warehouse_platform explicitly if warehouse lineage is missing."
                ),
                exc=e,
            )
            return []

    def _can_use_platform_api(self) -> bool:
        # The Platform API (reports/workbooks) is Cube Cloud only and is
        # authenticated with the Cube Cloud API key against the Control Plane host.
        return bool(
            self.config.deployment_type == CubeDeploymentType.CLOUD
            and self.config.cloud_api_key
            and self.config.deployment_id
        )

    def _platform_request(
        self, endpoint: str, params: Optional[Mapping[str, Union[str, int]]] = None
    ) -> Dict[str, object]:
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

    def _paginate_platform(
        self, endpoint: str, response_cls: Type[_PlatformResponse]
    ) -> Iterable[_PlatformResponse]:
        # Walk a cursor-paginated Platform API list endpoint, yielding one parsed
        # page at a time so callers can stream items without buffering raw pages.
        cursor: Optional[str] = None
        while True:
            params: Dict[str, Union[str, int]] = {"first": PLATFORM_API_PAGE_SIZE}
            if cursor:
                params["after"] = cursor
            raw = self._platform_request(endpoint, params=params)
            response = response_cls.model_validate(raw)
            yield response
            page_info = response.page_info
            if not (page_info and page_info.has_next_page and page_info.end_cursor):
                break
            cursor = page_info.end_cursor

    def get_reports(self) -> List[CubeReport]:
        if not self._can_use_platform_api():
            return []
        endpoint = API_ENDPOINT_REPORTS.format(deployment_id=self.config.deployment_id)
        reports: List[CubeReport] = []
        try:
            for page in self._paginate_platform(endpoint, CloudReportsResponse):
                reports.extend(CubeReport.from_cloud(r) for r in page.items)
        except (requests.RequestException, ValueError) as e:
            # Keep whatever pages succeeded before the failure rather than
            # discarding them, and surface the truncation in the report.
            self.report.warning(
                title="Could not list all Cube reports",
                message="Report ingestion may be incomplete.",
                exc=e,
            )
        return reports

    def get_workbooks(self) -> List[CubeWorkbook]:
        if not self._can_use_platform_api():
            return []
        endpoint = API_ENDPOINT_WORKBOOKS.format(
            deployment_id=self.config.deployment_id
        )
        workbooks: List[CubeWorkbook] = []
        try:
            for page in self._paginate_platform(endpoint, CloudWorkbooksResponse):
                workbooks.extend(CubeWorkbook.from_cloud(w) for w in page.items)
        except (requests.RequestException, ValueError) as e:
            self.report.warning(
                title="Could not list all Cube workbooks",
                message="Workbook ingestion may be incomplete.",
                exc=e,
            )
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

    def _get_cloud_entities(self, *, include_private: bool) -> List[CubeEntity]:
        entities: List[CubeEntity] = []
        self._skipped_cloud_entities = []
        body = {"include_private": True} if include_private else {}
        offset = 0
        while True:
            try:
                raw = self._request(
                    HTTP_METHOD_POST,
                    API_ENDPOINT_ENTITIES_ALL,
                    params={"offset": offset, "limit": METADATA_API_PAGE_SIZE},
                    json_body=body,
                    bearer=True,
                )
            except requests.HTTPError as e:
                # Only the first page can be a refused scope; a 403 after
                # include_private already succeeded has another cause.
                if (
                    include_private
                    and offset == 0
                    and e.response is not None
                    and e.response.status_code == HTTPStatus.FORBIDDEN
                ):
                    raise _PrivateEntitiesRefusedError(e) from e
                raise
            except requests.JSONDecodeError as e:
                raise _UnexpectedMetadataApiResponseError(
                    f"/v1/entities/all returned a non-JSON body at offset {offset}"
                ) from e
            try:
                response = CloudEntitiesResponse.model_validate(raw)
            except ValidationError as e:
                raise _UnexpectedMetadataApiResponseError(
                    f"Unexpected /v1/entities/all response at offset {offset}"
                ) from e
            page = response.data
            for raw_entity in page:
                if raw_entity is None:
                    # Nothing to match against /v1/meta or to lose, so this is
                    # counted rather than failing the run.
                    self.report.metadata_api_null_entities_skipped += 1
                    continue
                try:
                    entity = CloudEntity.model_validate(raw_entity)
                except ValidationError as e:
                    name = (
                        raw_entity.get("name") if isinstance(raw_entity, dict) else None
                    )
                    self._skipped_cloud_entities.append(
                        _SkippedCloudEntity(
                            name=name if isinstance(name, str) else None,
                            context=f"{name or '<unnamed entity>'} at offset {offset}",
                            error=e,
                        )
                    )
                    continue
                self._report_unusable_references(entity)
                entities.append(CubeEntity.from_cloud_entity(entity))

            pagination = response.pagination
            if pagination is None:
                break
            offset += pagination.limit or METADATA_API_PAGE_SIZE
            if offset >= pagination.total or not page:
                break
        return entities

    def _report_skipped_entity(
        self, skipped: _SkippedCloudEntity, *, in_meta: bool
    ) -> None:
        self.report.metadata_api_entities_skipped += 1
        if not in_meta:
            # Only the Metadata API returns this entity, so a failure stops
            # stale removal deleting it.
            self.report.failure(
                title="Skipped invalid Metadata API entity",
                message="The connector could not parse this entity, so it and "
                "its lineage are missing from this run; stale removal is skipped.",
                context=skipped.context,
                exc=skipped.error,
            )
            return
        if self.config.include_private:
            # /v1/meta still supplies the entity, but its private members only
            # come from the Metadata API.
            self.report.failure(
                title="Skipped invalid Metadata API entity",
                message="The connector could not parse this entity, so its "
                "Metadata API lineage and private members are missing from this "
                "run; stale removal is skipped.",
                context=skipped.context,
                exc=skipped.error,
            )
            return
        self.report.warning(
            title="Skipped invalid Metadata API entity",
            message="The connector could not parse this entity, so its "
            "Metadata API lineage is missing from this run.",
            context=skipped.context,
            exc=skipped.error,
        )

    def _report_unusable_references(self, entity: CloudEntity) -> None:
        skipped = entity.unusable_reference_count()
        if not skipped:
            return
        self.report.metadata_api_references_skipped += skipped
        self.report.warning(
            title="Skipped Metadata API references",
            message="Some cube or member references could not be used for "
            "lineage: a member reference without both cube and member, or a "
            "cube reference that is not of type upstream.",
            context=f"{entity.name}: {skipped} skipped",
        )
