import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.airbyte_utils import (
    apply_pattern,
    clean_uri,
    namespace_queues_for_catalog,
)
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    OAuth2GrantType,
)
from datahub.ingestion.source.airbyte.constants import (
    API_ENDPOINT_APPLICATIONS_TOKEN,
    API_ENDPOINT_CONNECTIONS,
    API_ENDPOINT_DESTINATIONS,
    API_ENDPOINT_JOBS,
    API_ENDPOINT_SOURCES,
    API_ENDPOINT_STREAMS,
    API_ENDPOINT_TAGS,
    API_ENDPOINT_WORKSPACES,
    API_FIELD_CLIENT_ID,
    API_FIELD_CLIENT_SECRET,
    API_FIELD_CONFIG_ID,
    API_FIELD_CONFIG_TYPES,
    API_FIELD_CONFIGURATIONS,
    API_FIELD_DESTINATION_ID,
    API_FIELD_GRANT_TYPE,
    API_FIELD_NAME,
    API_FIELD_REFRESH_TOKEN,
    API_FIELD_SOURCE_ID,
    API_FIELD_STATUS,
    API_FIELD_SYNC_CATALOG,
    API_JOB_CONFIG_TYPE_RESET,
    API_JOB_CONFIG_TYPE_SYNC,
    API_QUERY_LIMIT,
    API_QUERY_OFFSET,
    API_QUERY_UPDATED_AT_END,
    API_QUERY_UPDATED_AT_START,
    API_QUERY_WORKSPACE_ID,
    API_QUERY_WORKSPACE_IDS,
    API_RESPONSE_KEY_ACCESS_TOKEN,
    API_RESPONSE_KEY_DATA,
    API_RESPONSE_KEY_ERROR_DESCRIPTION,
    API_RESPONSE_KEY_EXPIRES_IN,
    API_RESPONSE_KEY_JOBS,
    API_RESPONSE_KEY_NEXT,
    API_RESPONSE_KEY_STREAMS,
    API_RESPONSE_KEY_TAGS,
    API_STATUS_INACTIVE,
    DEFAULT_TOKEN_EXPIRY_SECONDS,
    HTTP_AUTH_STATUS_CODES,
    HTTP_CONTENT_TYPE_FORM_URLENCODED,
    HTTP_CONTENT_TYPE_JSON,
    HTTP_HEADER_AUTHORIZATION,
    HTTP_HEADER_BEARER_PREFIX,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_METHOD_GET,
    HTTP_METHOD_POST,
    HTTP_PROTOCOL_HTTP,
    HTTP_PROTOCOL_HTTPS,
    JSON_SCHEMA_KEY_PROPERTIES,
    JSON_SCHEMA_KEY_TYPE,
    JSON_SCHEMA_TYPE_NULL,
    JSON_SCHEMA_TYPE_OBJECT,
    JSON_SCHEMA_TYPE_STRING,
    TOKEN_REFRESH_BUFFER_SECONDS,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConfigStreamRef,
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbyteSourcePartial,
    AirbyteStream,
    AirbyteStreamApiMetadata,
    AirbyteStreamConfig,
    AirbyteStreamsApiRow,
    AirbyteStreamSyncSettings,
    AirbyteSyncCatalog,
    AirbyteWorkspacePartial,
    PropertyFieldPath,
    StreamIdentifier,
    SyncCatalogBuildResult,
    SyncModeSplit,
)

logger = logging.getLogger(__name__)


class AirbyteApiError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class AirbyteAuthenticationError(AirbyteApiError):
    pass


class AirbyteBaseClient(ABC):
    def __init__(self, config: AirbyteClientConfig):
        self.config = config
        self.session = self._create_session()
        self._stream_api_metadata_cache: Dict[str, AirbyteStreamApiMetadata] = {}

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        session.headers.update({HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_JSON})

        if self.config.extra_headers:
            session.headers.update(self.config.extra_headers)

        session.verify = self.config.verify_ssl
        if self.config.verify_ssl and self.config.ssl_ca_cert:
            if os.path.isfile(self.config.ssl_ca_cert):
                session.verify = self.config.ssl_ca_cert
            else:
                logger.warning(
                    f"CA certificate file not found: {self.config.ssl_ca_cert}. Using default verification."
                )

        if not self.config.verify_ssl:
            logger.warning("SSL certificate verification is disabled")
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=[HTTP_METHOD_GET, HTTP_METHOD_POST],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount(HTTP_PROTOCOL_HTTP, adapter)
        session.mount(HTTP_PROTOCOL_HTTPS, adapter)

        return session

    def _do_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Inner wire hook. Subclasses override this (not `_make_request`) to add
        # behaviour around the request itself — e.g. the Cloud client's 401/403
        # token-refresh retry — while keeping the error-translation shell in
        # `_make_request` shared.
        #
        # Deliberately *don't* log response bodies, even at DEBUG: Airbyte API
        # payloads embed source/destination configs that often contain connector
        # credentials (JDBC URLs with passwords, S3 secrets, etc.).
        response = self.session.get(
            url, params=params, timeout=self.config.request_timeout
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError as e:
            # 2xx with a non-JSON body is a server/proxy bug, not a transport
            # blip — keep the HTTP status so callers do not treat it as
            # "unavailable" the way a connection error (no status) is.
            raise AirbyteApiError(
                f"Airbyte API returned invalid JSON (HTTP {response.status_code})",
                status_code=response.status_code,
            ) from e

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = self._get_full_url(endpoint)

        try:
            return self._do_get(url, params=params)
        except requests.HTTPError as e:
            error_message = f"Airbyte API request failed: {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get('message', e.response.text)}"
            except (ValueError, KeyError):
                error_message += f" - {e.response.text}"

            logger.error(error_message)
            status_code = e.response.status_code
            if status_code in HTTP_AUTH_STATUS_CODES:
                raise AirbyteAuthenticationError(
                    error_message, status_code=status_code
                ) from e
            raise AirbyteApiError(error_message, status_code=status_code) from e
        except requests.RequestException as e:
            error_message = f"Error connecting to Airbyte API: {str(e)}"
            logger.error(error_message)
            raise AirbyteApiError(error_message) from e

    def _paginate_results(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        result_key: str = API_RESPONSE_KEY_DATA,
        page_size: Optional[int] = None,
        limit: Optional[int] = None,
        next_page_token_key: str = API_RESPONSE_KEY_NEXT,
        offset_param: str = API_QUERY_OFFSET,
    ) -> Iterator[dict]:
        if not page_size:
            page_size = self.config.page_size

        if not params:
            params = {}

        params[API_QUERY_LIMIT] = page_size
        offset = 0
        total_items = 0

        while True:
            if offset > 0:
                params[offset_param] = offset

            response = self._make_request(endpoint, params=params)

            items = response.get(result_key, [])
            if not isinstance(items, list):
                # A malformed page silently truncating the rest of the
                # enumeration would yield incomplete metadata; surface it.
                raise AirbyteApiError(
                    f"Paginated response for {endpoint} returned non-list at "
                    f"key '{result_key}': got {type(items).__name__}"
                )

            for item in items:
                yield item
                total_items += 1

                if limit and total_items >= limit:
                    return

            if not items:
                break
            # Airbyte's Public API paginates by offset only; the next-token
            # check is kept for forward-compat with hypothetical cursors.
            next_token = response.get(next_page_token_key)
            if next_token == "":
                break

            offset += page_size

    @abstractmethod
    def _get_full_url(self, endpoint: str) -> str: ...

    @abstractmethod
    def _check_auth_before_request(self) -> None: ...

    def list_workspaces(
        self, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteWorkspacePartial]:
        self._check_auth_before_request()
        workspaces_data = list(
            self._paginate_results(
                endpoint=API_ENDPOINT_WORKSPACES, result_key=API_RESPONSE_KEY_DATA
            )
        )

        if pattern:
            workspaces_data = apply_pattern(workspaces_data, pattern)

        return [AirbyteWorkspacePartial.model_validate(w) for w in workspaces_data]

    def list_connections(
        self,
        workspace_id: str,
        pattern: Optional[AllowDenyPattern] = None,
        include_inactive: bool = False,
    ) -> List[AirbyteConnectionPartial]:
        self._check_auth_before_request()
        params = {API_QUERY_WORKSPACE_ID: workspace_id}
        connections = list(
            self._paginate_results(
                endpoint=API_ENDPOINT_CONNECTIONS,
                params=params,
                result_key=API_RESPONSE_KEY_DATA,
            )
        )

        if not include_inactive:
            connections = [
                conn
                for conn in connections
                if conn.get(API_FIELD_STATUS) != API_STATUS_INACTIVE
            ]

        if pattern:
            connections = apply_pattern(connections, pattern)

        return [AirbyteConnectionPartial.model_validate(c) for c in connections]

    def list_jobs(
        self,
        connection_id: str,
        workspace_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        self._check_auth_before_request()

        params = {
            API_FIELD_CONFIG_ID: connection_id,
            API_FIELD_CONFIG_TYPES: [
                API_JOB_CONFIG_TYPE_SYNC,
                API_JOB_CONFIG_TYPE_RESET,
            ],
            API_QUERY_LIMIT: limit,
        }

        if workspace_id:
            params[API_QUERY_WORKSPACE_ID] = workspace_id

        if start_date:
            params[API_QUERY_UPDATED_AT_START] = start_date

        if end_date:
            params[API_QUERY_UPDATED_AT_END] = end_date

        response = self._make_request(API_ENDPOINT_JOBS, params=params)
        # Older Airbyte versions return the list under "jobs"; newer ones
        # consolidated on "data".
        return response.get(API_RESPONSE_KEY_DATA) or response.get(
            API_RESPONSE_KEY_JOBS, []
        )

    def get_source(self, source_id: str) -> AirbyteSourcePartial:
        self._check_auth_before_request()
        source_data = self._make_request(f"{API_ENDPOINT_SOURCES}/{source_id}")
        return AirbyteSourcePartial.model_validate(source_data)

    def list_sources(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteSourcePartial]:
        self._check_auth_before_request()

        params = {API_QUERY_WORKSPACE_ID: workspace_id}
        sources_data = list(
            self._paginate_results(
                endpoint=API_ENDPOINT_SOURCES,
                params=params,
                result_key=API_RESPONSE_KEY_DATA,
            )
        )

        if pattern:
            sources_data = apply_pattern(sources_data, pattern)

        return [AirbyteSourcePartial.model_validate(s) for s in sources_data]

    def get_destination(self, destination_id: str) -> AirbyteDestinationPartial:
        self._check_auth_before_request()
        dest_data = self._make_request(f"{API_ENDPOINT_DESTINATIONS}/{destination_id}")
        return AirbyteDestinationPartial.model_validate(dest_data)

    def list_destinations(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteDestinationPartial]:
        self._check_auth_before_request()

        params = {API_QUERY_WORKSPACE_ID: workspace_id}
        destinations_data = list(
            self._paginate_results(
                endpoint=API_ENDPOINT_DESTINATIONS,
                params=params,
                result_key=API_RESPONSE_KEY_DATA,
            )
        )

        if pattern:
            destinations_data = apply_pattern(destinations_data, pattern)

        return [AirbyteDestinationPartial.model_validate(d) for d in destinations_data]

    def get_connection(self, connection_id: str) -> AirbyteConnectionPartial:
        self._check_auth_before_request()
        connection_data = self._make_request(
            f"{API_ENDPOINT_CONNECTIONS}/{connection_id}"
        )

        # Airbyte 1.x Public API exposes `configurations.streams` instead of the
        # legacy `syncCatalog.streams` shape. Synthesize the legacy structure so
        # downstream code can stay version-agnostic.
        config_streams: List[Dict[str, Any]] = []
        if not connection_data.get(API_FIELD_SYNC_CATALOG):
            configurations = connection_data.get(API_FIELD_CONFIGURATIONS) or {}
            if isinstance(configurations, dict):
                config_streams = configurations.get(API_RESPONSE_KEY_STREAMS) or []

        connection = AirbyteConnectionPartial.model_validate(connection_data)
        if not config_streams:
            return connection

        build_result = self._build_sync_catalog(
            config_streams,
            self._fetch_stream_api_metadata(connection_data.get(API_FIELD_SOURCE_ID)),
        )
        connection.sync_catalog = build_result.catalog
        connection.ambiguous_stream_namespaces = build_result.ambiguous
        connection.skipped_stream_payloads = build_result.skipped_stream_payloads
        connection.streams_api_unavailable = build_result.streams_api_unavailable
        connection.streams_api_unavailable_status_code = (
            build_result.streams_api_unavailable_status_code
        )
        connection.streams_api_unavailable_message = (
            build_result.streams_api_unavailable_message
        )
        connection.streams_api_namespaces_absent = (
            build_result.streams_api_namespaces_absent
        )
        return connection

    def _fetch_stream_api_metadata(
        self, source_id: Optional[str]
    ) -> AirbyteStreamApiMetadata:
        if not source_id:
            return AirbyteStreamApiMetadata()

        # Namespaces are a property of the source, but connections are walked one
        # at a time — cache so N connections over one source cost one call.
        cached = self._stream_api_metadata_cache.get(source_id)
        if cached is not None:
            return cached

        metadata = self._collect_stream_api_metadata(source_id)
        self._stream_api_metadata_cache[source_id] = metadata
        return metadata

    @staticmethod
    def _is_streams_api_unavailable_error(error: AirbyteApiError) -> bool:
        # 404: no /streams, or inaccessible source. 5xx / no status: transient
        # server or connection failure after retries. Other 4xx (400/422/…) and
        # successful-but-malformed payloads (status preserved on JSON decode
        # failures) are client/server bugs and must surface.
        if error.status_code is None:
            return True
        if error.status_code == 404:
            return True
        return error.status_code >= 500

    def _collect_stream_api_metadata(self, source_id: str) -> AirbyteStreamApiMetadata:
        try:
            detailed_streams = self.list_streams(source_id=source_id)
        except AirbyteAuthenticationError:
            raise
        except AirbyteApiError as e:
            if not self._is_streams_api_unavailable_error(e):
                raise
            # Raising would fail the whole connection and, via report.failure,
            # skip stale-entity removal for every other connection on the run.
            # Auth failures are AirbyteAuthenticationError from _make_request
            # and re-raise above.
            return AirbyteStreamApiMetadata(
                unavailable=True,
                unavailable_status_code=e.status_code,
                unavailable_message=str(e),
            )

        property_fields_by_stream: Dict[StreamIdentifier, List[PropertyFieldPath]] = {}
        namespaces_by_name: Dict[str, List[str]] = {}
        skipped_rows: List[str] = []
        validated_rows = 0
        for index, stream in enumerate(detailed_streams):
            try:
                row = AirbyteStreamsApiRow.model_validate(stream)
            except ValidationError as e:
                # One unreadable row costs only its own namespace and columns.
                skipped_rows.append(
                    f"/streams[{index}]: {e.error_count()} validation error(s)"
                )
                continue

            validated_rows += 1

            if not row.stream_name:
                continue

            if row.namespace:
                namespaces_by_name.setdefault(row.stream_name, []).append(row.namespace)

            if not row.property_fields:
                continue

            stream_id = StreamIdentifier(
                stream_name=row.stream_name, namespace=row.namespace
            )
            property_fields_by_stream[stream_id] = [
                PropertyFieldPath(path=path) for path in row.property_fields
            ]

        return AirbyteStreamApiMetadata(
            property_fields_by_stream=property_fields_by_stream,
            namespaces_by_name=namespaces_by_name,
            # Counted over readable rows only: rows that all failed validation
            # leave no namespaces either, and blaming the Airbyte version for
            # that sends operators after the wrong problem.
            namespaces_absent=validated_rows > 0 and not namespaces_by_name,
            skipped_rows=skipped_rows,
        )

    def _build_sync_catalog(
        self,
        config_streams: List[Dict[str, Any]],
        stream_api_metadata: AirbyteStreamApiMetadata,
    ) -> SyncCatalogBuildResult:
        stream_refs: List[AirbyteConfigStreamRef] = []
        skipped: List[str] = list(stream_api_metadata.skipped_rows)
        for index, stream in enumerate(config_streams):
            try:
                stream_refs.append(AirbyteConfigStreamRef.model_validate(stream))
            except ValidationError as e:
                # One unreadable stream must not cost the whole connection: the
                # caller would drop its datasets, lineage and DataFlow/DataJob.
                name = stream.get(API_FIELD_NAME) if isinstance(stream, dict) else None
                skipped.append(
                    f"configurations.streams[{index}]"
                    f"{f' ({name})' if name else ''}: "
                    f"{e.error_count()} invalid field(s)"
                )
        queue_result = namespace_queues_for_catalog(
            stream_refs, stream_api_metadata.namespaces_by_name
        )
        queues = {name: list(ns) for name, ns in queue_result.queues.items()}
        streams: List[AirbyteStreamConfig] = []
        for stream_ref in stream_refs:
            name = stream_ref.name or ""
            namespace = stream_ref.namespace or ""

            if not namespace:
                queued = queues.get(name)
                if queued:
                    namespace = queued.pop(0)

            stream_id = StreamIdentifier(stream_name=name, namespace=namespace)
            property_fields = stream_api_metadata.property_fields_by_stream.get(
                stream_id
            )

            streams.append(
                AirbyteStreamConfig(
                    stream=AirbyteStream(
                        name=name,
                        namespace=namespace if namespace else None,
                        json_schema=self._get_json_schema_for_stream(
                            stream_ref, property_fields
                        ),
                    ),
                    config=self._build_stream_config(stream_ref),
                )
            )

        return SyncCatalogBuildResult(
            catalog=AirbyteSyncCatalog(streams=streams),
            ambiguous=queue_result.ambiguous,
            skipped_stream_payloads=skipped,
            streams_api_unavailable=stream_api_metadata.unavailable,
            streams_api_unavailable_status_code=(
                stream_api_metadata.unavailable_status_code
            ),
            streams_api_unavailable_message=stream_api_metadata.unavailable_message,
            streams_api_namespaces_absent=stream_api_metadata.namespaces_absent,
        )

    def _build_stream_config(
        self, stream: AirbyteConfigStreamRef
    ) -> AirbyteStreamSyncSettings:
        sync_modes = SyncModeSplit.from_api_value(stream.sync_mode)

        return AirbyteStreamSyncSettings(
            selected=True,
            sync_mode=sync_modes.source_mode,
            destination_sync_mode=sync_modes.destination_mode,
            primary_key=stream.primary_key,
            cursor_field=stream.cursor_field,
            destination_namespace=stream.destination_namespace,
            alias_name=stream.alias_name,
            selected_fields=stream.selected_fields,
            field_selection_enabled=stream.field_selection_enabled,
        )

    def _get_json_schema_for_stream(
        self,
        stream: AirbyteConfigStreamRef,
        property_fields: Optional[List[PropertyFieldPath]] = None,
    ) -> Dict[str, Any]:
        # Schema sources in order of preference:
        #   1. propertyFields from `/streams` (most accurate)
        #   2. jsonSchema embedded in the legacy configurations payload
        #   3. empty schema — column-level lineage will be dropped
        if property_fields:
            properties = {
                field_path.field_name: {
                    JSON_SCHEMA_KEY_TYPE: [
                        JSON_SCHEMA_TYPE_NULL,
                        JSON_SCHEMA_TYPE_STRING,
                    ]
                }
                for field_path in property_fields
                if field_path.field_name
            }
            if properties:
                return {
                    JSON_SCHEMA_KEY_TYPE: JSON_SCHEMA_TYPE_OBJECT,
                    JSON_SCHEMA_KEY_PROPERTIES: properties,
                }

        return stream.json_schema

    def list_streams(
        self, source_id: Optional[str] = None, destination_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        # Older Airbyte versions 404 here; callers fall back to
        # configurations.streams rather than propagating the error. The namespace
        # field only appears from 1.7.0 onwards.
        self._check_auth_before_request()
        query_params = []
        if source_id:
            query_params.append(f"{API_FIELD_SOURCE_ID}={source_id}")
        if destination_id:
            query_params.append(f"{API_FIELD_DESTINATION_ID}={destination_id}")

        endpoint = API_ENDPOINT_STREAMS
        if query_params:
            endpoint = f"{endpoint}?{'&'.join(query_params)}"

        response = self._make_request(endpoint)
        return (
            response
            if isinstance(response, list)
            else response.get(API_RESPONSE_KEY_STREAMS, [])
        )

    def get_job(self, job_id: str) -> Dict[str, Any]:
        self._check_auth_before_request()
        return self._make_request(f"{API_ENDPOINT_JOBS}/{job_id}")

    def list_tags(self, workspace_id: str) -> List[Dict[str, Any]]:
        self._check_auth_before_request()
        response = self._make_request(
            f"{API_ENDPOINT_TAGS}?{API_QUERY_WORKSPACE_IDS}={workspace_id}"
        )
        # Older Airbyte versions return tags under "tags".
        return response.get(API_RESPONSE_KEY_DATA) or response.get(
            API_RESPONSE_KEY_TAGS, []
        )


class AirbyteOSSClient(AirbyteBaseClient):
    def __init__(self, config: AirbyteClientConfig):
        super().__init__(config)

        if not config.host_port:
            raise ValueError("host_port is required for open_source deployment")

        self.base_url = f"{clean_uri(config.host_port)}/api/public/v1"
        self._setup_authentication()

    def _setup_authentication(self) -> None:
        # OAuth2 -> API key/Bearer -> basic auth -> no-auth, in that order.
        # See https://docs.airbyte.com/using-airbyte/configuring-api-access
        if self.config.oauth2_client_id and self.config.oauth2_client_secret:
            self._setup_oauth_authentication()
        elif self.config.api_key:
            token = self.config.api_key.get_secret_value()
            self.session.headers.update(
                {HTTP_HEADER_AUTHORIZATION: f"{HTTP_HEADER_BEARER_PREFIX}{token}"}
            )
        elif self.config.username:
            password = (
                self.config.password.get_secret_value() if self.config.password else ""
            )
            self.session.auth = HTTPBasicAuth(self.config.username, password)

    def _setup_oauth_authentication(self) -> None:
        if not self.config.oauth2_client_id or not self.config.oauth2_client_secret:
            raise ValueError(
                "OAuth2 client ID and client secret are required for OAuth authentication"
            )
        self._request_oauth_token()

    def _request_oauth_token(self) -> None:
        if not self.config.oauth2_client_secret:
            raise ValueError("OAuth2 client secret is required")

        token_url = f"{self.base_url}{API_ENDPOINT_APPLICATIONS_TOKEN}"

        token_data = {
            API_FIELD_CLIENT_ID: self.config.oauth2_client_id,
            API_FIELD_CLIENT_SECRET: self.config.oauth2_client_secret.get_secret_value(),
            # OSS only exposes client_credentials.
            API_FIELD_GRANT_TYPE: OAuth2GrantType.CLIENT_CREDENTIALS.value,
        }

        try:
            response = requests.post(
                token_url,
                data=token_data,
                headers={HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_FORM_URLENCODED},
                timeout=self.config.request_timeout,
                verify=self.session.verify,
            )
            response.raise_for_status()
            token_response = response.json()
            access_token = token_response.get(API_RESPONSE_KEY_ACCESS_TOKEN)

            if not access_token:
                raise ValueError("No access_token in OAuth response")

            self.session.headers.update(
                {
                    HTTP_HEADER_AUTHORIZATION: f"{HTTP_HEADER_BEARER_PREFIX}{access_token}"
                }
            )

            expires_in = token_response.get(
                API_RESPONSE_KEY_EXPIRES_IN, DEFAULT_TOKEN_EXPIRY_SECONDS
            )
            self.token_expiry = time.time() + expires_in
        except requests.HTTPError as e:
            error_message = f"Failed to get OAuth2 token: HTTP {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get(API_RESPONSE_KEY_ERROR_DESCRIPTION, error_details.get('message', e.response.text))}"
            except (ValueError, KeyError):
                error_message += f" - {e.response.text}"
            logger.error(error_message)
            raise AirbyteAuthenticationError(
                error_message, status_code=e.response.status_code
            ) from e
        except Exception as e:
            error_message = f"Failed to get OAuth2 token: {str(e)}"
            logger.error(error_message)
            raise AirbyteAuthenticationError(error_message) from e

    def _check_token_expiry(self) -> None:
        if hasattr(self, "token_expiry") and time.time() >= (
            self.token_expiry - TOKEN_REFRESH_BUFFER_SECONDS
        ):
            self._request_oauth_token()

    def _check_auth_before_request(self) -> None:
        if self.config.oauth2_client_id and self.config.oauth2_client_secret:
            self._check_token_expiry()

    def _get_full_url(self, endpoint: str) -> str:
        return f"{self.base_url}{endpoint}"


class AirbyteCloudClient(AirbyteBaseClient):
    def __init__(self, config: AirbyteClientConfig):
        super().__init__(config)

        self.workspace_id = config.cloud_workspace_id
        if not self.workspace_id:
            raise ValueError("Workspace ID is required for Airbyte Cloud")

        self.base_url = config.cloud_api_url
        self.token_url = config.cloud_oauth_token_url
        self._setup_oauth_authentication()

    def _setup_oauth_authentication(self) -> None:
        if not self.config.oauth2_client_id or not self.config.oauth2_client_secret:
            raise ValueError(
                "OAuth2 client ID and client secret are required for Airbyte Cloud"
            )

        if self.config.oauth2_grant_type == OAuth2GrantType.REFRESH_TOKEN:
            if not self.config.oauth2_refresh_token:
                raise ValueError(
                    "OAuth2 refresh token is required for refresh_token grant type"
                )

        self._acquire_token()

    def _request_oauth_token(self, grant_type: OAuth2GrantType) -> None:
        if not self.config.oauth2_client_secret:
            raise ValueError("OAuth2 client secret is required")

        token_data = {
            API_FIELD_CLIENT_ID: self.config.oauth2_client_id,
            API_FIELD_CLIENT_SECRET: self.config.oauth2_client_secret.get_secret_value(),
            API_FIELD_GRANT_TYPE: grant_type.value,
        }

        if grant_type == OAuth2GrantType.REFRESH_TOKEN:
            if not self.config.oauth2_refresh_token:
                raise ValueError("OAuth2 refresh token is required")
            token_data[API_FIELD_REFRESH_TOKEN] = (
                self.config.oauth2_refresh_token.get_secret_value()
            )

        try:
            response = requests.post(
                self.token_url,
                data=token_data,
                headers={HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_FORM_URLENCODED},
                timeout=self.config.request_timeout,
                verify=self.session.verify,
            )
            response.raise_for_status()

            response_data = response.json()
            self.access_token = response_data.get(API_RESPONSE_KEY_ACCESS_TOKEN)
            expires_in = response_data.get(
                API_RESPONSE_KEY_EXPIRES_IN, DEFAULT_TOKEN_EXPIRY_SECONDS
            )
            self.token_expiry = time.time() + expires_in

            self.session.headers.update(
                {
                    HTTP_HEADER_AUTHORIZATION: (
                        f"{HTTP_HEADER_BEARER_PREFIX}{self.access_token}"
                    )
                }
            )
        except requests.HTTPError as e:
            error_message = f"Failed to get OAuth2 token via {grant_type.value}: HTTP {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get(API_RESPONSE_KEY_ERROR_DESCRIPTION, error_details.get('message', e.response.text))}"
            except (ValueError, KeyError):
                error_message += f" - {e.response.text}"
            logger.error(error_message)
            raise AirbyteAuthenticationError(
                error_message, status_code=e.response.status_code
            ) from e
        except Exception as e:
            error_message = (
                f"Failed to get OAuth2 token via {grant_type.value}: {str(e)}"
            )
            logger.error(error_message)
            raise AirbyteAuthenticationError(error_message) from e

    def _get_client_credentials_token(self) -> None:
        self._request_oauth_token(OAuth2GrantType.CLIENT_CREDENTIALS)

    def _refresh_oauth_token(self) -> None:
        self._request_oauth_token(OAuth2GrantType.REFRESH_TOKEN)

    def _acquire_token(self) -> None:
        self._request_oauth_token(self.config.oauth2_grant_type)

    def _do_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Override the wire hook (not `_make_request`) so the HTTP-to-error
        # translation stays shared and only the token-refresh-on-401/403
        # behaviour differs between OSS and Cloud.
        try:
            return super()._do_get(url, params=params)
        except requests.HTTPError as e:
            if e.response.status_code not in HTTP_AUTH_STATUS_CODES:
                raise
            logger.warning(
                "Received %s from Airbyte Cloud, attempting token refresh",
                e.response.status_code,
            )
            try:
                self._acquire_token()
                return super()._do_get(url, params=params)
            except Exception as refresh_exc:
                raise AirbyteAuthenticationError(
                    "Token refresh failed after 401/403 response"
                ) from refresh_exc

    def _check_token_expiry(self) -> None:
        if hasattr(self, "token_expiry") and time.time() >= (
            self.token_expiry - TOKEN_REFRESH_BUFFER_SECONDS
        ):
            self._acquire_token()

    def _get_full_url(self, endpoint: str) -> str:
        # Concatenate explicitly: `urljoin` drops trailing path segments when the
        # base URL lacks a trailing slash, so customer-supplied `cloud_api_url`
        # values like "https://eu.example.com/api/v1" lose their path prefix.
        return f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def _check_auth_before_request(self) -> None:
        self._check_token_expiry()

    def list_workspaces(
        self, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteWorkspacePartial]:
        # Cloud restricts each set of credentials to a single workspace, so this
        # is a single GET rather than a paginated list.
        self._check_auth_before_request()
        try:
            workspace_data = self._make_request(
                f"{API_ENDPOINT_WORKSPACES}/{self.workspace_id}"
            )
            workspaces_data = [workspace_data] if workspace_data else []

            if pattern:
                workspaces_data = apply_pattern(workspaces_data, pattern)

            return [AirbyteWorkspacePartial.model_validate(w) for w in workspaces_data]
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.info("Workspace %s not found", self.workspace_id)
                return []
            logger.error("Failed to get workspace: HTTP %s", e.response.status_code)
            raise
        except requests.RequestException as e:
            logger.error("Network error: %s", str(e))
            raise


def create_airbyte_client(config: AirbyteClientConfig) -> AirbyteBaseClient:
    if config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE:
        return AirbyteOSSClient(config)
    if config.deployment_type == AirbyteDeploymentType.CLOUD:
        return AirbyteCloudClient(config)
    raise ValueError(f"Invalid deployment type: {config.deployment_type}")
