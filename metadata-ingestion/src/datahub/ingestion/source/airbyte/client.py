import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.airbyte_utils import (
    apply_pattern,
    clean_uri,
    coerce_str,
    namespace_queues_for_catalog,
)
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    OAuth2GrantType,
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
)

logger = logging.getLogger(__name__)

CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"

DEFAULT_TOKEN_EXPIRY_SECONDS = 3600
# Refresh 10 minutes before expiry to avoid races between the "still valid"
# check and the actual API call landing on the server.
TOKEN_REFRESH_BUFFER_SECONDS = 600


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

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        session.headers.update({"Content-Type": CONTENT_TYPE_JSON})

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
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _do_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Inner wire hook. Subclasses override this (not `_make_request`) to
        # add behaviour around the request itself — e.g. the Cloud client's
        # 401/403 token-refresh retry — while keeping the error-translation
        # shell in `_make_request` shared.
        #
        # Deliberately *don't* log response bodies at DEBUG: Airbyte API
        # payloads embed source/destination configs that often contain
        # connector credentials (JDBC URLs with passwords, S3 secrets, etc.).
        response = self.session.get(
            url, params=params, timeout=self.config.request_timeout
        )
        response.raise_for_status()
        return response.json()

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
            raise AirbyteApiError(
                error_message, status_code=e.response.status_code
            ) from e
        except requests.RequestException as e:
            error_message = f"Error connecting to Airbyte API: {str(e)}"
            logger.error(error_message)
            raise AirbyteApiError(error_message) from e

    def _paginate_results(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        result_key: str = "data",
        page_size: Optional[int] = None,
        limit: Optional[int] = None,
        next_page_token_key: str = "next",
        offset_param: str = "offset",
    ) -> Iterator[dict]:
        if not page_size:
            page_size = self.config.page_size

        if not params:
            params = {}

        params["limit"] = page_size
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

            # Airbyte's Public API paginates by offset only; the next-token
            # check is kept for forward-compat with hypothetical cursors.
            if not items:
                break
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
            self._paginate_results(endpoint="/workspaces", result_key="data")
        )

        if pattern:
            workspaces_data = apply_pattern(workspaces_data, pattern)

        return [AirbyteWorkspacePartial.model_validate(w) for w in workspaces_data]

    def list_connections(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteConnectionPartial]:
        self._check_auth_before_request()
        params = {"workspaceId": workspace_id}
        connections = list(
            self._paginate_results(
                endpoint="/connections", params=params, result_key="data"
            )
        )

        active_connections = [
            conn for conn in connections if conn.get("status") != "inactive"
        ]

        if pattern:
            active_connections = apply_pattern(active_connections, pattern)

        return [AirbyteConnectionPartial.model_validate(c) for c in active_connections]

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
            "configId": connection_id,
            "configTypes": ["sync", "reset_connection"],
            "limit": limit,
        }

        if workspace_id:
            params["workspaceId"] = workspace_id

        if start_date:
            params["updatedAtStart"] = start_date

        if end_date:
            params["updatedAtEnd"] = end_date

        response = self._make_request("/jobs", params=params)
        # Older Airbyte versions return the list under "jobs"; newer ones
        # consolidated on "data".
        return response.get("data") or response.get("jobs", [])

    def get_source(self, source_id: str) -> AirbyteSourcePartial:
        self._check_auth_before_request()
        source_data = self._make_request(f"/sources/{source_id}")
        return AirbyteSourcePartial.model_validate(source_data)

    def list_sources(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteSourcePartial]:
        self._check_auth_before_request()

        params = {"workspaceId": workspace_id}
        sources_data = list(
            self._paginate_results(
                endpoint="/sources", params=params, result_key="data"
            )
        )

        if pattern:
            sources_data = apply_pattern(sources_data, pattern)

        return [AirbyteSourcePartial.model_validate(s) for s in sources_data]

    def get_destination(self, destination_id: str) -> AirbyteDestinationPartial:
        self._check_auth_before_request()
        dest_data = self._make_request(f"/destinations/{destination_id}")
        return AirbyteDestinationPartial.model_validate(dest_data)

    def list_destinations(
        self, workspace_id: str, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteDestinationPartial]:
        self._check_auth_before_request()

        params = {"workspaceId": workspace_id}
        destinations_data = list(
            self._paginate_results(
                endpoint="/destinations", params=params, result_key="data"
            )
        )

        if pattern:
            destinations_data = apply_pattern(destinations_data, pattern)

        return [AirbyteDestinationPartial.model_validate(d) for d in destinations_data]

    def get_connection(self, connection_id: str) -> AirbyteConnectionPartial:
        self._check_auth_before_request()
        connection_data = self._make_request(f"/connections/{connection_id}")

        # Airbyte 1.x Public API exposes `configurations.streams` instead of
        # the legacy `syncCatalog.streams` shape. Synthesize the legacy
        # structure so downstream code can stay version-agnostic.
        if not connection_data.get("syncCatalog") and connection_data.get(
            "configurations"
        ):
            configurations = connection_data.get("configurations", {})
            if "streams" in configurations:
                stream_api_metadata = self._fetch_stream_api_metadata(
                    connection_data.get("sourceId")
                )
                build_result = self._build_sync_catalog(
                    configurations["streams"],
                    stream_api_metadata,
                )
                connection_data["syncCatalog"] = build_result.catalog
                connection_data["ambiguous_stream_namespaces"] = build_result.ambiguous
                connection_data["positional_stream_namespaces"] = (
                    build_result.positional
                )

        return AirbyteConnectionPartial.model_validate(connection_data)

    def _fetch_stream_api_metadata(
        self, source_id: Optional[str]
    ) -> AirbyteStreamApiMetadata:
        if not source_id:
            return AirbyteStreamApiMetadata()

        try:
            detailed_streams = self.list_streams(source_id=source_id)
        except AirbyteAuthenticationError:
            raise
        except AirbyteApiError as e:
            if e.status_code == 404:
                logger.debug(
                    "Airbyte /streams unavailable (HTTP 404); skipping namespace "
                    "and property-field backfill for source_id=%s",
                    source_id,
                )
                return AirbyteStreamApiMetadata()
            raise

        property_fields_by_stream: Dict[StreamIdentifier, List[PropertyFieldPath]] = {}
        namespaces_by_name: Dict[str, List[str]] = {}
        for stream in detailed_streams:
            if not isinstance(stream, dict):
                continue

            row = AirbyteStreamsApiRow.model_validate(stream)
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
                PropertyFieldPath(path=field if isinstance(field, list) else [field])
                for field in row.property_fields
                if field
            ]

        return AirbyteStreamApiMetadata(
            property_fields_by_stream=property_fields_by_stream,
            namespaces_by_name=namespaces_by_name,
        )

    def _build_sync_catalog(
        self,
        config_streams: List[Dict[str, Any]],
        stream_api_metadata: AirbyteStreamApiMetadata,
    ) -> SyncCatalogBuildResult:
        stream_refs = [
            AirbyteConfigStreamRef.model_validate(stream) for stream in config_streams
        ]
        queue_result = namespace_queues_for_catalog(
            stream_refs, stream_api_metadata.namespaces_by_name
        )
        # Mutate a copy so frozen NamespaceQueueResult.queues stays intact for callers.
        queues = {name: list(ns) for name, ns in queue_result.queues.items()}
        streams: List[AirbyteStreamConfig] = []
        for stream, stream_ref in zip(config_streams, stream_refs, strict=True):
            name = coerce_str(stream_ref.name)
            namespace = coerce_str(stream_ref.namespace)

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
                            stream, property_fields
                        ),
                    ),
                    config=self._build_stream_config(stream),
                )
            )

        return SyncCatalogBuildResult(
            catalog=AirbyteSyncCatalog(streams=streams),
            ambiguous=queue_result.ambiguous,
            positional=queue_result.positional,
        )

    def _build_stream_config(self, stream: Dict[str, Any]) -> AirbyteStreamSyncSettings:
        sync_mode = stream.get("syncMode", "")

        settings = AirbyteStreamSyncSettings(
            selected=True,
            sync_mode=sync_mode.split("_")[0] if sync_mode else "full_refresh",
            destination_sync_mode=(
                sync_mode.split("_")[1] if "_" in sync_mode else "overwrite"
            ),
            primary_key=stream.get("primaryKey") or [],
            cursor_field=stream.get("cursorField") or [],
        )

        optional_aliases = {
            "destinationNamespace": "destination_namespace",
            "aliasName": "alias_name",
            "selectedFields": "selected_fields",
            "fieldSelectionEnabled": "field_selection_enabled",
        }
        updates = {
            field_name: stream[key]
            for key, field_name in optional_aliases.items()
            if key in stream
        }
        if updates:
            return settings.model_copy(update=updates)
        return settings

    def _get_json_schema_for_stream(
        self,
        stream: Dict[str, Any],
        property_fields: Optional[List[PropertyFieldPath]] = None,
    ) -> Dict[str, Any]:
        # Schema sources in order of preference:
        #   1. propertyFields from `/streams` (Airbyte 1.8+, most accurate)
        #   2. jsonSchema embedded in the legacy configurations payload
        #   3. empty schema — column-level lineage will be dropped
        if property_fields:
            properties = {
                field_path.field_name: {"type": ["null", "string"]}
                for field_path in property_fields
                if field_path.field_name
            }
            if properties:
                return {"type": "object", "properties": properties}

        return stream.get("jsonSchema") or stream.get("json_schema") or {}

    def list_streams(
        self, source_id: Optional[str] = None, destination_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        # `/streams` is Airbyte 1.8+. Older versions 404 — callers should
        # treat that as "fall back to configurations.streams" instead of
        # propagating the error.
        self._check_auth_before_request()
        query_params = []
        if source_id:
            query_params.append(f"sourceId={source_id}")
        if destination_id:
            query_params.append(f"destinationId={destination_id}")

        endpoint = "/streams"
        if query_params:
            endpoint = f"{endpoint}?{'&'.join(query_params)}"

        response = self._make_request(endpoint)
        return response if isinstance(response, list) else response.get("streams", [])

    def get_job(self, job_id: str) -> Dict[str, Any]:
        self._check_auth_before_request()
        return self._make_request(f"/jobs/{job_id}")

    def list_tags(self, workspace_id: str) -> List[Dict[str, Any]]:
        self._check_auth_before_request()
        response = self._make_request(f"/tags?workspaceIds={workspace_id}")
        # Older Airbyte versions return tags under "tags".
        return response.get("data") or response.get("tags", [])


class AirbyteOSSClient(AirbyteBaseClient):
    def __init__(self, config: AirbyteClientConfig):
        super().__init__(config)

        if not config.host_port:
            raise ValueError("host_port is required for open_source deployment")

        # See https://docs.airbyte.com/developers/api-documentation
        self.base_url = f"{clean_uri(config.host_port)}/api/public/v1"
        self._setup_authentication()

    def _setup_authentication(self) -> None:
        # OAuth2 → API key/Bearer → basic auth → no-auth (in that order).
        if self.config.oauth2_client_id and self.config.oauth2_client_secret:
            self._setup_oauth_authentication()
        elif self.config.api_key:
            token = self.config.api_key.get_secret_value()
            self.session.headers.update({"Authorization": f"Bearer {token}"})
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
        # OSS only exposes client_credentials.
        self._request_oauth_token()

    def _request_oauth_token(self) -> None:
        if not self.config.oauth2_client_secret:
            raise ValueError("OAuth2 client secret is required")

        # See https://docs.airbyte.com/using-airbyte/configuring-api-access
        token_url = f"{self.base_url}/applications/token"

        token_data = {
            "client_id": self.config.oauth2_client_id,
            "client_secret": self.config.oauth2_client_secret.get_secret_value(),
            "grant_type": "client_credentials",
        }

        try:
            response = requests.post(
                token_url,
                data=token_data,
                headers={"Content-Type": CONTENT_TYPE_FORM_URLENCODED},
                timeout=self.config.request_timeout,
                verify=self.session.verify,
            )
            response.raise_for_status()
            token_response = response.json()
            access_token = token_response.get("access_token")

            if not access_token:
                raise ValueError("No access_token in OAuth response")

            self.session.headers.update({"Authorization": f"Bearer {access_token}"})

            expires_in = token_response.get("expires_in", DEFAULT_TOKEN_EXPIRY_SECONDS)
            self.token_expiry = time.time() + expires_in
        except requests.HTTPError as e:
            error_message = f"Failed to get OAuth2 token: HTTP {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get('error_description', error_details.get('message', e.response.text))}"
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
            "client_id": self.config.oauth2_client_id,
            "client_secret": self.config.oauth2_client_secret.get_secret_value(),
            "grant_type": grant_type.value,
        }

        if grant_type == OAuth2GrantType.REFRESH_TOKEN:
            if not self.config.oauth2_refresh_token:
                raise ValueError("OAuth2 refresh token is required")
            token_data["refresh_token"] = (
                self.config.oauth2_refresh_token.get_secret_value()
            )

        try:
            response = requests.post(
                self.token_url,
                data=token_data,
                headers={"Content-Type": CONTENT_TYPE_FORM_URLENCODED},
                timeout=self.config.request_timeout,
                verify=self.session.verify,
            )
            response.raise_for_status()

            response_data = response.json()
            self.access_token = response_data.get("access_token")
            expires_in = response_data.get("expires_in", DEFAULT_TOKEN_EXPIRY_SECONDS)
            self.token_expiry = time.time() + expires_in

            self.session.headers.update(
                {"Authorization": f"Bearer {self.access_token}"}
            )
        except requests.HTTPError as e:
            error_message = f"Failed to get OAuth2 token via {grant_type.value}: HTTP {e.response.status_code}"
            try:
                error_details = e.response.json()
                error_message += f" - {error_details.get('error_description', error_details.get('message', e.response.text))}"
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
        # Override the wire hook (not `_make_request`) so the HTTP→error
        # translation logic stays shared and only the token-refresh-on-401/403
        # behaviour differs between OSS and Cloud.
        try:
            return super()._do_get(url, params=params)
        except requests.HTTPError as e:
            if e.response.status_code not in (401, 403):
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
        # Concatenate explicitly: `urljoin` drops trailing path segments
        # when the base URL lacks a trailing slash, so customer-supplied
        # `cloud_api_url` values like "https://eu.example.com/api/v1" lose
        # their path prefix.
        return f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def _check_auth_before_request(self) -> None:
        self._check_token_expiry()

    def list_workspaces(
        self, pattern: Optional[AllowDenyPattern] = None
    ) -> List[AirbyteWorkspacePartial]:
        # Cloud restricts each set of credentials to a single workspace, so
        # this is a single GET rather than a paginated list.
        self._check_auth_before_request()
        try:
            workspace_data = self._make_request(f"/workspaces/{self.workspace_id}")
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
