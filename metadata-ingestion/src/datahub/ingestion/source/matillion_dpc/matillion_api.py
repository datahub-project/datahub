import json
import logging
import time
from typing import Any, Dict, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel, ValidationError
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.matillion_dpc.config import MatillionAPIConfig
from datahub.ingestion.source.matillion_dpc.constants import (
    API_ENDPOINT_ARTIFACT_DETAILS,
    API_ENDPOINT_ENVIRONMENTS,
    API_ENDPOINT_LINEAGE_EVENTS,
    API_ENDPOINT_PIPELINE_EXECUTION_STEPS,
    API_ENDPOINT_PIPELINE_EXECUTIONS,
    API_ENDPOINT_PIPELINES,
    API_ENDPOINT_PROJECTS,
    API_ENDPOINT_SCHEDULES,
    API_ENDPOINT_STREAMING_PIPELINES,
    API_MAX_PAGE_SIZE,
    API_RESPONSE_FIELD_RESULTS,
    HTTP_CONTENT_TYPE_JSON,
    HTTP_HEADER_AUTHORIZATION,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_METHOD_GET,
    HTTP_PROTOCOL_HTTP,
    HTTP_PROTOCOL_HTTPS,
    HTTP_RETRY_ALLOWED_METHODS,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
    MATILLION_OAUTH_TOKEN_URL,
    OAUTH_AUDIENCE,
    OAUTH_GRANT_TYPE,
    OAUTH_TOKEN_EXPIRY_SECONDS,
    OAUTH_TOKEN_REFRESH_BUFFER_SECONDS,
)
from datahub.ingestion.source.matillion_dpc.models import (
    ArtifactDetailsWithAssets,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionPipelineExecutionStepResult,
    MatillionProject,
    MatillionSchedule,
    MatillionStreamingPipeline,
    PaginationParams,
    TokenPaginationParams,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class MatillionAPIClient:
    def __init__(self, config: MatillionAPIConfig):
        self.config = config
        self._token_expiry_time: Optional[float] = None
        self.session = self._create_session()

        logger.info("Using OAuth2 client credentials for authentication")
        self._generate_oauth_token()

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        retry_strategy = Retry(
            total=HTTP_RETRY_MAX_ATTEMPTS,
            backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
            status_forcelist=HTTP_RETRY_STATUS_CODES,
            allowed_methods=HTTP_RETRY_ALLOWED_METHODS,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount(HTTP_PROTOCOL_HTTP, adapter)
        session.mount(HTTP_PROTOCOL_HTTPS, adapter)

        session.headers.update(
            {
                HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_JSON,
            }
        )

        return session

    def _generate_oauth_token(self) -> None:
        """Generate OAuth2 access token using client credentials"""
        data = {
            "grant_type": OAUTH_GRANT_TYPE,
            "client_id": self.config.client_id.get_secret_value(),
            "client_secret": self.config.client_secret.get_secret_value(),
            "audience": OAUTH_AUDIENCE,
        }

        oauth_token_url = (
            self.config.custom_oauth_token_url or MATILLION_OAUTH_TOKEN_URL
        )

        logger.debug(f"Requesting OAuth2 access token from {oauth_token_url}")
        response = requests.post(
            oauth_token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=data,
            timeout=self.config.request_timeout_sec,
        )
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError("No access_token in OAuth2 response")

        self.session.headers.update(
            {
                HTTP_HEADER_AUTHORIZATION: f"Bearer {access_token}",
            }
        )

        self._token_expiry_time = time.time() + OAUTH_TOKEN_EXPIRY_SECONDS
        expiry_minutes = OAUTH_TOKEN_EXPIRY_SECONDS / 60
        refresh_minutes = (
            OAUTH_TOKEN_EXPIRY_SECONDS - OAUTH_TOKEN_REFRESH_BUFFER_SECONDS
        ) / 60
        logger.info(
            f"Successfully obtained OAuth2 access token "
            f"(expires in {expiry_minutes:.0f} minutes, will refresh after {refresh_minutes:.0f} minutes)"
        )

    def _should_refresh_token(self) -> bool:
        """Check if OAuth2 token should be refreshed"""
        if self._token_expiry_time is None:
            return False

        time_until_expiry = self._token_expiry_time - time.time()
        should_refresh = time_until_expiry < OAUTH_TOKEN_REFRESH_BUFFER_SECONDS

        if should_refresh:
            logger.debug(
                f"Token expires in {int(time_until_expiry)} seconds, refreshing (buffer: {OAUTH_TOKEN_REFRESH_BUFFER_SECONDS}s)"
            )

        return should_refresh

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid OAuth2 token, refreshing if necessary"""
        if self._should_refresh_token():
            logger.info("OAuth2 token expiring soon, refreshing proactively...")
            self._generate_oauth_token()
            logger.info("OAuth2 token refreshed successfully")

    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        url = f"{self.config.get_base_url()}/{endpoint}"

        self._ensure_valid_token()

        response = self.session.request(
            method,
            url,
            timeout=self.config.request_timeout_sec,
            **kwargs,
        )

        if response.status_code == 401:
            logger.warning(
                "Received 401 Unauthorized, refreshing OAuth2 token and retrying..."
            )
            self._generate_oauth_token()
            response = self.session.request(
                method,
                url,
                timeout=self.config.request_timeout_sec,
                **kwargs,
            )

        response.raise_for_status()
        return response.json()

    def _make_paginated_request(
        self,
        endpoint: str,
        pagination_params: Optional[PaginationParams] = None,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        all_items = []
        params = pagination_params or PaginationParams()

        request_params = additional_params.copy() if additional_params else {}
        request_params["size"] = params.size
        request_params["page"] = params.page

        while True:
            logger.debug(
                f"Fetching {endpoint} with page={params.page}, size={params.size}"
            )

            response = self._make_request(
                HTTP_METHOD_GET, endpoint, params=request_params
            )

            items = response.get(API_RESPONSE_FIELD_RESULTS, [])
            all_items.extend(items)

            logger.debug(
                f"Fetched {len(items)} items from {endpoint} (total so far: {len(all_items)})"
            )

            current_page = response.get("page", params.page)
            total = response.get("total", 0)
            size = response.get("size", params.size)
            total_pages = (
                (total + size - 1) // size if size > 0 else 0
            )  # Ceiling division

            if current_page + 1 >= total_pages or len(items) == 0:
                logger.info(
                    f"Finished fetching from {endpoint}: total items = {len(all_items)}"
                )
                break

            params.page += 1
            request_params["page"] = params.page

        return all_items

    def _make_token_paginated_request(
        self,
        endpoint: str,
        pagination_params: Optional[TokenPaginationParams] = None,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        all_items = []
        params = pagination_params or TokenPaginationParams()

        request_params = additional_params.copy() if additional_params else {}
        request_params["limit"] = params.limit

        while True:
            if params.pagination_token:
                request_params["paginationToken"] = params.pagination_token
                logger.debug(
                    f"Fetching {endpoint} with paginationToken, limit={params.limit}"
                )
            else:
                request_params.pop("paginationToken", None)
                logger.debug(f"Fetching {endpoint} with limit={params.limit}")

            response = self._make_request(
                HTTP_METHOD_GET, endpoint, params=request_params
            )

            items = response.get("results", [])
            all_items.extend(items)

            logger.debug(
                f"Fetched {len(items)} items from {endpoint} (total so far: {len(all_items)})"
            )

            next_token = response.get("more")
            if not next_token or len(items) == 0:
                logger.info(
                    f"Finished fetching from {endpoint}: total items = {len(all_items)}"
                )
                break

            params.pagination_token = next_token

        return all_items

    def _fetch_entities(
        self,
        endpoint: str,
        model_class: Type[T],
        entity_name: str,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> List[T]:
        response_data = self._make_paginated_request(
            endpoint, additional_params=additional_params
        )
        entities = []

        for item in response_data:
            try:
                entity = model_class.model_validate(item)
                entities.append(entity)
            except ValidationError as e:
                item_preview = json.dumps(item)[:500]
                logger.warning(
                    f"Failed to parse {entity_name}: {e}. Item data: {item_preview}..."
                )
                continue

        logger.info(f"Successfully fetched {len(entities)} {entity_name}s")
        return entities

    def _fetch_token_paginated_entities(
        self,
        endpoint: str,
        model_class: Type[T],
        entity_name: str,
        pagination_params: Optional[TokenPaginationParams] = None,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> List[T]:
        response_data = self._make_token_paginated_request(
            endpoint,
            pagination_params=pagination_params,
            additional_params=additional_params,
        )
        entities = []

        for item in response_data:
            try:
                entity = model_class.model_validate(item)
                entities.append(entity)
            except ValidationError as e:
                item_preview = json.dumps(item)[:500]
                logger.warning(
                    f"Failed to parse {entity_name}: {e}. Item data: {item_preview}..."
                )
                continue

        return entities

    def get_projects(self) -> List[MatillionProject]:
        return self._fetch_entities(API_ENDPOINT_PROJECTS, MatillionProject, "project")

    def get_environments(self, project_id: str) -> List[MatillionEnvironment]:
        endpoint = API_ENDPOINT_ENVIRONMENTS.format(projectId=project_id)
        return self._fetch_entities(endpoint, MatillionEnvironment, "environment")

    def get_pipelines(
        self, project_id: str, environment_name: str
    ) -> List[MatillionPipeline]:
        endpoint = API_ENDPOINT_PIPELINES.format(projectId=project_id)
        return self._fetch_entities(
            endpoint,
            MatillionPipeline,
            "pipeline",
            additional_params={"environmentName": environment_name},
        )

    def get_pipeline_executions(
        self, pipeline_name: Optional[str] = None, limit: int = 10
    ) -> List[MatillionPipelineExecution]:
        additional_params = {}
        if pipeline_name:
            additional_params["pipelineName"] = pipeline_name

        return self._fetch_token_paginated_entities(
            API_ENDPOINT_PIPELINE_EXECUTIONS,
            MatillionPipelineExecution,
            "pipeline execution",
            pagination_params=TokenPaginationParams(limit=limit),
            additional_params=additional_params,
        )

    def get_schedules(self, project_id: str) -> List[MatillionSchedule]:
        endpoint = API_ENDPOINT_SCHEDULES.format(projectId=project_id)
        return self._fetch_entities(endpoint, MatillionSchedule, "schedule")

    def get_streaming_pipelines(
        self, project_id: str
    ) -> List[MatillionStreamingPipeline]:
        endpoint = API_ENDPOINT_STREAMING_PIPELINES.format(projectId=project_id)
        return self._fetch_entities(
            endpoint, MatillionStreamingPipeline, "streaming pipeline"
        )

    def get_pipeline_execution_steps(
        self, project_id: str, pipeline_execution_id: str
    ) -> List[MatillionPipelineExecutionStepResult]:
        endpoint = API_ENDPOINT_PIPELINE_EXECUTION_STEPS.format(
            projectId=project_id, pipelineExecutionId=pipeline_execution_id
        )
        return self._fetch_entities(
            endpoint, MatillionPipelineExecutionStepResult, "pipeline execution step"
        )

    def get_artifact_details(
        self, project_id: str, environment_name: str
    ) -> Optional[ArtifactDetailsWithAssets]:
        endpoint = API_ENDPOINT_ARTIFACT_DETAILS.format(projectId=project_id)
        try:
            response = self._make_request(
                HTTP_METHOD_GET,
                endpoint,
                params={"environmentName": environment_name},
            )
            if not response:
                return None
            return ArtifactDetailsWithAssets.model_validate(response)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(
                    f"No artifact details found for project {project_id} environment {environment_name}"
                )
                return None
            raise
        except (ValidationError, ValueError) as e:
            logger.warning(
                f"Failed to parse artifact details for project {project_id} environment {environment_name}: {e}"
            )
            return None
        except Exception as e:
            logger.warning(
                f"Failed to fetch artifact details for project {project_id} environment {environment_name}: {e}"
            )
            return None

    def get_lineage_events(
        self,
        generated_from: str,
        generated_before: str,
        page: int = 0,
        size: int = API_MAX_PAGE_SIZE,
    ) -> List[Dict]:
        params = {
            "generatedFrom": generated_from,
            "generatedBefore": generated_before,
            "page": page,
            "size": min(size, API_MAX_PAGE_SIZE),
        }

        try:
            response_data = self._make_request(
                method=HTTP_METHOD_GET,
                endpoint=API_ENDPOINT_LINEAGE_EVENTS,
                params=params,
            )

            results = response_data.get(API_RESPONSE_FIELD_RESULTS, [])

            logger.info(
                f"Fetched {len(results)} lineage events (page {page}, "
                f"from {generated_from} to {generated_before})"
            )

            return results

        except HTTPError as e:
            if e.response.status_code == 404:
                logger.info(
                    f"No lineage events found for date range {generated_from} to {generated_before}"
                )
                return []
            raise
