import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel, ValidationError
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.matillion_dpc.config import MatillionAPIConfig
from datahub.ingestion.source.matillion_dpc.constants import (
    API_ENDPOINT_ENVIRONMENTS,
    API_ENDPOINT_LINEAGE_EVENTS,
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
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
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
        self.session = self._create_session()

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
                HTTP_HEADER_AUTHORIZATION: f"Bearer {self.config.api_token.get_secret_value()}",
                HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_JSON,
            }
        )

        return session

    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        url = f"{self.config.get_base_url()}/{endpoint}"

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
                logger.warning(f"Failed to parse {entity_name}: {e}")
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
                logger.warning(f"Failed to parse {entity_name}: {e}")
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
        self, pipeline_name: str, limit: int = 10
    ) -> List[MatillionPipelineExecution]:
        return self._fetch_token_paginated_entities(
            API_ENDPOINT_PIPELINE_EXECUTIONS,
            MatillionPipelineExecution,
            "pipeline execution",
            pagination_params=TokenPaginationParams(limit=limit),
            additional_params={"pipelineName": pipeline_name},
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
