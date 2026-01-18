import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel, ValidationError
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.matillion.config import MatillionAPIConfig
from datahub.ingestion.source.matillion.constants import (
    API_ENDPOINT_AGENTS,
    API_ENDPOINT_AUDIT_EVENTS,
    API_ENDPOINT_CONNECTIONS,
    API_ENDPOINT_CONNECTORS,
    API_ENDPOINT_CONSUMPTION,
    API_ENDPOINT_ENVIRONMENTS,
    API_ENDPOINT_PIPELINE_EXECUTIONS,
    API_ENDPOINT_PIPELINES,
    API_ENDPOINT_PROJECTS,
    API_ENDPOINT_REPOSITORIES,
    API_ENDPOINT_SCHEDULES,
    API_ENDPOINT_STREAMING_PIPELINES,
    API_PAGINATION_DEFAULT_PAGE,
    API_PAGINATION_DEFAULT_SIZE,
    API_RESPONSE_FIELD_CONTENT,
    ENTITY_NAME_AGENT,
    ENTITY_NAME_CONNECTION,
    ENTITY_NAME_ENVIRONMENT,
    ENTITY_NAME_LINEAGE,
    ENTITY_NAME_PIPELINE,
    ENTITY_NAME_PIPELINE_EXECUTION,
    ENTITY_NAME_PROJECT,
    ENTITY_NAME_REPOSITORY,
    ENTITY_NAME_SCHEDULE,
    ENTITY_NAME_STREAMING_PIPELINE,
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
from datahub.ingestion.source.matillion.models import (
    MatillionAgent,
    MatillionAuditEvent,
    MatillionConnection,
    MatillionConnector,
    MatillionConsumption,
    MatillionEnvironment,
    MatillionLineageGraph,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
    MatillionRepository,
    MatillionSchedule,
    MatillionStreamingPipeline,
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

        try:
            response = self.session.request(
                method,
                url,
                timeout=self.config.request_timeout_sec,
                **kwargs,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {e}")
            raise

    def _make_paginated_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        all_items = []
        page = API_PAGINATION_DEFAULT_PAGE
        size = (
            params.get("size", API_PAGINATION_DEFAULT_SIZE)
            if params
            else API_PAGINATION_DEFAULT_SIZE
        )

        request_params = params.copy() if params else {}
        request_params["size"] = size

        while True:
            request_params["page"] = page

            logger.debug(f"Fetching {endpoint} with page={page}, size={size}")

            response = self._make_request(
                HTTP_METHOD_GET, endpoint, params=request_params
            )

            items = response.get(API_RESPONSE_FIELD_CONTENT, [])
            all_items.extend(items)

            logger.debug(
                f"Fetched {len(items)} items from {endpoint} (total so far: {len(all_items)})"
            )

            current_page = response.get("number", page)
            total_pages = response.get("totalPages", 0)

            if current_page + 1 >= total_pages or len(items) == 0:
                logger.info(
                    f"Finished fetching from {endpoint}: total items = {len(all_items)}"
                )
                break

            page += 1

        return all_items

    def _fetch_entities(
        self, endpoint: str, model_class: Type[T], entity_name: str
    ) -> List[T]:
        response_data = self._make_paginated_request(endpoint)
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

    def _fetch_entity_by_id(
        self, endpoint: str, entity_id: str, model_class: Type[T], entity_name: str
    ) -> Optional[T]:
        try:
            response = self._make_request(HTTP_METHOD_GET, f"{endpoint}/{entity_id}")
            return model_class.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse {entity_name} {entity_id}: {e}")
            return None

    def get_projects(self) -> List[MatillionProject]:
        return self._fetch_entities(
            API_ENDPOINT_PROJECTS, MatillionProject, ENTITY_NAME_PROJECT
        )

    def get_project_by_id(self, project_id: str) -> Optional[MatillionProject]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_PROJECTS, project_id, MatillionProject, ENTITY_NAME_PROJECT
        )

    def get_environments(
        self, project_id: Optional[str] = None
    ) -> List[MatillionEnvironment]:
        endpoint = API_ENDPOINT_ENVIRONMENTS
        params = {}
        if project_id:
            params["projectId"] = project_id

        response_data = self._make_paginated_request(endpoint, params)
        environments = []

        for item in response_data:
            try:
                env = MatillionEnvironment.model_validate(item)
                environments.append(env)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_ENVIRONMENT}: {e}")
                continue

        logger.info(
            f"Successfully fetched {len(environments)} {ENTITY_NAME_ENVIRONMENT}s"
        )
        return environments

    def get_pipelines(
        self, project_id: Optional[str] = None
    ) -> List[MatillionPipeline]:
        endpoint = API_ENDPOINT_PIPELINES
        params = {}
        if project_id:
            params["projectId"] = project_id

        response_data = self._make_paginated_request(endpoint, params)
        pipelines = []

        for item in response_data:
            try:
                pipeline = MatillionPipeline.model_validate(item)
                pipelines.append(pipeline)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_PIPELINE}: {e}")
                continue

        logger.info(f"Successfully fetched {len(pipelines)} {ENTITY_NAME_PIPELINE}s")
        return pipelines

    def get_pipeline_by_id(self, pipeline_id: str) -> Optional[MatillionPipeline]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_PIPELINES, pipeline_id, MatillionPipeline, ENTITY_NAME_PIPELINE
        )

    def get_connections(
        self, project_id: Optional[str] = None
    ) -> List[MatillionConnection]:
        endpoint = API_ENDPOINT_CONNECTIONS
        params = {}
        if project_id:
            params["projectId"] = project_id

        response_data = self._make_paginated_request(endpoint, params)
        connections = []

        for item in response_data:
            try:
                connection = MatillionConnection.model_validate(item)
                connections.append(connection)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_CONNECTION}: {e}")
                continue

        logger.info(
            f"Successfully fetched {len(connections)} {ENTITY_NAME_CONNECTION}s"
        )
        return connections

    def get_agents(self) -> List[MatillionAgent]:
        return self._fetch_entities(
            API_ENDPOINT_AGENTS, MatillionAgent, ENTITY_NAME_AGENT
        )

    def get_pipeline_executions(
        self, pipeline_id: str, limit: int = 10
    ) -> List[MatillionPipelineExecution]:
        params = {"pipelineId": pipeline_id, "size": limit}
        response_data = self._make_paginated_request(
            API_ENDPOINT_PIPELINE_EXECUTIONS, params
        )
        executions = []

        for item in response_data:
            try:
                execution = MatillionPipelineExecution.model_validate(item)
                executions.append(execution)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_PIPELINE_EXECUTION}: {e}")
                continue

        return executions

    def get_schedules(
        self, pipeline_id: Optional[str] = None
    ) -> List[MatillionSchedule]:
        endpoint = API_ENDPOINT_SCHEDULES
        params = {}
        if pipeline_id:
            params["pipelineId"] = pipeline_id

        response_data = self._make_paginated_request(endpoint, params)
        schedules = []

        for item in response_data:
            try:
                schedule = MatillionSchedule.model_validate(item)
                schedules.append(schedule)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_SCHEDULE}: {e}")
                continue

        logger.debug(f"Successfully fetched {len(schedules)} {ENTITY_NAME_SCHEDULE}s")
        return schedules

    def get_repository_by_id(self, repository_id: str) -> Optional[MatillionRepository]:
        try:
            response = self._make_request(
                HTTP_METHOD_GET, f"{API_ENDPOINT_REPOSITORIES}/{repository_id}"
            )
            return MatillionRepository.model_validate(response)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Repository {repository_id} not found")
                return None
            raise
        except ValidationError as e:
            logger.warning(
                f"Failed to parse {ENTITY_NAME_REPOSITORY} {repository_id}: {e}"
            )
            return None

    def get_repositories(
        self, project_id: Optional[str] = None
    ) -> List[MatillionRepository]:
        endpoint = API_ENDPOINT_REPOSITORIES
        params = {}
        if project_id:
            params["projectId"] = project_id

        response_data = self._make_paginated_request(endpoint, params)
        repositories = []

        for item in response_data:
            try:
                repo = MatillionRepository.model_validate(item)
                repositories.append(repo)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_REPOSITORY}: {e}")
                continue

        logger.info(
            f"Successfully fetched {len(repositories)} {ENTITY_NAME_REPOSITORY}s"
        )
        return repositories

    def get_streaming_pipelines(
        self, project_id: Optional[str] = None, environment_id: Optional[str] = None
    ) -> List[MatillionStreamingPipeline]:
        endpoint = API_ENDPOINT_STREAMING_PIPELINES
        params = {}
        if project_id:
            params["projectId"] = project_id
        if environment_id:
            params["environmentId"] = environment_id

        response_data = self._make_paginated_request(endpoint, params)
        streaming_pipelines = []

        for item in response_data:
            try:
                streaming_pipeline = MatillionStreamingPipeline.model_validate(item)
                streaming_pipelines.append(streaming_pipeline)
            except ValidationError as e:
                logger.warning(f"Failed to parse {ENTITY_NAME_STREAMING_PIPELINE}: {e}")
                continue

        logger.info(
            f"Successfully fetched {len(streaming_pipelines)} {ENTITY_NAME_STREAMING_PIPELINE}s"
        )
        return streaming_pipelines

    def get_pipeline_lineage(self, pipeline_id: str) -> Optional[MatillionLineageGraph]:
        try:
            response = self._make_request(
                HTTP_METHOD_GET, f"{API_ENDPOINT_PIPELINES}/{pipeline_id}/lineage"
            )
            return MatillionLineageGraph.model_validate(response)
        except HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"No lineage data found for pipeline {pipeline_id}")
                return None
            raise
        except ValidationError as e:
            logger.warning(
                f"Failed to parse {ENTITY_NAME_LINEAGE} for pipeline {pipeline_id}: {e}"
            )
            return None

    def get_consumption(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[MatillionConsumption]:
        endpoint = API_ENDPOINT_CONSUMPTION
        params = {}
        if project_id:
            params["projectId"] = project_id
        if pipeline_id:
            params["pipelineId"] = pipeline_id
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date

        response_data = self._make_paginated_request(endpoint, params)
        consumption_records = []

        for item in response_data:
            try:
                consumption = MatillionConsumption.model_validate(item)
                consumption_records.append(consumption)
            except ValidationError as e:
                logger.warning(f"Failed to parse consumption data: {e}")
                continue

        logger.debug(
            f"Successfully fetched {len(consumption_records)} consumption records"
        )
        return consumption_records

    def get_audit_events(
        self,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[MatillionAuditEvent]:
        endpoint = API_ENDPOINT_AUDIT_EVENTS
        params = {}
        if resource_type:
            params["resourceType"] = resource_type
        if resource_id:
            params["resourceId"] = resource_id
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date

        response_data = self._make_paginated_request(endpoint, params)
        audit_events = []

        for item in response_data:
            try:
                event = MatillionAuditEvent.model_validate(item)
                audit_events.append(event)
            except ValidationError as e:
                logger.warning(f"Failed to parse audit event: {e}")
                continue

        logger.debug(f"Successfully fetched {len(audit_events)} audit events")
        return audit_events

    def get_connectors(self) -> List[MatillionConnector]:
        endpoint = API_ENDPOINT_CONNECTORS
        response_data = self._make_paginated_request(endpoint, {})
        connectors = []

        for item in response_data:
            try:
                connector = MatillionConnector.model_validate(item)
                connectors.append(connector)
            except ValidationError as e:
                logger.warning(f"Failed to parse connector: {e}")
                continue

        logger.debug(f"Successfully fetched {len(connectors)} connectors")
        return connectors
