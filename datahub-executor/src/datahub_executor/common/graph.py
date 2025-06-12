import logging
from typing import Any, Dict, Iterable, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ExecutionRequestKeyClass,
    ExecutionRequestResultClass,
    RemoteExecutorStatusClass,
)

from datahub_executor.common.constants import (
    CLI_EXECUTOR_ID,
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INDEX_THRESHOLD_MS,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_STATUS_DUPLICATE,
    DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME,
    DATAHUB_REMOTE_EXECUTOR_STATUS_ASPECT_NAME,
)
from datahub_executor.common.types import (
    ExecutionRequestStatus,
)
from datahub_executor.config import (
    DATAHUB_EXECUTOR_GRAPH_ES_BATCH_SIZE,
    DATAHUB_EXECUTOR_SERVER_CONFIG_REFRESH_INTERVAL,
    DATAHUB_GMS_TOKEN,
    DATAHUB_GMS_URL,
)

logger = logging.getLogger(__name__)


class DataHubExecutorGraph(DataHubGraph):
    """An extension of the DataHubGraph class that provides additional functionality for Assertion evaluation"""

    PAGINATION_ITERATIONS_WARNING_THRESHOLD = 1000

    CLIENT_CONFIG = DatahubClientConfig(
        server=DATAHUB_GMS_URL,
        # When token is not set, the client will automatically try to use
        # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
        token=DATAHUB_GMS_TOKEN,
        server_config_refresh_interval=DATAHUB_EXECUTOR_SERVER_CONFIG_REFRESH_INTERVAL,
    )

    def __init__(self) -> None:
        super().__init__(self.CLIENT_CONFIG)

    def cancel_ingestion_execution(
        self, ingestion_urn: str, execution_request_urn: str
    ) -> Dict[str, Any]:
        graph_query: str = """
            mutation cancelIngestionExecutionRequest($input: CancelIngestionExecutionRequestInput!) {
                cancelIngestionExecutionRequest(input: $input)
            }
        """
        variables = {
            "input": {
                "ingestionSourceUrn": ingestion_urn,
                "executionRequestUrn": execution_request_urn,
            },
        }
        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )
        return res["cancelIngestionExecutionRequest"]

    def set_execution_request_status(
        self, execution_request_urn: str, report: str, status: str, start_time: int = 0
    ) -> None:
        key_aspect = ExecutionRequestKeyClass(id=execution_request_urn)
        result_aspect = ExecutionRequestResultClass(
            status=status,
            startTimeMs=start_time,
            durationMs=None,
            report=report,
            structuredReport=None,
        )
        mcpw = MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            entityKeyAspect=key_aspect,
            aspect=result_aspect,
        )
        self.emit_mcp(mcpw, async_flag=False)

    def _get_aspect(self, entity: Dict, aspect: str, key: str = "value") -> Dict:
        return entity.get("aspects", {}).get(aspect, {}).get(key, {})

    def _entity_to_execution_request_status(
        self, entity: Dict
    ) -> ExecutionRequestStatus:
        key = self._get_aspect(entity, DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME)
        result = self._get_aspect(entity, DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME)
        input = self._get_aspect(entity, DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME)
        signal = self._get_aspect(entity, DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME)

        last_observed = self._get_aspect(
            entity, DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME, "systemMetadata"
        ).get("lastObserved", 0)

        ers = ExecutionRequestStatus.parse_obj(
            {
                "execution_request_urn": entity.get("urn"),
                "execution_request_id": key.get("id"),
                "executor_id": input.get(
                    "executorId", DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
                ),
                "ingestion_source_urn": input.get("source", {}).get("ingestionSource"),
                "status": result.get("status", "PENDING"),
                "last_observed": last_observed,
                "report": result.get("report", ""),
                "start_time": result.get("startTimeMs", 0),
                "request_time": input.get("requestedAt", 0),
                "raw_input_aspect": input,
                "raw_signal_aspect": signal,
            }
        )
        return ers

    def scroll_entities(self, entity: str, params: Dict) -> Iterable[Any]:
        headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        iteration = 0
        while True:
            url = f"{self.config.server}/openapi/v2/entity/{entity}"
            response = self._session.get(url, headers=headers, params=params)
            response.raise_for_status()

            json_resp = response.json()

            results = json_resp.get("results", [])
            for entry in results:
                yield entry

            scroll_id = json_resp.get("scrollId", None)
            if scroll_id is None:
                break
            params["scrollId"] = scroll_id

            iteration = iteration + 1
            if (iteration % self.PAGINATION_ITERATIONS_WARNING_THRESHOLD) == 0:
                logger.warning(
                    f"_scroll_entities: possible infinite loop; iterations: {iteration}"
                )

    def _get_execution_requests(
        self, overrides: Dict[str, Any]
    ) -> List[ExecutionRequestStatus]:
        params = {
            "aspectNames": [
                DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME,
            ],
            "count": "10",
            "sort": "requestTimeMs",
            "sortOrder": "ASCENDING",
            "systemMetadata": "true",
            "skipCache": "true",
        }
        params.update(overrides)
        retval: List[ExecutionRequestStatus] = []

        for entry in self.scroll_entities(
            entity=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME, params=params
        ):
            try:
                ers = self._entity_to_execution_request_status(entry)
                # Exclude tasks created by CLI
                if ers.executor_id != CLI_EXECUTOR_ID:
                    retval.append(ers)
            except Exception as e:
                logger.exception(f"Failed to parse execution request entry: {e}")

        return retval

    def get_recent_requests_for_sources(
        self, input: List
    ) -> Dict[str, List[ExecutionRequestStatus]]:
        retval: Dict[str, List[ExecutionRequestStatus]] = {}
        batch_size = DATAHUB_EXECUTOR_GRAPH_ES_BATCH_SIZE

        for i in range(0, len(input), batch_size):
            slice = input[i : i + batch_size]
            conditions = map(
                lambda x: '(ingestionSource:"{}" AND requestTimeMs: >={})'.format(*x),
                slice,
            )
            conditions_string = " OR ".join(conditions)
            params = {
                "query": "({}) AND requestTimeMs: >{} AND ((!(_exists_:executionResultStatus)) OR (!executionResultStatus:{}))".format(
                    conditions_string,
                    DATAHUB_EXECUTION_REQUEST_INDEX_THRESHOLD_MS,
                    DATAHUB_EXECUTION_REQUEST_STATUS_DUPLICATE,
                ),
            }
            entries = self._get_execution_requests(params)
            for entry in entries:
                retval[entry.ingestion_source_urn] = retval.get(
                    entry.ingestion_source_urn, []
                )
                retval[entry.ingestion_source_urn].append(entry)

        return retval

    def get_running_and_pending_ingestions(
        self,
    ) -> List[ExecutionRequestStatus]:
        params = {
            "query": "((!(_exists_:executionResultStatus)) AND requestTimeMs: >{}) OR (executionResultStatus:{})".format(
                DATAHUB_EXECUTION_REQUEST_INDEX_THRESHOLD_MS,
                DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
            ),
        }
        return self._get_execution_requests(params)

    def get_remote_executors(
        self, query: str = "*"
    ) -> Dict[str, RemoteExecutorStatusClass]:
        retval: Dict[str, RemoteExecutorStatusClass] = {}
        params = {
            "aspectNames": [
                DATAHUB_REMOTE_EXECUTOR_STATUS_ASPECT_NAME,
            ],
            "count": "10",
            "query": query,
            "systemMetadata": "false",
            "skipCache": "true",
            "includeSoftDelete": "true",
        }
        for entity in self.scroll_entities(
            entity=DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME, params=params
        ):
            urn = entity.get("urn")
            status_dict = (
                entity.get("aspects", {})
                .get(DATAHUB_REMOTE_EXECUTOR_STATUS_ASPECT_NAME, {})
                .get("value", {})
            )
            try:
                retval[urn] = RemoteExecutorStatusClass.from_obj(status_dict)
            except Exception as e:
                logger.error(
                    f"Failed to load RemoteExecutorStatus record with urn {urn}: {e}"
                )
        return retval
