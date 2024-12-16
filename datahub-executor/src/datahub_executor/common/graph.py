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
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INDEX_THRESHOLD_MS,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_STATUS_DUPLICATE,
    DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
    DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME,
    DATAHUB_REMOTE_EXECUTOR_STATUS_ASPECT_NAME,
)
from datahub_executor.common.types import ExecutionRequestStatus
from datahub_executor.config import DATAHUB_GMS_TOKEN, DATAHUB_GMS_URL

logger = logging.getLogger(__name__)


class DataHubExecutorGraph(DataHubGraph):
    """An extension of the DataHubGraph class that provides additional functionality for Assertion evaluation"""

    PAGINATION_ITERATIONS_WARNING_THRESHOLD = 1000

    CLIENT_CONFIG = DatahubClientConfig(
        server=DATAHUB_GMS_URL,
        # When token is not set, the client will automatically try to use
        # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
        token=DATAHUB_GMS_TOKEN,
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

    def _entity_to_execution_request_status(
        self, entity: Dict
    ) -> ExecutionRequestStatus:
        result_aspect = (
            entity.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME, {})
            .get("value", {})
        )
        input_aspect = (
            entity.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME, {})
            .get("value", {})
        )
        key_aspect = (
            entity.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME, {})
            .get("value", {})
        )

        ingestion_source_urn = input_aspect.get("source", {}).get("ingestionSource")
        execution_request_id = key_aspect.get("id")
        execution_request_status = result_aspect.get("status", "PENDING")
        report = result_aspect.get("report", "")
        request_time = input_aspect.get("requestedAt", 0)
        start_time = result_aspect.get("startTimeMs", 0)

        last_observed = (
            entity.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME, {})
            .get("systemMetadata", {})
            .get("lastObserved", 0)
        )

        ers = ExecutionRequestStatus.parse_obj(
            {
                "execution_request_urn": entity.get("urn"),
                "execution_request_id": execution_request_id,
                "ingestion_source_urn": ingestion_source_urn,
                "status": execution_request_status,
                "last_observed": last_observed,
                "report": report,
                "start_time": start_time,
                "request_time": request_time,
                "raw_input_aspect": input_aspect,
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
            ers = self._entity_to_execution_request_status(entry)
            retval.append(ers)

        return retval

    def get_recent_requests_for_sources(
        self, input: List
    ) -> Dict[str, List[ExecutionRequestStatus]]:
        retval: Dict[str, List[ExecutionRequestStatus]] = {}
        batch_size = 10

        for i in range(0, len(input), batch_size):
            slice = input[i : i + batch_size]
            conditions = map(
                lambda x: (
                    '(ingestionSource:"{}" AND requestTimeMs: >={} AND '
                    "(((!(_exists_:executionResultStatus)) AND requestTimeMs: >{}) OR (!executionResultStatus:{})))"
                ).format(
                    *(
                        x
                        + [
                            DATAHUB_EXECUTION_REQUEST_INDEX_THRESHOLD_MS,
                            DATAHUB_EXECUTION_REQUEST_STATUS_DUPLICATE,
                        ]
                    )
                ),
                slice,
            )
            conditions_string = " OR ".join(conditions)
            params = {
                "query": "requestTimeMs: >{} AND ({})".format(
                    DATAHUB_EXECUTION_REQUEST_INDEX_THRESHOLD_MS,
                    conditions_string,
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
            retval[urn] = RemoteExecutorStatusClass.from_obj(status_dict)
        return retval
