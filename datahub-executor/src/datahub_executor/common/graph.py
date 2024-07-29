import logging
from typing import Any, Dict, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ExecutionRequestKeyClass,
    ExecutionRequestResultClass,
)

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING,
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

    def abort_execution_request(self, execution_request_urn: str, report: str) -> None:
        key_aspect = ExecutionRequestKeyClass(id=execution_request_urn)
        result_aspect = ExecutionRequestResultClass(
            status="ABORTED",
            startTimeMs=0,
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
        result_aspect = entity.get("aspects", {}).get(
            DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME, {}
        )
        input_aspect = entity.get("aspects", {}).get(
            DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME, {}
        )
        key_aspect = entity.get("aspects", {}).get(
            DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME, {}
        )

        ingestion_source_urn = (
            input_aspect.get("value", {}).get("source", {}).get("ingestionSource")
        )
        execution_request_id = key_aspect.get("value", {}).get("id")
        execution_request_status = result_aspect.get("value", {}).get("status")
        last_observed = result_aspect.get("systemMetadata", {}).get("lastObserved", 0)

        ers = ExecutionRequestStatus.parse_obj(
            {
                "execution_request_urn": entity.get("urn"),
                "execution_request_id": execution_request_id,
                "ingestion_source_urn": ingestion_source_urn,
                "status": execution_request_status,
                "last_observed": last_observed,
            }
        )
        return ers

    def get_running_ingestions(
        self,
    ) -> List[ExecutionRequestStatus]:
        headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {
            "aspectNames": [
                DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            ],
            "count": "10",
            "query": f"executionResultStatus:{DATAHUB_EXECUTION_REQUEST_STATUS_RUNNING}",
            "sort": "urn",
            "sortOrder": "ASCENDING",
            "systemMetadata": "true",
            "skipCache": "true",
        }

        retval: List[ExecutionRequestStatus] = []

        iteration = 0
        while True:
            url = f"{self.config.server}/openapi/v2/entity/{DATAHUB_EXECUTION_REQUEST_ENTITY_NAME}"
            response = self._session.get(url, headers=headers, params=params)
            response.raise_for_status()

            json_resp = response.json()

            results = json_resp.get("results", [])
            for entry in results:
                ers = self._entity_to_execution_request_status(entry)
                retval.append(ers)

            scroll_id = json_resp.get("scrollId", None)
            if scroll_id is None:
                break
            params["scrollId"] = scroll_id

            iteration = iteration + 1
            if (iteration % self.PAGINATION_ITERATIONS_WARNING_THRESHOLD) == 0:
                logger.warning(
                    f"get_running_ingestions: possible infinite loop; iterations: {iteration}"
                )

        return retval
