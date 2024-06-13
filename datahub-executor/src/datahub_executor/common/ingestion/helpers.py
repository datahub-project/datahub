import json
import logging
import time
import uuid
from typing import List, Optional

from acryl.executor.execution.reporting_executor import (
    ReportingExecutor,
    ReportingExecutorConfig,
)
from acryl.executor.execution.task import TaskConfig
from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.request.signal_request import SignalRequest
from acryl.executor.secret.datahub_secret_store import DataHubSecretStoreConfig
from acryl.executor.secret.secret_store import SecretStoreConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ExecutionRequestInputClass,
    ExecutionRequestKeyClass,
    ExecutionRequestSourceClass,
    MetadataChangeLogClass,
)

from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_GET_SIGNAL_REQUEST_LIST_QUERY,
)
from datahub_executor.common.constants import RUN_INGEST_TASK_NAME
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.monitoring.metrics import (
    STATS_EXECUTION_FETCH_SIGNAL_ERRORS,
    STATS_EXECUTION_FETCH_SIGNAL_REQUESTS,
    STATS_INGESTION_HANDLER_CANCEL_REQUESTS,
    STATS_MCP_EMIT_ERRORS,
    STATS_MCP_EMIT_EVENTS,
)
from datahub_executor.config import (
    DATAHUB_EXECUTOR_WORKER_ID,
    DATAHUB_GMS_TOKEN,
    DATAHUB_GMS_URL,
)

logger = logging.getLogger(__name__)


def setup_ingestion_executor() -> ReportingExecutor:
    # Build default task config
    ingest_task_config = TaskConfig(
        name=RUN_INGEST_TASK_NAME,
        type="acryl.executor.execution.sub_process_ingestion_task.SubProcessIngestionTask",
        configs=dict({}),
    )
    test_connection_task_config = TaskConfig(
        name="TEST_CONNECTION",
        type="acryl.executor.execution.sub_process_test_connection_task.SubProcessTestConnectionTask",
        configs=dict({}),
    )

    # Build default executor config
    ingestion_executor_config = ReportingExecutorConfig(
        id=DATAHUB_EXECUTOR_WORKER_ID,
        task_configs=[ingest_task_config, test_connection_task_config],
        secret_stores=[
            SecretStoreConfig(type="env", config=dict({})),
            SecretStoreConfig(
                type="datahub",
                config=DataHubSecretStoreConfig(
                    graph_client_config=DatahubClientConfig(
                        server=DATAHUB_GMS_URL,
                        token=DATAHUB_GMS_TOKEN,
                    )
                ),
            ),
        ],
        graph_client_config=DatahubClientConfig(
            server=DATAHUB_GMS_URL, token=DATAHUB_GMS_TOKEN
        ),
    )

    return ReportingExecutor(ingestion_executor_config)


def extract_execution_request(
    event: MetadataChangeLogClass,
) -> Optional[ExecutionRequest]:
    if not event.aspect or not event.entityKeyAspect:
        logger.error(
            f"Unable to parse Execution Request Input, no aspect or entityKeyAspect {event.entityUrn}.."
        )
        return None

    aspect_dict = json.loads(event.aspect.value)
    entity_key_dict = json.loads(event.entityKeyAspect.value)

    if not aspect_dict:
        logger.error(f"Invalid Aspect, {event}")
        return None

    if not entity_key_dict:
        logger.error(f"Invalid entityKeyAspect, {event}")
        return None

    if (
        "executorId" in aspect_dict
        and not aspect_dict["executorId"] == DATAHUB_EXECUTOR_WORKER_ID
    ):
        logger.error(
            f"Ignoring ExecutionRequest for non-configured executorId {aspect_dict['executorId']}"
        )
        return None

    try:
        execution_request = ExecutionRequest(
            executor_id=aspect_dict["executorId"],
            exec_id=entity_key_dict["id"],
            name=aspect_dict["task"],
            args=aspect_dict["args"],
        )
    except Exception as e:
        logger.error(f"Error parsing Execution Request {e}")
        return None

    return execution_request


@STATS_EXECUTION_FETCH_SIGNAL_REQUESTS.time()
def fetch_execution_signal_requests(
    graph: DataHubGraph,
    ingestion_exec_ids: List[str],
) -> List[SignalRequest]:
    # convert the exec_id strings to dataHubExecutionRequest urns
    ingestion_entity_urns = [
        f"urn:li:dataHubExecutionRequest:{exec_id}" for exec_id in ingestion_exec_ids
    ]

    # hit the GMS endpoint to see if any Signal Requests exists on these urns.
    result = graph.execute_graphql(
        GRAPHQL_GET_SIGNAL_REQUEST_LIST_QUERY,
        variables={"input": {"urns": ingestion_entity_urns}},
    )

    if "error" in result and result["error"] is not None:
        STATS_EXECUTION_FETCH_SIGNAL_ERRORS.labels("GmsError").inc()
        logger.error(
            f"Received error while fetching signal requests from GMS! {result.get('error')}"
        )
        return []

    if (
        "listSignalRequests" not in result
        or "signalRequests" not in result["listSignalRequests"]
    ):
        STATS_EXECUTION_FETCH_SIGNAL_ERRORS.labels("IncompleteResults").inc()
        logger.error(
            "Found incomplete search results when fetching signal requests from GMS!"
        )
        return []

    signal_requests = []
    for signal_request in result["listSignalRequests"]["signalRequests"]:
        try:
            signal = SignalRequest.parse_obj(
                {
                    "exec_id": signal_request["execId"],
                    "executor_id": signal_request["executorId"],
                    "signal": signal_request["signal"],
                }
            )
        except Exception:
            STATS_EXECUTION_FETCH_SIGNAL_ERRORS.labels("ParseError").inc()
            logger.error(
                f"Failed to convert Signal Request object to Python object. {signal_request}"
            )
        else:
            # convert urns back to exec_id to process the signal request.
            signal.exec_id = signal.exec_id.replace(
                "urn:li:dataHubExecutionRequest:", ""
            )
            signal_requests.append(signal)

    return signal_requests


def handle_ingestion_signal_requests(
    graph: DataHubGraph,
    ingestion_executor: ReportingExecutor,
) -> None:
    ingestion_exec_ids = []
    if ingestion_executor.task_futures:
        for exec_id in ingestion_executor.task_futures.keys():
            task_future = ingestion_executor.task_futures[exec_id]
            if not task_future.done():
                ingestion_exec_ids.append(exec_id)

    # if we have an ingestion task running, this is the only time
    # we poll GMS for any signals related to these tasks.
    if len(ingestion_exec_ids) > 0:
        signal_requests = fetch_execution_signal_requests(graph, ingestion_exec_ids)
        for signal_request in signal_requests:
            STATS_INGESTION_HANDLER_CANCEL_REQUESTS.inc()
            logger.info(
                f"Got {signal_request.exec_id} signal for task {signal_request.exec_id}"
            )
            ingestion_executor.signal(signal_request)


def emit_execution_request_input(
    execution_request: ExecutionRequest,
) -> None:
    # before actually running, we generate a unique exec_id so the executor downstream don't complain
    exec_id = str(uuid.uuid4())

    # Construct the dataHubExecutionRequestInput aspect
    execution_input_aspect = ExecutionRequestInputClass(
        task=execution_request.name,
        args=execution_request.args,
        executorId=execution_request.executor_id,
        requestedAt=int(time.time() * 1000),
        source=ExecutionRequestSourceClass(
            type="SCHEDULED_INGESTION_SOURCE",
            ingestionSource=execution_request.args["urn"],
        ),
    )
    # Emit the dataHubExecutionRequestInput aspect
    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=ExecutionRequestKeyClass(id=exec_id),
        entityUrn=f"urn:li:dataHubExecutionRequest:{exec_id}",
        entityType="dataHubExecutionRequest",
        aspectName="dataHubExecutionRequestInput",
        aspect=execution_input_aspect,
        changeType="UPSERT",
    )

    try:
        STATS_MCP_EMIT_EVENTS.inc()
        graph = create_datahub_graph()
        graph.emit_mcp(mcpw)
    except Exception as e:
        STATS_MCP_EMIT_ERRORS.labels("GmsError").inc()
        logger.exception(
            f"An unknown error occurred when attempting to emit dataHubExecutionRequestInput - {e}"
        )
