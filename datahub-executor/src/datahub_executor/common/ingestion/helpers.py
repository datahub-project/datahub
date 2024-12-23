import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

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
    ExecutionRequestSignalClass,
    ExecutionRequestSourceClass,
    MetadataChangeLogClass,
)

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME,
    DATAHUB_EXECUTION_REQUEST_SQS_TRUNCATED_ASPECT_NAME,
    RUN_INGEST_TASK_NAME,
)
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.monitoring.metrics import (
    STATS_EXECUTION_FETCH_REQUESTS,
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


def setup_ingestion_executor(
    executor_instance_id: Optional[str] = None, executor_version: Optional[str] = None
) -> ReportingExecutor:
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
        executor_instance_id=executor_instance_id,
        executor_version=executor_version,
    )

    return ReportingExecutor(ingestion_executor_config)


def extract_execution_request(
    event: MetadataChangeLogClass,
    graph: Optional[DataHubGraph] = None,
) -> Optional[ExecutionRequest]:
    # If MCL delivered a truncated ExecutionRequest, fetch is from GMS
    if (
        event.aspectName is not None
        and event.aspectName == DATAHUB_EXECUTION_REQUEST_SQS_TRUNCATED_ASPECT_NAME
    ):
        if graph is None:
            logger.error(
                "Unable to fetch execution request: graph client is not provided"
            )
            return None

        if event.entityUrn is None:
            logger.error(
                "Unable to fetch execution request: entity urn is not provided"
            )
            return None

        ers = fetch_execution_requests(graph=graph, urns=[event.entityUrn])
        if len(ers) != 1:
            logger.error(f"Could not find ExecutionRequest: {event.entityUrn}")
            return None
        return ers[0]

    if not event.aspect or not event.entityKeyAspect:
        logger.error(
            f"Unable to parse ExecutionRequestInput: no aspect or entityKeyAspect; URN = {event.entityUrn}"
        )
        return None

    # Otherwise extract ExecutionRequest from the MCL event
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


def exec_id_to_urn(exec_id: str) -> str:
    return f"urn:li:{DATAHUB_EXECUTION_REQUEST_ENTITY_NAME}:{exec_id}"


@STATS_EXECUTION_FETCH_SIGNAL_REQUESTS.time()
def fetch_execution_signal_requests(
    graph: DataHubGraph, exec_ids: List[str]
) -> Dict[str, Any]:
    exec_id_urns = list(map(exec_id_to_urn, exec_ids))
    return graph.get_entities_v2(
        entity_name=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
        urns=exec_id_urns,
        aspects=[ExecutionRequestSignalClass.ASPECT_NAME],
    )


@STATS_EXECUTION_FETCH_REQUESTS.time()
def fetch_execution_requests(
    graph: DataHubGraph, urns: List[str]
) -> List[ExecutionRequest]:
    entities = graph.get_entities_v2(
        entity_name=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
        urns=urns,
        aspects=[
            DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME,
        ],
    )

    results: List[ExecutionRequest] = []
    for urn in urns:
        try:
            input_aspect = entities[urn][DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME][
                "value"
            ]
            key_aspect = entities[urn][DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME][
                "value"
            ]
            er = ExecutionRequest(
                executor_id=input_aspect["executorId"],
                exec_id=key_aspect["id"],
                name=input_aspect["task"],
                args=input_aspect["args"],
            )
            results.append(er)
        except Exception as e:
            logger.error(
                f"fetch_execution_requests: failed to parse entity aspect: {e}"
            )

    return results


def handle_ingestion_signal_requests(
    graph: DataHubGraph,
    ingestion_executor: ReportingExecutor,
) -> bool:
    ingestion_exec_ids = []
    if ingestion_executor.task_futures and hasattr(
        ingestion_executor.task_futures, "keys"
    ):
        for exec_id in ingestion_executor.task_futures.keys():
            task_future = ingestion_executor.task_futures[exec_id]
            if not task_future.done():
                ingestion_exec_ids.append(exec_id)

    # if we have an ingestion task running, this is the only time
    # we poll GMS for any signals related to these tasks.
    if len(ingestion_exec_ids) > 0:
        try:
            entities = fetch_execution_signal_requests(graph, ingestion_exec_ids)

            for exec_id in ingestion_exec_ids:
                signal_aspect = (
                    entities.get(exec_id_to_urn(exec_id), {})
                    .get(ExecutionRequestSignalClass.ASPECT_NAME, {})
                    .get("value", None)
                )

                if signal_aspect is None:
                    continue

                signal = ExecutionRequestSignalClass.from_obj(signal_aspect)
                signal_request = SignalRequest.parse_obj(
                    {
                        "exec_id": exec_id,
                        "executor_id": signal.executorId,
                        "signal": signal.signal,
                    }
                )
                ingestion_executor.signal(signal_request)

                STATS_INGESTION_HANDLER_CANCEL_REQUESTS.inc()
                logger.info(f"Got {signal.signal} signal for task {exec_id}")
            return True
        except Exception as e:
            STATS_EXECUTION_FETCH_SIGNAL_ERRORS.labels("Exception").inc()
            logger.error(f"Failed to fetch signal requests: {e}")
    return False


def emit_execution_request_input(input: ExecutionRequestInputClass) -> bool:
    exec_id = str(uuid.uuid4())

    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=ExecutionRequestKeyClass(id=exec_id),
        entityUrn=f"urn:li:dataHubExecutionRequest:{exec_id}",
        entityType="dataHubExecutionRequest",
        aspectName="dataHubExecutionRequestInput",
        aspect=input,
        changeType="UPSERT",
    )

    try:
        STATS_MCP_EMIT_EVENTS.inc()
        graph = create_datahub_graph()
        graph.emit_mcp(mcpw, async_flag=False)
        return True
    except Exception as e:
        STATS_MCP_EMIT_ERRORS.labels("GmsError").inc()
        logger.exception(
            f"An unknown error occurred when attempting to emit dataHubExecutionRequestInput - {e}"
        )
    return False


def build_execution_request_input_from_request(
    execution_request: ExecutionRequest,
) -> ExecutionRequestInputClass:
    # Construct the dataHubExecutionRequestInput aspect
    return ExecutionRequestInputClass(
        task=execution_request.name,
        args=execution_request.args,
        executorId=execution_request.executor_id,
        requestedAt=int(time.time() * 1000),
        source=ExecutionRequestSourceClass(
            type="SCHEDULED_INGESTION_SOURCE",
            ingestionSource=execution_request.args["urn"],
        ),
    )
