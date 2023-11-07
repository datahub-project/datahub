import json
import logging
from typing import List, Optional, Tuple

from acryl.executor.execution.reporting_executor import (
    ReportingExecutor,
    ReportingExecutorConfig,
)
from acryl.executor.execution.task import TaskConfig
from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.request.signal_request import SignalRequest
from acryl.executor.secret.datahub_secret_store import DataHubSecretStoreConfig
from acryl.executor.secret.secret_store import SecretStoreConfig
from celery import Celery
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata.schema_classes import MetadataChangeLogClass

from datahub_monitors.common.graph import DataHubAssertionGraph
from datahub_monitors.config import DATAHUB_SERVER, EXECUTOR_ID
from datahub_monitors.service.scheduler.monitors.graphql.query import (
    GRAPHQL_GET_SIGNAL_REQUEST_LIST_QUERY,
)
from datahub_monitors.service.scheduler.types import RUN_INGEST_TASK_NAME
from datahub_monitors.workers.config import CeleryConfig, update_celery_config
from datahub_monitors.workers.resolvers.executor_config_resolver import (
    ExecutorConfigResolver,
)

from .assertion_executor import AssertionExecutor

logger = logging.getLogger(__name__)


def setup_ingestion_executor() -> ReportingExecutor:
    # Build default task config
    ingest_task_config = TaskConfig(
        name=RUN_INGEST_TASK_NAME,
        type="acryl.executor.execution.sub_process_ingestion_task.SubProcessIngestionTask",
        configs=dict({}),
    )

    # Build default executor config
    ingestion_executor_config = ReportingExecutorConfig(
        id=EXECUTOR_ID,
        task_configs=[ingest_task_config],
        secret_stores=[
            SecretStoreConfig(type="env", config=dict({})),
            SecretStoreConfig(
                type="datahub",
                config=DataHubSecretStoreConfig(
                    graph_client_config=DatahubClientConfig(
                        server=DATAHUB_SERVER,
                        token=None,
                    )
                ),
            ),
        ],
        graph_client_config=DatahubClientConfig(server=DATAHUB_SERVER, token=None),
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

    if "task" in aspect_dict and not aspect_dict["task"] == RUN_INGEST_TASK_NAME:
        logger.error(
            f"Unsupported Task type {aspect_dict['task']} provided. Skipping execution of {event.entityUrn}.."
        )
        return None

    if "executorId" in aspect_dict and not aspect_dict["executorId"] == EXECUTOR_ID:
        logger.error(
            f"Ignoring ExecutionRequest for non-configured executorId {aspect_dict['executorId']}"
        )
        return None

    try:
        execution_request = ExecutionRequest(
            executor_id=aspect_dict["executorId"],
            exec_id=entity_key_dict["id"],
            name=RUN_INGEST_TASK_NAME,
            args={
                "urn": aspect_dict["source"]["ingestionSource"],
                "recipe": aspect_dict["args"]["recipe"],
                "version": aspect_dict["args"]["version"],
                "debug_mode": aspect_dict["args"]["debug_mode"],
            },
        )
    except Exception as e:
        logger.error(f"Error parsing Execution Request {e}")
        return None

    return execution_request


def extract_execution_request_signal(
    event: MetadataChangeLogClass,
) -> Tuple[Optional[SignalRequest], Optional[str]]:
    exec_id = None
    if not event.entityUrn:
        logger.error(f"Unable to parse Execution Request Signal, no entityUrn {event}")
        return None, exec_id

    if not event.aspect:
        logger.error(
            f"Unable to parse Execution Request Signal, no aspect {event.entityUrn}.."
        )
        return None, exec_id

    aspect_dict = json.loads(event.aspect.value)

    try:
        if not aspect_dict["executorId"] == EXECUTOR_ID:
            logger.error(
                f"Ignoring SignalRequest for non-configured executorId ${aspect_dict['executorId']}"
            )
            return None, exec_id

        urn_parts = event.entityUrn.split(":")
        exec_id = urn_parts[-1]
        signal_request = SignalRequest(
            exec_id=exec_id,
            executor_id=aspect_dict["executorId"],
            signal=aspect_dict["signal"],
        )
    except Exception as e:
        logger.error(f"Error parsing Execution Signal {e}")
        return None, exec_id

    return signal_request, exec_id


def fetch_execution_signal_requests(
    graph: DataHubAssertionGraph,
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
        logger.error(
            f"Received error while fetching signal requests from GMS! {result.get('error')}"
        )
        return []

    if (
        "listSignalRequests" not in result
        or "signalRequests" not in result["listSignalRequests"]
    ):
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


def signal_requests_check(
    graph: DataHubAssertionGraph,
    ingestion_executor: ReportingExecutor,
    assertion_executor: AssertionExecutor,
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
            ingestion_executor.signal(signal_request)

    # we also check for any running assertions here
    assertion_task_ids = []
    if assertion_executor.task_futures:
        for task_id in assertion_executor.task_futures.keys():
            task_future = assertion_executor.task_futures[task_id]
            if not task_future.done():
                assertion_task_ids.append(task_id)

    if len(assertion_task_ids) > 0:
        pass
        # TODO - Query a GMS API to see if these task_ids have any signals for them
        # this doesn't exist yet.


def update_celery_credentials(app: Celery, is_startup: bool, queue_name: str) -> None:
    executor_config_resolver = ExecutorConfigResolver()

    if is_startup:
        executor_configs = executor_config_resolver.get_executor_configs()
        config = update_celery_config(CeleryConfig(), executor_configs)
        app.config_from_object(config)
    else:
        current_queues = (
            app.conf.transport_options["predefined_queues"].keys()
            if "predefined_queues" in app.conf.transport_options
            else []
        )
        # if we don't know about this queue, we force a refresh of the config
        if queue_name and queue_name not in current_queues:
            did_refresh = True
            executor_configs = executor_config_resolver.fetch_executor_configs()
        else:
            (
                did_refresh,
                executor_configs,
            ) = executor_config_resolver.refresh_executor_configs()

        if did_refresh:
            config = update_celery_config(CeleryConfig(), executor_configs)
            app.config_from_object(config)

            if "predefined_queues" in app.conf.transport_options:
                for queue_name in app.conf.transport_options[
                    "predefined_queues"
                ].keys():
                    if queue_name not in current_queues:
                        # this is a newly added queue, let's tell celery!
                        logger.info(f"Adding new queue to celery config {queue_name}")
                        app.control.add_consumer(queue_name)
