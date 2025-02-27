import logging
from queue import Queue
from threading import Thread
from typing import Optional

import fastapi
from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.common.aspect_builder import build_assertion_run_event
from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_GET_ASSERTION_QUERY,
    GRAPHQL_GET_DATASET_QUERY,
)
from datahub_executor.common.constants import (
    DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
    RUN_ASSERTION_TASK_NAME,
)
from datahub_executor.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
)
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationSpec,
    AssertionSourceType,
    CronSchedule,
    Monitor,
)
from datahub_executor.config import (
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS,
    DATAHUB_EXECUTOR_POOL_ID,
)
from datahub_executor.worker.remote import apply_remote_assertion_request

from .types import (
    AssertionEvaluationParametersSchema,
    AssertionResultErrorSchema,
    AssertionResultSchema,
    AssertionsResultItemSchema,
    AssertionsResultSchema,
    EvaluateAssertionInputSchema,
    EvaluateAssertionUrnInputSchema,
    EvaluateAssertionUrnsInputSchema,
)

logger = logging.getLogger(__name__)


graph = create_datahub_graph()
engine = None
tp = ThreadPoolExecutorWithQueueSizeLimit(
    max_workers=DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS,
    name="assertions_api",
)

async_queue: Queue = Queue(maxsize=DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS * 10)
async_queue_thread = None


def handle_evaluate_assertion_urns_sync(
    input: EvaluateAssertionUrnsInputSchema,
) -> AssertionsResultSchema:
    global tp

    results = []
    q: Queue = Queue(maxsize=DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS)

    def worker(urn: str) -> None:
        try:
            assertion_input = EvaluateAssertionUrnInputSchema(
                assertionUrn=urn, dryRun=input.dryRun
            )
            assertion_result = handle_evaluate_assertion_urn(
                assertion_input, input.asyncFlag
            )
            result = AssertionsResultItemSchema(urn=urn, result=assertion_result)
        except Exception as e:
            logger.warning(e)
            item_error = AssertionResultErrorSchema(type="UNKNOWN_ERROR")
            item_result = AssertionResultSchema(type="UNKNOWN", error=item_error)
            result = AssertionsResultItemSchema(urn=urn, result=item_result)
        q.put(result)

    def producer() -> None:
        for urn in input.urns:
            tp.submit(worker, urn)

    prod = Thread(target=producer)
    prod.start()

    # Consume results
    for _ in input.urns:
        result = q.get()
        results.append(result)

    prod.join()

    return AssertionsResultSchema(results=results)


def handle_evaluate_assertion_urns(
    input: EvaluateAssertionUrnsInputSchema,
) -> Optional[AssertionsResultSchema]:
    global async_queue
    if input.asyncFlag:
        async_queue.put(input)
        return AssertionsResultSchema(results=[])
    else:
        return handle_evaluate_assertion_urns_sync(input)


def _evaluate_assertion(
    engine: AssertionEngine,
    assertion: Assertion,
    parameters: AssertionEvaluationParameters,
    dry_run: bool,
    async_flag: bool,
) -> Optional[AssertionResultSchema]:
    monitor_urn = assertion.monitor.get("urn", None) if assertion.monitor else None
    executor_id = (
        assertion.monitor.get("executor_id", DATAHUB_EXECUTOR_EMBEDDED_POOL_ID)
        if assertion.monitor
        else DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
    )
    context = AssertionEvaluationContext(
        dry_run=dry_run,
        monitor_urn=monitor_urn,
    )
    is_embedded = DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED and (
        DATAHUB_EXECUTOR_POOL_ID == executor_id
    )

    if async_flag and not is_embedded:
        assertion_spec = AssertionEvaluationSpec(
            assertion=assertion,
            parameters=parameters,
            context=context,
            schedule=CronSchedule(
                cron="",
                timezone="",
            ),
        )
        execution_request = ExecutionRequest(
            executor_id=executor_id,
            exec_id=monitor_urn,
            name=RUN_ASSERTION_TASK_NAME,
            args={
                "urn": monitor_urn,
                "assertion_spec": assertion_spec.dict(by_alias=True),
                "context": context.__dict__,
            },
        )
        apply_remote_assertion_request(execution_request, execution_request.executor_id)
        return None

    # call the engine evaluate method to run the actual assertion
    result = engine.evaluate(assertion, parameters, context)

    # convert assertion and evaluation result to run event
    # TODO it seems 'raw_info_aspect' is None (at least when triggered via API)
    run_event = build_assertion_run_event(assertion, result)

    # convert run_event to a pydantic schema for API return data
    return AssertionResultSchema.from_orm(run_event.result)


def handle_post_evaluate_assertion(
    assertion_input: EvaluateAssertionInputSchema,
    engine: AssertionEngine,
    table_name: Optional[str],
    qualified_name: Optional[str],
    async_flag: bool,
) -> Optional[AssertionResultSchema]:
    # Setup the test assertion
    entity = AssertionEntity(
        urn=assertion_input.entityUrn,
        platformUrn=assertion_input.connectionUrn,  # this makes the assumption that connectionUrn coming from the front-end is the same as the platformUrn
        platformInstance=None,
        subTypes=None,
        table_name=table_name,
        qualified_name=qualified_name,
    )
    assertion = Assertion(
        urn="urn:li:assertion:test",
        connectionUrn=assertion_input.connectionUrn,
        type=assertion_input.type,
        entity=entity,
        freshnessAssertion=assertion_input.assertion.freshness_assertion,
        volumeAssertion=assertion_input.assertion.volume_assertion,
        sqlAssertion=assertion_input.assertion.sql_assertion,
        fieldAssertion=assertion_input.assertion.field_assertion,
        schemaAssertion=assertion_input.assertion.schema_assertion,
    )

    return _evaluate_assertion(
        engine=engine,
        assertion=assertion,
        parameters=assertion_input.parameters.to_internal_params(),
        dry_run=assertion_input.dryRun,
        async_flag=async_flag,
    )


def handle_evaluate_assertion(
    assertion_input: EvaluateAssertionInputSchema,
) -> Optional[AssertionResultSchema]:
    global graph, engine

    if engine is None:
        raise fastapi.HTTPException(
            status_code=500,
            detail="Engine is not initialized",
        )

    result = graph.execute_graphql(
        GRAPHQL_GET_DATASET_QUERY,
        variables={"datasetUrn": assertion_input.entityUrn},
    )
    table_name = None
    qualified_name = None

    if dataset := result.get("dataset", None):
        table_name = (
            dataset["properties"]["name"]
            if "properties" in dataset
            and dataset["properties"] is not None
            and "name" in dataset["properties"]
            else None
        )
        qualified_name = (
            dataset["properties"]["qualifiedName"]
            if "properties" in dataset
            and dataset["properties"] is not None
            and "qualifiedName" in dataset["properties"]
            else None
        )

    return handle_post_evaluate_assertion(
        assertion_input, engine, table_name, qualified_name, False
    )


def _get_parameters_for_assertion(
    assertion: Assertion,
    supplied_parameters: Optional[AssertionEvaluationParametersSchema],
) -> AssertionEvaluationParameters:
    if supplied_parameters:
        return supplied_parameters.to_internal_params()

    if assertion.monitor is not None:
        try:
            monitor = Monitor.parse_obj(assertion.monitor)
            if monitor.assertion_monitor is None:
                raise ValueError("assertion_monitor is empty")

            for candidate in monitor.assertion_monitor.assertions:
                if candidate.assertion.urn == assertion.urn:
                    return candidate.parameters
        except Exception as e:
            logger.error(
                f"Failed to parse monitor object for assertion {assertion.urn}: {e}"
            )
            raise fastapi.HTTPException(
                status_code=422,
                detail=f"Failed to parse monitor object for assertion {assertion.urn}",
            )

    logger.error(f"No assertion evaluation parameters supplied for {assertion.urn}")
    raise fastapi.HTTPException(
        status_code=422,
        detail=f"No assertion evaluation parameters supplied for {assertion.urn}",
    )


def handle_post_evaluate_assertion_urn(
    assertion: Assertion,
    assertion_urn_input: EvaluateAssertionUrnInputSchema,
    engine: AssertionEngine,
    async_flag: bool,
) -> Optional[AssertionResultSchema]:
    if not assertion.source_type == AssertionSourceType.NATIVE:
        logger.error(f"Cannot evaluate non-native assertion {assertion.urn}")
        raise fastapi.HTTPException(
            status_code=422, detail="Can only evaluate native defined assertions"
        )

    parameters = _get_parameters_for_assertion(
        assertion, assertion_urn_input.parameters
    )

    return _evaluate_assertion(
        engine=engine,
        assertion=assertion,
        parameters=parameters,
        dry_run=assertion_urn_input.dryRun,
        async_flag=async_flag,
    )


def handle_evaluate_assertion_urn(
    assertion_urn_input: EvaluateAssertionUrnInputSchema, async_flag: bool
) -> AssertionResultSchema:
    global graph, engine

    if engine is None:
        raise fastapi.HTTPException(
            status_code=500,
            detail="Engine is not initialized",
        )

    result = graph.execute_graphql(
        GRAPHQL_GET_ASSERTION_QUERY,
        variables={"assertionUrn": assertion_urn_input.assertionUrn},
    )

    try:
        assertion = Assertion.parse_obj(result["assertion"])
    except Exception as e:
        # on non-existent assertion, graphql is still returning a result in result["assertion"] but the parsing fails
        logger.warning(e)
        raise fastapi.HTTPException(status_code=404)

    return handle_post_evaluate_assertion_urn(
        assertion, assertion_urn_input, engine, async_flag
    )


def async_queue_worker() -> None:
    global async_queue
    while True:
        input = async_queue.get()
        if input is None:
            return
        else:
            handle_evaluate_assertion_urns_sync(input)


def async_queue_start() -> None:
    global async_queue_thread, engine
    engine = create_assertion_engine(graph)
    async_queue_thread = Thread(target=async_queue_worker)
    async_queue_thread.start()


def async_queue_stop() -> None:
    global async_queue, async_queue_thread
    async_queue.put(None)
    if async_queue_thread is not None:
        async_queue_thread.join()
