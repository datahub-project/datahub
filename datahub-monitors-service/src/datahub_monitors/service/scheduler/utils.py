import logging
import time
import uuid

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ExecutionRequestInputClass,
    ExecutionRequestKeyClass,
    ExecutionRequestSourceClass,
)

from datahub_monitors.common.helpers import create_datahub_graph

logger = logging.getLogger(__name__)


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
        graph = create_datahub_graph()
        graph.emit_mcp(mcpw)
    except Exception as e:
        logger.exception(
            f"An unknown error occurred when attempting to emit dataHubExecutionRequestInput - {e}"
        )
