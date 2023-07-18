import json
import logging
import time
from typing import Dict, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionResultClass,
    AssertionResultErrorClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    SystemMetadataClass,
)

from datahub_monitors.assertion.result.handler import AssertionResultHandler
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
    AssertionEvaluationResultError,
    AssertionResultType,
)

logger = logging.getLogger(__name__)


class AssertionRunEventResultHandler(AssertionResultHandler):
    """An assertion result handler that produces AssertionRunEvent objects back to DataHub."""

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def _parameters_to_native_results(
        self, parameters: dict
    ) -> Optional[Dict[str, str]]:
        results = {}
        if "events" in parameters and parameters["events"] is not None:
            # Serialize the first event.
            events = parameters["events"]
            if len(events) > 0:
                # We found a matching events(s). Lets serialize them!
                events_objs = [
                    ({"type": event.event_type.name, "time": event.event_time})
                    for event in events
                ]
                events_str = json.dumps(events_objs, separators=(",", ":"))
                results["events"] = events_str
        return results

    def _extract_error(
        self, error: AssertionEvaluationResultError
    ) -> AssertionResultErrorClass:
        return AssertionResultErrorClass(
            type=error.type.value, properties=error.properties
        )

    def _extract_result(
        self,
        assertion: Assertion,
        result: AssertionEvaluationResult,
        context: AssertionEvaluationContext,
    ) -> AssertionResultClass:
        logger.info(
            f"Attempting to produce Assertion Run Event for assertion run with urn {assertion.urn}. Result {result.type}"
        )  # TODO - debug

        parameters = result.parameters
        error = None
        native_results = None
        if parameters is not None:
            native_results = self._parameters_to_native_results(parameters)
        if result.error is not None:
            error = self._extract_error(result.error)

        return AssertionResultClass(
            AssertionResultTypeClass.SUCCESS
            if result.type == AssertionResultType.SUCCESS
            else AssertionResultTypeClass.FAILURE
            if result.type == AssertionResultType.FAILURE
            else AssertionResultTypeClass.INIT
            if result.type == AssertionResultType.INIT
            else AssertionResultTypeClass.ERROR,
            None,
            None,
            None,
            None,
            native_results,
            None,
            error,
        )

    def handle(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        result: AssertionEvaluationResult,
        context: AssertionEvaluationContext,
    ) -> None:
        now_ms = int(time.time() * 1000)
        run_id = f"native-{assertion.urn}-{str(now_ms)}"
        status = AssertionRunStatusClass.COMPLETE
        event_result = self._extract_result(assertion, result, context)
        run_event = AssertionRunEventClass(
            now_ms,
            f"{assertion.urn}-{str(now_ms)}",
            assertion.entity.urn,
            status,
            assertion.urn,
            event_result,
            None,
            None,
            None,
            None,
            None,
        )

        mcpw = MetadataChangeProposalWrapper(
            entityUrn=assertion.urn,
            aspect=run_event,
            systemMetadata=SystemMetadataClass(runId=run_id, lastObserved=now_ms),
        )

        # Now emit an MCP
        try:
            self.graph.emit_mcp(mcpw)
            logger.info(
                f"Successfully produced AssertionRunEvent MCP for assertion with urn {assertion.urn} for entity {assertion.entity.urn}, result type {result.type}."
            )
        except Exception:
            logger.exception(
                f"Failed to produce AssertionRunEvent MCP for assertion with urn {assertion.urn} for entity {assertion.entity.urn}, result type {result.type}. This means that assertion results will NOT be viewable!"
            )
