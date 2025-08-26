import json
import logging
import time
from typing import Optional, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import AssertionInfoClass, SystemMetadataClass

from datahub_executor.common.assertion.engine.transformer.transformer import (
    AssertionTransformer,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    EmbeddedAssertion,
    RawAspect,
)

logger = logging.getLogger(__name__)


class EmbeddedAssertionsTransformer(AssertionTransformer):
    @classmethod
    def create(cls, graph: DataHubGraph) -> "AssertionTransformer":
        return EmbeddedAssertionsTransformer(graph)

    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def transform(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> Tuple[Assertion, AssertionEvaluationParameters, AssertionEvaluationContext]:
        if (
            context.evaluation_spec
            and context.evaluation_spec.context
            and context.evaluation_spec.context.has_embedded_assertions
        ):
            # This is smart assertion
            try:
                assertion = self._update_assertion(assertion, context)
            except Exception as e:
                logger.error(
                    f"Unable to update smart assertion due to error {e},"
                    "will continue to execute existing assertion."
                )
        return assertion, parameters, context

    def _update_assertion(
        self,
        assertion: Assertion,
        context: AssertionEvaluationContext,
    ) -> Assertion:
        # First, confirm that we have access to everything we need to update assertion in GMS.
        # In ideal scenario, following asserts should never fail.
        assert context.evaluation_spec
        assert context.evaluation_spec.context
        assert context.evaluation_spec.context.embedded_assertions

        # Second, we find the embedded assertion that's suited to run at current time instance
        embedded_assertion_to_run: Optional[EmbeddedAssertion] = None
        now_ms = int(time.time() * 1000)
        for embedded_assertion in context.evaluation_spec.context.embedded_assertions:
            if not embedded_assertion.evaluation_time_window or (
                embedded_assertion.evaluation_time_window.start_time_millis <= now_ms
                and embedded_assertion.evaluation_time_window.end_time_millis >= now_ms
            ):
                embedded_assertion_to_run = embedded_assertion
                break

        # If there is no matching embedded assertion, there is no need to update assertion. Return early.
        if not embedded_assertion_to_run:
            return assertion

        logger.debug(
            f"Updating smart assertion with embedded assertion {assertion.urn}"
        )
        run_id = f"smart-assertion-{assertion.urn}-{str(now_ms)}"
        mcpw = MetadataChangeProposalWrapper(
            entityUrn=assertion.urn,
            aspect=AssertionInfoClass.from_obj(
                json.loads(embedded_assertion_to_run.raw_assertion)
            ),
            systemMetadata=SystemMetadataClass(runId=run_id, lastObserved=now_ms),
        )
        self.graph.emit_mcp(mcpw)

        return Assertion.parse_obj(
            dict(
                **dict(embedded_assertion_to_run.assertion),
                urn=assertion.urn,
                entity=assertion.entity,
                connectionUrn=assertion.connection_urn,
                raw_info_aspect=RawAspect(
                    aspectName="assertionInfo",
                    payload=embedded_assertion_to_run.raw_assertion,
                ),
            ),
        )
