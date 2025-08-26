import logging
import time
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SystemMetadataClass

from datahub_executor.common.aspect_builder import build_assertion_run_event
from datahub_executor.common.assertion.result.handler import AssertionResultHandler
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
)

logger = logging.getLogger(__name__)


class AssertionRunEventResultHandler(AssertionResultHandler):
    """An assertion result handler that produces AssertionRunEvent objects back to DataHub."""

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def handle(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        result: AssertionEvaluationResult,
        context: AssertionEvaluationContext,
    ) -> None:
        if context.dry_run:
            # We should not produce assertion results for dry runs.
            return

        now_ms = int(time.time() * 1000)
        run_id = f"native-{assertion.urn}-{str(now_ms)}"
        run_event = build_assertion_run_event(assertion, result, context)

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
