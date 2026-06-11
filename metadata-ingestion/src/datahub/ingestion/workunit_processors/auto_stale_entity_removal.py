from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    auto_stale_entity_removal,
)


class AutoStaleEntityRemovalProcessor(WorkunitProcessor):
    """Soft-delete stale entities that appeared in the previous run but not in the current run."""

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return ctx.stale_entity_removal_context is not None and isinstance(
            ctx.source_report, StaleEntityRemovalSourceReport
        )

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        assert ctx.stale_entity_removal_context is not None
        assert isinstance(ctx.source_report, StaleEntityRemovalSourceReport)
        removal_ctx = ctx.stale_entity_removal_context
        self.handler = StaleEntityRemovalHandler(
            state_provider=removal_ctx.state_provider,
            report=ctx.source_report,
            config=ctx.source_config,  # type: ignore[arg-type]
            state_type_class=removal_ctx.state_type_class,
            pipeline_name=ctx.pipeline_context.pipeline_name,
            run_id=ctx.pipeline_context.run_id,
            platform=ctx.infer_platform(),
        )

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(self.handler, stream)
