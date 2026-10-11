from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    auto_stale_entity_removal,
)


@dataclass
class AutoStaleEntityRemovalProcessorReport(WorkunitProcessorReport):
    """Report for AutoStaleEntityRemovalProcessor metrics."""

    pass


# TODO: StaleEntityRemoval reports directly to the source report, so this processor report is currently empty.
# Consider moving removal metrics here and keeping only stale entity identification metrics in the source report.
# That will require to refactor all source reports that are currently inheriting from StaleEntityRemovalSourceReport


class AutoStaleEntityRemovalProcessor(
    WorkunitProcessor[AutoStaleEntityRemovalProcessorReport]
):
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
