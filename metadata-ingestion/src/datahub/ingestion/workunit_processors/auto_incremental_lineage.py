from dataclasses import dataclass
from typing import Iterable, Optional

from datahub.emitter.mce_builder import set_aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.ingestion.workunit_processors.transform_operation import (
    apply_transform_operation_length,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    UpstreamLineageClass,
)


@dataclass
class AutoIncrementalLineageProcessorReport(WorkunitProcessorReport):
    """Report for AutoIncrementalLineageProcessor metrics."""

    num_transform_operations_truncated: int = 0


class AutoIncrementalLineageProcessor(
    WorkunitProcessor[AutoIncrementalLineageProcessorReport]
):
    """Convert lineage aspects to incremental patches when incremental_lineage is enabled."""

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        max_length = getattr(ctx.source_config, "max_transform_operation_length", None)
        self.max_transform_operation_length: Optional[int] = (
            max_length if isinstance(max_length, int) else None
        )

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "incremental_lineage", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            urn = wu.get_urn()

            if isinstance(wu.metadata, MetadataChangeEventClass):
                lineage_aspect = wu.get_aspect_of_type(UpstreamLineageClass)
                set_aspect(wu.metadata, None, UpstreamLineageClass)
                if len(wu.metadata.proposedSnapshot.aspects) > 0:
                    yield wu

                if lineage_aspect and lineage_aspect.upstreams:
                    self._maybe_truncate_transform_operations(lineage_aspect)
                    yield convert_upstream_lineage_to_patch(
                        urn, lineage_aspect, wu.metadata.systemMetadata
                    )
            elif isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
                wu.metadata.aspect, UpstreamLineageClass
            ):
                lineage_aspect = wu.metadata.aspect
                if lineage_aspect.upstreams:
                    self._maybe_truncate_transform_operations(lineage_aspect)
                    yield convert_upstream_lineage_to_patch(
                        urn, lineage_aspect, wu.metadata.systemMetadata
                    )
            else:
                yield wu

    def _maybe_truncate_transform_operations(
        self, lineage_aspect: UpstreamLineageClass
    ) -> None:
        if self.max_transform_operation_length is None:
            return
        self.report.num_transform_operations_truncated += (
            apply_transform_operation_length(
                lineage_aspect.fineGrainedLineages,
                self.max_transform_operation_length,
            )
        )
