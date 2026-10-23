import re
from dataclasses import dataclass
from typing import Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    MetadataChangeEventClass,
    UpstreamLineageClass,
)

# SqlParsingAggregator prefixes the logic with a label, e.g. "SQL: " or "COPY: ".
_LABEL_PREFIX = re.compile(r"^([A-Z]+): ")
_ELLIPSIS = "..."


def truncate_transform_operation(value: str, max_length: int) -> Optional[str]:
    """Cut the logic after any label to max_length characters plus "..."; 0 keeps only the label, or None if unlabelled."""
    match = _LABEL_PREFIX.match(value)
    label = match.group(1) if match else None
    logic = value[match.end() :] if match else value
    if max_length == 0:
        return label
    if len(logic) <= max_length + len(_ELLIPSIS):
        return value
    truncated = f"{logic[:max_length]}{_ELLIPSIS}"
    return f"{label}: {truncated}" if label else truncated


@dataclass
class EnsureTransformOperationLengthProcessorReport(WorkunitProcessorReport):
    num_transform_operations_truncated: int = 0


class EnsureTransformOperationLengthProcessor(
    WorkunitProcessor[EnsureTransformOperationLengthProcessorReport]
):
    """Truncate transformOperation in dataset and data job fine-grained lineage."""

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        max_length = getattr(ctx.source_config, "max_transform_operation_length", None)
        assert isinstance(max_length, int)
        self.max_length = max_length

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return (
            getattr(ctx.source_config, "max_transform_operation_length", None)
            is not None
        )

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            # For a raw MetadataChangeProposal, get_aspect_of_type returns a copy.
            if isinstance(
                wu.metadata, (MetadataChangeProposalWrapper, MetadataChangeEventClass)
            ):
                upstream_lineage = wu.get_aspect_of_type(UpstreamLineageClass)
                if upstream_lineage:
                    self._truncate(upstream_lineage.fineGrainedLineages)
                datajob_lineage = wu.get_aspect_of_type(DataJobInputOutputClass)
                if datajob_lineage:
                    self._truncate(datajob_lineage.fineGrainedLineages)
            yield wu

    def _truncate(
        self, fine_grained_lineages: Optional[List[FineGrainedLineageClass]]
    ) -> None:
        for fine_grained in fine_grained_lineages or []:
            if fine_grained.transformOperation is None:
                continue
            truncated = truncate_transform_operation(
                fine_grained.transformOperation, self.max_length
            )
            if truncated != fine_grained.transformOperation:
                fine_grained.transformOperation = truncated
                self.report.num_transform_operations_truncated += 1
