from dataclasses import dataclass
from typing import Iterable

from datahub.emitter.mce_builder import set_aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_ownership_helper import (
    convert_ownership_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    OwnershipClass,
)


@dataclass
class AutoIncrementalOwnershipProcessorReport(WorkunitProcessorReport):
    """Report for AutoIncrementalOwnershipProcessor metrics."""

    pass


class AutoIncrementalOwnershipProcessor(
    WorkunitProcessor[AutoIncrementalOwnershipProcessorReport]
):
    """Convert ownership aspects to incremental patches when incremental_ownership is enabled."""

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "incremental_ownership", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            urn = wu.get_urn()

            if isinstance(wu.metadata, MetadataChangeEventClass):
                if urn.startswith("urn:li:dataset:"):
                    ownership_aspect = wu.get_aspect_of_type(OwnershipClass)
                    set_aspect(wu.metadata, None, OwnershipClass)
                    if len(wu.metadata.proposedSnapshot.aspects) > 0:
                        yield wu

                    if ownership_aspect and ownership_aspect.owners:
                        yield convert_ownership_to_patch(
                            urn, ownership_aspect, wu.metadata.systemMetadata
                        )
                else:
                    yield wu
            elif (
                isinstance(wu.metadata, MetadataChangeProposalWrapper)
                and isinstance(wu.metadata.aspect, OwnershipClass)
                and wu.metadata.entityType == "dataset"
            ):
                ownership_aspect = wu.metadata.aspect
                if ownership_aspect.owners:
                    yield convert_ownership_to_patch(
                        urn, ownership_aspect, wu.metadata.systemMetadata
                    )
            else:
                yield wu
