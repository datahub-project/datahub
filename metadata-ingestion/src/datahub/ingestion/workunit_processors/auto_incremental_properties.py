from dataclasses import dataclass
from typing import Iterable

from datahub.emitter.mce_builder import set_aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_properties_helper import (
    convert_dataset_properties_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    MetadataChangeEventClass,
)


@dataclass
class AutoIncrementalPropertiesProcessorReport(WorkunitProcessorReport):
    """Report for AutoIncrementalPropertiesProcessor metrics."""

    pass


class AutoIncrementalPropertiesProcessor(
    WorkunitProcessor[AutoIncrementalPropertiesProcessorReport]
):
    """Convert dataset properties to incremental patches when incremental_properties is enabled."""

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return bool(getattr(ctx.source_config, "incremental_properties", False))

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            urn = wu.get_urn()

            if isinstance(wu.metadata, MetadataChangeEventClass):
                properties_aspect = wu.get_aspect_of_type(DatasetPropertiesClass)
                set_aspect(wu.metadata, None, DatasetPropertiesClass)
                if len(wu.metadata.proposedSnapshot.aspects) > 0:
                    yield wu

                if properties_aspect:
                    yield convert_dataset_properties_to_patch(
                        urn, properties_aspect, wu.metadata.systemMetadata
                    )
            elif isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
                wu.metadata.aspect, DatasetPropertiesClass
            ):
                properties_aspect = wu.metadata.aspect
                if properties_aspect:
                    yield convert_dataset_properties_to_patch(
                        urn, properties_aspect, wu.metadata.systemMetadata
                    )
            else:
                yield wu
