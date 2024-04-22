from typing import Iterable, Optional

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import datahub_guid, set_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    MetadataChangeEventClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder


def convert_upstream_lineage_to_patch(
    urn: str,
    aspect: UpstreamLineageClass,
    system_metadata: Optional[SystemMetadataClass],
) -> MetadataWorkUnit:
    patch_builder = DatasetPatchBuilder(urn, system_metadata)
    for upstream in aspect.upstreams:
        patch_builder.add_upstream_lineage(upstream)
    for fine_upstream in aspect.fineGrainedLineages or []:
        patch_builder.add_fine_grained_upstream_lineage(fine_upstream)
    mcp = next(iter(patch_builder.build()))
    return MetadataWorkUnit(id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp)


def get_fine_grained_lineage_key(fine_upstream: FineGrainedLineageClass) -> str:
    return datahub_guid(
        {
            "upstreams": sorted(fine_upstream.upstreams or []),
            "downstreams": sorted(fine_upstream.downstreams or []),
            "transformOperation": fine_upstream.transformOperation,
        }
    )


def auto_incremental_lineage(
    incremental_lineage: bool,
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    if not incremental_lineage:
        yield from stream
        return  # early exit

    for wu in stream:
        urn = wu.get_urn()

        lineage_aspect: Optional[UpstreamLineageClass] = wu.get_aspect_of_type(
            UpstreamLineageClass
        )
        if isinstance(wu.metadata, MetadataChangeEventClass):
            set_aspect(
                wu.metadata, None, UpstreamLineageClass
            )  # we'll handle upstreamLineage separately below
            if len(wu.metadata.proposedSnapshot.aspects) > 0:
                yield wu

        if lineage_aspect:
            if lineage_aspect.upstreams:
                yield convert_upstream_lineage_to_patch(
                    urn, lineage_aspect, wu.metadata.systemMetadata
                )


class IncrementalLineageConfigMixin(ConfigModel):
    incremental_lineage: bool = Field(
        default=False,
        description="When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
    )
