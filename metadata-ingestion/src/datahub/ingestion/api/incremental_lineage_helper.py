from typing import ClassVar, Iterable, List, Optional, Union

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import datahub_guid, set_aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    FineGrainedLineageClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder


class PatchEntityAspect:
    SKIPPABLE_ATTRIBUTES: ClassVar[List[str]] = [
        "ASPECT_INFO",
        "ASPECT_NAME",
        "ASPECT_TYPE",
        "RECORD_SCHEMA",
    ]
    aspect: Union[ChartInfoClass, DashboardInfoClass]
    patch_builder: DashboardPatchBuilder
    attributes: List[str]

    def __init__(
        self,
        # The PatchEntityAspect can patch any Aspect, however to silent the lint Union is added for DashboardInfoClass
        # We can use it with any Aspect
        aspect: Union[DashboardInfoClass],
        patch_builder: DashboardPatchBuilder,
    ):
        self.aspect = aspect
        self.patch_builder = patch_builder
        self.attributes = dir(self.aspect)

    def is_attribute_includable(self, attribute_name: str) -> bool:
        """
        a child class can override this to add additional attributes to skip while generating patch aspect
        """
        if (
            attribute_name.startswith("__")
            or attribute_name.startswith("_")
            or attribute_name in PatchEntityAspect.SKIPPABLE_ATTRIBUTES
        ):
            return False

        return True

    def attribute_path(self, attribute_name: str) -> str:
        """
        a child class can override this if path is not equal to attribute_name
        """
        return f"/{attribute_name}"

    def patch(self) -> Optional[MetadataChangeProposalClass]:
        # filter property
        properties = {
            attr: getattr(self.aspect, attr)
            for attr in self.attributes
            if self.is_attribute_includable(attr)
            and not callable(getattr(self.aspect, attr))
        }

        for property_ in properties:
            if properties[property_]:
                self.patch_builder.add_patch(
                    aspect_name=self.aspect.ASPECT_NAME,
                    op="add",
                    path=self.attribute_path(property_),
                    value=properties[property_],
                )

        mcps: List[MetadataChangeProposalClass] = list(self.patch_builder.build())
        if mcps:
            return mcps[0]

        return None


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


def convert_chart_info_to_patch(
    urn: str, aspect: ChartInfoClass, system_metadata: Optional[SystemMetadataClass]
) -> Optional[MetadataWorkUnit]:
    patch_builder = ChartPatchBuilder(urn, system_metadata)

    if aspect.customProperties:
        for key in aspect.customProperties:
            patch_builder.add_custom_property(
                key, str(aspect.customProperties.get(key))
            )

    if aspect.inputEdges:
        for inputEdge in aspect.inputEdges:
            patch_builder.add_input_edge(inputEdge)

    values = patch_builder.build()
    if values:
        mcp = next(iter(values))
        return MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
        )
    return None


def convert_dashboard_info_to_patch(
    urn: str, aspect: DashboardInfoClass, system_metadata: Optional[SystemMetadataClass]
) -> Optional[MetadataWorkUnit]:
    patch_builder = DashboardPatchBuilder(urn, system_metadata)

    patch_entity_aspect: PatchEntityAspect = PatchEntityAspect(
        aspect=aspect,
        patch_builder=patch_builder,
    )

    mcp: Optional[MetadataChangeProposalClass] = patch_entity_aspect.patch()

    if mcp:
        return MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
        )

    return None


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

        if isinstance(wu.metadata, MetadataChangeEventClass):
            lineage_aspect = wu.get_aspect_of_type(UpstreamLineageClass)
            set_aspect(wu.metadata, None, UpstreamLineageClass)
            if len(wu.metadata.proposedSnapshot.aspects) > 0:
                yield wu

            if lineage_aspect and lineage_aspect.upstreams:
                yield convert_upstream_lineage_to_patch(
                    urn, lineage_aspect, wu.metadata.systemMetadata
                )
        elif isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, UpstreamLineageClass
        ):
            lineage_aspect = wu.metadata.aspect
            if lineage_aspect.upstreams:
                yield convert_upstream_lineage_to_patch(
                    urn, lineage_aspect, wu.metadata.systemMetadata
                )
        else:
            yield wu


class IncrementalLineageConfigMixin(ConfigModel):
    incremental_lineage: bool = Field(
        default=False,
        description="When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
    )
