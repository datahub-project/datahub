import logging
from typing import Iterable, Optional

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
    SystemMetadataClass,
    UpstreamLineageClass,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


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

    patch_builder.set_chart_url(aspect.chartUrl).set_external_url(
        aspect.externalUrl
    ).set_type(aspect.type).set_title(aspect.title).set_access(
        aspect.access
    ).set_last_modified(aspect.lastModified).set_last_refreshed(
        aspect.lastRefreshed
    ).set_description(aspect.description).add_inputs(aspect.inputs)

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

    if aspect.customProperties:
        for key in aspect.customProperties:
            patch_builder.add_custom_property(
                key, str(aspect.customProperties.get(key))
            )

    if aspect.datasetEdges:
        for datasetEdge in aspect.datasetEdges:
            patch_builder.add_dataset_edge(datasetEdge)

    if aspect.chartEdges:
        for chartEdge in aspect.chartEdges:
            patch_builder.add_chart_edge(chartEdge)

    if aspect.title:
        patch_builder.set_title(aspect.title)

    if aspect.description:
        patch_builder.set_description(aspect.description)

    if aspect.charts:
        patch_builder.add_charts(aspect.charts)

    if aspect.dashboardUrl:
        patch_builder.set_dashboard_url(aspect.dashboardUrl)

    if aspect.datasets:
        patch_builder.add_datasets(aspect.datasets)

    if aspect.dashboards:
        for dashboard in aspect.dashboards:
            patch_builder.add_dashboard(dashboard)

    if aspect.access:
        patch_builder.set_access(aspect.access)

    if aspect.lastRefreshed:
        patch_builder.set_last_refreshed(aspect.lastRefreshed)

    if aspect.lastModified:
        patch_builder.set_last_modified(last_modified=aspect.lastModified)

    values = patch_builder.build()

    if values:
        logger.debug(
            f"Generating patch DashboardInfo MetadataWorkUnit for dashboard {aspect.title}"
        )
        mcp = next(iter(values))
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
