import logging
from typing import Callable, Optional

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.error import InvalidUrnError

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


def convert_datajob_input_output_to_patch(
    urn: str,
    aspect: DataJobInputOutputClass,
    system_metadata: Optional[SystemMetadataClass],
) -> Optional[MetadataWorkUnit]:
    """Convert a full dataJobInputOutput aspect into an additive patch.

    A full upsert of this aspect replaces the `inputDatasetEdges` /
    `inputDatajobEdges` / `outputDatasetEdges` fields, which is where DataHub stores
    lineage added by hand, so re-stating it deletes the user's edges.

    Note the patch writes the `*Edges` fields while a full upsert writes the plain
    `inputDatasets` / `inputDatajobs` / `outputDatasets` arrays. The patch template
    server-side only supports the `*Edges` paths, so entries already in the plain
    arrays from earlier non-patch runs cannot be removed here and will remain until
    cleaned up out of band.
    """
    patch_builder = DataJobPatchBuilder(urn, system_metadata)

    def _add(add: Callable[[str], object], edge_urn: str, kind: str) -> None:
        try:
            add(edge_urn)
        except (ValueError, InvalidUrnError):
            # A malformed or wrong-typed URN costs its own edge, not the aspect. Both
            # are needed: a prefix mismatch raises ValueError, while an unparseable URN
            # raises InvalidUrnError, which does not subclass it.
            logger.warning("Skipping %s edge %s on %s", kind, edge_urn, urn)

    for dataset_urn in aspect.inputDatasets or []:
        _add(patch_builder.add_input_dataset, dataset_urn, "input dataset")
    for dataset_urn in aspect.outputDatasets or []:
        _add(patch_builder.add_output_dataset, dataset_urn, "output dataset")
    for datajob_urn in aspect.inputDatajobs or []:
        _add(patch_builder.add_input_datajob, datajob_urn, "input datajob")
    for field_urn in aspect.inputDatasetFields or []:
        _add(patch_builder.add_input_dataset_field, field_urn, "input dataset field")
    for field_urn in aspect.outputDatasetFields or []:
        _add(patch_builder.add_output_dataset_field, field_urn, "output dataset field")
    for fine_upstream in aspect.fineGrainedLineages or []:
        try:
            patch_builder.add_fine_grained_lineage(fine_upstream)
        except TypeError:
            # The patch path keys on a single downstream, so a multi-downstream entry
            # can't be expressed. Drop it rather than lose the whole aspect.
            logger.warning(
                "Skipping column lineage for %s: a patch needs exactly one downstream, got %d",
                urn,
                len(fine_upstream.downstreams or []),
            )

    values = patch_builder.build()
    if not values:
        # Nothing addable in the aspect; a patch would be an empty no-op.
        return None
    if len(values) > 1:
        # Every patch above targets dataJobInputOutput with no array_primary_keys, so
        # the builder groups them into one MCP. Warn rather than drop silently if that
        # ever stops holding.
        logger.warning(
            "Expected one patch MCP for %s, got %d; emitting the first",
            urn,
            len(values),
        )
    mcp = next(iter(values))
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


class IncrementalLineageConfigMixin(ConfigModel):
    incremental_lineage: bool = Field(
        default=False,
        description="When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
    )
