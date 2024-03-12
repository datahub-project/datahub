import copy
from typing import Dict, Iterable, Optional

from datahub.emitter.mce_builder import datahub_guid, set_aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    MetadataChangeEventClass,
    SystemMetadataClass,
    UpstreamClass,
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
    mcp = next(iter(patch_builder.build()))
    return MetadataWorkUnit(id=f"{urn}-upstreamLineage", mcp_raw=mcp)


def get_fine_grained_lineage_key(fine_upstream: FineGrainedLineageClass) -> str:
    return datahub_guid(
        {
            "upstreams": sorted(fine_upstream.upstreams or []),
            "downstreams": sorted(fine_upstream.downstreams or []),
            "transformOperation": fine_upstream.transformOperation,
        }
    )


def _merge_upstream_lineage(
    new_aspect: UpstreamLineageClass, gms_aspect: UpstreamLineageClass
) -> UpstreamLineageClass:
    merged_aspect = copy.deepcopy(gms_aspect)

    upstreams_map: Dict[str, UpstreamClass] = {
        upstream.dataset: upstream for upstream in merged_aspect.upstreams
    }

    upstreams_updated = False
    fine_upstreams_updated = False

    for table_upstream in new_aspect.upstreams:
        if table_upstream.dataset not in upstreams_map or (
            table_upstream.auditStamp.time
            > upstreams_map[table_upstream.dataset].auditStamp.time
        ):
            upstreams_map[table_upstream.dataset] = table_upstream
            upstreams_updated = True

    if upstreams_updated:
        merged_aspect.upstreams = list(upstreams_map.values())

    if new_aspect.fineGrainedLineages and merged_aspect.fineGrainedLineages:
        fine_upstreams_map: Dict[str, FineGrainedLineageClass] = {
            get_fine_grained_lineage_key(fine_upstream): fine_upstream
            for fine_upstream in merged_aspect.fineGrainedLineages
        }
        for column_upstream in new_aspect.fineGrainedLineages:
            column_upstream_key = get_fine_grained_lineage_key(column_upstream)

            if column_upstream_key not in fine_upstreams_map or (
                column_upstream.confidenceScore
                > fine_upstreams_map[column_upstream_key].confidenceScore
            ):
                fine_upstreams_map[column_upstream_key] = column_upstream
                fine_upstreams_updated = True

        if fine_upstreams_updated:
            merged_aspect.fineGrainedLineages = list(fine_upstreams_map.values())
    else:
        merged_aspect.fineGrainedLineages = (
            new_aspect.fineGrainedLineages or gms_aspect.fineGrainedLineages
        )

    return merged_aspect


def _lineage_wu_via_read_modify_write(
    graph: DataHubGraph,
    urn: str,
    aspect: UpstreamLineageClass,
    system_metadata: Optional[SystemMetadataClass],
) -> MetadataWorkUnit:
    gms_aspect = graph.get_aspect(urn, UpstreamLineageClass)
    if gms_aspect:
        new_aspect = _merge_upstream_lineage(aspect, gms_aspect)
    else:
        new_aspect = aspect

    return MetadataChangeProposalWrapper(
        entityUrn=urn, aspect=new_aspect, systemMetadata=system_metadata
    ).as_workunit()


def auto_incremental_lineage(
    graph: Optional[DataHubGraph],
    incremental_lineage: bool,
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    if not incremental_lineage:
        yield from stream
        return  # early exit

    for wu in stream:
        lineage_aspect: Optional[UpstreamLineageClass] = wu.get_aspect_of_type(
            UpstreamLineageClass
        )
        urn = wu.get_urn()

        if lineage_aspect:
            if isinstance(wu.metadata, MetadataChangeEventClass):
                set_aspect(
                    wu.metadata, None, UpstreamLineageClass
                )  # we'll emit upstreamLineage separately below
                if len(wu.metadata.proposedSnapshot.aspects) > 0:
                    yield wu

            if lineage_aspect.fineGrainedLineages:
                if graph is None:
                    raise ValueError(
                        "Failed to handle incremental lineage, DataHubGraph is missing. "
                        "Use `datahub-rest` sink OR provide `datahub-api` config in recipe. "
                    )
                yield _lineage_wu_via_read_modify_write(
                    graph, urn, lineage_aspect, wu.metadata.systemMetadata
                )
            elif lineage_aspect.upstreams:
                yield convert_upstream_lineage_to_patch(
                    urn, lineage_aspect, wu.metadata.systemMetadata
                )
        else:
            yield wu
