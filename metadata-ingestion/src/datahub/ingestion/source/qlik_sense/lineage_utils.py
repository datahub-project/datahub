from typing import Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.ingestion.source.qlik_sense.source_utils import schema_field_parent
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


def script_column_lineage(
    dataset_urn: str, table: ScriptTableLineage
) -> Optional[List[FineGrainedLineage]]:
    return [
        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=upstreams,
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[builder.make_schema_field_urn(dataset_urn, column)],
        )
        for column, upstreams in table.column_upstreams.items()
        if upstreams
    ] or None


def patch_upstreams(
    dataset_urn: str,
    upstream_urns: List[str],
    lineage_type: str,
    fine_grained: Optional[List[FineGrainedLineage]] = None,
) -> Iterable[MetadataWorkUnit]:
    """Adds lineage without replacing the edges an earlier run wrote."""
    if not upstream_urns and not fine_grained:
        return
    yield convert_upstream_lineage_to_patch(
        dataset_urn,
        UpstreamLineage(
            upstreams=[
                Upstream(dataset=urn, type=lineage_type) for urn in upstream_urns
            ],
            fineGrainedLineages=fine_grained or None,
        ),
        None,
    )


def stale_field_lineage(
    current: List[FineGrainedLineage],
    stored: List[FineGrainedLineage],
    owned: Set[str],
) -> List[FineGrainedLineage]:
    """The column edges from owned tables that this run no longer stores,
    each as a lineage holding only those upstream fields."""
    stored_edges = {
        (downstream, upstream)
        for lineage in stored
        for downstream in lineage.downstreams or []
        for upstream in lineage.upstreams or []
    }
    stale = []
    for lineage in current:
        # A column edge can only be removed by patch through its single
        # downstream field; one another tool wrote with none or several
        # is not this connector's.
        if len(lineage.downstreams or []) != 1:
            continue
        upstreams = [
            upstream
            for upstream in lineage.upstreams or []
            if schema_field_parent(upstream) in owned
            and not any(
                (downstream, upstream) in stored_edges
                for downstream in lineage.downstreams or []
            )
        ]
        if upstreams:
            stale_lineage = FineGrainedLineage.from_obj(lineage.to_obj())
            stale_lineage.upstreams = upstreams
            stale.append(stale_lineage)
    return stale
