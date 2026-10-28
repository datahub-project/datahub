from collections import defaultdict
from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws_messaging.constants import (
    LINEAGE_RELATIONSHIP_PROPERTY,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaMetadataClass,
    UpstreamClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path


def mapped_field_lineage(
    *,
    upstream_urn: str,
    upstream_schema: SchemaMetadataClass,
    downstream_urn: str,
    downstream_schema: SchemaMetadataClass,
    upstream_path_for: Callable[[str], Optional[str]],
) -> List[FineGrainedLineageClass]:
    """Field-to-field lineage where each downstream field comes from one known
    upstream path. Only fields present on both sides, matched case-insensitively
    on the simple field path."""
    upstream_paths: Dict[str, str] = {}
    for upstream_field in upstream_schema.fields:
        path = get_simple_field_path_from_v2_field_path(upstream_field.fieldPath)
        upstream_paths.setdefault(path.lower(), path)
    lineages: List[FineGrainedLineageClass] = []
    for downstream_field in downstream_schema.fields:
        path = get_simple_field_path_from_v2_field_path(downstream_field.fieldPath)
        source_path = upstream_path_for(path)
        upstream_path = upstream_paths.get(source_path.lower()) if source_path else None
        if upstream_path is None:
            continue
        lineages.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[make_schema_field_urn(upstream_urn, upstream_path)],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(downstream_urn, path)],
            )
        )
    return lineages


def identity_field_lineage(
    *,
    upstream_urn: str,
    upstream_schema: SchemaMetadataClass,
    downstream_urn: str,
    downstream_schema: SchemaMetadataClass,
) -> List[FineGrainedLineageClass]:
    """Lineage for a hop that passes the message through unchanged."""
    return mapped_field_lineage(
        upstream_urn=upstream_urn,
        upstream_schema=upstream_schema,
        downstream_urn=downstream_urn,
        downstream_schema=downstream_schema,
        upstream_path_for=lambda path: path,
    )


class SchemaLookup:
    """Schemas fetched in this run, falling back to what DataHub already holds."""

    def __init__(self, graph: Optional[DataHubGraph]):
        self.graph = graph
        self.fetched: Dict[str, SchemaMetadataClass] = {}
        self._from_graph: Dict[str, Optional[SchemaMetadataClass]] = {}

    def get(self, urn: str) -> Optional[SchemaMetadataClass]:
        if urn in self.fetched:
            return self.fetched[urn]
        if self.graph is None:
            return None
        if urn not in self._from_graph:
            self._from_graph[urn] = self.graph.get_schema_metadata(urn)
        return self._from_graph[urn]


class DatasetLineageCollector:
    """Gathers dataset-to-dataset edges and writes them as patches.

    A queue's upstreams come from both sources (its own redrive policy from sqs,
    subscriptions from sns) and possibly from other accounts' recipes, so a full
    upstreamLineage write from any one of them would erase the rest."""

    def __init__(self, report: AwsMessagingReportBase):
        self.report = report
        self._edges: Dict[str, Dict[str, LineageRelationship]] = defaultdict(dict)
        self._fine_grained: Dict[str, List[FineGrainedLineageClass]] = defaultdict(list)

    def add(
        self, *, upstream: str, downstream: str, relationship: LineageRelationship
    ) -> None:
        if upstream != downstream:
            self._edges[downstream].setdefault(upstream, relationship)

    def add_fine_grained(
        self, downstream: str, lineages: List[FineGrainedLineageClass]
    ) -> None:
        self._fine_grained[downstream].extend(lineages)
        self.report.column_lineage_edges_emitted += len(lineages)

    def workunits(self) -> Iterable[MetadataWorkUnit]:
        for downstream in sorted(self._edges):
            patch = DatasetPatchBuilder(downstream)
            for upstream, relationship in sorted(self._edges[downstream].items()):
                patch.add_upstream_lineage(
                    UpstreamClass(
                        dataset=upstream,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                        properties={LINEAGE_RELATIONSHIP_PROPERTY: relationship.value},
                    )
                )
                self.report.lineage_edges_emitted += 1
            for fine_grained in self._fine_grained.get(downstream, []):
                patch.add_fine_grained_lineage(fine_grained)
            for mcp in patch.build():
                # The downstream may be another account's queue; patching it must
                # not make this source responsible for deleting it later.
                yield MetadataWorkUnit(
                    id=f"{downstream}-upstreamLineage-patch",
                    mcp_raw=mcp,
                    is_primary_source=False,
                )
