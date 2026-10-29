from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.ingestion.source.aws_messaging.constants import (
    DOWNSTREAM_OF_RELATIONSHIP,
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
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path


def identity_field_lineage(
    *,
    upstream_urn: str,
    upstream_schema: SchemaMetadataClass,
    downstream_urn: str,
    downstream_schema: SchemaMetadataClass,
) -> List[FineGrainedLineageClass]:
    """Field-to-field lineage for a hop that passes the message through unchanged:
    every field present on both sides, matched case-insensitively on its simple
    path."""
    upstream_paths: Dict[str, str] = {}
    for upstream_field in upstream_schema.fields:
        path = get_simple_field_path_from_v2_field_path(upstream_field.fieldPath)
        upstream_paths.setdefault(path.lower(), path)
    lineages: List[FineGrainedLineageClass] = []
    for downstream_field in downstream_schema.fields:
        path = get_simple_field_path_from_v2_field_path(downstream_field.fieldPath)
        upstream_path = upstream_paths.get(path.lower())
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


@dataclass
class StaleLineage:
    edges: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    fine_grained: Dict[str, List[FineGrainedLineageClass]] = field(
        default_factory=lambda: defaultdict(list)
    )


class DatasetLineageCollector:
    """Gathers dataset-to-dataset edges and writes them as patches.

    A queue's upstreams come from both sources (its own redrive policy from sqs,
    subscriptions from sns) and possibly from other accounts' recipes, so a full
    upstreamLineage write from any one of them would erase the rest. Stale edges
    are removed one by one instead, and only those this source can vouch for: an
    edge of one of its own relationship types, from an upstream it read in full."""

    def __init__(
        self,
        *,
        report: AwsMessagingReportBase,
        graph: Optional[DataHubGraph],
        relationships: FrozenSet[LineageRelationship],
    ):
        self.report = report
        self.graph = graph
        self.relationships = {r.value for r in relationships}
        self._edges: Dict[str, Dict[str, LineageRelationship]] = defaultdict(dict)
        self._fine_grained: Dict[str, List[FineGrainedLineageClass]] = defaultdict(list)
        # Datasets this run emitted itself. Their patches stay primary: the status
        # processor treats a non-primary workunit as "status already handled", so a
        # non-primary patch would leave an owned queue without a Status aspect.
        self.owned: Set[str] = set()
        # Upstreams whose outgoing edges this run read in full. An unreadable queue
        # is owned but not complete: its edges are unknown, not gone.
        self.complete: Set[str] = set()

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
        stale = self._stale_lineage()
        downstreams = set(self._edges) | set(stale.edges) | set(stale.fine_grained)
        for downstream in sorted(downstreams):
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
            for upstream in sorted(stale.edges.get(downstream, ())):
                patch.remove_upstream_lineage(upstream)
                self.report.lineage_edges_removed += 1
            for fine_grained in stale.fine_grained.get(downstream, []):
                patch.remove_fine_grained_lineage(fine_grained)
                self.report.column_lineage_edges_removed += 1
            for mcp in patch.build():
                # A downstream this run didn't emit may be another account's queue;
                # patching it must not make this source responsible for deleting it.
                yield MetadataWorkUnit(
                    id=f"{downstream}-upstreamLineage-patch",
                    mcp_raw=mcp,
                    is_primary_source=downstream in self.owned,
                )

    def _stale_lineage(self) -> StaleLineage:
        """Edges in DataHub of a relationship this source writes, from a complete
        upstream, that this run didn't see; and on such edges, field edges from
        that upstream that this run didn't emit (a renamed or dropped field, or a
        subscription that turned raw delivery off)."""
        stale = StaleLineage()
        if self.graph is None:
            return stale
        emitted_fields = {
            (downstream_field, upstream_field)
            for lineages in self._fine_grained.values()
            for lineage in lineages
            for downstream_field in lineage.downstreams or []
            for upstream_field in lineage.upstreams or []
        }
        lineage_cache: Dict[str, Optional[UpstreamLineageClass]] = {}
        for upstream in sorted(self.complete):
            try:
                for related in self.graph.get_related_entities(
                    upstream,
                    [DOWNSTREAM_OF_RELATIONSHIP],
                    RelationshipDirection.INCOMING,
                ):
                    downstream = related.urn
                    if downstream not in lineage_cache:
                        lineage_cache[downstream] = self.graph.get_aspect(
                            downstream, UpstreamLineageClass
                        )
                    lineage = lineage_cache[downstream]
                    current = upstream in self._edges.get(downstream, {})
                    if lineage is None or not (
                        current or self._written_by_this_source(lineage, upstream)
                    ):
                        continue
                    if not current:
                        stale.edges[downstream].add(upstream)
                    stale.fine_grained[downstream].extend(
                        _stale_field_edges(lineage, upstream, emitted_fields)
                    )
            except Exception as e:
                # Removal is best effort; leaving an edge in place is safe.
                self.report.warning(
                    title="Stale lineage not checked",
                    message=(
                        "Could not read existing lineage from DataHub; stale edges "
                        "from this resource are kept this run."
                    ),
                    context=upstream,
                    exc=e,
                )
        return stale

    def _written_by_this_source(
        self, lineage: UpstreamLineageClass, upstream: str
    ) -> bool:
        return any(
            edge.dataset == upstream
            and (edge.properties or {}).get(LINEAGE_RELATIONSHIP_PROPERTY)
            in self.relationships
            for edge in lineage.upstreams
        )


def _stale_field_edges(
    lineage: UpstreamLineageClass,
    upstream: str,
    emitted: Set[Tuple[str, str]],
) -> List[FineGrainedLineageClass]:
    stale: List[FineGrainedLineageClass] = []
    for existing in lineage.fineGrainedLineages or []:
        if len(existing.downstreams or []) != 1:
            continue
        downstream_field = (existing.downstreams or [])[0]
        for upstream_field in existing.upstreams or []:
            if (
                SchemaFieldUrn.from_string(upstream_field).parent == upstream
                and (downstream_field, upstream_field) not in emitted
            ):
                stale.append(
                    FineGrainedLineageClass(
                        upstreamType=existing.upstreamType,
                        upstreams=[upstream_field],
                        downstreamType=existing.downstreamType,
                        downstreams=[downstream_field],
                        transformOperation=existing.transformOperation,
                        query=existing.query,
                    )
                )
    return stale
