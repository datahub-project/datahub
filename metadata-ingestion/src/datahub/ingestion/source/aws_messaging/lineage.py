from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from requests.exceptions import RequestException

from datahub.configuration.common import GraphError
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
from datahub.utilities.urns.error import InvalidUrnError
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

    def __init__(self, graph: Optional[DataHubGraph], report: AwsMessagingReportBase):
        self.graph = graph
        self.report = report
        self.fetched: Dict[str, SchemaMetadataClass] = {}
        # Schemas Glue couldn't give this run; DataHub's copy may be stale, so
        # these count as unknown rather than falling back.
        self.unreadable: Set[str] = set()
        self._from_graph: Dict[str, Optional[SchemaMetadataClass]] = {}

    def get(self, urn: str) -> Optional[SchemaMetadataClass]:
        if urn in self.fetched:
            return self.fetched[urn]
        if urn in self.unreadable or self.graph is None:
            return None
        if urn not in self._from_graph:
            try:
                self._from_graph[urn] = self.graph.get_schema_metadata(urn)
            except (GraphError, RequestException) as e:
                # Treated as no schema: the hop's field edges are left as they are.
                self.report.warning(
                    title="Schema not read from DataHub",
                    message="Could not read a dataset's schema from DataHub; its field lineage is left unchanged.",
                    context=urn,
                    exc=e,
                )
                self._from_graph[urn] = None
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
        # (upstream, downstream) hops whose field lineage this run worked out, and
        # those it skipped (e.g. no schema on one side). A skipped hop's field edges
        # are unknown, not gone.
        self._field_hops_read: Set[Tuple[str, str]] = set()
        self._field_hops_unknown: Set[Tuple[str, str]] = set()
        # Datasets this run emitted itself. Their patches stay primary: the status
        # processor treats a non-primary workunit as "status already handled", so a
        # non-primary patch would leave an owned queue without a Status aspect.
        self.owned: Set[str] = set()
        # Upstreams whose outgoing edges this run read in full. An unreadable queue
        # is owned but not complete: its edges are unknown, not gone.
        self.complete: Set[str] = set()
        # Relationships whose edges into owned datasets were all seen this run,
        # checked from the downstream side because the upstream isn't this
        # source's (an S3 dataset, say). Each maps to a check of (downstream,
        # upstream): only an edge this run could have seen can be called gone, so
        # not one from another account's bucket, say.
        self.downstream_relationships: Dict[str, Callable[[str, str], bool]] = {}
        # Upstreams with an edge this run couldn't resolve (its other end wasn't
        # ingested). The edge may still exist in AWS, so it must not read as gone.
        self._unknown: Set[str] = set()

    def mark_unknown(self, upstream: str) -> None:
        self._unknown.add(upstream)

    def add(
        self, *, upstream: str, downstream: str, relationship: LineageRelationship
    ) -> None:
        if upstream != downstream:
            self._edges[downstream].setdefault(upstream, relationship)

    def add_fine_grained(
        self,
        *,
        upstream: str,
        downstream: str,
        lineages: List[FineGrainedLineageClass],
    ) -> None:
        """Field lineage for a hop; an empty list means the hop has none."""
        self._field_hops_read.add((upstream, downstream))
        self._fine_grained[downstream].extend(lineages)
        self.report.column_lineage_edges_emitted += len(lineages)

    def fine_grained_unknown(self, *, upstream: str, downstream: str) -> None:
        self._field_hops_unknown.add((upstream, downstream))

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
        subscription that turned raw delivery off). Field edges are only retracted
        with their table edge or on a hop whose field lineage this run worked out."""
        stale = StaleLineage()
        # Same guard as StaleEntityRemovalHandler: a failed run (e.g. a listing
        # aborted mid-pagination) didn't see every edge, so none can be called gone.
        if self.graph is None or self.report.failures:
            return stale
        emitted_fields = {
            (downstream_field, upstream_field)
            for lineages in self._fine_grained.values()
            for lineage in lineages
            for downstream_field in lineage.downstreams or []
            for upstream_field in lineage.upstreams or []
        }
        lineage_cache: Dict[str, Optional[UpstreamLineageClass]] = {}
        field_checks: List[Tuple[str, str, UpstreamLineageClass]] = []
        for upstream in sorted(self.complete - self._unknown):
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
                    if not current or self._field_lineage_read(upstream, downstream):
                        field_checks.append((upstream, downstream, lineage))
            except (GraphError, RequestException) as e:
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
        if self.downstream_relationships:
            self._stale_into_owned(stale, lineage_cache)
        # Outside the graph-read loop so a malformed field edge can't block
        # table-level removal for its upstream.
        for upstream, downstream, lineage in field_checks:
            stale.fine_grained[downstream].extend(
                self._stale_field_edges(lineage, upstream, emitted_fields)
            )
        return stale

    def _field_lineage_read(self, upstream: str, downstream: str) -> bool:
        hop = (upstream, downstream)
        return hop in self._field_hops_read and hop not in self._field_hops_unknown

    def _stale_field_edges(
        self,
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
                try:
                    parent = SchemaFieldUrn.from_string(upstream_field).parent
                except InvalidUrnError as e:
                    # Another writer's malformed field edge; leave it in place.
                    self.report.warning(
                        title="Stale field lineage not checked",
                        message="An existing field lineage edge has an unparseable upstream urn; it is kept.",
                        context=f"{downstream_field} <- {upstream_field}",
                        exc=e,
                    )
                    continue
                if (
                    parent == upstream
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

    def _stale_into_owned(
        self,
        stale: StaleLineage,
        lineage_cache: Dict[str, Optional[UpstreamLineageClass]],
    ) -> None:
        assert self.graph is not None
        for downstream in sorted(self.owned):
            try:
                if downstream not in lineage_cache:
                    lineage_cache[downstream] = self.graph.get_aspect(
                        downstream, UpstreamLineageClass
                    )
                lineage = lineage_cache[downstream]
                for edge in lineage.upstreams if lineage else []:
                    relationship = (edge.properties or {}).get(
                        LINEAGE_RELATIONSHIP_PROPERTY
                    )
                    if (
                        relationship in self.downstream_relationships
                        and self.downstream_relationships[relationship](
                            downstream, edge.dataset
                        )
                        and edge.dataset not in self._edges.get(downstream, {})
                    ):
                        stale.edges[downstream].add(edge.dataset)
            except (GraphError, RequestException) as e:
                self.report.warning(
                    title="Stale lineage not checked",
                    message=(
                        "Could not read existing lineage from DataHub; stale edges "
                        "into this resource are kept this run."
                    ),
                    context=downstream,
                    exc=e,
                )

    def _written_by_this_source(
        self, lineage: UpstreamLineageClass, upstream: str
    ) -> bool:
        return any(
            edge.dataset == upstream
            and (edge.properties or {}).get(LINEAGE_RELATIONSHIP_PROPERTY)
            in self.relationships
            for edge in lineage.upstreams
        )
