from collections import defaultdict
from typing import Dict, FrozenSet, Iterable, Optional, Set

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
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder


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

    def workunits(self) -> Iterable[MetadataWorkUnit]:
        stale = self._stale_edges()
        for downstream in sorted(set(self._edges) | set(stale)):
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
            for upstream in sorted(stale.get(downstream, ())):
                patch.remove_upstream_lineage(upstream)
                self.report.lineage_edges_removed += 1
            for mcp in patch.build():
                # A downstream this run didn't emit may be another account's queue;
                # patching it must not make this source responsible for deleting it.
                yield MetadataWorkUnit(
                    id=f"{downstream}-upstreamLineage-patch",
                    mcp_raw=mcp,
                    is_primary_source=downstream in self.owned,
                )

    def _stale_edges(self) -> Dict[str, Set[str]]:
        """Downstream -> upstreams to remove: edges in DataHub of a relationship
        this source writes, from a complete upstream, that this run didn't see."""
        stale: Dict[str, Set[str]] = defaultdict(set)
        if self.graph is None:
            return stale
        lineage_cache: Dict[str, Optional[UpstreamLineageClass]] = {}
        for upstream in sorted(self.complete):
            try:
                for related in self.graph.get_related_entities(
                    upstream,
                    [DOWNSTREAM_OF_RELATIONSHIP],
                    RelationshipDirection.INCOMING,
                ):
                    downstream = related.urn
                    if upstream in self._edges.get(downstream, {}):
                        continue
                    if downstream not in lineage_cache:
                        lineage_cache[downstream] = self.graph.get_aspect(
                            downstream, UpstreamLineageClass
                        )
                    if self._written_by_this_source(
                        lineage_cache[downstream], upstream
                    ):
                        stale[downstream].add(upstream)
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
        self, lineage: Optional[UpstreamLineageClass], upstream: str
    ) -> bool:
        if lineage is None:
            return False
        return any(
            edge.dataset == upstream
            and (edge.properties or {}).get(LINEAGE_RELATIONSHIP_PROPERTY)
            in self.relationships
            for edge in lineage.upstreams
        )
