from collections import defaultdict
from typing import Dict, Iterable

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.constants import (
    LINEAGE_RELATIONSHIP_PROPERTY,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass
from datahub.specific.dataset import DatasetPatchBuilder


class DatasetLineageCollector:
    """Gathers dataset-to-dataset edges from every pass and writes them as patches.

    A queue's upstreams come from several passes (its own redrive policy, SNS
    subscriptions) and possibly from another account's recipe, so a full
    upstreamLineage write from any one of them would erase the rest."""

    def __init__(self, report: AwsMessagingSourceReport):
        self.report = report
        self._edges: Dict[str, Dict[str, LineageRelationship]] = defaultdict(dict)

    def add(
        self, *, upstream: str, downstream: str, relationship: LineageRelationship
    ) -> None:
        if upstream != downstream:
            self._edges[downstream].setdefault(upstream, relationship)

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
            for mcp in patch.build():
                yield MetadataWorkUnit(
                    id=f"{downstream}-upstreamLineage-patch", mcp_raw=mcp
                )
