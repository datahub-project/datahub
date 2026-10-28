from collections import defaultdict
from typing import Dict, Iterable, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.constants import (
    LINEAGE_RELATIONSHIP_PROPERTY,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass
from datahub.specific.dataset import DatasetPatchBuilder


class DatasetLineageCollector:
    """Gathers dataset-to-dataset edges and writes them as patches.

    A queue's upstreams come from both sources (its own redrive policy from sqs,
    subscriptions from sns) and possibly from other accounts' recipes, so a full
    upstreamLineage write from any one of them would erase the rest."""

    def __init__(self, report: AwsMessagingReportBase):
        self.report = report
        self._edges: Dict[str, Dict[str, LineageRelationship]] = defaultdict(dict)
        # Datasets this run emitted itself. Their patches stay primary: the status
        # processor treats a non-primary workunit as "status already handled", so a
        # non-primary patch would leave an owned queue without a Status aspect.
        self.owned: Set[str] = set()

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
                # A downstream this run didn't emit may be another account's queue;
                # patching it must not make this source responsible for deleting it.
                yield MetadataWorkUnit(
                    id=f"{downstream}-upstreamLineage-patch",
                    mcp_raw=mcp,
                    is_primary_source=downstream in self.owned,
                )
