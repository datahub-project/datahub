from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Union

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class AwsMessagingReportBase(StaleEntityRemovalSourceReport):
    account_id: Optional[str] = None
    region: Optional[str] = None
    lineage_edges_emitted: int = 0
    lineage_edges_removed: int = 0
    # True when tag:GetResources was unavailable and tags were read per resource.
    tags_read_per_resource: bool = False
    schemas_attached: int = 0
    # "<name>: <reason>" for schema lookups that failed (not naming-convention misses).
    schema_resolution_failures: LossyList[str] = field(default_factory=LossyList)
    schema_naming_convention_misses: LossyList[str] = field(default_factory=LossyList)
    column_lineage_edges_emitted: int = 0
    column_lineage_edges_removed: int = 0
    # Reason -> count for edges that got table-level lineage only.
    column_lineage_skipped: Dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    # "<from> -> <to>: <reason>" for edges left out because one end could not
    # be resolved to an entity that exists.
    lineage_edges_skipped: LossyList[str] = field(default_factory=LossyList)

    def report_edge_skipped(self, upstream: str, downstream: str, reason: str) -> None:
        self.lineage_edges_skipped.append(f"{upstream} -> {downstream}: {reason}")

    def report_listing_failure(
        self,
        *,
        pages_fetched: int,
        exc: Union[ClientError, BotoCoreError],
        api_label: str,
        optional: bool = False,
    ) -> None:
        # Optional listings (lineage helpers, not the queues or topics themselves)
        # only warn: a denied helper is a valid setup, and a failure there mustn't
        # block stale removal of the datasets. The primary listing is the source's
        # reason to run, so any failure there fails the run: a denied first page
        # would otherwise end green with nothing ingested, and a later failure
        # leaves stateful ingestion soft-deleting everything past that point.
        code = aws_error_code(exc)
        if optional and pages_fetched > 0:
            self.warning(
                title="Listing incomplete",
                message="An optional listing failed partway through; some lineage is missing this run.",
                context=f"{api_label} returned {code} after {pages_fetched} page(s)",
                exc=exc,
            )
        elif optional:
            self.warning(
                title="Listing denied",
                message="The listing call failed on its first page; this part is skipped.",
                context=f"{api_label} returned {code}",
                exc=exc,
            )
        elif pages_fetched == 0:
            self.failure(
                title="Listing denied",
                message="The listing call failed on its first page, so nothing was ingested.",
                context=f"{api_label} returned {code}",
                exc=exc,
            )
        else:
            self.failure(
                title="Listing aborted mid-pagination",
                message=(
                    "Listing failed partway through; later resources were not scanned. "
                    "Re-run before relying on stale-entity removal."
                ),
                context=f"{api_label} returned {code} after {pages_fetched} page(s)",
                exc=exc,
            )


@dataclass
class SqsSourceReport(AwsMessagingReportBase):
    queues_scanned: int = 0
    filtered_queues: LossyList[str] = field(default_factory=LossyList)
    queues_failed: LossyList[str] = field(default_factory=LossyList)


@dataclass
class SnsSourceReport(AwsMessagingReportBase):
    topics_scanned: int = 0
    subscriptions_scanned: int = 0
    subscription_attribute_calls: int = 0
    filtered_topics: LossyList[str] = field(default_factory=LossyList)
    topics_failed: LossyList[str] = field(default_factory=LossyList)
