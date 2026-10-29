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
    # True when tag:GetResources was unavailable and tags were read per resource.
    tags_read_per_resource: bool = False
    schemas_attached: int = 0
    # "<name>: <reason>" for schema lookups that failed (not naming-convention misses).
    schema_resolution_failures: LossyList[str] = field(default_factory=LossyList)
    schema_naming_convention_misses: LossyList[str] = field(default_factory=LossyList)
    column_lineage_edges_emitted: int = 0
    lambda_functions_emitted: int = 0
    eventbridge_rules_emitted: int = 0
    eventbridge_pipes_emitted: int = 0
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
        # A first-page failure usually means the call isn't permitted, which is a
        # valid setup. A later failure leaves the listing incomplete, and stateful
        # ingestion would soft-delete everything past that point, so it fails.
        # Optional listings (lineage helpers, not the queues or topics themselves)
        # only warn: a failure there mustn't block stale removal of the datasets.
        code = aws_error_code(exc)
        if optional and pages_fetched > 0:
            self.warning(
                title="Listing incomplete",
                message="An optional listing failed partway through; some lineage is missing this run.",
                context=f"{api_label} returned {code} after {pages_fetched} page(s)",
                exc=exc,
            )
        elif pages_fetched == 0:
            self.warning(
                title="Listing denied",
                message="The listing call failed on its first page; this part is skipped.",
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
    filtered_topics: LossyList[str] = field(default_factory=LossyList)
    topics_failed: LossyList[str] = field(default_factory=LossyList)
