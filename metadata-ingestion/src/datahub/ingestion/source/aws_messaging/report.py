from dataclasses import dataclass, field
from typing import Optional, Union

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class AwsMessagingSourceReport(StaleEntityRemovalSourceReport):
    account_id: Optional[str] = None
    region: Optional[str] = None

    queues_scanned: int = 0
    topics_scanned: int = 0
    filtered_queues: LossyList[str] = field(default_factory=LossyList)
    filtered_topics: LossyList[str] = field(default_factory=LossyList)
    queues_failed: LossyList[str] = field(default_factory=LossyList)
    topics_failed: LossyList[str] = field(default_factory=LossyList)
    subscriptions_scanned: int = 0
    lambda_functions_emitted: int = 0
    eventbridge_rules_emitted: int = 0
    eventbridge_pipes_emitted: int = 0
    lineage_edges_emitted: int = 0
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
    ) -> None:
        # A first-page failure usually means the section isn't permitted, which is
        # a valid setup. A later failure leaves the listing incomplete, and stateful
        # ingestion would soft-delete everything past that point, so it fails.
        code = aws_error_code(exc)
        if pages_fetched == 0:
            self.warning(
                title="Listing denied",
                message="The listing call failed on its first page; this section is skipped.",
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
