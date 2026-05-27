from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class KinesisSourceReport(StaleEntityRemovalSourceReport):
    """Custom report for Kinesis ingestion.

    Tracks per-resource scan counts and recoverable-error contexts so users can
    diagnose ingestion outcomes from the report summary alone.
    """

    # Entity counts
    streams_scanned: int = 0
    delivery_streams_scanned: int = 0

    # Filtered entities (pattern-based)
    filtered_streams: LossyList[str] = field(default_factory=LossyList)
    filtered_delivery_streams: LossyList[str] = field(default_factory=LossyList)

    # Recoverable error contexts
    streams_failed: LossyList[str] = field(default_factory=LossyList)
    delivery_streams_failed: LossyList[str] = field(default_factory=LossyList)
    unsupported_destinations: LossyList[str] = field(default_factory=LossyList)
    # Distinct from unsupported_destinations: the handler MATCHED but couldn't
    # build a URN (e.g. malformed BucketARN, unparseable JDBC URL, missing
    # Snowflake db/schema/table). Records "<delivery_stream>: <handler_name>"
    # so users can debug "Firehose DataJob exists but no lineage edge".
    destination_parse_failures: LossyList[str] = field(default_factory=LossyList)
    schema_resolution_failures: LossyList[str] = field(default_factory=LossyList)

    # Stateful ingestion config tracking
    include_streams: bool = False
    include_firehose: bool = False
    glue_schema_registry_enabled: bool = False

    # Identity resolved at source init via sts:GetCallerIdentity. Populated only when the
    # user did not set platform_instance explicitly; lets users see which account the
    # connector actually hit when diagnosing "zero streams returned" scenarios.
    account_id: Optional[str] = None
    platform_instance_resolved: Optional[str] = None

    # Firehose SchemaConfiguration -> Glue table lineage edges.
    # `firehose_glue_schema_lineage_emitted` counts edges successfully added to
    # the DataJob's inputDatasets. `firehose_glue_schema_skipped` records
    # "<delivery_stream_name>: <reason>" for any SchemaConfiguration that was
    # present but missing required fields (CatalogId / DatabaseName / TableName)
    # — surfaces strict-mode skips so users can spot real-AWS shapes our checks
    # may be too strict for.
    firehose_glue_schema_lineage_emitted: int = 0
    firehose_glue_schema_skipped: LossyList[str] = field(default_factory=LossyList)

    # GSR naming-convention probe misses (use_naming_convention=true, stream name
    # didn't match any schema in the registry). This is the EXPECTED outcome for
    # streams without GSR schemas, not an error — surfaced separately from
    # schema_resolution_failures (which is for real errors: AccessDenied,
    # ValidationException, explicit-mapping resolution failures, etc.).
    gsr_naming_convention_misses: LossyList[str] = field(default_factory=LossyList)

    def report_stream_scanned(self) -> None:
        self.streams_scanned += 1

    def report_stream_filtered(self, stream_name: str) -> None:
        self.filtered_streams.append(stream_name)

    def report_delivery_stream_scanned(self) -> None:
        self.delivery_streams_scanned += 1

    def report_delivery_stream_filtered(self, delivery_stream_name: str) -> None:
        self.filtered_delivery_streams.append(delivery_stream_name)

    def report_stream_failed(self, stream_name: str, reason: str) -> None:
        self.streams_failed.append(f"{stream_name}: {reason}")

    def report_delivery_stream_failed(
        self, delivery_stream_name: str, reason: str
    ) -> None:
        self.delivery_streams_failed.append(f"{delivery_stream_name}: {reason}")

    def report_unsupported_destination(
        self, destination_type: str, delivery_stream_name: str
    ) -> None:
        self.unsupported_destinations.append(
            f"{delivery_stream_name}: type={destination_type}"
        )

    def report_destination_parse_failure(
        self, delivery_stream_name: str, handler_name: str
    ) -> None:
        self.destination_parse_failures.append(
            f"{delivery_stream_name}: handler={handler_name}"
        )

    def report_schema_resolution_failure(self, stream_name: str, reason: str) -> None:
        self.schema_resolution_failures.append(f"{stream_name}: {reason}")

    def report_firehose_glue_schema_emitted(self) -> None:
        self.firehose_glue_schema_lineage_emitted += 1

    def report_firehose_glue_schema_skipped(
        self, delivery_stream_name: str, reason: str
    ) -> None:
        self.firehose_glue_schema_skipped.append(f"{delivery_stream_name}: {reason}")

    def report_gsr_naming_convention_miss(self, stream_name: str) -> None:
        self.gsr_naming_convention_misses.append(stream_name)
