"""ThoughtSpot connector ingestion report."""

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.perf_timer import PerfTimer


@dataclass
class ThoughtSpotReport(StaleEntityRemovalSourceReport):
    """Ingestion report for ThoughtSpot source."""

    workspaces_scanned: int = 0
    liveboards_scanned: int = 0
    answers_scanned: int = 0
    datasets_scanned: int = 0

    api_errors: int = 0

    # Per-batch drop counters for malformed TS API payloads. Aggregated rather
    # than per-item so a noisy batch doesn't flood the report with hundreds of
    # near-identical warnings.
    malformed_liveboards_dropped: int = 0
    malformed_answers_dropped: int = 0
    malformed_logical_tables_dropped: int = 0
    malformed_visualizations_dropped: int = 0
    malformed_source_tables_dropped: int = 0
    malformed_column_sources_dropped: int = 0

    # SQL parser telemetry — mirrors Mode connector field names so
    # operators reading multiple connector reports see consistent
    # naming. ``num_sql_parsed`` is the denominator;
    # ``num_sql_parser_failures`` is the sum of table_error +
    # column_error. ``sql_parsing_total_sec`` aggregates wall-clock
    # so the operator can spot when SQL parsing dominates run time.
    num_sql_parsed: int = 0
    num_sql_parser_success: int = 0
    num_sql_parser_failures: int = 0
    num_sql_parser_table_error: int = 0
    num_sql_parser_column_error: int = 0
    sql_parsing_total_sec: float = 0.0

    # Per-reason breakdown for ``_resolve_external_upstream`` returning
    # ``None``. Previously only the "connection not found" case was
    # counted (and not even surfaced in the report), so every other
    # reason a Table's cross-platform lineage failed to resolve was
    # completely silent — no log line, no counter, no warning. These
    # make each reason visible in the run report.
    num_external_lineage_skipped_internal: int = 0
    num_external_lineage_unresolvable_connection: int = 0
    num_external_lineage_skipped_unmapped_connection_type: int = 0
    num_external_lineage_skipped_missing_database: int = 0

    # Per-phase wall-clock timers. At 10K-dashboard scale the dominant
    # cost is TML enrichment inside ``liveboard_extraction_time`` —
    # exposing per-phase wall-clock makes that obvious in the run report
    # instead of forcing operators to instrument it themselves.
    # Each timer is wrapped via ``with self.report.<phase>_time:`` blocks
    # in ``ThoughtSpotSource.get_workunits_internal``.
    workspace_extraction_time: PerfTimer = field(default_factory=PerfTimer)
    liveboard_extraction_time: PerfTimer = field(default_factory=PerfTimer)
    answer_extraction_time: PerfTimer = field(default_factory=PerfTimer)
    dataset_extraction_time: PerfTimer = field(default_factory=PerfTimer)
    usage_emission_time: PerfTimer = field(default_factory=PerfTimer)

    def report_workspace_scanned(self) -> None:
        """Increment workspace scan counter."""
        self.workspaces_scanned += 1

    def report_liveboard_scanned(self) -> None:
        """Increment liveboard scan counter."""
        self.liveboards_scanned += 1

    def report_answer_scanned(self) -> None:
        """Increment answer scan counter."""
        self.answers_scanned += 1

    def report_dataset_scanned(self) -> None:
        """Increment dataset scan counter."""
        self.datasets_scanned += 1

    def report_api_error(self) -> None:
        """Increment API error counter."""
        self.api_errors += 1

    def report_external_lineage_skipped_internal(self) -> None:
        """Table has no federated connection to resolve (in-memory/FALCON/
        DEFAULT data, or an unrecognized table-level type) — expected, not
        a failure."""
        self.num_external_lineage_skipped_internal += 1

    def report_external_lineage_unresolvable_connection(self) -> None:
        """Table's connection id isn't in the connection lookup (stale
        reference, deleted connection, or no read access)."""
        self.num_external_lineage_unresolvable_connection += 1

    def report_external_lineage_skipped_unmapped_connection_type(self) -> None:
        """Table resolved to a connection whose ``data_source_type`` isn't
        in the platform map."""
        self.num_external_lineage_skipped_unmapped_connection_type += 1

    def report_external_lineage_skipped_missing_database(self) -> None:
        """Table resolved to a known platform but TS didn't return enough
        physical database/table information to build an upstream URN."""
        self.num_external_lineage_skipped_missing_database += 1
