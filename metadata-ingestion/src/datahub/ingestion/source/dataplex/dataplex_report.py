"""Reporting for Dataplex source."""

from dataclasses import dataclass, field

from datahub.ingestion.source.dataplex.dataplex_entries import DataplexEntriesReport
from datahub.ingestion.source.dataplex.dataplex_glossary import DataplexGlossaryReport
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class DataplexReport(StaleEntityRemovalSourceReport):
    """Report for Dataplex ingestion."""

    entries_report: DataplexEntriesReport = field(default_factory=DataplexEntriesReport)
    lineage_report: DataplexLineageReport = field(default_factory=DataplexLineageReport)
    glossary_report: DataplexGlossaryReport = field(
        default_factory=DataplexGlossaryReport
    )

    # Export extraction method (extraction_method: export) observability.
    # Failed jobs / aborted blob reads are additionally reported as source
    # failures, which suppresses stale-entity soft-deletion for the run.
    export_jobs_submitted: int = 0
    export_jobs_succeeded: int = 0
    export_jobs_failed: int = 0
    export_blobs_read: int = 0
    export_blobs_read_failed: int = 0
    export_entries_read: int = 0
    export_malformed_lines_skipped: int = 0
    export_locations_with_no_output: int = 0

    def is_export_partial(self) -> bool:
        """True when any entity may be missing from this run's export stream.

        Locations with no output count as potentially partial: even when the
        location is legitimately empty (reported as a warning, not a failure),
        this run's stream cannot prove its previous entities still exist.
        """
        return (
            self.export_jobs_failed > 0
            or self.export_blobs_read_failed > 0
            or self.export_locations_with_no_output > 0
        )


# Alias for consistency with other sources
DataplexSourceReport = DataplexReport
