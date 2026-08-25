"""Reporting for Dataplex source."""

from dataclasses import dataclass, field
from typing import List, Optional

from datahub.ingestion.source.dataplex.dataplex_entries import DataplexEntriesReport
from datahub.ingestion.source.dataplex.dataplex_glossary import DataplexGlossaryReport
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class ExportJobInfo:
    """Per-job status for ``extraction_method: export`` (submit mode).

    Updated on every poll cycle so a long-running export shows visible
    progress in the periodic report instead of appearing hung.
    """

    location: str
    job_id: str
    output_path: str
    state: Optional[str] = None
    elapsed_seconds: int = 0
    entries_read: int = 0


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
    export_jobs: List[ExportJobInfo] = field(default_factory=list)

    def is_export_partial(self) -> bool:
        """True when any entity may be missing from this run's export stream.

        Observational only: stale-entity soft-deletion is gated solely by
        ``report.failure()``, not by this helper. The cases can diverge — a
        legitimately empty location is only a warning (deletion proceeds)
        yet still counts as potentially partial here, because this run's
        stream cannot prove that location's previous entities still exist.
        """
        return (
            self.export_jobs_failed > 0
            or self.export_blobs_read_failed > 0
            or self.export_locations_with_no_output > 0
        )


# Alias for consistency with other sources
DataplexSourceReport = DataplexReport
