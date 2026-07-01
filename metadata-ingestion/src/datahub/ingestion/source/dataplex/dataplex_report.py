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


# Alias for consistency with other sources
DataplexSourceReport = DataplexReport
