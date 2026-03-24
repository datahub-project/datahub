"""Unit tests for Dataplex report delegation behavior."""

from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport


def test_report_entry_group_and_entry_delegate_to_entries_report() -> None:
    report = DataplexReport()

    report.report_entry_group("projects/p/locations/us/entryGroups/g1", filtered=True)
    report.report_entry(
        entry_name="projects/p/locations/us/entryGroups/g1/entries/e1",
        filtered_missing_fqn=False,
        filtered_fqn=False,
        filtered_name=False,
    )

    assert report.entries_report.entry_groups_seen == 1
    assert report.entries_report.entry_groups_filtered == 1
    assert report.entries_report.entries_seen == 1
    assert report.entries_report.entries_processed == 1
