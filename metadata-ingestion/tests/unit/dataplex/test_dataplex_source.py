"""Unit tests for Dataplex source orchestration logic."""

from unittest.mock import Mock, patch

from google.api_core import exceptions

from datahub.ingestion.source.dataplex.dataplex import DataplexSource


def test_process_project_wraps_entities_and_lineage_workunits() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.entries_processor.process_project.return_value = [Mock()]
    source.config = Mock(include_lineage=True)
    source.lineage_extractor = Mock()
    source.report = Mock()

    lineage_workunit = Mock()
    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.auto_workunit",
            return_value=[Mock()],
        ) as auto_workunit_mock,
        patch.object(
            source, "_get_lineage_workunits", return_value=[lineage_workunit]
        ) as lineage_workunits_mock,
    ):
        workunits = list(source._process_project("project-1"))

    assert len(workunits) == 2
    auto_workunit_mock.assert_called_once()
    source.entries_processor.process_project.assert_called_once_with("project-1")
    lineage_workunits_mock.assert_called_once_with("project-1")


def test_process_project_reports_google_api_failure() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.entries_processor.process_project.return_value = [Mock()]
    source.config = Mock(include_lineage=False)
    source.lineage_extractor = None
    source.report = Mock()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit",
        side_effect=exceptions.GoogleAPICallError("boom"),
    ):
        workunits = list(source._process_project("project-1"))

    assert workunits == []
    source.report.report_failure.assert_called_once()
