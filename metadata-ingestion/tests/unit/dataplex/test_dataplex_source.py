"""Unit tests for Dataplex source orchestration logic."""

from unittest.mock import Mock, patch

from google.api_core import exceptions

from datahub.ingestion.source.dataplex.dataplex import DataplexSource
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport


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


def test_source_init_wires_clients_processor_and_lineage_extractor() -> None:
    config = DataplexConfig(
        project_ids=["project-1"],
        include_lineage=True,
        stateful_ingestion={"enabled": True},
        enable_stateful_lineage_ingestion=True,
    )
    ctx = Mock()
    ctx.pipeline_name = "pipeline"
    ctx.run_id = "run-1"

    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.StatefulIngestionSourceBase.__init__",
            autospec=True,
            side_effect=lambda source, _config, _ctx: setattr(source, "ctx", _ctx),
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.CatalogServiceClient"
        ) as catalog_client_cls,
        patch("datahub.ingestion.source.dataplex.dataplex.LineageClient"),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.RedundantLineageRunSkipHandler"
        ) as redundant_handler_cls,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.DataplexLineageExtractor"
        ) as lineage_extractor_cls,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.DataplexEntriesProcessor"
        ) as entries_processor_cls,
    ):
        source = DataplexSource(ctx, config)

    assert isinstance(source.report, DataplexReport)
    catalog_client_cls.assert_called_once()
    redundant_handler_cls.assert_called_once()
    lineage_extractor_cls.assert_called_once()
    entries_processor_cls.assert_called_once()


def test_source_init_without_lineage_sets_lineage_members_to_none() -> None:
    config = DataplexConfig(
        project_ids=["project-1"],
        include_lineage=False,
        enable_stateful_lineage_ingestion=False,
    )
    ctx = Mock()

    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.StatefulIngestionSourceBase.__init__",
            return_value=None,
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.CatalogServiceClient"
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.LineageClient"
        ) as lineage_cls,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.DataplexLineageExtractor"
        ) as lineage_extractor_cls,
    ):
        source = DataplexSource(ctx, config)

    lineage_cls.assert_not_called()
    lineage_extractor_cls.assert_not_called()
    assert source.lineage_client is None
    assert source.lineage_extractor is None


def test_test_connection_success() -> None:
    with patch(
        "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.CatalogServiceClient"
    ) as catalog_client_cls:
        catalog_client = catalog_client_cls.return_value
        catalog_client.list_entry_groups.return_value = [Mock()]
        report = DataplexSource.test_connection({"project_ids": ["project-1"]})
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_test_connection_handles_google_api_error() -> None:
    with patch(
        "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.CatalogServiceClient",
        side_effect=exceptions.GoogleAPICallError("boom"),
    ):
        report = DataplexSource.test_connection({"project_ids": ["project-1"]})
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is not None
    assert "Failed to connect to Dataplex" in report.basic_connectivity.failure_reason


def test_test_connection_handles_unexpected_error() -> None:
    with patch(
        "datahub.ingestion.source.dataplex.dataplex.DataplexConfig.model_validate",
        side_effect=RuntimeError("unexpected"),
    ):
        report = DataplexSource.test_connection({"project_ids": ["project-1"]})
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is not None
    assert "Unexpected error: unexpected" in report.basic_connectivity.failure_reason


def test_get_report_returns_source_report_instance() -> None:
    source = object.__new__(DataplexSource)
    source.report = DataplexReport()
    assert source.get_report() is source.report


def test_get_workunit_processors_includes_stale_entity_processor() -> None:
    source = object.__new__(DataplexSource)
    source.config = Mock()
    source.ctx = Mock()

    stale_processor = Mock()
    stale_handler = Mock()
    stale_handler.workunit_processor = stale_processor

    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.StatefulIngestionSourceBase.get_workunit_processors",
            return_value=[None],
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.StaleEntityRemovalHandler.create",
            return_value=stale_handler,
        ),
    ):
        processors = source.get_workunit_processors()

    assert processors == [None, stale_processor]


def test_get_workunits_internal_iterates_all_projects() -> None:
    source = object.__new__(DataplexSource)
    source.config = Mock()
    source.config.project_ids = ["project-1", "project-2"]

    wu1, wu2 = Mock(), Mock()
    with patch.object(
        source, "_process_project", side_effect=[[wu1], [wu2]]
    ) as process:
        workunits = list(source.get_workunits_internal())

    assert workunits == [wu1, wu2]
    assert process.call_count == 2


def test_get_lineage_workunits_handles_empty_entries() -> None:
    source = object.__new__(DataplexSource)
    source.lineage_extractor = Mock()
    source.entry_data_by_project = {}
    source.report = Mock()

    assert list(source._get_lineage_workunits("project-1")) == []


def test_get_lineage_workunits_yields_from_extractor() -> None:
    source = object.__new__(DataplexSource)
    source.lineage_extractor = Mock()
    source.entry_data_by_project = {
        "project-1": {
            EntryDataTuple(
                dataplex_entry_short_name="entry-1",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-1",
                dataplex_entry_fqn="bigquery:project-1.ds.table",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="project-1.ds.table",
            )
        }
    }
    source.report = Mock()

    lineage_wu = Mock()
    source.lineage_extractor.get_lineage_workunits.return_value = [lineage_wu]

    assert list(source._get_lineage_workunits("project-1")) == [lineage_wu]


def test_get_lineage_workunits_reports_failure_on_exception() -> None:
    source = object.__new__(DataplexSource)
    source.lineage_extractor = Mock()
    source.entry_data_by_project = {
        "project-1": {
            EntryDataTuple(
                dataplex_entry_short_name="entry-1",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-1",
                dataplex_entry_fqn="bigquery:project-1.ds.table",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="project-1.ds.table",
            )
        }
    }
    source.report = Mock()
    source.lineage_extractor.get_lineage_workunits.side_effect = RuntimeError("boom")

    assert list(source._get_lineage_workunits("project-1")) == []
    source.report.report_failure.assert_called_once()


def test_create_uses_model_validate_and_constructs_source() -> None:
    ctx = Mock()
    config = DataplexConfig(project_ids=["project-1"])
    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.DataplexConfig.model_validate",
            return_value=config,
        ) as validate_mock,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.DataplexSource.__init__",
            return_value=None,
        ) as init_mock,
    ):
        source = DataplexSource.create({"project_ids": ["project-1"]}, ctx)
    validate_mock.assert_called_once()
    init_mock.assert_called_once()
    assert isinstance(source, DataplexSource)
