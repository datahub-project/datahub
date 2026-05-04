"""Unit tests for Dataplex source orchestration logic."""

from contextlib import nullcontext
from unittest.mock import Mock, patch

from google.api_core import exceptions

from datahub.ingestion.source.dataplex.dataplex import DataplexSource
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport


def test_get_workunits_internal_wraps_project_processing() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.entries_processor.process_entries.return_value = iter([Mock()])
    source.config = Mock()
    source.config.project_ids = ["project-1"]
    source._project_ids = ["project-1"]
    source.config.include_lineage = False
    source.config.include_glossaries = False
    source.lineage_extractor = None
    source.glossary_processor = None
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.auto_workunit",
            return_value=[Mock()],
        ) as auto_workunit_mock,
    ):
        workunits = list(source.get_workunits_internal())

    assert len(workunits) == 1
    auto_workunit_mock.assert_called_once()
    source.entries_processor.process_entries.assert_called_once_with(
        project_ids=["project-1"],
        max_workers=source.config.max_workers_entries,
    )


def test_get_workunits_internal_reports_project_google_api_failure() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.project_ids = ["project-1"]
    source._project_ids = ["project-1"]
    source.config.include_lineage = False
    source.config.include_glossaries = False
    source.lineage_extractor = None
    source.glossary_processor = None
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit",
        side_effect=exceptions.GoogleAPICallError("boom"),
    ):
        workunits = list(source.get_workunits_internal())

    assert workunits == []
    source.report.warning.assert_called_once()


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
        patch(
            "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.BusinessGlossaryServiceClient"
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex._resolve_project_numbers",
            return_value={"project-1": "123456789"},
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.google.auth.default",
            return_value=(Mock(), "project-1"),
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.google.auth.transport.requests.AuthorizedSession"
        ),
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
        patch(
            "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.BusinessGlossaryServiceClient"
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex._resolve_project_numbers",
            return_value={"project-1": "123456789"},
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.google.auth.default",
            return_value=(Mock(), "project-1"),
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.google.auth.transport.requests.AuthorizedSession"
        ),
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
    source.entries_processor = Mock()
    source.entries_processor.process_entries.return_value = iter([])
    source.config = Mock()
    source.config.project_ids = ["project-1", "project-2"]
    source._project_ids = ["project-1", "project-2"]
    source.config.include_lineage = True
    source.config.include_glossaries = False
    source.config.lineage_locations = ["us-central1"]
    source.lineage_extractor = Mock()
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = []
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    wu1 = Mock()
    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.auto_workunit",
            return_value=[wu1],
        ) as auto_workunit_mock,
        patch.object(
            source.lineage_extractor, "get_lineage_workunits", return_value=[]
        ),
    ):
        workunits = list(source.get_workunits_internal())

    assert workunits == [wu1]
    assert auto_workunit_mock.call_count == 1
    source.entries_processor.process_entries.assert_called_once_with(
        project_ids=["project-1", "project-2"],
        max_workers=source.config.max_workers_entries,
    )
    # entries stage + lineage stage (lineage returns early because entry_data is empty)
    assert source.report.new_stage.call_count == 2
    source.lineage_extractor.get_lineage_workunits.assert_not_called()
    source.report.new_stage.assert_any_call(
        "Processing entries from Universal Catalog (parallel)"
    )
    source.report.new_stage.assert_any_call(
        "Extracting Dataplex lineage across configured projects (parallel)"
    )


def test_get_workunits_internal_skips_lineage_stage_when_disabled() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.project_ids = ["project-1"]
    source._project_ids = ["project-1"]
    source.config.include_lineage = False
    source.config.include_glossaries = False
    source.lineage_extractor = None
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = []
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        workunits = list(source.get_workunits_internal())

    assert workunits == []
    source.report.new_stage.assert_called_once_with(
        "Processing entries from Universal Catalog (parallel)"
    )


def test_get_workunits_internal_handles_empty_entries_for_lineage() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.project_ids = ["project-1"]
    source._project_ids = ["project-1"]
    source.config.include_lineage = True
    source.config.include_glossaries = False
    source.config.lineage_locations = ["us-central1"]
    source.lineage_extractor = Mock()
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = []
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        assert list(source.get_workunits_internal()) == []


def test_get_workunits_internal_yields_from_lineage_extractor() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.include_lineage = True
    source.config.include_glossaries = False
    source.config.project_ids = ["project-1"]
    source._project_ids = ["project-1"]
    source.config.lineage_locations = ["us-central1"]
    source.lineage_extractor = Mock()
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = [
        EntryDataTuple(
            dataplex_entry_short_name="entry-1",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-1",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:project-1.ds.table",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="project-1.ds.table",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
    ]
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()
    lineage_wu = Mock()
    source.lineage_extractor.get_lineage_workunits.return_value = [lineage_wu]

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        assert list(source.get_workunits_internal()) == [lineage_wu]
    source.lineage_extractor.get_lineage_workunits.assert_called_once_with(
        source.ctx_data.entry_data,
        active_lineage_project_location_pairs=[("project-1", "us-central1")],
        max_workers=source.config.max_workers_lineage,
    )
    args, _kwargs = source.lineage_extractor.get_lineage_workunits.call_args
    assert len(args[0]) == 1
    source.report.info.assert_called_once()


def test_get_workunits_internal_uses_configured_project_location_cross_product() -> (
    None
):
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.include_lineage = True
    source.config.include_glossaries = False
    source.config.project_ids = ["project-1", "project-2"]
    source._project_ids = ["project-1", "project-2"]
    source.config.lineage_locations = ["us-central1"]
    source.lineage_extractor = Mock()
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = [
        EntryDataTuple(
            dataplex_entry_short_name="entry-1",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-1",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:project-1.ds.table",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="project-1.ds.table",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
    ]
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    source.lineage_extractor.get_lineage_workunits.return_value = []

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        list(source.get_workunits_internal())

    source.lineage_extractor.get_lineage_workunits.assert_called_once_with(
        source.ctx_data.entry_data,
        active_lineage_project_location_pairs=[
            ("project-1", "us-central1"),
            ("project-2", "us-central1"),
        ],
        max_workers=source.config.max_workers_lineage,
    )
    source.report.info.assert_called_once()


def test_get_workunits_internal_reports_lineage_failure_on_exception() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.include_lineage = True
    source.config.include_glossaries = False
    source.config.project_ids = ["project-1"]
    source._project_ids = ["project-1"]
    source.config.lineage_locations = ["us-central1"]
    source.lineage_extractor = Mock()
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = [
        EntryDataTuple(
            dataplex_entry_short_name="entry-1",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-1",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:project-1.ds.table",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="project-1.ds.table",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
    ]
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()
    source.lineage_extractor.get_lineage_workunits.side_effect = RuntimeError("boom")

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        assert list(source.get_workunits_internal()) == []
    source.report.warning.assert_called_once()


def test_get_workunits_internal_unions_entries_across_projects_for_lineage() -> None:
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.config = Mock()
    source.config.include_lineage = True
    source.config.include_glossaries = False
    source.config.project_ids = ["project-1", "project-2"]
    source._project_ids = ["project-1", "project-2"]
    source.config.lineage_locations = ["us-central1"]
    source.lineage_extractor = Mock()
    source.glossary_processor = None
    source.ctx_data = Mock()
    source.ctx_data.entry_data = [
        EntryDataTuple(
            dataplex_entry_short_name="entry-1",
            dataplex_entry_name="projects/p1/locations/us/entryGroups/g/entries/entry-1",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:project-1.ds.table",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="project-1.ds.table",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        ),
        EntryDataTuple(
            dataplex_entry_short_name="entry-2",
            dataplex_entry_name="projects/p2/locations/us/entryGroups/g/entries/entry-2",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:project-2.ds.table",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="project-2.ds.table",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        ),
    ]
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    lineage_wu = Mock()
    source.lineage_extractor.get_lineage_workunits.return_value = [lineage_wu]

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        assert list(source.get_workunits_internal()) == [lineage_wu]
    source.lineage_extractor.get_lineage_workunits.assert_called_once()
    args, _kwargs = source.lineage_extractor.get_lineage_workunits.call_args
    assert len(args[0]) == 2


def test_project_ids_property_uses_explicit_project_ids() -> None:
    """When project_ids is set, no Resource Manager API call is made."""
    source = object.__new__(DataplexSource)
    source.config = DataplexConfig(project_ids=["project-1", "project-2"])
    source._credentials = None
    source.report = Mock()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.resolve_gcp_projects"
    ) as resolve_mock:
        from datahub.ingestion.source.common.gcp_project_filter import GcpProject

        resolve_mock.return_value = [
            GcpProject(id="project-1", name="project-1"),
            GcpProject(id="project-2", name="project-2"),
        ]
        resolved = source._project_ids

    assert resolved == ["project-1", "project-2"]
    # When project_ids is explicit, projects_client is None (no Resource Manager call).
    assert resolve_mock.call_args.kwargs["projects_client"] is None


def test_project_ids_property_uses_pattern_when_no_explicit_ids() -> None:
    """When only project_id_pattern is set, projects come from the shared resolver."""
    source = object.__new__(DataplexSource)
    source.config = DataplexConfig(project_id_pattern={"allow": ["^prod-.*"]})
    source._credentials = Mock()
    source.report = Mock()

    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.resourcemanager_v3.ProjectsClient"
        ) as projects_client_cls,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.resolve_gcp_projects"
        ) as resolve_mock,
    ):
        from datahub.ingestion.source.common.gcp_project_filter import GcpProject

        resolve_mock.return_value = [
            GcpProject(id="prod-a", name="prod-a"),
            GcpProject(id="prod-b", name="prod-b"),
        ]
        resolved = source._project_ids

    assert resolved == ["prod-a", "prod-b"]
    projects_client_cls.assert_called_once_with(credentials=source._credentials)
    assert (
        resolve_mock.call_args.kwargs["projects_client"]
        is projects_client_cls.return_value
    )


def test_project_ids_property_is_cached() -> None:
    """The cached_property resolves once and reuses the result."""
    source = object.__new__(DataplexSource)
    source.config = DataplexConfig(project_ids=["project-1"])
    source._credentials = None
    source.report = Mock()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.resolve_gcp_projects"
    ) as resolve_mock:
        from datahub.ingestion.source.common.gcp_project_filter import GcpProject

        resolve_mock.return_value = [GcpProject(id="project-1", name="project-1")]
        first = source._project_ids
        second = source._project_ids

    assert first == second
    resolve_mock.assert_called_once()


def test_test_connection_pattern_only_resolves_and_lists_entry_groups() -> None:
    """In pattern-only mode, test_connection must validate Resource Manager
    access (by resolving at least one project) and Dataplex API access (by
    listing entry groups on the resolved project)."""
    from datahub.ingestion.source.common.gcp_project_filter import GcpProject

    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.CatalogServiceClient"
        ) as catalog_client_cls,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.resourcemanager_v3.ProjectsClient"
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.resolve_gcp_projects",
            return_value=[GcpProject(id="prod-app", name="prod-app")],
        ) as resolve_mock,
    ):
        catalog_client = catalog_client_cls.return_value
        catalog_client.list_entry_groups.return_value = iter([Mock()])
        report = DataplexSource.test_connection(
            {"project_id_pattern": {"allow": ["^prod-.*"]}}
        )

    resolve_mock.assert_called_once()
    catalog_client.list_entry_groups.assert_called_once()
    parent = catalog_client.list_entry_groups.call_args.kwargs["request"].parent
    assert "projects/prod-app" in parent
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_test_connection_pattern_only_no_projects_resolved_fails() -> None:
    """If Resource Manager returns no projects (or access is denied),
    test_connection must report basic_connectivity as not capable."""
    with (
        patch(
            "datahub.ingestion.source.dataplex.dataplex.dataplex_v1.CatalogServiceClient"
        ) as catalog_client_cls,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.resourcemanager_v3.ProjectsClient"
        ),
        patch(
            "datahub.ingestion.source.dataplex.dataplex.resolve_gcp_projects",
            return_value=[],
        ),
    ):
        report = DataplexSource.test_connection(
            {"project_id_pattern": {"allow": ["^prod-.*"]}}
        )

    catalog_client = catalog_client_cls.return_value
    catalog_client.list_entry_groups.assert_not_called()
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is not None
    assert "No GCP projects matched" in report.basic_connectivity.failure_reason


def test_get_workunits_internal_with_empty_resolved_projects() -> None:
    """When resolve_gcp_projects yields no projects (pattern matches nothing or
    Resource Manager error), get_workunits_internal still completes; the
    underlying failure has been surfaced on the report by the resolver."""
    source = object.__new__(DataplexSource)
    source.entries_processor = Mock()
    source.entries_processor.process_entries.return_value = iter([])
    source.config = Mock()
    source.config.project_ids = []
    source._project_ids = []  # primes the cached_property cache
    source.config.include_lineage = False
    source.config.include_glossaries = False
    source.lineage_extractor = None
    source.glossary_processor = None
    source.report = Mock()
    source.report.new_stage.return_value = nullcontext()

    with patch(
        "datahub.ingestion.source.dataplex.dataplex.auto_workunit", return_value=[]
    ):
        workunits = list(source.get_workunits_internal())

    assert workunits == []
    source.entries_processor.process_entries.assert_called_once_with(
        project_ids=[], max_workers=source.config.max_workers_entries
    )


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
