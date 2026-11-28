import itertools
import json
import logging
from types import GeneratorType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
from unittest import mock

import pytest
import requests
from pydantic import ValidationError

from datahub.configuration.common import ConfigurationError
from datahub.emitter.emit_mode import EmitMode
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy import spill_cache
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyAuthError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    ReportObjectLookupKind,
    UsageTargetKind,
)
from datahub.ingestion.source.microstrategy.lineage import (
    ModelFieldLineage,
    ModelLineageIndex,
    WarehouseLineageContext,
)
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    MetricEnrichment,
    MicroStrategyObject,
    ModelTablesResponse,
    PredefinedFolder,
    PredefinedFolderResolution,
    Project,
    ReportDefinition,
    ReportPrompt,
    SqlView,
    object_cache_key,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.source import (
    MicroStrategySource,
    _LazyProjectLineage,
    _ReportObjectLookup,
    _ReportSqlOutcome,
)
from datahub.ingestion.source.microstrategy.usage import UsageBucket
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartUsageStatisticsClass,
    ContainerClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    InputFieldsClass,
    OwnershipClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_from_sql_statements
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.sentinels import unset
from tests.unit.microstrategy.conftest import (
    ConnectionModelTablesClient,
    ModelTablesClient,
    ReportSqlModelTablesClient,
    install_client,
    model_table,
)


def _source(extra_config: dict | None = None) -> MicroStrategySource:
    config_dict = {
        "base_url": "https://mstr.example.com/MicroStrategyLibrary",
        "extract_warehouse_lineage": True,
    }
    if extra_config:
        config_dict.update(extra_config)
    config = MicroStrategyConfig.model_validate(config_dict)
    return MicroStrategySource(config, PipelineContext(run_id="test"))


_PROJECT = Project.model_validate({"id": "project-1", "name": "Sales Analytics"})


def _standard_predefined_folders(folder_types: List[int]) -> List[PredefinedFolder]:
    """The Reports folder every project has, when the request asks for it."""
    if 7 not in folder_types:
        return []
    return [
        PredefinedFolder.model_validate(
            {"id": "reports-folder-id", "name": "Reports", "folderType": 7}
        )
    ]


def _dashboard(source_warehouse: dict | None = None) -> DashboardDefinition:
    dataset = {
        "id": "ds-1",
        "name": "Sales Cube",
        "availableObjects": {
            "metrics": [
                {
                    "id": "metric-1",
                    "name": "Revenue",
                    "dataType": "double",
                }
            ]
        },
    }
    if source_warehouse:
        dataset["sourceWarehouse"] = source_warehouse

    return DashboardDefinition.from_api_response(
        object_id="dash-1",
        object_name="Sales Dashboard",
        response={"result": {"definition": {"datasets": [dataset]}}},
    )


def test_unmapped_dataset_warehouse_does_not_borrow_project_context() -> None:
    # The project's warehouse may be another platform; its URNs would be wrong.
    source = _source()
    dataset = _dashboard(
        {"id": "wh-1", "name": "Legacy DB", "database": {"type": "unknown_db"}}
    ).datasets[0]
    project_context = WarehouseLineageContext(platform="snowflake", env="PROD")

    assert (
        source._dataset_warehouse_context("project-1", dataset, project_context) is None
    )
    assert (
        source._dataset_warehouse_context(
            "project-1", _dashboard().datasets[0], project_context
        )
        == project_context
    )


def test_attach_dataset_warehouse_upstreams_uses_dataset_source_warehouse() -> None:
    source = _source()
    dashboard = _dashboard(
        {
            "id": "source-1",
            "name": "Sales Warehouse",
            "database": {
                "type": "snow_flake",
                "name": "SALES_DB",
                "schema": "SALES",
            },
        }
    )

    source._attach_dataset_warehouse_upstreams(
        [
            {
                "id": "ds-1",
                "sqlStatement": "select * from fact_sales",
            }
        ],
        dashboard,
        context=None,
        entries={},
        project_id="project-1",
    )

    assert dashboard.datasets[0].warehouse_upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]


def test_attach_dataset_warehouse_upstreams_skips_without_context() -> None:
    source = _source()
    dashboard = _dashboard()

    source._attach_dataset_warehouse_upstreams(
        [
            {
                "id": "ds-1",
                "sqlStatement": "select * from fact_sales",
            }
        ],
        dashboard,
        context=None,
        entries={},
        project_id="project-1",
    )

    assert dashboard.datasets[0].warehouse_upstream_urns == []


def test_report_source_dataset_uses_report_definition_source_and_fields() -> None:
    report_object = MicroStrategyObject.model_validate(
        {"id": "report-1", "name": "Sales Report", "type": "3"}
    )
    report_definition = ReportDefinition.from_api_response(
        object_id="report-1",
        object_name="Sales Report",
        response={
            "result": {
                "definition": {
                    "dataSource": {"id": "cube-1", "name": "Sales Cube"},
                    "availableObjects": [
                        {"id": "metric-1", "name": "Revenue", "type": "metric"}
                    ],
                }
            }
        },
    )

    dataset = MicroStrategySource._report_source_dataset(
        report_object,
        report_definition,
    )

    assert dataset is not None
    assert dataset.id == "cube-1"
    assert dataset.name == "Sales Cube"
    assert dataset.available_objects["metrics"][0]["id"] == "metric-1"


def test_dashboard_report_chart_edges_are_filtered_by_report_pattern() -> None:
    source = _source(
        {
            "extract_reports": True,
            "report_pattern": {"allow": ["^Sales Report$"]},
            "include_personal_folders": True,
        }
    )
    dashboard = _dashboard()
    dashboard.dependencies = [
        MicroStrategyObject.model_validate(
            {"id": "report-1", "name": "Sales Report", "type": "3"}
        ),
        MicroStrategyObject.model_validate(
            {"id": "report-2", "name": "Operations Report", "type": "3"}
        ),
        MicroStrategyObject.model_validate(
            {"id": "dash-2", "name": "Other Dashboard", "type": "55"}
        ),
    ]

    assert source._dashboard_report_chart_urns(_PROJECT, dashboard) == [
        source.mapper.report_urn("project-1", "report-1")
    ]


def test_dashboard_drops_chart_edges_for_reports_the_report_pass_skips() -> None:
    # A shared dossier can embed a report saved in a personal folder; the
    # report is never emitted, so the dashboard must not list it.
    source = _source({"extract_reports": True})

    class FakeClient:
        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            return []

        def get_report_object(
            self, project_id: str, report_id: str
        ) -> MicroStrategyObject:
            folder = "Profiles/jdoe/My Reports" if report_id == "report-2" else "Shared"
            return MicroStrategyObject.model_validate(
                {"id": report_id, "name": report_id, "location": f"/{folder}"}
            )

    source.client = FakeClient()  # type: ignore[assignment]
    dashboard = _dashboard()
    dashboard.dependencies = [
        MicroStrategyObject.model_validate(
            {"id": report_id, "name": report_id, "type": "3"}
        )
        for report_id in ("report-1", "report-2")
    ]

    assert source._dashboard_report_chart_urns(_PROJECT, dashboard) == [
        source.mapper.report_urn("project-1", "report-1")
    ]


def test_report_sql_lineage_attaches_upstreams_to_report_source_dataset() -> None:
    source = _source({"extract_report_sql_lineage": True})
    report_object = MicroStrategyObject.model_validate(
        {"id": "report-1", "name": "Sales Report", "type": "3"}
    )
    dataset = DatasetObject.model_validate({"id": "cube-1", "name": "Sales Cube"})

    class FakeClient:
        deleted = False

        def create_report_instance(self, project_id: str, report_id: str) -> str:
            return "instance-1"

        def get_report_sql_view(
            self,
            project_id: str,
            report_id: str,
            instance_id: str,
        ) -> SqlView:
            return SqlView(sql_statement="select * from fact_sales")

        def delete_report_instance(
            self,
            project_id: str,
            report_id: str,
            instance_id: str,
        ) -> bool:
            self.deleted = True
            return True

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    source._enrich_report_sql_lineage(
        project_id="project-1",
        report_object=report_object,
        dataset=dataset,
        context=WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        ),
        entry=None,
    )

    assert fake_client.deleted is True
    assert dataset.warehouse_upstream_urns == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]


def test_test_connection_reports_capable_when_login_and_projects_succeed() -> None:
    with mock.patch(
        "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
    ):
        report = MicroStrategySource.test_connection(
            {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_test_connection_reports_login_failure() -> None:
    with mock.patch(
        "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
    ) as client_cls:
        client_cls.return_value.login.side_effect = MicroStrategyAPIError(
            "invalid credentials"
        )
        report = MicroStrategySource.test_connection(
            {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason


def test_test_connection_handles_invalid_config_without_raising() -> None:
    report = MicroStrategySource.test_connection({})

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason


def test_get_project_source_warehouses_falls_back_to_datasource_inventory() -> None:
    source = _source()
    datasource = Datasource.model_validate(
        {
            "id": "source-1",
            "name": "Sales Warehouse",
            "database": {"type": "snow_flake"},
        }
    )

    class FakeClient:
        def list_project_datasources(self, project_id: str) -> List[Datasource]:
            raise MicroStrategyAPIError("forbidden")

        def list_datasources(self, project_id: str) -> List[Datasource]:
            return [datasource]

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_project_source_warehouses("project-1") == [datasource]
    assert len(source.report.warnings) == 1


def test_get_project_source_warehouses_returns_empty_when_both_apis_fail() -> None:
    source = _source()

    class FakeClient:
        def list_project_datasources(self, project_id: str) -> List[Datasource]:
            raise MicroStrategyAPIError("forbidden")

        def list_datasources(self, project_id: str) -> List[Datasource]:
            raise MicroStrategyAPIError("also forbidden")

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_project_source_warehouses("project-1") == []
    assert source.report.source_warehouse_api_failures == 1
    assert len(source.report.warnings) == 1


def test_get_dashboard_definition_falls_back_to_document_definition() -> None:
    source = _source(
        {
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": "dash-1", "name": "Sales Dashboard"}
    )

    class FakeClient:
        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a dossier")

        def get_document_definition(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            return {
                "result": {
                    "definition": {"datasets": [{"id": "ds-1", "name": "Sales Cube"}]}
                }
            }

    source.client = FakeClient()  # type: ignore[assignment]

    dashboard = source._get_dashboard_definition("project-1", dashboard_object)

    assert dashboard is not None
    assert [dataset.id for dataset in dashboard.datasets] == ["ds-1"]


def test_get_dashboard_definition_returns_none_when_both_apis_fail() -> None:
    source = _source(
        {
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": "dash-1", "name": "Sales Dashboard"}
    )

    class FakeClient:
        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a dossier")

        def get_document_definition(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a document either")

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_dashboard_definition("project-1", dashboard_object) is None
    assert len(source.report.warnings) == 1


def test_metric_model_fact_ids_terminates_on_metric_cycles() -> None:
    source = _source()
    models: Dict[str, Dict[str, Any]] = {
        "METRIC-A": {
            "expression": {
                "tokens": [
                    {"target": {"objectId": "fact-1", "subType": "fact"}},
                    {"target": {"objectId": "metric-b", "subType": "metric"}},
                ]
            }
        },
        "METRIC-B": {
            "expression": {
                "tokens": [
                    {"target": {"objectId": "fact-2", "subType": "fact"}},
                    {"target": {"objectId": "metric-a", "subType": "metric"}},
                ]
            }
        },
    }

    class FakeClient:
        def get_metric_model(
            self, project_id: str, metric_id: str, stop: object = None
        ) -> Dict[str, Any]:
            return models[metric_id.upper()]

    source.client = FakeClient()  # type: ignore[assignment]

    fact_ids = source._metric_model_fact_ids(
        "project-1",
        models["METRIC-A"],
        visited={"METRIC-A"},
    )

    assert fact_ids == ["FACT-1", "FACT-2"]


def test_get_metric_model_failure_warns_and_degrades() -> None:
    source = _source()

    class FakeClient:
        def get_metric_model(
            self, project_id: str, metric_id: str, stop: object = None
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("404 Not Found")

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._get_metric_model("project-1", "metric-x") == {}
    assert source.report.metric_expression_api_failures == 1
    assert any(
        sample.startswith("metric-x")
        for sample in source.report.failed_metric_model_ids
    )
    # Surfaced once so operators notice cross-project / access-limited metrics.
    assert len(source.report.warnings) == 1


def test_per_dashboard_error_boundary_continues_with_next_dashboard() -> None:
    source = _source(
        {
            "extract_warehouse_lineage": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
            "extract_model_lineage": False,
        }
    )
    dashboards = [
        MicroStrategyObject.model_validate(
            {
                "id": "dash-1",
                "name": "Broken Dashboard",
                "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
            }
        ),
        MicroStrategyObject.model_validate(
            {
                "id": "dash-2",
                "name": "Healthy Dashboard",
                "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
            }
        ),
    ]

    class FakeClient:
        def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
            return iter(dashboards)

        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            return _standard_predefined_folders(folder_types)

        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            if dossier_id == "dash-1":
                raise ValueError("unexpected definition failure")
            return {"result": {"definition": {"datasets": [], "chapters": []}}}

    source.client = FakeClient()  # type: ignore[assignment]

    workunits = list(
        source._process_project_dashboards(
            _PROJECT,
            _LazyProjectLineage(source, "project-1", []),
        )
    )
    urns = {workunit.get_urn() for workunit in workunits}

    assert source.mapper.dashboard_urn("project-1", "dash-2") in urns
    assert source.mapper.dashboard_urn("project-1", "dash-1") not in urns
    assert len(source.report.warnings) == 1


def _report_scope_source(extra_config: dict | None = None) -> MicroStrategySource:
    config = {
        "extract_reports": True,
        "extract_report_definitions": False,
        "extract_metric_expressions": False,
    }
    config.update(extra_config or {})
    return _source(config)


class _ReportSearchClient:
    def __init__(self) -> None:
        self.search_calls = 0
        self.object_info_calls: List[str] = []

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _standard_predefined_folders(folder_types)

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        self.search_calls += 1
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {"id": "REPORT-1", "name": "Linked Report", "type": "3"}
                ),
                MicroStrategyObject.model_validate(
                    {"id": "REPORT-2", "name": "Unlinked Report", "type": "3"}
                ),
            ]
        )

    def get_report_object(self, project_id: str, report_id: str) -> MicroStrategyObject:
        self.object_info_calls.append(report_id)
        return MicroStrategyObject.model_validate(
            {
                "id": report_id,
                "name": "Linked Report",
                "type": "3",
                "ancestors": [{"id": "folder-1", "name": "Shared Reports"}],
            }
        )


def test_reports_scoped_to_dashboard_linked_by_default() -> None:
    source = _report_scope_source()
    client = _ReportSearchClient()
    install_client(source, client)

    workunits = list(
        source._process_project_reports(
            _PROJECT,
            _LazyProjectLineage(source, "project-1", []),
            {"REPORT-1"},
        )
    )
    urns = {workunit.get_urn() for workunit in workunits}

    assert source.mapper.report_urn("project-1", "REPORT-1") in urns
    assert source.mapper.report_urn("project-1", "REPORT-2") not in urns
    # Linked reports are fetched directly by id; the expensive report library
    # enumeration must not run at all.
    assert client.search_calls == 0
    assert client.object_info_calls == ["REPORT-1"]


def test_linked_report_fetch_failure_skips_that_report() -> None:
    source = _report_scope_source()

    class FailingClient(_ReportSearchClient):
        def get_report_object(
            self, project_id: str, report_id: str
        ) -> MicroStrategyObject:
            if report_id == "REPORT-1":
                raise MicroStrategyAPIError("object info failed")
            return super().get_report_object(project_id, report_id)

    client = FailingClient()
    install_client(source, client)

    workunits = list(
        source._process_project_reports(
            _PROJECT,
            _LazyProjectLineage(source, "project-1", []),
            {"REPORT-1", "REPORT-3"},
        )
    )
    urns = {workunit.get_urn() for workunit in workunits}

    assert source.mapper.report_urn("project-1", "REPORT-1") not in urns
    assert source.mapper.report_urn("project-1", "REPORT-3") in urns
    assert len(source.report.warnings) == 1


def test_report_enumeration_skipped_when_no_dashboard_links() -> None:
    source = _report_scope_source()

    class FakeClient:
        search_calls = 0

        def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
            self.search_calls += 1
            return iter([])

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    workunits = list(
        source._process_project_reports(
            _PROJECT,
            _LazyProjectLineage(source, "project-1", []),
            set(),
        )
    )

    # The expensive report library search must not run at all when no
    # ingested dashboard referenced a report.
    assert workunits == []
    assert fake_client.search_calls == 0


def test_reports_not_scoped_when_independent_reports_enabled() -> None:
    source = _report_scope_source({"extract_independent_reports": True})
    client = _ReportSearchClient()
    install_client(source, client)

    workunits = list(
        source._process_project_reports(
            _PROJECT,
            _LazyProjectLineage(source, "project-1", []),
            {"REPORT-1"},
        )
    )
    urns = {workunit.get_urn() for workunit in workunits}

    assert source.mapper.report_urn("project-1", "REPORT-1") in urns
    assert source.mapper.report_urn("project-1", "REPORT-2") in urns
    assert client.search_calls == 1
    assert client.object_info_calls == []


def test_auth_loss_aborts_remaining_projects() -> None:
    source = _source(
        {
            "extract_reports": False,
            "extract_source_warehouses": False,
        }
    )

    class FakeClient:
        def __init__(self) -> None:
            self.projects_started: List[str] = []

        def login(self) -> None:
            pass

        def close(self) -> None:
            pass

        def list_projects(self) -> List[Project]:
            return [
                Project.model_validate({"id": "project-1", "name": "First"}),
                Project.model_validate({"id": "project-2", "name": "Second"}),
            ]

        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            return _standard_predefined_folders(folder_types)

        def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
            self.projects_started.append(project_id)
            raise MicroStrategyAuthError("session invalidated and re-login failed")

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    list(source.get_workunits_internal())

    # The run must stop at the first project once auth is irrecoverable
    # instead of failing every remaining project the same way.
    assert fake_client.projects_started == ["project-1"]
    assert len(source.report.failures) == 1


def test_run_cleanup_closes_the_client_when_an_earlier_close_raises() -> None:
    source = _source()
    client = mock.MagicMock()
    client.list_projects.return_value = []
    source.client = client
    sqlglot_logger = logging.getLogger("sqlglot")
    filters_before = list(sqlglot_logger.filters)

    with (
        mock.patch.object(
            source.lineage, "close", side_effect=RuntimeError("lineage close failed")
        ),
        pytest.raises(RuntimeError, match="lineage close failed"),
    ):
        list(source.get_workunits_internal())

    client.close.assert_called_once_with()
    assert sqlglot_logger.filters == filters_before


def test_reports_not_scoped_without_dashboard_extraction() -> None:
    # Reports-only recipes have no dashboard linkage to scope by.
    source = _report_scope_source({"extract_dashboards": False})
    source.client = _ReportSearchClient()  # type: ignore[assignment]

    workunits = list(
        source._process_project_reports(
            _PROJECT,
            _LazyProjectLineage(source, "project-1", []),
            set(),
        )
    )
    urns = {workunit.get_urn() for workunit in workunits}

    assert source.mapper.report_urn("project-1", "REPORT-1") in urns
    assert source.mapper.report_urn("project-1", "REPORT-2") in urns


def test_resolve_visualization_bindings_uses_dataset_scoped_derived_objects() -> None:
    source = _source()
    derived_id = "A" * 32
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Store Sales To Plan",
            "datasets": [
                {"id": "ds-retail", "name": "Retail WTD"},
                {"id": "ds-service", "name": "Service WTD"},
            ],
            "visualizations": [
                {
                    "key": "viz-1",
                    "name": "Store Sales WTD",
                    "metrics": [{"id": derived_id, "type": "metric"}],
                },
                # Already explicitly bound: must not be touched.
                {
                    "key": "viz-2",
                    "name": "Bound Viz",
                    "datasets": ["ds-service"],
                    "metrics": [{"id": derived_id, "type": "metric"}],
                },
            ],
        }
    )

    class FakeClient:
        def get_model_document(
            self,
            project_id: str,
            document_id: str,
            instance_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            return {
                "datasets": [
                    {
                        "information": {"objectId": derived_id[:-1] + "0"},
                        "derivedMetrics": [],
                    },
                    {
                        "information": {"objectId": "ds-retail"},
                        "derivedMetrics": [{"information": {"objectId": derived_id}}],
                    },
                ]
            }

    source.client = FakeClient()  # type: ignore[assignment]

    source._resolve_visualization_bindings("project-1", dashboard)

    assert dashboard.visualizations[0].datasets == ["ds-retail"]
    assert dashboard.visualizations[1].datasets == ["ds-service"]
    assert source.report.visualizations_bound_by_derived_objects == 1


def test_resolve_visualization_bindings_survives_modeling_api_failure() -> None:
    source = _source()
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Dash",
            "datasets": [
                {"id": "ds-1", "name": "A"},
                {"id": "ds-2", "name": "B"},
            ],
            "visualizations": [
                {
                    "key": "viz-1",
                    "name": "Viz",
                    "metrics": [{"id": "B" * 32, "type": "metric"}],
                }
            ],
        }
    )

    class FakeClient:
        def get_model_document(
            self,
            project_id: str,
            document_id: str,
            instance_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("modeling access denied", status_code=403)

    source.client = FakeClient()  # type: ignore[assignment]

    source._resolve_visualization_bindings("project-1", dashboard)

    assert dashboard.visualizations[0].datasets == []
    assert source.report.visualizations_bound_by_derived_objects == 0


def test_resolve_visualization_bindings_ignores_owners_outside_dashboard() -> None:
    source = _source()
    derived_id = "A" * 32
    dashboard = DashboardDefinition.model_validate(
        {
            "id": "dash-1",
            "name": "Dash",
            "datasets": [
                {"id": "ds-1", "name": "A"},
                {"id": "ds-2", "name": "B"},
            ],
            "visualizations": [
                {
                    "key": "viz-1",
                    "name": "Viz",
                    "metrics": [{"id": derived_id, "type": "metric"}],
                }
            ],
        }
    )

    class FakeClient:
        def get_model_document(
            self,
            project_id: str,
            document_id: str,
            instance_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            # The derived object's owner is NOT one of the dashboard datasets.
            return {
                "datasets": [
                    {
                        "information": {"objectId": "ds-elsewhere"},
                        "derivedMetrics": [{"information": {"objectId": derived_id}}],
                    }
                ]
            }

    source.client = FakeClient()  # type: ignore[assignment]

    source._resolve_visualization_bindings("project-1", dashboard)

    assert dashboard.visualizations[0].datasets == []
    assert source.report.visualizations_bound_by_derived_objects == 0


def test_resolve_visualization_bindings_memoizes_modeling_failure() -> None:
    source = _source()

    def _dashboard_with_two_datasets(dash_id: str) -> DashboardDefinition:
        return DashboardDefinition.model_validate(
            {
                "id": dash_id,
                "name": dash_id,
                "datasets": [
                    {"id": "ds-1", "name": "A"},
                    {"id": "ds-2", "name": "B"},
                ],
                "visualizations": [
                    {
                        "key": "viz-1",
                        "name": "Viz",
                        "metrics": [{"id": "B" * 32, "type": "metric"}],
                    }
                ],
            }
        )

    class FakeClient:
        calls = 0

        def get_model_document(
            self,
            project_id: str,
            document_id: str,
            instance_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            self.calls += 1
            raise MicroStrategyAPIError("modeling access denied", status_code=403)

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    source._resolve_visualization_bindings(
        "project-1", _dashboard_with_two_datasets("dash-1")
    )
    source._resolve_visualization_bindings(
        "project-1", _dashboard_with_two_datasets("dash-2")
    )

    # The project-scoped failure is remembered; no per-dashboard re-attempts.
    assert fake_client.calls == 1
    assert len(source.report.warnings) == 1


@pytest.mark.parametrize(
    "status, project_wide", [(403, True), (501, True), (404, False), (500, False)]
)
def test_model_document_failure_scope(status: int, project_wide: bool) -> None:
    # Missing access applies to the project; the endpoint answers 404 or 500
    # for some documents while others still work.
    source = _source()

    class FakeClient:
        def get_model_document(
            self, project_id: str, document_id: str
        ) -> Dict[str, object]:
            if document_id == "dash-1":
                raise MicroStrategyAPIError("failed", status_code=status)
            return {"id": document_id}

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._model_document("project-1", "dash-1") is None
    second = source._model_document("project-1", "dash-2")
    assert second == (None if project_wide else {"id": "dash-2"})


@pytest.mark.parametrize(
    "view_media, first_endpoint",
    [(None, "dossier"), (0x6000_0000, "dossier"), (0x0800_0000, "document")],
)
def test_definition_endpoint_follows_view_media(
    view_media: Optional[int], first_endpoint: str
) -> None:
    # Without viewMedia a 14081 object may be a dossier, and the document
    # endpoint answers for dossiers too, minus their visualizations.
    source = _source(
        {
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
        }
    )
    calls: List[str] = []

    class FakeClient:
        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            calls.append("dossier")
            return {"id": dossier_id, "datasets": []}

        def get_document_definition(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            calls.append("document")
            return {"id": document_id, "datasets": []}

    source.client = FakeClient()  # type: ignore[assignment]
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": "dash-1", "name": "Dash", "subtype": "14081", "viewMedia": view_media}
    )

    source._get_dashboard_definition("project-1", dashboard_object)

    assert calls == [first_endpoint]


def test_model_document_is_read_once_per_dashboard() -> None:
    # Visualization binding and document derived metrics both need it.
    source = _source()

    class FakeClient:
        def __init__(self) -> None:
            self.calls: List[str] = []

        def get_model_document(
            self, project_id: str, document_id: str
        ) -> Dict[str, object]:
            self.calls.append(document_id)
            return {"id": document_id}

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    assert source._model_document("project-1", "dash-1") == {"id": "dash-1"}
    assert source._model_document("project-1", "dash-1") == {"id": "dash-1"}
    assert source._model_document("project-1", "dash-2") == {"id": "dash-2"}
    assert fake_client.calls == ["dash-1", "dash-2"]


def test_predefined_folder_labels_resolves_shared_reports() -> None:
    source = _source()

    class FakeClient:
        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            assert folder_types == [1, 7, 39]
            return [
                PredefinedFolder.model_validate(
                    {"id": "project-root-id", "name": "Analytics", "folderType": 39}
                ),
                PredefinedFolder.model_validate(
                    {
                        "id": "public-objects-id",
                        "name": "Public Objects",
                        "folderType": 1,
                    }
                ),
                PredefinedFolder.model_validate(
                    {"id": "reports-folder-id", "name": "Reports", "folderType": 7}
                ),
            ]

    source.client = FakeClient()  # type: ignore[assignment]

    resolution = source._predefined_folders("project-1")
    assert resolution.labels == {"REPORTS-FOLDER-ID": "Shared Reports"}
    assert resolution.hidden_ids == {"PROJECT-ROOT-ID", "PUBLIC-OBJECTS-ID"}


def test_predefined_folder_labels_disabled_by_config_skips_client_call() -> None:
    source = _source({"use_predefined_folder_names": False})

    class FakeClient:
        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            raise AssertionError("should not be called when the flag is disabled")

    source.client = FakeClient()  # type: ignore[assignment]

    assert source._predefined_folders("project-1") == PredefinedFolderResolution.empty()


@pytest.mark.parametrize("answer", ["error", "empty", "unrecognized"])
def test_predefined_folder_failure_skips_project_content(answer: str) -> None:
    # Raw folder names would mint different container URNs, so the project's
    # dashboards and reports are skipped and the run fails, which also keeps
    # stale-entity removal from deleting the containers earlier runs emitted.
    source = _source({"extract_source_warehouses": False})

    class FakeClient:
        def list_projects(self) -> List[Project]:
            return [Project.model_validate({"id": "project-1", "name": "First"})]

        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            if answer == "error":
                raise MicroStrategyAPIError("not supported on this version")
            if answer == "unrecognized":
                return [
                    PredefinedFolder.model_validate(
                        {"id": "f-1", "name": "Other", "folderType": 99}
                    )
                ]
            # Every project has predefined folders, so none at all is an
            # unrecognized answer.
            return []

        def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
            raise AssertionError("dashboards must not be searched")

        def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
            raise AssertionError("reports must not be searched")

    source.client = FakeClient()  # type: ignore[assignment]

    workunits = list(source._process_project(source.client.list_projects()[0]))

    assert all("container" in wu.get_urn() for wu in workunits)
    assert len(source.report.failures) == 1


def test_predefined_folder_auth_error_propagates() -> None:
    source = _source()

    class FakeClient:
        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            raise MicroStrategyAuthError("session invalidated")

    source.client = FakeClient()  # type: ignore[assignment]

    with pytest.raises(MicroStrategyAuthError):
        source._predefined_folders("project-1")


def test_lazy_project_lineage_resolves_once_and_caches_failures() -> None:
    source = _source({"extract_model_lineage": True})

    class FakeClient:
        model_table_calls = 0

        def list_model_tables(self, *args: Any, **kwargs: Any) -> ModelTablesResponse:
            self.model_table_calls += 1
            raise ValueError("unexpected model tables failure")

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    datasource = Datasource.model_validate(
        {"id": "source-1", "name": "Warehouse", "database": {"type": "snowflake"}}
    )
    lineage_context = _LazyProjectLineage(source, "project-1", [datasource])

    # Reported once against the project, then cached: later accesses return
    # None without re-calling the API.
    assert lineage_context.model_lineage_index is None
    assert lineage_context.model_lineage_index is None
    assert fake_client.model_table_calls == 1
    assert [w.title for w in source.report.warnings] == [
        "Project lineage context unavailable"
    ]


def test_fetch_project_model_tables_requests_logical_table_information() -> None:
    # Regression: the semantic-model mapper reads each table's top-level
    # "information" block for the logical table id/name (needed to resolve
    # attribute-hierarchy relationships and to keep role-played tables
    # distinct), but MicroStrategy's `fields` parameter is a root-level
    # whitelist -- omitting "information" there means the API never returns
    # it at all, regardless of what the mapper does with the response.
    source = _source({"extract_model_lineage": True})

    class FakeClient:
        def __init__(self) -> None:
            self.fields_requested: List[str] = []

        def list_model_tables(self, *args: Any, **kwargs: Any) -> ModelTablesResponse:
            self.fields_requested.append(kwargs.get("fields", ""))
            return ModelTablesResponse(tables=[], total=0)

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    source.fetch_project_model_tables("project-1")

    assert fake_client.fields_requested
    assert all("information" in fields for fields in fake_client.fields_requested)


def test_fetch_project_model_tables_unrecognized_payload_is_a_failed_fetch() -> None:
    # [] would let stale removal soft-delete the semantic model.
    source = _source({"extract_model_lineage": True})

    class FakeClient:
        def list_model_tables(
            self, *args: object, **kwargs: object
        ) -> ModelTablesResponse:
            return ModelTablesResponse(tables=None, total=None)

    # Duck-typed stand-in for MicroStrategyClient.
    source.client = FakeClient()  # type: ignore[assignment]

    assert source.fetch_project_model_tables("project-1") is None
    assert [f.title for f in source.report.failures] == [
        "Unrecognized model tables response shape"
    ]


def test_fetch_project_model_tables_without_a_total_stops_at_a_short_page() -> None:
    source = _source({"extract_model_lineage": True, "page_size": 2})
    offsets: List[int] = []

    class FakeClient:
        def list_model_tables(
            self, project_id: str, limit: int, offset: int, fields: str
        ) -> ModelTablesResponse:
            offsets.append(offset)
            count = 2 if offset == 0 else 1
            return ModelTablesResponse(
                tables=[{"id": f"t{offset + i}"} for i in range(count)], total=None
            )

    source.client = FakeClient()  # type: ignore[assignment]

    tables = source.fetch_project_model_tables("project-1")

    assert tables is not None and len(tables) == 3
    assert offsets == [0, 2]
    assert not source.report.failures


def test_fetch_project_model_tables_unrecognized_page_mid_pagination_fails() -> None:
    source = _source({"extract_model_lineage": True, "page_size": 2})

    class FakeClient:
        def list_model_tables(
            self, project_id: str, limit: int, offset: int, fields: str
        ) -> ModelTablesResponse:
            if offset == 0:
                return ModelTablesResponse(tables=[{"id": "t0"}, {"id": "t1"}], total=5)
            return ModelTablesResponse(tables=None, total=None)

    source.client = FakeClient()  # type: ignore[assignment]

    assert source.fetch_project_model_tables("project-1") is None
    assert len(source.report.failures) == 1
    assert source.report.model_tables_scanned == 0


def test_fetch_project_model_tables_empty_page_before_total_is_a_failure() -> None:
    source = _source({"extract_model_lineage": True, "page_size": 1000})

    class FakeClient:
        def list_model_tables(
            self, project_id: str, limit: int, offset: int, fields: str
        ) -> ModelTablesResponse:
            tables: List[Dict[str, object]] = (
                []
                if offset == 2000
                else [{"id": f"t{offset + i}"} for i in range(limit)]
            )
            return ModelTablesResponse(tables=tables, total=5000)

    source.client = FakeClient()  # type: ignore[assignment]

    assert source.fetch_project_model_tables("project-1") is None
    assert [f.title for f in source.report.failures] == [
        "MicroStrategy Model Tables Truncated"
    ]


def test_project_lineage_apis_skipped_when_all_dashboards_filtered() -> None:
    source = _source(
        {
            "dashboard_pattern": {"allow": ["^Allowed Only$"]},
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )

    class FakeClient:
        def __init__(self) -> None:
            self.model_table_calls = 0
            self.connection_calls = 0

        def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
            return iter(
                [
                    MicroStrategyObject.model_validate(
                        {"id": "dash-1", "name": "Filtered Out"}
                    )
                ]
            )

        def list_model_tables(self, *args: Any, **kwargs: Any) -> ModelTablesResponse:
            self.model_table_calls += 1
            return ModelTablesResponse(tables=[], total=0)

        def get_datasource_connection(self, *args: Any, **kwargs: Any) -> Any:
            self.connection_calls += 1
            raise AssertionError("should not be called for filtered projects")

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    # A resolvable warehouse context ensures model-table pagination WOULD
    # run if the lazy context were resolved.
    datasource = Datasource.model_validate(
        {"id": "source-1", "name": "Warehouse", "database": {"type": "snowflake"}}
    )
    lineage_context = _LazyProjectLineage(source, "project-1", [datasource])
    workunits = list(source._process_project_dashboards(_PROJECT, lineage_context))

    assert workunits == []
    assert list(source.report.filtered_dashboards) == ["Filtered Out"]
    # Model-table and connection APIs must not be touched when every
    # dashboard was pattern-filtered.
    assert fake_client.model_table_calls == 0
    assert fake_client.connection_calls == 0


def test_model_tables_shared_between_lineage_and_semantic_model() -> None:
    source = _source(
        {"extract_model_lineage": True, "emit_semantic_model_entities": True}
    )

    class FakeClient:
        def __init__(self) -> None:
            self.model_table_calls = 0

        def list_model_tables(self, *args: Any, **kwargs: Any) -> ModelTablesResponse:
            self.model_table_calls += 1
            return ModelTablesResponse(
                tables=[
                    {
                        "physicalTable": {"tableName": "T1"},
                        "attributes": [],
                        "facts": [],
                    }
                ],
                total=1,
            )

    fake_client = FakeClient()
    source.client = fake_client  # type: ignore[assignment]

    datasource = Datasource.model_validate(
        {"id": "source-1", "name": "Warehouse", "database": {"type": "snowflake"}}
    )
    lineage_context = _LazyProjectLineage(source, "project-1", [datasource])

    # Classic model-lineage path and the semantic-model path both read the
    # same underlying data.
    _ = lineage_context.model_lineage_index
    _ = lineage_context.model_tables

    assert fake_client.model_table_calls == 1


class _FlakyModelTablesClient:
    def __init__(self) -> None:
        self.model_table_calls = 0

    def list_model_tables(self, *args: object, **kwargs: object) -> ModelTablesResponse:
        self.model_table_calls += 1
        if self.model_table_calls == 1:
            raise MicroStrategyAPIError("Read timed out. (read timeout=30)")
        return ModelTablesResponse(
            tables=[{"physicalTable": {"tableName": "T1"}, "attributes": []}],
            total=1,
        )


def test_semantic_model_retries_a_failed_model_tables_fetch() -> None:
    source = _source({"emit_semantic_model_entities": True})
    client = _FlakyModelTablesClient()
    # Duck-typed stand-in for MicroStrategyClient.
    install_client(source, client)
    lineage_context = _LazyProjectLineage(source, "project-1", [])

    assert lineage_context.model_tables is None
    assert lineage_context.model_tables is None
    tables = lineage_context.model_tables_retrying_failure()

    assert tables is not None and len(tables) == 1
    assert client.model_table_calls == 2
    assert lineage_context.model_tables_retrying_failure() == tables
    assert client.model_table_calls == 2


@pytest.mark.parametrize("status_code", [403, 501])
def test_refused_model_tables_fetch_is_neither_retried_nor_double_counted(
    status_code: int,
) -> None:
    source = _source({"emit_semantic_model_entities": True})
    calls: List[int] = []

    class FakeClient:
        def list_model_tables(self, *args: object, **kwargs: object) -> None:
            calls.append(1)
            raise MicroStrategyAPIError("refused", status_code=status_code)

    source.client = FakeClient()  # type: ignore[assignment]
    lineage_context = _LazyProjectLineage(source, "project-1", [])

    assert lineage_context.model_tables is None
    assert lineage_context.model_tables_retrying_failure() is None

    assert len(calls) == 1
    assert source.report.model_lineage_api_failures == 1


def test_unrecognized_model_tables_payload_is_not_retried() -> None:
    source = _source({"emit_semantic_model_entities": True})
    calls: List[int] = []

    class FakeClient:
        def list_model_tables(self, *args: object, **kwargs: object) -> Any:
            calls.append(1)
            return ModelTablesResponse(tables=None, total=None)

    source.client = FakeClient()  # type: ignore[assignment]
    lineage_context = _LazyProjectLineage(source, "project-1", [])

    assert lineage_context.model_tables is None
    assert lineage_context.model_tables_retrying_failure() is None

    assert len(calls) == 1
    assert len(source.report.failures) == 1


_REPORT_KEY = object_cache_key("project-1", "report")
_METRIC_KEY = object_cache_key("project-1", "metric")


class _FixedTablesLineage(_LazyProjectLineage):
    def __init__(
        self, source: MicroStrategySource, tables: List[Dict[str, object]]
    ) -> None:
        super().__init__(source, "project-1", [])
        self._tables = tables

    def model_tables_retrying_failure(self) -> Optional[List[Dict[str, object]]]:
        return self._tables


def _fill_project_caches(source: MicroStrategySource) -> None:
    source._report_sql_statements.put(_REPORT_KEY, _ReportSqlOutcome(sql=None))
    source._type3_objects.put(
        _REPORT_KEY,
        _ReportObjectLookup.from_fetch(ReportObjectLookupKind.DATASET, None),
    )
    source._fingerprint_objects.put(
        _REPORT_KEY,
        _ReportObjectLookup.from_fetch(ReportObjectLookupKind.DATASET, None),
    )
    source._report_derived_metrics.put(_REPORT_KEY, None)
    source._report_v2_payloads.put(_REPORT_KEY, {})
    source._metric_models.put(_METRIC_KEY, None)
    source._model_document_slot.key = object_cache_key("project-1", "doc")
    source._model_document_slot.value = {}
    source.lineage.warehouse_lineage_from_sql(
        "select a from t",
        WarehouseLineageContext(platform="snowflake", env="PROD"),
        [],
    )


def _project_cache_values(source: MicroStrategySource) -> List[object]:
    return [
        source._report_sql_statements.get(_REPORT_KEY),
        source._type3_objects.get(_REPORT_KEY),
        source._fingerprint_objects.get(_REPORT_KEY),
        source._report_derived_metrics.get(_REPORT_KEY),
        source._report_v2_payloads.get(_REPORT_KEY),
        source._metric_models.get(_METRIC_KEY),
        source._model_document_slot.value,
    ]


_EMPTY_PROJECT_CACHES: List[object] = [unset, unset, unset, unset, unset, unset, None]


def _assert_project_caches_empty(source: MicroStrategySource) -> None:
    assert _project_cache_values(source) == _EMPTY_PROJECT_CACHES
    executed = source.report.sql_parses_executed
    source.lineage.warehouse_lineage_from_sql(
        "select a from t",
        WarehouseLineageContext(platform="snowflake", env="PROD"),
        [],
    )
    assert source.report.sql_parses_executed == executed + 1


def test_project_reset_empties_every_per_project_cache() -> None:
    source = _source()
    _fill_project_caches(source)

    source._reset_project_caches()

    _assert_project_caches_empty(source)
    assert source.report.cache_project_resets == 1


def test_a_failing_cache_reset_does_not_skip_the_others() -> None:
    source = _source()
    _fill_project_caches(source)

    with (
        mock.patch.object(
            source._type3_objects, "reset", side_effect=OSError("disk error")
        ),
        pytest.raises(OSError),
    ):
        source._reset_project_caches()

    # Closing the project's spill file also empties the cache whose reset failed.
    _assert_project_caches_empty(source)
    assert source.report.cache_project_resets == 1


def test_report_caches_are_released_when_project_content_raises() -> None:
    source = _source(
        {"extract_source_warehouses": False, "emit_semantic_model_entities": False}
    )
    _fill_project_caches(source)

    def failing_content(
        project: Project, lineage_context: _LazyProjectLineage
    ) -> Iterator[MetadataWorkUnit]:
        raise RuntimeError("project failed")
        yield

    with (
        mock.patch.object(
            source, "_process_project_content", side_effect=failing_content
        ),
        pytest.raises(RuntimeError),
    ):
        list(source._process_project(_PROJECT))

    # Metric models stay for the semantic model until the project is done.
    assert _project_cache_values(source) == [
        unset,
        unset,
        unset,
        unset,
        unset,
        None,
        None,
    ]


def test_each_project_starts_with_empty_caches_and_the_run_closes_them() -> None:
    source = _source()
    projects = [
        _PROJECT,
        Project.model_validate({"id": "project-2", "name": "Second Project"}),
    ]
    values_at_start: List[List[object]] = []

    def process_project(project: Project) -> Iterator[MetadataWorkUnit]:
        values_at_start.append(_project_cache_values(source))
        _fill_project_caches(source)
        return iter([])

    with (
        mock.patch.object(source.client, "login"),
        mock.patch.object(source.client, "close"),
        mock.patch.object(source.client, "list_projects", return_value=projects),
        mock.patch.object(source, "_process_project", side_effect=process_project),
    ):
        assert list(source.get_workunits_internal()) == []

    assert values_at_start == [_EMPTY_PROJECT_CACHES, _EMPTY_PROJECT_CACHES]
    assert source.report.cache_project_resets == 2
    with pytest.raises(RuntimeError):
        source._metric_models.get(_METRIC_KEY)


def test_semantic_model_keeps_metric_models_but_not_report_caches() -> None:
    source = _source({"emit_semantic_model_entities": True})
    metric_key = object_cache_key("project-1", "METRIC-1")
    report_key = object_cache_key("project-1", "REPORT-1")
    source._metric_models.put(metric_key, {"id": "metric-1"})
    source._report_sql_statements.put(report_key, _ReportSqlOutcome(sql="select 1"))
    caches_at_emit: List[Tuple[bool, bool]] = []

    def fake_emit(**kwargs: object) -> Iterator[MetadataWorkUnit]:
        caches_at_emit.append(
            (
                source._metric_models.get(metric_key) is not unset,
                source._report_sql_statements.get(report_key) is not unset,
            )
        )
        return iter([])

    with mock.patch(
        "datahub.ingestion.source.microstrategy.source."
        "MicroStrategySemanticModelMapper.emit",
        side_effect=fake_emit,
    ):
        list(
            source._process_project_semantic_model(
                _PROJECT,
                _FixedTablesLineage(source, [{"physicalTable": {"tableName": "T1"}}]),
            )
        )
    source._reset_project_caches()

    assert caches_at_emit == [(True, False)]
    assert source._metric_models.get(metric_key) is unset
    assert source.report.cache_project_resets == 1


@pytest.mark.parametrize(
    "model_tables, expect_failure",
    [
        # A warning here lets stale removal soft-delete the semantic model.
        (None, True),
        ([], False),
    ],
)
def test_semantic_model_unavailable_fails_only_when_the_fetch_failed(
    model_tables: Optional[List[Dict[str, object]]], expect_failure: bool
) -> None:
    source = _source({"emit_semantic_model_entities": True})

    class FakeLineageContext:
        def model_tables_retrying_failure(
            self,
        ) -> Optional[List[Dict[str, object]]]:
            return model_tables

        warehouse_context = None

    workunits = list(
        source._process_project_semantic_model(
            _PROJECT,
            FakeLineageContext(),  # type: ignore[arg-type]
        )
    )

    assert workunits == []
    assert bool(source.report.failures) is expect_failure
    assert bool(source.report.warnings) is not expect_failure


def _assert_semantic_model_called_only_when_enabled(enabled: bool) -> None:
    project = Project.model_validate({"id": "project-1", "name": "Project 1"})
    source = _source(
        {
            "emit_semantic_model_entities": enabled,
            "extract_dashboards": False,
            "extract_reports": False,
            "extract_source_warehouses": False,
        }
    )
    calls: List[bool] = []

    def fake_process_project_semantic_model(
        project: Project, lineage_context: _LazyProjectLineage
    ) -> Iterator[Any]:
        calls.append(True)
        return iter([])

    source._process_project_semantic_model = (  # type: ignore[method-assign]
        fake_process_project_semantic_model
    )
    list(source._process_project(project))
    assert bool(calls) is enabled


def test_process_project_calls_semantic_model_only_when_enabled() -> None:
    _assert_semantic_model_called_only_when_enabled(True)
    _assert_semantic_model_called_only_when_enabled(False)


@pytest.mark.parametrize("flag", [True, False])
def test_extract_derived_metrics_flag_gates_attachment(flag: bool) -> None:
    source = _source(
        {
            "extract_derived_metrics": flag,
            "extract_warehouse_lineage": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
            "extract_model_lineage": False,
        }
    )

    class FakeClient:
        def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
            return iter(
                [MicroStrategyObject.model_validate({"id": "dash-1", "name": "Dash"})]
            )

        def get_predefined_folders(
            self, project_id: str, folder_types: List[int]
        ) -> List[PredefinedFolder]:
            return _standard_predefined_folders(folder_types)

        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            return {"result": {"definition": {"datasets": [], "chapters": []}}}

    source.client = FakeClient()  # type: ignore[assignment]

    with mock.patch.object(source.mapper, "attach_derived_metrics") as attach:
        list(
            source._process_project_dashboards(
                _PROJECT,
                _LazyProjectLineage(source, "project-1", []),
            )
        )
    assert attach.called is flag


class _DatasetLookupClient:
    """Two dossiers sharing one dataset; counts object-info lookups."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.lookups: List[str] = []

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": dash_id,
                        "name": f"Dash {dash_id}",
                        "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
                    }
                )
                for dash_id in ("dash-1", "dash-2")
            ]
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _standard_predefined_folders(folder_types)

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        return {
            "result": {
                "definition": {
                    "datasets": [{"id": "ds-shared", "name": "Daily Sales Report"}],
                    "chapters": [],
                }
            }
        }

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        self.lookups.append(object_id)
        if self.fail:
            raise MicroStrategyAPIError("boom")
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": "Daily Sales Report",
                "type": "3",
                "subtype": "768",
                "ancestors": [
                    {"id": "f-1", "name": "Shared Reports"},
                    {"id": "f-2", "name": "Regional Sales"},
                ],
            }
        )


def _dataset_lookup_source() -> MicroStrategySource:
    return _source(
        {
            "extract_warehouse_lineage": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
            "extract_model_lineage": False,
        }
    )


def _dataset_container_parents(workunits: List[Any]) -> Dict[str, str]:
    parents: Dict[str, str] = {}
    for workunit in workunits:
        container = workunit.get_aspect_of_type(ContainerClass)
        if container and workunit.get_urn().startswith("urn:li:dataset:"):
            parents[workunit.get_urn()] = container.container
    return parents


def test_dataset_object_info_is_fetched_once_per_dataset_across_dossiers() -> None:
    source = _dataset_lookup_source()
    client = _DatasetLookupClient()
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # The same dataset under two dossiers is two entities but one lookup.
    assert client.lookups == ["ds-shared"]
    assert source.report.dataset_object_lookups == 1
    own_folder = source.mapper.folder_key(
        "project-1", "Shared Reports/Regional Sales"
    ).as_urn()
    parents = _dataset_container_parents(workunits)
    assert len(parents) == 2
    assert set(parents.values()) == {own_folder}


def test_dataset_object_lookup_failure_falls_back_to_dossier_folder() -> None:
    source = _dataset_lookup_source()
    client = _DatasetLookupClient(fail=True)
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # Failure is memoized (one attempt), counted, and degrades to the
    # dossier's folder and URL rather than failing the dashboards.
    assert client.lookups == ["ds-shared"]
    assert source.report.dataset_object_lookup_failures == 1
    dossier_folder = source.mapper.folder_key("project-1", "Shared Reports").as_urn()
    parents = _dataset_container_parents(workunits)
    assert len(parents) == 2
    assert set(parents.values()) == {dossier_folder}
    urls = set()
    for workunit in workunits:
        properties = workunit.get_aspect_of_type(DatasetPropertiesClass)
        if properties is not None:
            urls.add(properties.externalUrl)
    assert urls == {
        "https://mstr.example.com/MicroStrategyLibrary/app/project-1/dash-1",
        "https://mstr.example.com/MicroStrategyLibrary/app/project-1/dash-2",
    }


class _ReportDerivedClient(_DatasetLookupClient):
    """One dossier over a report-backed dataset whose grid shows a derived
    metric; the report definition endpoints can be made to fail."""

    def __init__(
        self,
        model_fails: bool = False,
        v2_fails: bool = False,
        subtype: str = "768",
    ) -> None:
        super().__init__()
        self.model_fails = model_fails
        self.v2_fails = v2_fails
        self.subtype = subtype
        self.model_calls: List[str] = []
        self.v2_calls: List[str] = []

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "dash-1",
                        "name": "D",
                        "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
                    }
                )
            ]
        )

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        return {
            "definition": {
                "datasets": [
                    {
                        "id": "ds-shared",
                        "name": "Daily Sales Report",
                        "availableObjects": {
                            "metrics": [{"id": "M-NET", "name": "Net Sales Amt"}]
                        },
                    }
                ],
                "chapters": [
                    {
                        "key": "ch",
                        "pages": [
                            {
                                "key": "pg",
                                "name": "YESTERDAY",
                                "visualizations": [
                                    {
                                        "key": "viz",
                                        "name": "Grid",
                                        "runtimeDefinition": {
                                            "definition": {
                                                "grid": {
                                                    "columnSets": [
                                                        {
                                                            "key": "cs",
                                                            "name": "RETAIL",
                                                            "columns": [
                                                                {
                                                                    "type": "templateMetrics",
                                                                    "elements": [
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "M-NET",
                                                                            "name": "Net Sales Amt",
                                                                        },
                                                                        {
                                                                            "type": "metric",
                                                                            "id": "D-SALES",
                                                                            "name": "RTL % PLN",
                                                                            "derived": True,
                                                                        },
                                                                    ],
                                                                }
                                                            ],
                                                        }
                                                    ]
                                                }
                                            }
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        }

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        self.lookups.append(object_id)
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": "Daily Sales Report",
                "type": "3",
                "subtype": self.subtype,
            }
        )

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.model_calls.append(report_id)
        if self.model_fails:
            raise MicroStrategyAPIError("404")
        return {
            "information": {"objectId": report_id, "name": "Daily Sales Report"},
            "dataSource": {
                "dataTemplate": {
                    "units": [
                        {
                            "type": "metrics",
                            "elements": [
                                {
                                    "id": "D-SALES",
                                    "name": "SALES PLAN",
                                    "subType": "derived_metric",
                                    "expression": {
                                        "text": "([Net Sales Amt]/[Plan])-1"
                                    },
                                }
                            ],
                        }
                    ]
                }
            },
        }

    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        self.v2_calls.append(report_id)
        if self.v2_fails:
            raise MicroStrategyAPIError("403")
        return {
            "definition": {
                "availableObjects": {
                    "metrics": [
                        {
                            "id": "M-NET",
                            "name": "Net Sales Amt",
                            "type": "metric",
                        },
                        {
                            "id": "D-SALES",
                            "name": "SALES PLAN",
                            "type": "metric",
                            "derived": True,
                        },
                    ]
                }
            }
        }


def _derived_field(workunits: List[Any], name: str) -> Any:
    for workunit in workunits:
        schema = workunit.get_aspect_of_type(SchemaMetadataClass)
        if schema:
            for schema_field in schema.fields:
                if schema_field.fieldPath == name:
                    return schema_field
    return None


def test_report_derived_metrics_come_from_model_report_definition() -> None:
    source = _dataset_lookup_source()
    client = _ReportDerivedClient()
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.model_calls == ["ds-shared"]
    assert client.v2_calls == []
    assert source.report.report_derived_metrics_extracted == 1
    assert source.report.report_definition_failures == 0
    rtl = _derived_field(workunits, "SALES PLAN")
    assert rtl is not None
    assert "([Net Sales Amt]/[Plan])-1" in (rtl.description or "")
    assert _derived_field(workunits, "RTL % PLN") is None


def test_report_derived_metrics_fall_back_to_v2_definition_names() -> None:
    source = _dataset_lookup_source()
    client = _ReportDerivedClient(model_fails=True)
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.model_calls == ["ds-shared"]
    assert client.v2_calls == ["ds-shared"]
    assert source.report.report_definition_failures == 0
    rtl = _derived_field(workunits, "SALES PLAN")
    assert rtl is not None
    # The v2 definition named it but exposed no formula, and the field says
    # which endpoint answered so the gap is diagnosable from the field.
    assert "v2 definition supplies names only" in (rtl.description or "")
    assert source.report.report_model_definition_failures == 1
    assert source.report.report_model_definitions_empty == 0
    assert source.report.report_definitions_without_expressions == 0
    samples = list(source.report.report_model_definition_failure_samples)
    assert len(samples) == 1 and samples[0].startswith("ds-shared: ")
    assert [w.title for w in source.report.warnings] == [_MODEL_UNAVAILABLE_TITLE]


_MODEL_UNAVAILABLE_TITLE = (
    "Modeling report definition unavailable; derived metric formulas omitted"
)


class _TwoReportClient(_ReportDerivedClient):
    """Two dossiers over two different report-backed datasets in one project;
    the Modeling endpoint fails with an HTTP status for both."""

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate({"id": "dash-1", "name": "D1"}),
                MicroStrategyObject.model_validate({"id": "dash-2", "name": "D2"}),
            ]
        )

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        definition = super().get_dossier_definition(project_id, dossier_id)
        definition["definition"]["datasets"][0]["id"] = f"ds-{dossier_id}"
        return definition

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.model_calls.append(report_id)
        raise MicroStrategyAPIError(
            "MicroStrategy API request failed: GET /api/model/reports/x: 403",
            status_code=403,
            url="https://mstr.example.com/api/model/reports/x",
        )


def test_model_definition_failure_is_counted_per_report_and_warned_once() -> None:
    source = _dataset_lookup_source()
    client = _TwoReportClient()
    install_client(source, client)

    list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert sorted(client.model_calls) == ["ds-dash-1", "ds-dash-2"]
    assert sorted(client.v2_calls) == ["ds-dash-1", "ds-dash-2"]
    assert source.report.report_model_definition_failures == 2
    samples = list(source.report.report_model_definition_failure_samples)
    assert sorted(s.split(":")[0] for s in samples) == ["ds-dash-1", "ds-dash-2"]
    # The HTTP status the client recorded is what tells 403 (privilege) from
    # 404 (server without the Modeling service) apart in the report.
    assert all("HTTP 403" in s for s in samples)
    warnings = [
        w for w in source.report.warnings if w.title == _MODEL_UNAVAILABLE_TITLE
    ]
    assert len(warnings) == 1
    assert any(
        f"first_report_id={report_id}" in (warnings[0].context or [""])[0]
        for report_id in ["ds-dash-1", "ds-dash-2"]
    )


class _ModelWithoutExpressionClient(_ReportDerivedClient):
    """The Modeling endpoint answers and flags the derived metric, but its
    formula sits under a key the walker does not read."""

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.model_calls.append(report_id)
        return {
            "information": {"objectId": report_id, "name": "Daily Sales Report"},
            "dataSource": {
                "dataTemplate": {
                    "units": [
                        {
                            "type": "metrics",
                            "elements": [
                                {
                                    "id": "D-SALES",
                                    "name": "SALES PLAN",
                                    "subType": "derived_metric",
                                    "derived": True,
                                    "definition": {"formulaText": "[A]/[B]-1"},
                                }
                            ],
                        }
                    ]
                }
            },
        }


def test_model_definition_without_expressions_is_counted() -> None:
    source = _dataset_lookup_source()
    client = _ModelWithoutExpressionClient()
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.model_calls == ["ds-shared"]
    assert client.v2_calls == []
    assert source.report.report_definitions_without_expressions == 1
    assert source.report.report_model_definition_failures == 0
    rtl = _derived_field(workunits, "SALES PLAN")
    assert rtl is not None
    assert "Formula not present in the Modeling API report definition" in (
        rtl.description or ""
    )


class _ModelEmptyClient(_ReportDerivedClient):
    """The Modeling endpoint answers with a report definition in which the
    walker recognises no derived metric at all, so the
    v2 definition ends up supplying the names."""

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.model_calls.append(report_id)
        return {
            "information": {"objectId": report_id, "name": "Daily Sales Report"},
            "sourceType": "normal",
            "dataSource": {
                "dataTemplate": {
                    "units": [
                        {
                            "type": "attribute",
                            "id": "A-REGION",
                            "name": "Region Number",
                            "forms": [{"id": "F1", "name": "NUMBER"}],
                        }
                    ]
                }
            },
        }


def test_model_definition_empty_is_counted() -> None:
    source = _dataset_lookup_source()
    client = _ModelEmptyClient()
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # Modeling answered (no failure), found nothing (counted), v2 supplied names.
    assert client.model_calls == ["ds-shared"]
    assert client.v2_calls == ["ds-shared"]
    assert source.report.report_model_definition_failures == 0
    assert source.report.report_model_definitions_empty == 1
    assert source.report.report_definitions_without_expressions == 0
    assert source.report.warnings == []
    rtl = _derived_field(workunits, "SALES PLAN")
    assert rtl is not None
    assert "v2 definition supplies names only" in (rtl.description or "")


class _ModelEmbeddedClient(_ReportDerivedClient):
    """The live Modeling report definition shape: grid column elements with
    the plain "metric" subtype, report-level derived metrics marked
    isEmbedded, no expression anywhere; the metric model endpoint can then
    answer (or refuse) for the embedded metric's id."""

    def __init__(self, metric_model_fails: bool = False) -> None:
        super().__init__()
        self.metric_model_fails = metric_model_fails
        self.metric_model_calls: List[str] = []

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.model_calls.append(report_id)
        return {
            "information": {"objectId": report_id, "name": "Daily Sales Report"},
            "sourceType": "normal",
            "dataSource": {
                "dataTemplate": {
                    "units": [
                        {
                            "id": "A-REGION",
                            "name": "Region Number",
                            "type": "attribute",
                        },
                        {
                            "type": "metrics",
                            "elements": [
                                {
                                    "id": "M-NET",
                                    "name": "Net Sales Amt",
                                    "subType": "metric",
                                    "evaluationOrder": 0,
                                },
                                {
                                    "id": "D-SALES",
                                    "name": "New Metric",
                                    "subType": "metric",
                                    "evaluationOrder": 1,
                                },
                            ],
                        },
                    ]
                }
            },
            "grid": {
                "viewTemplate": {
                    "columns": {
                        "units": [
                            {
                                "type": "metrics",
                                "elements": [
                                    {
                                        "id": "M-NET",
                                        "name": "Net Sales Amt",
                                        "subType": "metric",
                                        "isEmbedded": False,
                                        "alias": "Net Sales Amt",
                                    },
                                    {
                                        "id": "D-SALES",
                                        "name": "New Metric",
                                        "subType": "metric",
                                        "isEmbedded": True,
                                        "alias": "SALES PLAN",
                                    },
                                ],
                            }
                        ],
                        "hiddenMetrics": [],
                    }
                }
            },
        }

    def get_metric_model(
        self, project_id: str, metric_id: str, stop: object = None
    ) -> Dict[str, Any]:
        self.metric_model_calls.append(metric_id)
        if self.metric_model_fails:
            raise MicroStrategyAPIError("404 Not Found", status_code=404, url="x")
        return {
            "information": {"objectId": metric_id, "name": "New Metric"},
            "expression": {
                "text": "([Net Sales Amt]/[Store Net Sales Amt])-1",
                "tokens": [
                    {
                        "type": "object_reference",
                        "value": "Net Sales Amt",
                        "target": {
                            "objectId": "M-NET",
                            "name": "Net Sales Amt",
                            "type": "metric",
                        },
                    },
                ],
            },
        }


def _embedded_metric_source() -> MicroStrategySource:
    return _source(
        {
            "extract_warehouse_lineage": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": True,
            "extract_model_lineage": False,
        }
    )


def test_embedded_report_metric_formula_comes_from_metric_model() -> None:
    source = _embedded_metric_source()
    client = _ModelEmbeddedClient()
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # The Modeling definition named the embedded metric, so v2 was not needed,
    # and only the embedded (formula-less) metric went to the model endpoint.
    assert client.model_calls == ["ds-shared"]
    assert client.v2_calls == []
    # M-NET is the dataset's catalog metric, enriched as before; the embedded
    # metric is looked up exactly once on top of that.
    assert client.metric_model_calls == ["M-NET", "D-SALES"]
    assert source.report.report_model_definitions_empty == 0
    assert source.report.report_derived_metric_models_resolved == 1
    assert source.report.metric_expression_api_failures == 0
    rtl = _derived_field(workunits, "SALES PLAN")
    assert rtl is not None
    assert "([Net Sales Amt]/[Store Net Sales Amt])-1" in (rtl.description or "")
    props = json.loads(rtl.jsonProps or "{}")
    assert props["microstrategyDerivedMetricSource"] == "report"
    assert "microstrategyMetricExpressionText" in props
    # Displayed under the grid alias, with the stored object name kept.
    assert props["microstrategyObjectName"] == "New Metric"


def test_embedded_report_metric_without_model_keeps_name_and_counts_failure() -> None:
    source = _embedded_metric_source()
    client = _ModelEmbeddedClient(metric_model_fails=True)
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.metric_model_calls == ["M-NET", "D-SALES"]
    assert source.report.report_derived_metric_models_resolved == 0
    assert source.report.metric_expression_api_failures == 2
    assert any(
        sample.startswith("D-SALES (HTTP 404)")
        for sample in source.report.failed_metric_model_ids
    )
    rtl = _derived_field(workunits, "SALES PLAN")
    assert rtl is not None
    assert "Formula not present in the Modeling API report definition" in (
        rtl.description or ""
    )


def test_report_derived_metrics_keep_grid_provenance_when_definitions_fail() -> None:
    source = _dataset_lookup_source()
    client = _ReportDerivedClient(model_fails=True, v2_fails=True)
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert source.report.report_definition_failures == 1
    assert source.report.report_derived_metrics_extracted == 0
    grid_only = _derived_field(workunits, "RTL % PLN")
    assert grid_only is not None
    assert "visualization 'Grid'" in (grid_only.description or "")


def test_report_derived_metrics_skip_cube_datasets() -> None:
    source = _dataset_lookup_source()
    client = _ReportDerivedClient(subtype="776")
    install_client(source, client)

    list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.model_calls == []
    assert client.v2_calls == []


_LINEAGE_PARSER_FN = (
    "datahub.ingestion.source.microstrategy.lineage.create_lineage_from_sql_statements"
)
_ALL_FAILED_TITLE = "Every MicroStrategy SQL view failed to parse"


def _warehouse_dashboard() -> DashboardDefinition:
    return _dashboard(
        {"id": "s", "name": "W", "database": {"type": "snow_flake", "name": "DB"}}
    )


def test_configuration_error_escapes_the_dashboard_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Per-dashboard boundaries swallow API errors to keep going; an
    # environmental error must not be reduced to a "Failed to Process
    # Dashboard" warning.
    source = _source({"extract_warehouse_lineage": False})

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise ConfigurationError("parser missing")

    monkeypatch.setattr(source, "_consume_prefetched_dashboard", boom)
    source.client = _DatasetLookupClient()  # type: ignore[assignment]

    with pytest.raises(ConfigurationError):
        list(
            source._process_project_dashboards(
                _PROJECT, _LazyProjectLineage(source, "project-1", [])
            )
        )


def _fail_parses(monkeypatch: pytest.MonkeyPatch, fail_first_n: int) -> None:
    calls = {"count": 0}
    real = create_lineage_from_sql_statements

    def flaky(**kwargs: Any) -> Any:
        calls["count"] += 1
        if calls["count"] <= fail_first_n:
            raise ValueError("boom")
        return real(**kwargs)

    monkeypatch.setattr(_LINEAGE_PARSER_FN, flaky)


def test_every_sql_view_failing_raises_one_loud_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    _fail_parses(monkeypatch, fail_first_n=2)
    source._attach_dataset_warehouse_upstreams(
        [
            {"id": "ds-1", "sqlStatement": "select a from DB.S.T"},
            {"id": "ds-1", "sqlStatement": "select b from DB.S.U"},
        ],
        _warehouse_dashboard(),
        None,
        entries={},
        project_id="project-1",
    )
    assert source.report.sql_views_parsed == 2
    assert source.report.sql_parse_failure_count == 2

    source._warn_if_every_sql_view_failed()

    loud = [
        entry for entry in source.report.warnings if entry.title == _ALL_FAILED_TITLE
    ]
    assert len(loud) == 1
    assert any(
        ctx.startswith("first_failure=Sales Cube (ds-1)")
        for ctx in (loud[0].context or [])
    )


def test_all_failed_warning_counts_reused_failed_sql_views(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A cached failure must still count as failed for the second document.
    source = _source()
    _fail_parses(monkeypatch, fail_first_n=2)
    for _ in range(2):
        source._attach_dataset_warehouse_upstreams(
            [{"id": "ds-1", "sqlStatement": "select a from DB.S.T"}],
            _warehouse_dashboard(),
            None,
            entries={},
            project_id="project-1",
        )
    assert source.report.sql_views_parsed == 2
    assert source.report.sql_parse_failure_count == 1
    assert source.report.sql_parse_failure_cache_hits == 1

    source._warn_if_every_sql_view_failed()

    loud = [
        entry for entry in source.report.warnings if entry.title == _ALL_FAILED_TITLE
    ]
    assert len(loud) == 1


def test_all_failed_warning_is_silent_when_any_sql_view_parses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    _fail_parses(monkeypatch, fail_first_n=1)
    source._attach_dataset_warehouse_upstreams(
        [
            {"id": "ds-1", "sqlStatement": "select a from DB.S.T"},
            {"id": "ds-1", "sqlStatement": "select b from DB.S.U"},
        ],
        _warehouse_dashboard(),
        None,
        entries={},
        project_id="project-1",
    )
    assert source.report.sql_views_parsed == 2
    assert source.report.sql_parse_failure_count == 1

    source._warn_if_every_sql_view_failed()

    assert not any(entry.title == _ALL_FAILED_TITLE for entry in source.report.warnings)


_PERSONAL_ANCESTORS = [
    {"id": "project-root-id", "name": "Sales Analytics"},
    {"id": "profiles-id", "name": "Profiles"},
    {"id": "user-id", "name": "jdoe"},
    {"id": "my-reports-id", "name": "My Reports"},
]
_SHARED_ANCESTORS = [
    {"id": "project-root-id", "name": "Sales Analytics"},
    {"id": "public-objects-id", "name": "Public Objects"},
    {"id": "reports-folder-id", "name": "Reports"},
]
_PERSONAL_ROOT_TITLE = "Personal folder root not resolved"


def _personal_folder_source(extra_config: dict | None = None) -> MicroStrategySource:
    config = {
        "extract_warehouse_lineage": False,
        "extract_visualization_details": False,
        "extract_dashboard_dependencies": False,
        "extract_metric_expressions": False,
        "extract_model_lineage": False,
        "extract_derived_metrics": False,
    }
    config.update(extra_config or {})
    return _source(config)


class _PersonalFolderClient:
    """A personal copy of a dossier beside the shared original. The shared
    dossier embeds a dataset that is itself filed under a personal folder."""

    def __init__(
        self, resolve_by_id: bool = True, personal_call_fails: bool = False
    ) -> None:
        self.resolve_by_id = resolve_by_id
        self.personal_call_fails = personal_call_fails
        self.predefined_calls: List[List[int]] = []
        self.definition_calls: List[str] = []
        self.object_info_calls: List[Tuple[str, int]] = []

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "dash-personal",
                        "name": "Copy of Sales",
                        "ancestors": _PERSONAL_ANCESTORS,
                    }
                ),
                MicroStrategyObject.model_validate(
                    {
                        "id": "dash-shared",
                        "name": "Sales",
                        "ancestors": _SHARED_ANCESTORS,
                    }
                ),
            ]
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        self.predefined_calls.append(list(folder_types))
        if folder_types == [19, 20]:
            if self.personal_call_fails:
                raise MicroStrategyAPIError("forbidden", status_code=403)
            if not self.resolve_by_id:
                return []
            return [
                PredefinedFolder.model_validate(
                    {"id": "user-id", "name": "jdoe", "folderType": 19}
                ),
                PredefinedFolder.model_validate(
                    {"id": "my-reports-id", "name": "My Reports", "folderType": 20}
                ),
            ]
        return [
            PredefinedFolder.model_validate(
                {"id": "project-root-id", "name": "Sales Analytics", "folderType": 39}
            ),
            PredefinedFolder.model_validate(
                {"id": "public-objects-id", "name": "Public Objects", "folderType": 1}
            ),
            PredefinedFolder.model_validate(
                {"id": "reports-folder-id", "name": "Reports", "folderType": 7}
            ),
        ]

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        self.object_info_calls.append((object_id, object_type))
        if object_type == 8:
            # The principal's own profile folder: its parent is Profiles.
            return MicroStrategyObject.model_validate(
                {
                    "id": object_id,
                    "name": "jdoe",
                    "type": "8",
                    "ancestors": _PERSONAL_ANCESTORS[:2],
                }
            )
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": "Personal Dataset",
                "type": "3",
                "subtype": "768",
                "ancestors": _PERSONAL_ANCESTORS,
            }
        )

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        self.definition_calls.append(dossier_id)
        return {
            "result": {
                "definition": {
                    "datasets": [{"id": "ds-personal", "name": "Personal Dataset"}],
                    "chapters": [],
                }
            }
        }


def _container_names(workunits: List[Any]) -> set:
    names = set()
    for workunit in workunits:
        properties = workunit.get_aspect_of_type(ContainerPropertiesClass)
        if properties is not None:
            names.add(properties.name)
    return names


def _run_dashboards(source: MicroStrategySource) -> List[Any]:
    return list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )


def test_personal_folder_objects_skipped_by_resolved_profiles_root_id() -> None:
    source = _personal_folder_source()
    client = _PersonalFolderClient()
    install_client(source, client)

    workunits = _run_dashboards(source)

    # Resolution: one extra predefined call plus the profile folder's object
    # info; the Profiles root is that folder's parent.
    assert [19, 20] in client.predefined_calls
    assert ("user-id", 8) in client.object_info_calls
    assert source._personal_folders("project-1").root_ids == {
        "PROFILES-ID",
        "USER-ID",
        "MY-REPORTS-ID",
    }
    assert source.report.personal_folder_roots_resolved == 1
    # The personal copy is dropped before its definition is fetched.
    assert client.definition_calls == ["dash-shared"]
    assert source.report.personal_folder_objects_skipped == 1
    assert list(source.report.personal_folder_objects_skipped_samples) == [
        "Copy of Sales"
    ]
    urns = {workunit.get_urn() for workunit in workunits}
    assert source.mapper.dashboard_urn("project-1", "dash-shared") in urns
    assert source.mapper.dashboard_urn("project-1", "dash-personal") not in urns
    # No container under the Profiles tree, even for the personally filed
    # dataset the shared dossier embeds; that dataset is kept and parented
    # under the dossier's own folder instead.
    assert _container_names(workunits) == {"Shared Reports"}
    parents = _dataset_container_parents(workunits)
    assert set(parents.values()) == {
        source.mapper.folder_key("project-1", "Shared Reports").as_urn()
    }


def test_personal_folder_objects_skipped_by_name_when_root_unresolved() -> None:
    source = _personal_folder_source()
    client = _PersonalFolderClient(resolve_by_id=False)
    install_client(source, client)

    workunits = _run_dashboards(source)

    assert source._personal_folders("project-1").by_name
    assert source.report.personal_folder_roots_resolved == 0
    assert ("user-id", 8) not in client.object_info_calls
    assert client.definition_calls == ["dash-shared"]
    assert source.report.personal_folder_objects_skipped == 1
    assert _container_names(workunits) == {"Shared Reports"}


def test_personal_folder_resolution_failure_falls_back_to_names_once() -> None:
    source = _personal_folder_source()
    client = _PersonalFolderClient(personal_call_fails=True)
    install_client(source, client)

    _run_dashboards(source)

    assert client.predefined_calls.count([19, 20]) == 1
    assert source.report.personal_folder_objects_skipped == 1
    warnings = [w for w in source.report.warnings if w.title == _PERSONAL_ROOT_TITLE]
    assert len(warnings) == 1


def test_include_personal_folders_keeps_everything_and_skips_resolution() -> None:
    source = _personal_folder_source({"include_personal_folders": True})
    client = _PersonalFolderClient()
    install_client(source, client)

    workunits = _run_dashboards(source)

    assert [19, 20] not in client.predefined_calls
    assert sorted(client.definition_calls) == ["dash-personal", "dash-shared"]
    assert source.report.personal_folder_objects_skipped == 0
    urns = {workunit.get_urn() for workunit in workunits}
    assert source.mapper.dashboard_urn("project-1", "dash-personal") in urns
    assert {"Profiles", "jdoe", "My Reports", "Shared Reports"} <= _container_names(
        workunits
    )


class _PersonalReportClient(_ReportSearchClient):
    def __init__(self) -> None:
        super().__init__()
        self.definition_calls: List[str] = []

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "REPORT-PERSONAL",
                        "name": "My Copy",
                        "type": "3",
                        "ancestors": _PERSONAL_ANCESTORS,
                    }
                ),
                MicroStrategyObject.model_validate(
                    {
                        "id": "REPORT-SHARED",
                        "name": "Shared",
                        "type": "3",
                        "ancestors": _SHARED_ANCESTORS,
                    }
                ),
            ]
        )

    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        self.definition_calls.append(report_id)
        return {}


def test_personal_folder_reports_skipped_before_definition_fetch() -> None:
    source = _report_scope_source(
        {"extract_independent_reports": True, "extract_report_definitions": True}
    )
    client = _PersonalReportClient()
    install_client(source, client)

    workunits = list(
        source._process_project_reports(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # _ReportSearchClient resolves no predefined folders: name fallback.
    assert client.definition_calls == ["REPORT-SHARED"]
    assert source.report.personal_folder_objects_skipped == 1
    urns = {workunit.get_urn() for workunit in workunits}
    assert source.mapper.report_urn("project-1", "REPORT-SHARED") in urns
    assert source.mapper.report_urn("project-1", "REPORT-PERSONAL") not in urns


_SALES_PROJECT = Project.model_validate({"id": "project-1", "name": "Sales_Project"})
_COMPANY_ANCESTORS = [
    {"id": "project-root-id", "name": "Sales_Project"},
    {"id": "public-objects-id", "name": "Public Objects"},
    {"id": "reports-folder-id", "name": "Reports"},
    {"id": "f-finance", "name": "Finance"},
    {"id": "f-close", "name": "Monthly Close"},
]
_OTHER_ANCESTORS = [
    {"id": "project-root-id", "name": "Sales_Project"},
    {"id": "public-objects-id", "name": "Public Objects"},
    {"id": "reports-folder-id", "name": "Reports"},
    {"id": "f-other", "name": "Other Team"},
]
_CLOSE_PATTERN = "^Sales_Project/Shared Reports/Finance/Monthly Close$"
_UNRESOLVED_TITLE = "Folder path unresolved"


def _adhoc_predefined_folders(folder_types: List[int]) -> List[PredefinedFolder]:
    if folder_types == [19, 20]:
        return []
    return [
        PredefinedFolder.model_validate(
            {"id": "project-root-id", "name": "Sales_Project", "folderType": 39}
        ),
        PredefinedFolder.model_validate(
            {"id": "public-objects-id", "name": "Public Objects", "folderType": 1}
        ),
        PredefinedFolder.model_validate(
            {"id": "reports-folder-id", "name": "Reports", "folderType": 7}
        ),
    ]


class _FolderPathClient:
    """Three dossiers: one under the reporting folder, one under a
    sibling folder, and one whose search result carries no ancestry."""

    def __init__(self) -> None:
        self.definition_calls: List[str] = []

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "dash-company",
                        "name": "Company P&L",
                        "ancestors": _COMPANY_ANCESTORS,
                    }
                ),
                MicroStrategyObject.model_validate(
                    {
                        "id": "dash-other",
                        "name": "Other Team KPIs",
                        "ancestors": _OTHER_ANCESTORS,
                    }
                ),
                MicroStrategyObject.model_validate(
                    {"id": "dash-unknown", "name": "Unfiled"}
                ),
            ]
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _adhoc_predefined_folders(folder_types)

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        self.definition_calls.append(dossier_id)
        return {
            "result": {
                "definition": {
                    "datasets": [{"id": f"ds-{dossier_id}", "name": "Dataset"}],
                    "chapters": [
                        {
                            "key": "ch-1",
                            "name": "Chapter",
                            "pages": [
                                {
                                    "key": "pg-1",
                                    "name": "Page",
                                    "visualizations": [
                                        {"key": f"viz-{dossier_id}", "name": "Grid"}
                                    ],
                                }
                            ],
                        }
                    ],
                }
            }
        }

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": "Dataset",
                "type": "3",
                "subtype": "768",
                "ancestors": _OTHER_ANCESTORS,
            }
        )


def _run_adhoc_dashboards(source: MicroStrategySource) -> List[Any]:
    return list(
        source._process_project_dashboards(
            _SALES_PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )


def test_folder_path_pattern_allows_only_the_matching_nested_path() -> None:
    source = _personal_folder_source(
        {"folder_path_pattern": {"allow": [_CLOSE_PATTERN]}}
    )
    client = _FolderPathClient()
    install_client(source, client)

    workunits = _run_adhoc_dashboards(source)

    # Denied dossiers cost no definition fetch; the unfiled one cannot be
    # matched, and a configured pattern treats that as not matching.
    assert client.definition_calls == ["dash-company"]
    assert source.report.dashboards_filtered_by_folder_path == 2
    assert list(source.report.folder_path_filtered_samples) == [
        "Other Team KPIs (Sales_Project/Shared Reports/Other Team)",
        "Unfiled (unresolved)",
    ]
    assert source.report.folder_paths_unresolved == 1
    urns = {workunit.get_urn() for workunit in workunits}
    assert source.mapper.dashboard_urn("project-1", "dash-company") in urns
    assert source.mapper.dashboard_urn("project-1", "dash-other") not in urns
    assert source.mapper.dashboard_urn("project-1", "dash-unknown") not in urns
    # Charts and datasets follow their dashboard: one of each, for the
    # allowed dossier only.
    assert (
        source.mapper.chart_urn("project-1", "dash-company", "viz-dash-company") in urns
    )
    assert sum(1 for w in workunits if w.get_aspect_of_type(ChartInfoClass)) == 1
    assert (
        sum(1 for w in workunits if w.get_aspect_of_type(DatasetPropertiesClass)) == 1
    )


def test_folder_path_pattern_deny_wins_over_allow() -> None:
    source = _personal_folder_source(
        {
            "folder_path_pattern": {
                "allow": ["^Sales_Project/Shared Reports/"],
                "deny": ["^Sales_Project/Shared Reports/Finance/"],
            }
        }
    )
    client = _FolderPathClient()
    install_client(source, client)

    _run_adhoc_dashboards(source)

    assert client.definition_calls == ["dash-other"]
    assert source.report.dashboards_filtered_by_folder_path == 2


def test_folder_path_pattern_default_ingests_everything() -> None:
    source = _personal_folder_source()
    client = _FolderPathClient()
    install_client(source, client)

    _run_adhoc_dashboards(source)

    # Default allow-all: no behaviour change, and the unfiled dossier is let
    # through rather than silently dropped, with the gap recorded.
    assert sorted(client.definition_calls) == [
        "dash-company",
        "dash-other",
        "dash-unknown",
    ]
    assert source.report.dashboards_filtered_by_folder_path == 0
    assert source.report.folder_paths_unresolved == 1
    assert [w.title for w in source.report.warnings] == [_UNRESOLVED_TITLE]


def test_folder_path_unresolved_is_skipped_once_a_pattern_is_set() -> None:
    source = _personal_folder_source(
        {"folder_path_pattern": {"allow": ["^Sales_Project/"]}}
    )
    client = _FolderPathClient()
    install_client(source, client)

    _run_adhoc_dashboards(source)

    # Both filed dossiers match; the unfiled one cannot be shown to, so it
    # is skipped and counted rather than widening the scope.
    assert sorted(client.definition_calls) == ["dash-company", "dash-other"]
    assert source.report.folder_paths_unresolved == 1
    assert source.report.dashboards_filtered_by_folder_path == 1
    assert list(source.report.folder_path_filtered_samples) == ["Unfiled (unresolved)"]
    assert [w.title for w in source.report.warnings] == [_UNRESOLVED_TITLE]


class _FolderPathReportClient(_ReportSearchClient):
    def __init__(self) -> None:
        super().__init__()
        self.definition_calls: List[str] = []

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _adhoc_predefined_folders(folder_types)

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        self.search_calls += 1
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "REPORT-COMPANY",
                        "name": "Company Detail",
                        "type": "3",
                        "ancestors": _COMPANY_ANCESTORS,
                    }
                ),
                MicroStrategyObject.model_validate(
                    {
                        "id": "REPORT-OTHER",
                        "name": "Other Detail",
                        "type": "3",
                        "ancestors": _OTHER_ANCESTORS,
                    }
                ),
            ]
        )

    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        self.definition_calls.append(report_id)
        return {}


def test_folder_path_pattern_scopes_reports_before_definition_fetch() -> None:
    source = _report_scope_source(
        {
            "extract_independent_reports": True,
            "extract_report_definitions": True,
            "folder_path_pattern": {"allow": [_CLOSE_PATTERN]},
        }
    )
    client = _FolderPathReportClient()
    install_client(source, client)

    workunits = list(
        source._process_project_reports(
            _SALES_PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.definition_calls == ["REPORT-COMPANY"]
    assert source.report.reports_filtered_by_folder_path == 1
    assert list(source.report.folder_path_filtered_samples) == [
        "Other Detail (Sales_Project/Shared Reports/Other Team)"
    ]
    urns = {workunit.get_urn() for workunit in workunits}
    assert source.mapper.report_urn("project-1", "REPORT-COMPANY") in urns
    assert source.mapper.report_urn("project-1", "REPORT-OTHER") not in urns


def test_folder_path_pattern_applies_to_dashboard_linked_reports() -> None:
    # A report an allowed dashboard links to is still filtered on its own
    # path. _ReportSearchClient resolves no predefined folders, so the raw
    # ancestor name is the path.
    source = _report_scope_source(
        {"folder_path_pattern": {"deny": ["^Sales Analytics/Shared Reports"]}}
    )
    client = _ReportSearchClient()
    install_client(source, client)

    workunits = list(
        source._process_project_reports(
            _PROJECT, _LazyProjectLineage(source, "project-1", []), {"REPORT-1"}
        )
    )

    assert client.object_info_calls == ["REPORT-1"]
    assert source.report.reports_filtered_by_folder_path == 1
    assert list(source.report.folder_path_filtered_samples) == [
        "Linked Report (Sales Analytics/Shared Reports)"
    ]
    assert workunits == []


class _InstanceModelClient(_ModelEmbeddedClient):
    """Records the instance lifecycle around the Modeling definition reads:
    Modeling report instances for GET /api/model/reports/{id}, and any
    dashboard execution around GET /api/model/documents/{id} (there must be
    none). Every method the instance option calls is defined, so a missing
    one cannot be swallowed by a broad except."""

    def __init__(
        self,
        instance_fails: bool = False,
        model_document: Optional[Dict[str, Any]] = None,
        dashboard_instance_fails: bool = False,
        model_document_fails: bool = False,
    ) -> None:
        super().__init__()
        self.instance_fails = instance_fails
        self.created: List[str] = []
        self.deleted: List[str] = []
        self.instance_ids_seen: List[Optional[str]] = []
        self.model_document = model_document or {}
        self.dashboard_instance_fails = dashboard_instance_fails
        self.model_document_fails = model_document_fails
        self.dashboard_created: List[str] = []
        self.dashboard_deleted: List[str] = []
        self.document_instance_ids_seen: List[Optional[str]] = []

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        if self.dashboard_instance_fails:
            raise MicroStrategyAPIError("Read timed out (180s)")
        self.dashboard_created.append(dossier_id)
        return f"dinst-{dossier_id}"

    def delete_dossier_instance(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> bool:
        self.dashboard_deleted.append(instance_id)
        return True

    def get_model_document(
        self,
        project_id: str,
        document_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.document_instance_ids_seen.append(instance_id)
        if self.model_document_fails:
            raise MicroStrategyAPIError("404 Not Found")
        return self.model_document

    def create_model_report_instance(self, project_id: str, report_id: str) -> str:
        if self.instance_fails:
            raise MicroStrategyAPIError("500 report instance failed")
        self.created.append(report_id)
        return f"inst-{report_id}"

    def delete_model_report_instance(
        self, project_id: str, report_id: str, instance_id: str
    ) -> bool:
        self.deleted.append(instance_id)
        return True

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        raise AssertionError("an execution instance is never used for a model read")

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.instance_ids_seen.append(instance_id)
        return super().get_model_report(project_id, report_id, instance_id)


def test_report_metrics_read_statically_unless_instance_option_is_on() -> None:
    # Default behaviour: no report is executed and the Modeling definition is
    # read without an instance header.
    source = _embedded_metric_source()
    client = _InstanceModelClient()
    install_client(source, client)

    list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.created == []
    assert client.deleted == []
    assert client.instance_ids_seen == [None]
    assert source.report.report_model_instances_created == 0


def test_report_instance_is_released_when_auth_is_lost() -> None:
    source = _embedded_metric_source()
    source.config.resolve_report_metrics_via_instance = True

    class AuthLostClient(_InstanceModelClient):
        def get_model_report(
            self,
            project_id: str,
            report_id: str,
            instance_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            raise MicroStrategyAuthError("session invalidated")

    client = AuthLostClient()
    install_client(source, client)

    with pytest.raises(MicroStrategyAuthError):
        source._report_derived_metric_definitions("project-1", "ds-shared")

    assert client.created == ["ds-shared"]
    assert client.deleted == ["inst-ds-shared"]


def test_instance_option_executes_report_and_releases_the_instance() -> None:
    source = _embedded_metric_source()
    source.config.resolve_report_metrics_via_instance = True
    client = _InstanceModelClient()
    install_client(source, client)

    list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # Executed once, handed to the Modeling read, and cleaned up afterwards so
    # a long run does not leave instances alive on the server.
    assert client.created == ["ds-shared"]
    assert client.instance_ids_seen == ["inst-ds-shared"]
    assert client.deleted == ["inst-ds-shared"]
    assert source.report.report_model_instances_created == 1


def test_instance_failure_falls_back_to_the_static_definition() -> None:
    # Executing a report can fail or time out; that must degrade to the
    # long-standing static read rather than losing the definition entirely.
    source = _embedded_metric_source()
    source.config.resolve_report_metrics_via_instance = True
    client = _InstanceModelClient(instance_fails=True)
    install_client(source, client)

    list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.created == []
    assert client.deleted == []
    assert client.instance_ids_seen == [None]
    assert source.report.report_model_instance_failures == 1
    assert source.report.report_model_instances_created == 0


def _forecast_model_document() -> Dict[str, Any]:
    # Document-level derived metric defined in the dashboard on top of the
    # dataset, in the shape GET /api/model/documents/{id} nests it.
    return {
        "datasets": [
            {
                "information": {"objectId": "ds-shared", "name": "Retail Sales"},
                "derivedMetrics": [
                    {
                        "information": {"objectId": "F" * 32, "name": "Forecast"},
                        "expression": {
                            "text": "[Percent to Grand Total]*[BF Plan Net Sales]"
                        },
                    }
                ],
                "derivedAttributes": [
                    {
                        "information": {"objectId": "A" * 32, "name": "Band"},
                        "forms": [
                            {
                                "id": "B" * 32,
                                "expressions": [{"expression": {"text": "attr"}}],
                            }
                        ],
                    }
                ],
            }
        ]
    }


def test_document_derived_metrics_are_not_read_unless_instance_option_is_on() -> None:
    source = _embedded_metric_source()
    client = _InstanceModelClient(model_document=_forecast_model_document())
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # Default output is unchanged: no model document read for derived metrics.
    assert client.dashboard_created == []
    assert client.document_instance_ids_seen == []
    assert _derived_field(workunits, "Forecast") is None


def test_document_derived_metric_formula_read_from_static_model_document() -> None:
    source = _embedded_metric_source()
    source.config.resolve_report_metrics_via_instance = True
    client = _InstanceModelClient(model_document=_forecast_model_document())
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    # Read without an instance: the Modeling service rejects a dashboard
    # execution instance, so the dashboard is never executed for this read.
    assert client.dashboard_created == []
    assert client.document_instance_ids_seen == [None]
    assert source.report.document_derived_metric_definitions == 1
    assert source.report.document_derived_metric_expressions == 1

    forecast = _derived_field(workunits, "Forecast")
    assert forecast is not None
    assert "[Percent to Grand Total]*[BF Plan Net Sales]" in (
        forecast.description or ""
    )
    props = json.loads(forecast.jsonProps or "{}")
    assert props["microstrategyDerivedMetricSource"] == "document"
    # The derived attribute's form expression is never read as a metric.
    assert _derived_field(workunits, "Band") is None


def test_model_document_failure_is_counted_and_skipped() -> None:
    source = _embedded_metric_source()
    source.config.resolve_report_metrics_via_instance = True
    client = _InstanceModelClient(model_document_fails=True)
    install_client(source, client)

    workunits = list(
        source._process_project_dashboards(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )

    assert client.document_instance_ids_seen == [None]
    assert client.dashboard_created == []
    assert source.report.document_model_definition_failures == 1
    assert _derived_field(workunits, "Forecast") is None


class _ExecutionClient:
    """Dossier instance lifecycle for the shared-instance and timeout-limit
    tests: creation can time out (a requests.Timeout cause, as the client
    raises it), fail fast, or succeed."""

    def __init__(self, outcomes: Optional[List[str]] = None) -> None:
        # One entry per creation: "ok", "timeout" or "error".
        self.outcomes = list(outcomes or [])
        self.created: List[str] = []
        self.deleted: List[str] = []
        self.sql_view_instances: List[str] = []

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        self.created.append(dossier_id)
        if outcome == "timeout":
            error = MicroStrategyAPIError("Read timed out (180s)")
            error.__cause__ = requests.ReadTimeout()
            raise error
        if outcome == "error":
            raise MicroStrategyAPIError("500 Server Error", status_code=500)
        return f"inst-{dossier_id}"

    def delete_dossier_instance(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> bool:
        self.deleted.append(instance_id)
        return True

    def get_dossier_datasets_sql(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> List[Dict[str, object]]:
        self.sql_view_instances.append(instance_id)
        return []


def _dossier_object(dossier_id: str) -> MicroStrategyObject:
    return MicroStrategyObject.model_validate({"id": dossier_id, "name": dossier_id})


def test_dashboard_instance_is_shared_and_released_once() -> None:
    # Creating an instance executes the dashboard, so visualization details
    # and SQL-view lineage share one rather than executing it twice.
    source = _source()
    client = _ExecutionClient()
    install_client(source, client)
    dossier = _dossier_object("dash-1")

    first = source._dashboard_instance("project-1", dossier)
    source._enrich_warehouse_lineage("project-1", dossier, _dashboard(), None)

    assert first == "inst-dash-1"
    assert client.created == ["dash-1"]
    assert client.sql_view_instances == ["inst-dash-1"]
    assert client.deleted == []
    assert source.report.dashboard_instances_reused == 1

    source._release_dashboard_instance("project-1", dossier)
    source._release_dashboard_instance("project-1", dossier)
    assert client.deleted == ["inst-dash-1"]


def test_timed_out_dashboard_is_not_executed_twice() -> None:
    source = _source()
    client = _ExecutionClient(["timeout"])
    install_client(source, client)
    dossier = _dossier_object("dash-1")

    with pytest.raises(MicroStrategyAPIError):
        source._dashboard_instance("project-1", dossier)
    # The second consumer gets the remembered failure without a second wait.
    source._enrich_warehouse_lineage("project-1", dossier, _dashboard(), None)

    assert client.created == ["dash-1"]
    assert client.sql_view_instances == []
    assert source.report.execution_timeouts == 1
    source._release_dashboard_instance("project-1", dossier)
    assert client.deleted == []


def test_consecutive_timeouts_stop_executions_in_the_project() -> None:
    source = _source({"max_consecutive_execution_timeouts": 2})
    client = _ExecutionClient(["timeout", "timeout"])
    install_client(source, client)

    for dossier_id in ("dash-1", "dash-2", "dash-3"):
        dossier = _dossier_object(dossier_id)
        source._enrich_warehouse_lineage("project-1", dossier, _dashboard(), None)
        source._release_dashboard_instance("project-1", dossier)
    # Another project is unaffected.
    other = _dossier_object("dash-9")
    source._enrich_warehouse_lineage("project-2", other, _dashboard(), None)

    assert client.created == ["dash-1", "dash-2", "dash-9"]
    assert source.report.execution_timeouts == 2
    assert source.report.executions_skipped_after_timeouts == 1
    assert list(source.report.execution_timeout_limit_projects) == ["project-1"]
    assert any(
        warning.title == "Stopped executing content after repeated timeouts"
        for warning in source.report.warnings
    )


def test_fast_failures_and_successes_do_not_trip_the_timeout_limit() -> None:
    # Only a timeout costs a wait. A fast error neither counts nor resets;
    # a success resets the run of timeouts.
    source = _source({"max_consecutive_execution_timeouts": 2})
    client = _ExecutionClient(["timeout", "error", "ok", "timeout", "error"])
    install_client(source, client)

    for index in range(5):
        dossier = _dossier_object(f"dash-{index}")
        source._enrich_warehouse_lineage("project-1", dossier, _dashboard(), None)
        source._release_dashboard_instance("project-1", dossier)

    assert len(client.created) == 5
    assert source.report.execution_timeouts == 2
    assert source.report.executions_skipped_after_timeouts == 0
    assert list(source.report.execution_timeout_limit_projects) == []


def test_timeout_limit_zero_disables_it() -> None:
    source = _source({"max_consecutive_execution_timeouts": 0})
    client = _ExecutionClient(["timeout"] * 4)
    install_client(source, client)

    for index in range(4):
        dossier = _dossier_object(f"dash-{index}")
        source._enrich_warehouse_lineage("project-1", dossier, _dashboard(), None)
        source._release_dashboard_instance("project-1", dossier)

    assert len(client.created) == 4
    assert source.report.executions_skipped_after_timeouts == 0


_DOCUMENT_SOURCE_WAREHOUSE = {
    "id": "source-1",
    "name": "Sales Warehouse",
    "database": {"type": "snow_flake", "name": "SALES_DB", "schema": "SALES"},
}


_DOCUMENT_DATASET_NAMES = {
    "rep-1": "Daily Sales",
    "rep-2": "Daily Returns",
    "cube-1": "Sales Cube",
}


class _DocumentClient:
    """Report Services documents read through the document definition
    endpoint, by default one over two grid-report datasets and one cube; a
    report instance can be made to fail fast or time out per report."""

    def __init__(
        self,
        documents: Optional[Dict[str, List[str]]] = None,
        failing_report_ids: Optional[Set[str]] = None,
        timing_out_report_ids: Optional[Set[str]] = None,
    ) -> None:
        # Document id -> the ids of its datasets.
        self.documents = documents or {"doc-1": ["rep-1", "rep-2", "cube-1"]}
        self.failing_report_ids = failing_report_ids or set()
        self.timing_out_report_ids = timing_out_report_ids or set()
        self.report_instances_created: List[str] = []
        self.report_sql_view_instances: List[str] = []
        self.report_instances_deleted: List[str] = []
        self.dashboard_instances_created: List[str] = []
        self.dossier_sql_view_calls: List[str] = []

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": document_id,
                        "name": f"Document {document_id}",
                        "type": "55",
                        "subtype": "14081",
                        "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
                    }
                )
                for document_id in self.documents
            ]
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _standard_predefined_folders(folder_types)

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        raise MicroStrategyAPIError("not a dossier", status_code=400)

    def get_document_definition(
        self, project_id: str, document_id: str
    ) -> Dict[str, Any]:
        return {
            "result": {
                "definition": {
                    "datasets": [
                        {
                            "id": dataset_id,
                            "name": _DOCUMENT_DATASET_NAMES.get(dataset_id, dataset_id),
                            "sourceWarehouse": _DOCUMENT_SOURCE_WAREHOUSE,
                        }
                        for dataset_id in self.documents[document_id]
                    ]
                }
            }
        }

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": object_id,
                "type": "3",
                "subtype": "776" if object_id.startswith("cube") else "768",
                "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
            }
        )

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        self.dashboard_instances_created.append(dossier_id)
        return f"inst-{dossier_id}"

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        self.dashboard_instances_created.append(document_id)
        return f"inst-{document_id}"

    def get_dossier_datasets_sql(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> List[Dict[str, object]]:
        self.dossier_sql_view_calls.append(instance_id)
        return []

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        self.report_instances_created.append(report_id)
        if report_id in self.timing_out_report_ids:
            error = MicroStrategyAPIError("Read timed out (180s)")
            error.__cause__ = requests.ReadTimeout()
            raise error
        if report_id in self.failing_report_ids:
            raise MicroStrategyAPIError("500 Server Error", status_code=500)
        return f"inst-{report_id}"

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        self.report_sql_view_instances.append(instance_id)
        table = report_id.replace("-", "_")
        return SqlView(sql_statement=f"select * from fact_{table}")

    def delete_report_instance(
        self, project_id: str, report_id: str, instance_id: str
    ) -> bool:
        self.report_instances_deleted.append(instance_id)
        return True


def _document_source(extra_config: dict | None = None) -> MicroStrategySource:
    config = {
        "extract_visualization_details": False,
        "extract_dashboard_dependencies": False,
        "extract_metric_expressions": False,
        "extract_model_lineage": False,
        "extract_derived_metrics": False,
    }
    config.update(extra_config or {})
    return _source(config)


def _document_dataset_urn(
    source: MicroStrategySource, dataset_id: str, document_id: str = "doc-1"
) -> str:
    return source.lineage.dataset_urn(
        "project-1",
        document_id,
        DatasetObject.model_validate({"id": dataset_id, "name": dataset_id}),
    )


def _warehouse_table_urn(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.{table},PROD)"


def _dataset_upstreams(workunits: List[Any]) -> Dict[str, List[str]]:
    upstreams: Dict[str, List[str]] = {}
    for workunit in workunits:
        lineage = workunit.get_aspect_of_type(UpstreamLineageClass)
        if lineage is not None:
            upstreams[workunit.get_urn()] = [
                upstream.dataset for upstream in lineage.upstreams
            ]
    return upstreams


def test_document_datasets_get_sql_lineage_through_report_instances() -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})
    client = _DocumentClient()
    install_client(source, client)

    workunits = _run_dashboards(source)

    # One report instance per grid-report dataset, each read and released;
    # the document itself is never executed and the dossier SQL view, which
    # carries no SQL for a document instance, is never requested.
    assert client.report_instances_created == ["rep-1", "rep-2"]
    assert client.report_sql_view_instances == ["inst-rep-1", "inst-rep-2"]
    assert client.report_instances_deleted == ["inst-rep-1", "inst-rep-2"]
    assert client.dashboard_instances_created == []
    assert client.dossier_sql_view_calls == []
    assert _dataset_upstreams(workunits) == {
        _document_dataset_urn(source, "rep-1"): [_warehouse_table_urn("fact_rep_1")],
        _document_dataset_urn(source, "rep-2"): [_warehouse_table_urn("fact_rep_2")],
    }
    assert source.report.document_dataset_sql_views_scanned == 2
    assert source.report.document_dataset_sql_views_parsed == 2
    assert source.report.document_dataset_sql_view_failures == 0
    assert source.report.document_datasets_skipped_not_grid_report == 1
    assert source.report.warehouse_lineage_sql_views_scanned == 2
    assert source.report.warehouse_lineage_edges == 2


def test_document_datasets_are_left_alone_unless_enabled() -> None:
    source = _document_source()
    client = _DocumentClient()
    install_client(source, client)

    workunits = _run_dashboards(source)

    # Neither a report instance nor a document instance is created and the
    # dossier SQL view is not requested; the datasets are still emitted,
    # without warehouse lineage.
    assert client.report_instances_created == []
    assert client.dashboard_instances_created == []
    assert client.dossier_sql_view_calls == []
    assert _dataset_upstreams(workunits) == {}
    dataset_urns = {
        workunit.get_urn()
        for workunit in workunits
        if workunit.get_urn().startswith("urn:li:dataset:")
    }
    assert dataset_urns == {
        _document_dataset_urn(source, dataset_id)
        for dataset_id in ("rep-1", "rep-2", "cube-1")
    }
    assert source.report.document_dataset_sql_views_scanned == 0
    assert source.report.document_datasets_skipped_not_grid_report == 0


def test_document_dataset_failure_is_counted_and_the_rest_still_emitted() -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})
    client = _DocumentClient(failing_report_ids={"rep-1"})
    install_client(source, client)

    workunits = _run_dashboards(source)

    assert client.report_instances_created == ["rep-1", "rep-2"]
    assert client.report_instances_deleted == ["inst-rep-2"]
    assert _dataset_upstreams(workunits) == {
        _document_dataset_urn(source, "rep-2"): [_warehouse_table_urn("fact_rep_2")],
    }
    assert source.report.document_dataset_sql_view_failures == 1
    assert source.report.document_dataset_sql_views_scanned == 1
    assert source.report.document_dataset_sql_views_parsed == 1
    assert source.report.report_sql_view_api_failures == 1
    assert any(
        warning.title == "Failed to Create Report Instance for Warehouse Lineage"
        for warning in source.report.warnings
    )


def test_document_dataset_timeouts_count_towards_the_execution_limit() -> None:
    source = _document_source(
        {
            "extract_document_dataset_sql_lineage": True,
            "max_consecutive_execution_timeouts": 1,
        }
    )
    client = _DocumentClient(timing_out_report_ids={"rep-1"})
    install_client(source, client)

    _run_dashboards(source)

    # The first dataset's timeout trips the project limit, so the second
    # grid report is not executed; the cube was never a candidate.
    assert client.report_instances_created == ["rep-1"]
    assert source.report.execution_timeouts == 1
    assert source.report.executions_skipped_after_timeouts == 1
    assert list(source.report.execution_timeout_limit_projects) == ["project-1"]
    assert source.report.document_dataset_sql_view_failures == 1


def test_grid_report_shared_by_documents_is_executed_once() -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})
    client = _DocumentClient(documents={"doc-1": ["rep-1"], "doc-2": ["rep-1"]})
    install_client(source, client)

    workunits = _run_dashboards(source)

    # Dataset URNs are document-scoped but the report's SQL is not: one
    # instance, one SQL view, one delete, lineage on both datasets.
    assert client.report_instances_created == ["rep-1"]
    assert client.report_sql_view_instances == ["inst-rep-1"]
    assert client.report_instances_deleted == ["inst-rep-1"]
    assert _dataset_upstreams(workunits) == {
        _document_dataset_urn(source, "rep-1", "doc-1"): [
            _warehouse_table_urn("fact_rep_1")
        ],
        _document_dataset_urn(source, "rep-1", "doc-2"): [
            _warehouse_table_urn("fact_rep_1")
        ],
    }
    assert source.report.document_dataset_sql_views_reused == 1
    assert source.report.document_dataset_sql_views_scanned == 2
    assert source.report.document_dataset_sql_views_parsed == 2
    assert source.report.document_dataset_sql_view_failures == 0
    assert source.report.warehouse_lineage_edges == 2


def test_shared_report_sql_is_parsed_once_and_progress_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})
    client = _DocumentClient(documents={"doc-1": ["rep-1"], "doc-2": ["rep-1"]})
    install_client(source, client)

    with (
        caplog.at_level(logging.INFO),
        mock.patch(
            "datahub.ingestion.source.microstrategy.source."
            "MSTR_PROGRESS_LOG_INTERVAL_DASHBOARDS",
            2,
        ),
    ):
        _run_dashboards(source)

    report = source.report
    assert report.report_sql_fetched == 1
    assert report.sql_parses_executed == 1
    assert report.sql_parse_cache_hits == 1
    progress_lines = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("MicroStrategy progress:")
    ]
    assert len(progress_lines) == 1
    assert progress_lines[0].startswith("MicroStrategy progress: 2 dashboards")


def test_project_caches_are_dropped_at_the_project_boundary() -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})
    client = _DocumentClient(documents={"doc-1": ["rep-1"]})
    install_client(source, client)
    _run_dashboards(source)
    metric_key = object_cache_key("project-1", "METRIC-1")
    source._metric_models.put(metric_key, {})

    source._reset_project_caches()
    _run_dashboards(source)

    assert client.report_instances_created == ["rep-1", "rep-1"]
    assert source.report.sql_parses_executed == 2
    assert source._metric_models.get(metric_key) is unset
    assert source.report.cache_project_resets == 1


def test_failed_shared_report_is_not_retried_for_the_next_document() -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})
    client = _DocumentClient(
        documents={"doc-1": ["rep-1"], "doc-2": ["rep-1"]},
        failing_report_ids={"rep-1"},
    )
    install_client(source, client)

    workunits = _run_dashboards(source)

    # The remembered failure is served to the second document without a
    # second instance attempt, and is counted once.
    assert client.report_instances_created == ["rep-1"]
    assert _dataset_upstreams(workunits) == {}
    assert source.report.document_dataset_sql_view_failures == 1
    assert source.report.document_dataset_sql_views_reused == 1
    assert source.report.document_dataset_sql_views_scanned == 0
    assert source.report.report_sql_view_api_failures == 1


def test_malformed_report_sql_view_drops_only_the_lineage() -> None:
    source = _document_source({"extract_document_dataset_sql_lineage": True})

    class MalformedSqlViewClient(_DocumentClient):
        def get_report_sql_view(
            self, project_id: str, report_id: str, instance_id: str
        ) -> SqlView:
            return SqlView.model_validate({"sqlStatement": ["not", "a", "string"]})

    client = MalformedSqlViewClient(documents={"doc-1": ["rep-1"], "doc-2": ["rep-1"]})
    install_client(source, client)

    workunits = _run_dashboards(source)

    # Both documents are still emitted; the failure is cached and counted once.
    dashboard_urns = {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:dashboard:")
    }
    assert len(dashboard_urns) == 2
    assert client.report_instances_created == ["rep-1"]
    assert client.report_instances_deleted == ["inst-rep-1"]
    assert source.report.report_sql_view_api_failures == 1


def test_report_sql_lineage_reuses_sql_read_for_a_document_dataset() -> None:
    source = _document_source(
        {
            "extract_document_dataset_sql_lineage": True,
            "extract_report_sql_lineage": True,
        }
    )
    client = _DocumentClient(documents={"doc-1": ["rep-1"]})
    install_client(source, client)
    _run_dashboards(source)
    dataset = DatasetObject.model_validate({"id": "source", "name": "Daily Sales"})

    source._enrich_report_sql_lineage(
        project_id="project-1",
        report_object=MicroStrategyObject.model_validate(
            {"id": "rep-1", "name": "Daily Sales", "type": "3"}
        ),
        dataset=dataset,
        context=WarehouseLineageContext(
            platform="snowflake",
            env="PROD",
            database="SALES_DB",
            schema="SALES",
        ),
        entry=None,
    )

    # The extract_reports path shares the cache: no second execution.
    assert client.report_instances_created == ["rep-1"]
    assert dataset.warehouse_upstream_urns == [_warehouse_table_urn("fact_rep_1")]


def test_document_definition_without_visualizations_is_counted() -> None:
    source = _source(
        {
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
        }
    )
    document = MicroStrategyObject.model_validate(
        {"id": "doc-1", "name": "Daily Sales Summary", "subtype": "14081"}
    )
    dossier = MicroStrategyObject.model_validate(
        {"id": "dash-1", "name": "Sales Dashboard", "subtype": "14080"}
    )

    class FakeClient:
        def get_dossier_definition(
            self, project_id: str, dossier_id: str
        ) -> Dict[str, Any]:
            raise MicroStrategyAPIError("not a dossier")

        def get_document_definition(
            self, project_id: str, document_id: str
        ) -> Dict[str, Any]:
            return {
                "id": document_id,
                "name": "Daily Sales Summary",
                "datasets": [{"id": "ds-1", "name": "Daily Sales - Dataset"}],
                "prompts": [],
            }

    source.client = FakeClient()  # type: ignore[assignment]

    first = source._get_dashboard_definition("project-1", document)
    second = source._get_dashboard_definition("project-1", document)
    plain = source._get_dashboard_definition("project-1", dossier)

    assert first is not None and second is not None and plain is not None
    assert [dataset.id for dataset in first.datasets] == ["ds-1"]
    # Counted per document, logged once per project, never for dossiers.
    assert source.report.document_definitions_without_visualizations == 2


_CHAPTER_PROPERTY_URN = "urn:li:structuredProperty:io.acryl.microstrategy.chapter"
_PAGE_PROPERTY_URN = "urn:li:structuredProperty:io.acryl.microstrategy.page"
_PAGE_POSITION_PROPERTY_URN = (
    "urn:li:structuredProperty:io.acryl.microstrategy.pagePosition"
)
_DEFINITION_URNS = [
    _CHAPTER_PROPERTY_URN,
    _PAGE_PROPERTY_URN,
    _PAGE_POSITION_PROPERTY_URN,
]


class _PagedDossierClient:
    """One project with one dossier of two pages, one visualization each."""

    def login(self) -> None:
        pass

    def close(self) -> None:
        pass

    def list_projects(self) -> List[Project]:
        return [_PROJECT]

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _standard_predefined_folders(folder_types)

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {"id": "dash-1", "name": "Sales", "type": "55"}
                )
            ]
        )

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        return {
            "definition": {
                "datasets": [{"id": "ds-1", "name": "Sales Cube"}],
                "chapters": [
                    {
                        "key": "ch-1",
                        "name": "Overview",
                        "pages": [
                            {
                                "key": "pg-1",
                                "name": "Summary",
                                "visualizations": [{"key": "viz-a", "name": "A"}],
                            },
                            {
                                "key": "pg-2",
                                "name": "Trend",
                                "visualizations": [{"key": "viz-b", "name": "B"}],
                            },
                        ],
                    }
                ],
            }
        }


def _paged_dossier_source(extra_config: Dict[str, Any]) -> MicroStrategySource:
    source = _source(
        {
            "extract_reports": False,
            "extract_cubes": False,
            "extract_source_warehouses": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
            "extract_derived_metrics": False,
            "extract_warehouse_lineage": False,
            **extra_config,
        }
    )
    source.client = _PagedDossierClient()  # type: ignore[assignment]
    return source


def _definition_urns(workunits: List[Any]) -> List[str]:
    return [
        workunit.get_urn()
        for workunit in workunits
        if workunit.get_aspect_of_type(StructuredPropertyDefinitionClass) is not None
    ]


def _structured_property_values(
    workunits: List[Any],
) -> Dict[str, List[Tuple[str, List[object]]]]:
    values: Dict[str, List[Tuple[str, List[object]]]] = {}
    for workunit in workunits:
        aspect = workunit.get_aspect_of_type(StructuredPropertiesClass)
        if aspect is not None:
            values[workunit.get_urn()] = [
                (assignment.propertyUrn, list(assignment.values))
                for assignment in aspect.properties
            ]
    return values


def test_chapter_page_structured_properties_off_by_default_emit_nothing() -> None:
    source = _paged_dossier_source({})

    workunits = list(source.get_workunits_internal())

    assert _definition_urns(workunits) == []
    assert _structured_property_values(workunits) == {}
    assert source.report.chapter_page_structured_property_definitions_emitted == 0
    assert source.report.charts_with_chapter_page_structured_properties == 0
    # The charts themselves are still there, with their custom properties.
    chart_urn = source.mapper.chart_urn("project-1", "dash-1", "viz-a")
    assert chart_urn in {workunit.get_urn() for workunit in workunits}


def test_chapter_page_structured_property_definitions_precede_the_charts() -> None:
    source = _paged_dossier_source({"emit_chapter_page_structured_properties": True})

    workunits = list(source.get_workunits_internal())

    # Three definitions, once each, in display order, all ahead of any chart.
    assert _definition_urns(workunits) == _DEFINITION_URNS
    urns = [workunit.get_urn() for workunit in workunits]
    last_definition = max(urns.index(urn) for urn in _DEFINITION_URNS)
    first_chart = min(
        index for index, urn in enumerate(urns) if urn.startswith("urn:li:chart:")
    )
    assert last_definition < first_chart
    assert source.report.chapter_page_structured_property_definitions_emitted == 3
    assert _structured_property_values(workunits) == {
        source.mapper.chart_urn("project-1", "dash-1", "viz-a"): [
            (_CHAPTER_PROPERTY_URN, ["Overview"]),
            (_PAGE_PROPERTY_URN, ["Summary"]),
            (_PAGE_POSITION_PROPERTY_URN, [1.0]),
        ],
        source.mapper.chart_urn("project-1", "dash-1", "viz-b"): [
            (_CHAPTER_PROPERTY_URN, ["Overview"]),
            (_PAGE_PROPERTY_URN, ["Trend"]),
            (_PAGE_POSITION_PROPERTY_URN, [2.0]),
        ],
    }
    assert source.report.charts_with_chapter_page_structured_properties == 2


def test_chapter_page_structured_property_definitions_sync_through_graph() -> None:
    source = _paged_dossier_source({"emit_chapter_page_structured_properties": True})
    graph = mock.MagicMock()
    source.ctx.graph = graph

    workunits = list(source.get_workunits_internal())

    # With a graph the definitions are persisted synchronously before any
    # chart value can reach GMS, and are not also emitted as work units.
    assert [
        call.args[0].entityUrn for call in graph.emit_mcp.call_args_list
    ] == _DEFINITION_URNS
    assert all(
        call.kwargs == {"emit_mode": EmitMode.SYNC_PRIMARY}
        for call in graph.emit_mcp.call_args_list
    )
    assert _definition_urns(workunits) == []
    assert source.report.chapter_page_structured_property_definitions_emitted == 3
    assert len(_structured_property_values(workunits)) == 2


# A dossier and a document (or second dossier) share report "ds-shared"; one more report is unused.
class _SharedReportClient:
    def __init__(self, second: str = "document", with_sql: bool = False) -> None:
        self.second = second
        self.with_sql = with_sql
        self.sql_view_reads: List[str] = []
        self.report_definitions: List[str] = []
        self.report_instances: List[str] = []

    def list_project_datasources(self, project_id: str) -> List[Datasource]:
        return [
            Datasource.model_validate(
                {
                    "id": "source-1",
                    "name": "Sales Warehouse",
                    "database": {
                        "type": "snow_flake",
                        "name": "SALES_DB",
                        "schema": "SALES",
                    },
                }
            )
        ]

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        self.report_instances.append(report_id)
        return f"ri-{report_id}"

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        table = report_id.replace("-", "_")
        return SqlView(sql_statement=f"select amt from fact_{table}")

    def delete_report_instance(self, *args: object) -> bool:
        return True

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        dossier = {"type": "55", "subtype": "14081", "viewMedia": 1879072805}
        document = {"type": "55", "subtype": "14081"}
        second = (
            {"id": "doc-1", "name": "Sales Document", **document}
            if self.second == "document"
            else {"id": "dash-2", "name": "Cost Dossier", **dossier}
        )
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        **item,
                        "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
                    }
                )
                for item in (
                    {"id": "dash-1", "name": "Sales Dossier", **dossier},
                    second,
                )
            ]
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return []

    def _dataset(self) -> Dict[str, Any]:
        dataset: Dict[str, Any] = {
            "id": "ds-shared",
            "name": "Daily Sales Report",
            "description": "Embedded copy",
            "availableObjects": {
                "metrics": [
                    {
                        "id": "M-NET",
                        "name": "Net Sales Amt",
                        "dataType": "double",
                    }
                ]
            },
        }
        if self.with_sql:
            dataset["sourceWarehouse"] = {
                "id": "source-1",
                "name": "Sales Warehouse",
                "database": {
                    "type": "snow_flake",
                    "name": "SALES_DB",
                    "schema": "SALES",
                },
            }
        return dataset

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        if dossier_id == "doc-1":
            raise MicroStrategyAPIError("not a dossier")
        return {
            "result": {
                "definition": {
                    "datasets": [self._dataset()],
                    "chapters": [
                        {
                            "key": "ch",
                            "pages": [
                                {
                                    "key": "pg",
                                    "name": "P",
                                    "visualizations": [
                                        {
                                            "key": f"viz-{dossier_id}",
                                            "name": "Sales Grid",
                                            "datasets": ["ds-shared"],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            }
        }

    def get_document_definition(
        self, project_id: str, document_id: str
    ) -> Dict[str, Any]:
        return {
            "result": {
                "definition": {
                    "datasets": [self._dataset()],
                    "chapters": [{"key": "ch-1", "name": "Summary", "pages": []}],
                }
            }
        }

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": "Daily Sales Report",
                "type": "3",
                "subtype": "768",
                "owner": {"username": "report_owner"},
                "ancestors": [
                    {"id": "f-1", "name": "Shared Reports"},
                    {"id": "f-2", "name": "Regional Sales"},
                ],
            }
        )

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                self.get_object_info(project_id, "ds-shared", 3),
                MicroStrategyObject.model_validate(
                    {
                        "id": "report-solo",
                        "name": "Standalone Report",
                        "type": "3",
                        "subtype": "768",
                        "ancestors": [
                            {"id": "f-1", "name": "Shared Reports"},
                            {"id": "f-3", "name": "Ad Hoc"},
                        ],
                    }
                ),
            ]
        )

    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        self.report_definitions.append(report_id)
        return {
            "result": {
                "definition": {
                    "description": "The report's own description",
                    "dataSource": {"id": "cube-1", "name": "Sales Cube"},
                    "availableObjects": {
                        "metrics": [
                            {
                                "id": "M-NET",
                                "name": "Net Sales Amt",
                                "dataType": "double",
                            },
                            {
                                "id": "M-GROSS",
                                "name": "Gross Sales Amt",
                                "dataType": "double",
                            },
                        ]
                    },
                    "prompts": [{"id": "p-1"}],
                }
            }
        }

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        return f"i-{dossier_id}"

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        return f"i-{document_id}"

    def delete_dossier_instance(self, *args: object) -> bool:
        return True

    def delete_document_instance(self, *args: object) -> bool:
        return True

    def get_dossier_datasets_sql(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> List[Dict[str, str]]:
        self.sql_view_reads.append(dossier_id)
        return [
            {"id": "ds-shared", "sqlStatement": "select net_sales_amt from fact_sales"}
        ]


_SHARING_DASHBOARD = MicroStrategyObject.model_validate(
    {"id": "dash-sharing", "name": "Sharing Dashboard"}
)


_SHARING_DASHBOARD = MicroStrategyObject.model_validate(
    {"id": "dash-sharing", "name": "Sharing Dashboard"}
)


def _read_report_as_dataset(
    source: MicroStrategySource,
    report_object: MicroStrategyObject,
    lineage_context: _LazyProjectLineage,
) -> None:
    source._process_report_as_dataset(
        _PROJECT.id,
        report_object,
        lineage_context,
        source._prefetch_report(_PROJECT, report_object, lineage_context),
    )


def _per_report_source(
    extra_config: Optional[Mapping[str, object]] = None,
) -> MicroStrategySource:
    config: Dict[str, object] = {
        "dataset_identity": "per_report",
        "use_predefined_folder_names": False,
        "extract_warehouse_lineage": False,
        "extract_visualization_details": False,
        "extract_dashboard_dependencies": False,
        "extract_metric_expressions": False,
        "extract_model_lineage": False,
        "extract_derived_metrics": False,
        "extract_source_warehouses": False,
    }
    config.update(extra_config or {})
    return _source(config)


_AspectT = TypeVar("_AspectT", bound=_Aspect)


def _aspects_by_urn(
    workunits: List[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> Dict[str, _AspectT]:
    found: Dict[str, _AspectT] = {}
    for workunit in workunits:
        aspect = workunit.get_aspect_of_type(aspect_type)
        if aspect is not None:
            assert workunit.get_urn() not in found, "aspect emitted twice"
            found[workunit.get_urn()] = aspect
    return found


_SHARED_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.ds-shared,PROD)"
)
_SOLO_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.report-solo,PROD)"
)


def _shared_report_run(
    client: Optional[_SharedReportClient] = None,
) -> Tuple[MicroStrategySource, List[MetadataWorkUnit]]:
    source = _per_report_source()
    source.client = client or _SharedReportClient()  # type: ignore[assignment]
    return source, list(source._process_project(_PROJECT))


def test_per_report_shared_report_is_one_dataset_with_both_dashboard_ids() -> None:
    _, workunits = _shared_report_run()

    properties = _aspects_by_urn(workunits, DatasetPropertiesClass)
    assert list(properties) == [_SHARED_URN]
    shared = properties[_SHARED_URN]
    assert shared.qualifiedName == "project-1.ds-shared"
    assert shared.name == "Daily Sales Report"
    assert shared.customProperties["microstrategyReportId"] == "ds-shared"
    assert shared.customProperties["microstrategyDashboardCount"] == "2"
    assert shared.customProperties["microstrategyDashboardIds"] == "dash-1,doc-1"
    assert shared.customProperties["microstrategyObjectSubtype"] == "768"
    assert "microstrategyDashboardId" not in shared.customProperties
    assert shared.externalUrl == (
        "https://mstr.example.com/MicroStrategyLibrary/app/project-1/ds-shared"
    )


def test_per_report_charts_and_documents_point_at_the_shared_dataset() -> None:
    source, workunits = _shared_report_run()

    charts = _aspects_by_urn(workunits, ChartInfoClass)
    assert [chart.inputs for chart in charts.values()] == [[_SHARED_URN]]
    dashboards = _aspects_by_urn(workunits, DashboardInfoClass)
    document = dashboards[source.mapper.dashboard_urn("project-1", "doc-1")]
    assert document.datasetEdges is not None
    assert [edge.destinationUrn for edge in document.datasetEdges] == [_SHARED_URN]
    dossier = dashboards[source.mapper.dashboard_urn("project-1", "dash-1")]
    assert dossier.datasetEdges is None


def test_per_report_shared_dataset_sits_in_the_report_folder() -> None:
    source, workunits = _shared_report_run()

    own_folder = source.mapper.folder_key(
        "project-1", "Shared Reports/Regional Sales"
    ).as_urn()
    assert _dataset_container_parents(workunits) == {_SHARED_URN: own_folder}


def test_per_report_shared_dataset_is_owned_by_the_report_owner() -> None:
    _, workunits = _shared_report_run()

    ownership = _aspects_by_urn(workunits, OwnershipClass)
    assert [owner.owner for owner in ownership[_SHARED_URN].owners] == [
        "urn:li:corpuser:report_owner"
    ]


def test_per_report_shared_report_is_looked_up_and_counted_once() -> None:
    source, _ = _shared_report_run()

    assert source.report.reports_deduplicated == 1
    assert source.report.report_datasets_emitted == 1
    assert source.report.datasets_scanned == 1
    assert source.report.dataset_object_lookups == 1


class _UnfiledReportClient(_SharedReportClient):
    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        return MicroStrategyObject.model_validate(
            {"id": object_id, "name": "Daily Sales Report", "type": "3"}
        )


def test_per_report_dataset_without_a_folder_sits_in_the_first_dashboard_folder() -> (
    None
):
    source, workunits = _shared_report_run(_UnfiledReportClient())

    assert _dataset_container_parents(workunits) == {
        _SHARED_URN: source.mapper.folder_key("project-1", "Shared Reports").as_urn()
    }


class _MissingVisualizationClient(_SharedReportClient):
    def get_dossier_visualization(self, **kwargs: object) -> Dict[str, object]:
        raise RuntimeError("missing dossier visualization")


def test_per_report_failed_dashboard_is_not_recorded_on_the_shared_dataset() -> None:
    source = _per_report_source({"extract_visualization_details": True})
    source.client = _MissingVisualizationClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    shared = _aspects_by_urn(workunits, DatasetPropertiesClass)[_SHARED_URN]
    assert shared.customProperties["microstrategyDashboardIds"] == "doc-1"
    assert shared.customProperties["microstrategyDashboardCount"] == "1"
    assert source.report.reports_deduplicated == 0
    assert len(source.report.warnings) == 1


def test_per_report_each_project_gets_its_own_dataset() -> None:
    source = _per_report_source()
    source.client = _SharedReportClient()  # type: ignore[assignment]
    other_project = Project.model_validate({"id": "project-2", "name": "Finance"})

    workunits = [
        *source._process_project(_PROJECT),
        *source._process_project(other_project),
    ]

    properties = _aspects_by_urn(workunits, DatasetPropertiesClass)
    assert sorted(properties) == [
        _SHARED_URN,
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-2.ds-shared,PROD)",
    ]
    assert {
        dataset.customProperties["microstrategyDashboardCount"]
        for dataset in properties.values()
    } == {"2"}
    assert source.report.report_datasets_emitted == 2


def test_per_dashboard_default_keeps_one_dataset_per_dashboard() -> None:
    source = _per_report_source({"dataset_identity": "per_dashboard"})
    source.client = _SharedReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    properties = _aspects_by_urn(workunits, DatasetPropertiesClass)
    assert sorted(properties) == [
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.dash-1.ds-shared,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.doc-1.ds-shared,PROD)",
    ]
    for urn, dataset_properties in properties.items():
        assert dataset_properties.qualifiedName == urn.split(",")[1]
        assert "microstrategyDashboardId" in dataset_properties.customProperties
    charts = _aspects_by_urn(workunits, ChartInfoClass)
    assert [chart.inputs for chart in charts.values()] == [
        [
            "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.dash-1.ds-shared,PROD)"
        ]
    ]
    assert source.report.reports_deduplicated == 0
    assert source.report.report_datasets_emitted == 0
    assert source.report.datasets_scanned == 2


def test_per_dashboard_dataset_urns_ignore_dataset_id_case() -> None:
    source = _per_report_source({"dataset_identity": "per_dashboard"})
    source.client = _DivergingReportClient("DS-SHARED")  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    assert sorted(_aspects_by_urn(workunits, DatasetPropertiesClass)) == [
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.dash-1.ds-shared,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:microstrategy,project-1.dash-2.ds-shared,PROD)",
    ]


def test_per_report_report_pass_upgrades_schema_and_emits_no_chart() -> None:
    source = _per_report_source(
        {"extract_reports": True, "extract_independent_reports": True}
    )
    client = _SharedReportClient()
    install_client(source, client)

    workunits = list(source._process_project(_PROJECT))

    solo_urn = _SOLO_URN
    properties = _aspects_by_urn(workunits, DatasetPropertiesClass)
    assert sorted(properties) == [_SHARED_URN, solo_urn]
    # The report's own definition adds the field the dossier's copy lacks.
    schemas = _aspects_by_urn(workunits, SchemaMetadataClass)
    assert [f.fieldPath for f in schemas[_SHARED_URN].fields] == [
        "Gross Sales Amt",
        "Net Sales Amt",
    ]
    shared = properties[_SHARED_URN]
    assert shared.description == "The report's own description"
    assert shared.customProperties["microstrategyDashboardCount"] == "2"
    assert shared.customProperties["microstrategyReportPromptCount"] == "1"
    assert shared.customProperties["microstrategyReportSourceId"] == "cube-1"
    assert shared.customProperties["microstrategyReportSourceName"] == "Sales Cube"
    solo = properties[solo_urn]
    assert solo.customProperties["microstrategyDashboardCount"] == "0"
    assert "microstrategyDashboardIds" not in solo.customProperties
    assert solo.externalUrl == (
        "https://mstr.example.com/MicroStrategyLibrary/app/project-1/report-solo"
    )
    assert (
        _dataset_container_parents(workunits)[solo_urn]
        == source.mapper.folder_key("project-1", "Shared Reports/Ad Hoc").as_urn()
    )
    # The Report chart was a second copy of the report.
    assert list(_aspects_by_urn(workunits, ChartInfoClass)) == [
        source.mapper.chart_urn("project-1", "dash-1", "viz-dash-1")
    ]
    assert source.mapper.report_urn("project-1", "report-solo") not in {
        workunit.get_urn() for workunit in workunits
    }
    assert sorted(client.report_definitions) == ["ds-shared", "report-solo"]
    assert source.report.reports_scanned == 2
    assert source.report.charts_scanned == 1
    assert source.report.report_datasets_emitted == 2
    assert source.report.reports_deduplicated == 1


def test_per_report_extract_cubes_off_still_emits_report_pass_datasets() -> None:
    source = _per_report_source(
        {
            "extract_cubes": False,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    source.client = _SharedReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    # The report pass emits both, including the one no dashboard embeds.
    assert sorted(_aspects_by_urn(workunits, DatasetPropertiesClass)) == [
        _SHARED_URN,
        _SOLO_URN,
    ]


def test_per_report_without_cubes_or_reports_is_rejected() -> None:
    with pytest.raises(ValidationError, match="per_report"):
        _per_report_source({"extract_cubes": False, "extract_reports": False})


def test_per_report_parses_a_shared_report_sql_view_once() -> None:
    source = _per_report_source({"extract_warehouse_lineage": True})
    client = _SharedReportClient(second="dossier", with_sql=True)
    install_client(source, client)

    with mock.patch.object(
        source.lineage,
        "warehouse_lineage_from_sql",
        wraps=source.lineage.warehouse_lineage_from_sql,
    ) as parse:
        workunits = list(source._process_project(_PROJECT))

    # The shared report's SQL is parsed for the first dossier only.
    assert sorted(client.sql_view_reads) == ["dash-1", "dash-2"]
    assert parse.call_count == 1
    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)
    assert [upstream.dataset for upstream in lineage[_SHARED_URN].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]
    assert source.report.warehouse_lineage_edges == 1
    assert source.report.reports_deduplicated == 1


def test_per_report_report_pass_does_not_execute_a_report_a_dossier_settled() -> None:
    source = _per_report_source(
        {
            "extract_warehouse_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_source_warehouses": True,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    client = _SharedReportClient(with_sql=True)
    install_client(source, client)

    with mock.patch.object(
        source.lineage,
        "warehouse_lineage_from_sql",
        wraps=source.lineage.warehouse_lineage_from_sql,
    ) as parse:
        workunits = list(source._process_project(_PROJECT))

    # The report pass executes only the report no dashboard already settled.
    assert client.report_instances == ["report-solo"]
    assert parse.call_count == 2
    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)
    assert {
        urn.split(",")[1]: [u.dataset.split(",")[1] for u in aspect.upstreams]
        for urn, aspect in lineage.items()
    } == {
        "project-1.ds-shared": ["sales_db.sales.fact_sales"],
        "project-1.report-solo": ["sales_db.sales.fact_report_solo"],
    }


class _DivergingReportClient(_SharedReportClient):
    def __init__(self, second_dataset_id: str = "ds-shared") -> None:
        super().__init__(second="dossier", with_sql=True)
        self.second_dataset_id = second_dataset_id

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        definition = super().get_dossier_definition(project_id, dossier_id)
        dataset = definition["result"]["definition"]["datasets"][0]
        dataset["availableObjects"]["metrics"][0]["name"] = "net_dossier_amt"
        visualization = definition["result"]["definition"]["chapters"][0]["pages"][0][
            "visualizations"
        ][0]
        visualization["metrics"] = [{"id": "M-NET"}]
        if dossier_id == "dash-1":
            del dataset["sourceWarehouse"]
        else:
            dataset["id"] = self.second_dataset_id
        return definition

    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        definition = super().get_report_definition(project_id, report_id)
        metrics = definition["result"]["definition"]["availableObjects"]["metrics"]
        metrics[0]["name"] = "net_report_amt"
        return definition

    def get_dossier_datasets_sql(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> List[Dict[str, Any]]:
        self.sql_view_reads.append(dossier_id)
        return [
            {
                "id": self.second_dataset_id,
                "sqlStatement": "select net_dossier_amt from fact_dossier",
            }
        ]

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        return SqlView(sql_statement="select net_report_amt from fact_report")


def _field_lineage_downstream_paths(aspect: UpstreamLineageClass) -> List[str]:
    return [
        urn.split(",")[-1].rstrip(")")
        for lineage in aspect.fineGrainedLineages or []
        for urn in lineage.downstreams or []
    ]


@pytest.mark.parametrize("second_dataset_id", ["ds-shared", "DS-SHARED"])
def test_per_report_later_dashboard_warehouse_settles_lineage_the_first_skipped(
    second_dataset_id: str,
) -> None:
    source = _per_report_source({"extract_warehouse_lineage": True})
    client = _DivergingReportClient(second_dataset_id)
    install_client(source, client)

    workunits = list(source._process_project(_PROJECT))

    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)
    assert [upstream.dataset for upstream in lineage[_SHARED_URN].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_dossier,PROD)"
    ]
    assert _field_lineage_downstream_paths(lineage[_SHARED_URN]) == ["net_dossier_amt"]


def _input_field_paths(aspect: InputFieldsClass) -> List[str]:
    return [
        field.schemaFieldUrn.rsplit(",", 1)[-1].rstrip(")") for field in aspect.fields
    ]


def test_per_report_report_pass_keeps_fields_the_charts_reference() -> None:
    source = _per_report_source(
        {
            "extract_warehouse_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_source_warehouses": True,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    source.client = _DivergingReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    schema = _aspects_by_urn(workunits, SchemaMetadataClass)[_SHARED_URN]
    schema_paths = {field.fieldPath for field in schema.fields}
    assert schema_paths == {"Gross Sales Amt", "net_report_amt", "net_dossier_amt"}
    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)[_SHARED_URN]
    assert _field_lineage_downstream_paths(lineage) == ["net_dossier_amt"]
    chart_paths = [
        path
        for aspect in _aspects_by_urn(workunits, InputFieldsClass).values()
        for path in _input_field_paths(aspect)
    ]
    assert chart_paths
    assert set(chart_paths) <= schema_paths


class _UnmappedWarehouseClient(_SharedReportClient):
    def _dataset(self) -> Dict[str, Any]:
        dataset = super()._dataset()
        dataset["sourceWarehouse"] = {
            "id": "legacy-1",
            "name": "Legacy DB",
            "database": {"type": "unknown_db"},
        }
        return dataset


def test_per_report_report_pass_does_not_borrow_project_warehouse_for_unmapped() -> (
    None
):
    source = _per_report_source(
        {
            "extract_warehouse_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_source_warehouses": True,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    client = _UnmappedWarehouseClient()
    install_client(source, client)

    workunits = list(source._process_project(_PROJECT))

    # The project's warehouse may be another platform, so its URNs would be wrong.
    assert client.report_instances == ["report-solo"]
    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)
    assert _SHARED_URN not in lineage


class _EmptyReportDefinitionClient(_SharedReportClient):
    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        return {"result": {"definition": {"description": "Empty definition"}}}


def test_per_report_report_pass_without_a_schema_keeps_dossier_lineage() -> None:
    source = _per_report_source(
        {
            "extract_warehouse_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_source_warehouses": True,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    client = _EmptyReportDefinitionClient(with_sql=True)
    install_client(source, client)

    workunits = list(source._process_project(_PROJECT))

    assert client.report_instances == ["report-solo"]
    schema = _aspects_by_urn(workunits, SchemaMetadataClass)[_SHARED_URN]
    assert [field.fieldPath for field in schema.fields] == ["Net Sales Amt"]
    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)
    assert [upstream.dataset for upstream in lineage[_SHARED_URN].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.fact_sales,PROD)"
    ]


class _MetricModelRecordingClient(_SharedReportClient):
    def __init__(self) -> None:
        super().__init__()
        self.metric_models: List[str] = []

    def get_metric_model(
        self, project_id: str, metric_id: str, stop: object = None
    ) -> Dict[str, Any]:
        self.metric_models.append(metric_id)
        return {}


def test_per_report_report_pass_enriches_the_report_metrics() -> None:
    source = _per_report_source({"extract_metric_expressions": True})
    client = _MetricModelRecordingClient()
    install_client(source, client)
    report_object = client.get_object_info("project-1", "ds-shared", 3)

    _read_report_as_dataset(
        source, report_object, _LazyProjectLineage(source, "project-1", [])
    )

    assert client.metric_models == ["M-NET", "M-GROSS"]


class _AttributeReportClient(_SharedReportClient):
    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        definition = super().get_report_definition(project_id, report_id)
        definition["result"]["definition"]["availableObjects"]["attributes"] = [
            {"id": "A-REGION", "name": "Region"}
        ]
        return definition


def test_per_report_report_pass_attaches_model_lineage() -> None:
    source = _per_report_source({"extract_reports": True})
    client = _AttributeReportClient()
    install_client(source, client)
    warehouse_table = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.sales.dim_region,PROD)"
    )
    region_column = f"urn:li:schemaField:({warehouse_table},region_name)"
    seeded = source._shared_report_dataset(
        "project-1",
        _SHARING_DASHBOARD,
        DatasetObject.model_validate({"id": "ds-shared", "name": "R"}),
    )
    seeded.warehouse_upstream_urns = [warehouse_table]
    index = ModelLineageIndex(
        fact_upstreams={},
        attribute_upstreams={
            "A-REGION": [ModelFieldLineage(warehouse_table, [region_column])]
        },
        attribute_form_upstreams={},
    )

    with mock.patch.object(
        _LazyProjectLineage,
        "model_lineage_index",
        new_callable=mock.PropertyMock,
        return_value=index,
    ):
        _read_report_as_dataset(
            source,
            client.get_object_info("project-1", "ds-shared", 3),
            _LazyProjectLineage(source, "project-1", []),
        )
    workunits = list(source._gen_report_dataset_workunits("project-1"))

    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)[_SHARED_URN]
    assert [
        (edge.upstreams, edge.downstreams) for edge in lineage.fineGrainedLineages or []
    ] == [([region_column], [f"urn:li:schemaField:({_SHARED_URN},Region)"])]


def test_per_report_document_reuses_lineage_a_dossier_settled() -> None:
    source = _per_report_source(
        {
            "extract_warehouse_lineage": True,
            "extract_document_dataset_sql_lineage": True,
            "extract_source_warehouses": True,
        }
    )
    client = _SharedReportClient(with_sql=True)
    install_client(source, client)

    workunits = list(source._process_project(_PROJECT))

    assert client.report_instances == []
    assert source.report.document_dataset_sql_views_reused == 1
    lineage = _aspects_by_urn(workunits, UpstreamLineageClass)
    assert len(lineage[_SHARED_URN].upstreams) == 1


class _FailingReportSearchClient(_SharedReportClient):
    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        yield self.get_object_info(project_id, "ds-shared", 3)
        raise RuntimeError("report search failed")


def test_per_report_report_pass_failure_still_emits_registered_datasets() -> None:
    source = _per_report_source(
        {"extract_reports": True, "extract_independent_reports": True}
    )
    source.client = _FailingReportSearchClient()  # type: ignore[assignment]
    workunits: List[MetadataWorkUnit] = []

    with pytest.raises(RuntimeError, match="report search failed"):
        for workunit in source._process_project(_PROJECT):
            workunits.append(workunit)

    # Charts and dashboards already point at these datasets.
    assert list(_aspects_by_urn(workunits, DatasetPropertiesClass)) == [_SHARED_URN]
    assert source.report.report_datasets_emitted == 1


def test_per_report_dashboard_pass_auth_failure_still_emits_registered_datasets() -> (
    None
):
    source = _per_report_source()
    client = _SharedReportClient()
    install_client(source, client)
    first_dossier = client.get_dossier_definition

    def fail_second_dashboard(project_id: str, dossier_id: str) -> Dict[str, Any]:
        if dossier_id == "doc-1":
            raise MicroStrategyAuthError("session lost")
        return first_dossier(project_id, dossier_id)

    object_lookups: List[str] = []

    def record_lookup(
        project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        object_lookups.append(object_id)
        raise AssertionError("no API call after the session is lost")

    client.get_dossier_definition = fail_second_dashboard  # type: ignore[method-assign]
    client.get_object_info = record_lookup  # type: ignore[method-assign]
    workunits: List[MetadataWorkUnit] = []

    with pytest.raises(MicroStrategyAuthError):
        for workunit in source._process_project(_PROJECT):
            workunits.append(workunit)

    assert list(_aspects_by_urn(workunits, DatasetPropertiesClass)) == [_SHARED_URN]
    assert object_lookups == []


def test_per_report_auth_failure_during_emission_fails_the_run() -> None:
    source = _per_report_source()
    client = _SharedReportClient()
    install_client(source, client)

    def lose_session(
        project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        raise MicroStrategyAuthError("session lost")

    client.get_object_info = lose_session  # type: ignore[method-assign]

    with pytest.raises(MicroStrategyAuthError):
        list(source._process_project(_PROJECT))
    assert source.report.warnings == []


def test_per_report_one_bad_dataset_does_not_drop_the_rest_or_register_usage() -> None:
    source = _per_report_source(
        {
            "extract_reports": True,
            "extract_independent_reports": True,
            "extract_usage_statistics": True,
        }
    )
    source.client = _SharedReportClient()  # type: ignore[assignment]

    real = source.mapper.gen_report_dataset_workunits

    def fail_for_shared(
        project_id: str, dataset: DatasetObject, *args: Any, **kwargs: Any
    ) -> Iterable[MetadataWorkUnit]:
        if dataset.id == "ds-shared":
            raise RuntimeError("bad report")
        return real(project_id, dataset, *args, **kwargs)

    with mock.patch.object(
        source.mapper, "gen_report_dataset_workunits", side_effect=fail_for_shared
    ):
        workunits = list(source._process_project(_PROJECT))

    assert list(_aspects_by_urn(workunits, DatasetPropertiesClass)) == [_SOLO_URN]
    assert len(source.report.failures) == 1
    assert "DS-SHARED" not in source._usage_targets
    assert source._usage_targets["REPORT-SOLO"].urn == _SOLO_URN
    assert source._usage_targets["REPORT-SOLO"].kind == UsageTargetKind.DATASET


def test_per_report_report_pass_drops_stale_field_lineage_and_reads_the_sql_again() -> (
    None
):
    source = _per_report_source(
        {
            "extract_warehouse_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_source_warehouses": True,
            "extract_reports": True,
        }
    )
    client = _SharedReportClient(with_sql=True)
    install_client(source, client)
    lineage_context = _LazyProjectLineage(source, "project-1", [])
    list(source._process_project_dashboards(_PROJECT, lineage_context))
    entry = source._report_dataset_entry("project-1", "ds-shared")
    assert entry is not None and entry.lineage_resolved
    entry.dataset.field_warehouse_upstreams["Ghost Field"] = []
    assert client.report_instances == []

    _read_report_as_dataset(
        source,
        client.get_object_info("project-1", "ds-shared", 3),
        lineage_context,
    )

    # A path the merged dataset no longer has cannot keep lineage, and the
    # lineage is read again against the dataset's current fields.
    assert client.report_instances == ["ds-shared"]
    assert "Ghost Field" not in entry.dataset.field_warehouse_upstreams


class _SingleDashboardClient(_SharedReportClient):
    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return itertools.islice(super().search_dashboards(project_id), 1)


def test_per_report_dataset_is_emitted_when_its_only_dashboard_fails_after_charts() -> (
    None
):
    source = _per_report_source()
    source.client = _SingleDashboardClient()  # type: ignore[assignment]

    with mock.patch.object(
        source.mapper,
        "gen_dashboard_workunits",
        side_effect=RuntimeError("dashboard failed"),
    ):
        workunits = list(source._process_project(_PROJECT))

    charts = _aspects_by_urn(workunits, ChartInfoClass)
    assert [chart.inputs for chart in charts.values()] == [[_SHARED_URN]]
    shared = _aspects_by_urn(workunits, DatasetPropertiesClass)[_SHARED_URN]
    assert "microstrategyDashboardIds" not in shared.customProperties
    assert shared.customProperties["microstrategyDashboardCount"] == "0"
    assert source.report.warnings


@pytest.mark.parametrize("error", [RuntimeError("bad report"), ConfigurationError("x")])
def test_per_report_failed_emission_leaves_none_of_the_datasets_workunits(
    error: Exception,
) -> None:
    source = _per_report_source(
        {"extract_reports": True, "extract_independent_reports": True}
    )
    source.client = _SharedReportClient()  # type: ignore[assignment]
    real = source.mapper.gen_report_dataset_workunits

    def yield_one_then_fail(
        project_id: str, dataset: DatasetObject, *args: Any, **kwargs: Any
    ) -> Iterable[MetadataWorkUnit]:
        generated = iter(real(project_id, dataset, *args, **kwargs))
        if dataset.id == "ds-shared":
            yield next(generated)
            raise error
        yield from generated

    workunits: List[MetadataWorkUnit] = []
    with mock.patch.object(
        source.mapper, "gen_report_dataset_workunits", side_effect=yield_one_then_fail
    ):
        if isinstance(error, ConfigurationError):
            with pytest.raises(ConfigurationError):
                for workunit in source._process_project(_PROJECT):
                    workunits.append(workunit)
        else:
            workunits = list(source._process_project(_PROJECT))

    urns = {workunit.get_urn() for workunit in workunits}
    shared_folder = source.mapper.folder_key(
        "project-1", "Shared Reports/Regional Sales"
    ).as_urn()
    assert _SHARED_URN not in urns
    assert shared_folder not in urns
    if not isinstance(error, ConfigurationError):
        assert _SOLO_URN in urns
        assert len(source.report.failures) == 1


class _ReportSearchMissesSharedClient(_SharedReportClient):
    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return itertools.islice(super().search_reports(project_id), 1, None)


def test_per_report_dashboard_embedded_report_is_not_emitted_without_cubes() -> None:
    source = _per_report_source(
        {
            "extract_cubes": False,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    source.client = _ReportSearchMissesSharedClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    assert list(_aspects_by_urn(workunits, DatasetPropertiesClass)) == [_SOLO_URN]


@pytest.mark.parametrize(
    "extra_config",
    [
        {"extract_dashboards": False, "extract_reports": False},
        {"extract_cubes": False, "extract_reports": False},
    ],
)
def test_per_report_needs_a_dataset_source(extra_config: Dict[str, bool]) -> None:
    with pytest.raises(ValidationError, match="per_report"):
        _per_report_source(extra_config)


def test_per_report_reports_alone_are_a_dataset_source() -> None:
    _per_report_source({"extract_dashboards": False, "extract_reports": True})


def test_per_dashboard_report_chart_records_prompt_and_filter_properties() -> None:
    source = _per_report_source(
        {
            "dataset_identity": "per_dashboard",
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    source.client = _SharedReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    chart = _aspects_by_urn(workunits, ChartInfoClass)[
        source.mapper.report_urn("project-1", "report-solo")
    ]
    assert chart.customProperties["microstrategyReportPromptCount"] == "1"
    assert chart.customProperties["microstrategyReportHasFilter"] == "false"


def test_per_report_dataset_omits_prompt_and_filter_without_a_report_definition() -> (
    None
):
    source = _per_report_source(
        {"extract_reports": True, "extract_independent_reports": True}
    )
    client = _SharedReportClient()

    def definition_unavailable(project_id: str, report_id: str) -> Dict[str, Any]:
        raise MicroStrategyAPIError("403")

    client.get_report_definition = definition_unavailable  # type: ignore[method-assign]
    install_client(source, client)

    workunits = list(source._process_project(_PROJECT))

    properties = _aspects_by_urn(workunits, DatasetPropertiesClass)
    assert sorted(properties) == [_SHARED_URN, _SOLO_URN]
    for value in properties.values():
        assert "microstrategyReportPromptCount" not in value.customProperties
        assert "microstrategyReportHasFilter" not in value.customProperties


class _DescriptionOnlyReportClient(_SharedReportClient):
    def get_report_definition(self, project_id: str, report_id: str) -> Dict[str, Any]:
        return {"result": {"definition": {"description": "Only a description"}}}


def test_per_report_report_description_applies_without_available_objects() -> None:
    source = _per_report_source(
        {"extract_reports": True, "extract_independent_reports": True}
    )
    source.client = _DescriptionOnlyReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    shared = _aspects_by_urn(workunits, DatasetPropertiesClass)[_SHARED_URN]
    assert shared.description == "Only a description"


class _DerivedReportClient(_SharedReportClient):
    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "information": {"objectId": report_id, "name": "Daily Sales Report"},
            "dataSource": {
                "dataTemplate": {
                    "units": [
                        {
                            "type": "metrics",
                            "elements": [
                                {
                                    "id": "D-PLAN",
                                    "name": "Sales Plan",
                                    "subType": "derived_metric",
                                    "expression": {"text": "[Net Sales Amt] * 2"},
                                }
                            ],
                        }
                    ]
                }
            },
        }


def test_per_report_report_pass_attaches_report_derived_metrics() -> None:
    source = _per_report_source(
        {
            "extract_reports": True,
            "extract_independent_reports": True,
            "extract_derived_metrics": True,
        }
    )
    source.client = _DerivedReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    schema = _aspects_by_urn(workunits, SchemaMetadataClass)[_SHARED_URN]
    derived = [field for field in schema.fields if field.fieldPath == "Sales Plan"]
    assert len(derived) == 1
    assert "[Net Sales Amt] * 2" in (derived[0].description or "")


def test_per_report_dashboard_with_an_unregistered_dataset_does_not_abort() -> None:
    source = _per_report_source()
    dashboard = _dashboard()
    dashboard_object = MicroStrategyObject.model_validate(
        {"id": dashboard.id, "name": dashboard.name}
    )

    source._record_report_dataset_dashboard("project-1", dashboard_object, dashboard)

    assert len(source.report.warnings) == 1


class _PredefinedFoldersSharedReportClient(_SharedReportClient):
    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        return _standard_predefined_folders(folder_types)

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": "Daily Sales Report",
                "type": "3",
                "subtype": "768",
                "ancestors": [
                    {"id": "public-objects-id", "name": "Public Objects"},
                    {"id": "reports-folder-id", "name": "Reports"},
                    {"id": "f-2", "name": "Regional Sales"},
                ],
            }
        )


def test_per_report_dataset_folder_uses_predefined_folder_names() -> None:
    source = _per_report_source({"use_predefined_folder_names": True})
    source.client = _PredefinedFoldersSharedReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    assert _dataset_container_parents(workunits) == {
        _SHARED_URN: source.mapper.folder_key(
            "project-1", "Public Objects/Shared Reports/Regional Sales"
        ).as_urn()
    }


def test_per_report_personally_filed_dataset_sits_in_the_dashboard_folder() -> None:
    source = _per_report_source({"use_predefined_folder_names": True})
    source.client = _PersonalFolderClient()  # type: ignore[assignment]

    workunits = _run_dashboards(source)
    workunits += list(source._gen_report_dataset_workunits("project-1"))

    assert _container_names(workunits) == {"Shared Reports"}
    assert set(_dataset_container_parents(workunits).values()) == {
        source.mapper.folder_key("project-1", "Shared Reports").as_urn()
    }


class _UnfiledSoloReportClient(_SharedReportClient):
    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "report-solo",
                        "name": "Standalone Report",
                        "type": "3",
                        "subtype": "768",
                    }
                )
            ]
        )


def test_per_report_independent_report_without_ancestry_sits_in_the_project() -> None:
    source = _per_report_source(
        {
            "extract_cubes": False,
            "extract_reports": True,
            "extract_independent_reports": True,
        }
    )
    source.client = _UnfiledSoloReportClient()  # type: ignore[assignment]

    workunits = list(source._process_project(_PROJECT))

    assert _dataset_container_parents(workunits) == {
        _SOLO_URN: source.mapper.project_key("project-1").as_urn()
    }


def test_per_report_usage_lands_on_report_datasets_as_dataset_usage_only() -> None:
    source = _per_report_source(
        {
            "extract_reports": True,
            "extract_independent_reports": True,
            "extract_usage_statistics": True,
        }
    )
    source.client = _SharedReportClient()  # type: ignore[assignment]
    list(source._process_project(_PROJECT))
    buckets = [
        UsageBucket(
            object_guid=guid,
            project_guid="project-1",
            object_name=guid,
            bucket_start_ms=1_700_000_000_000,
            view_count=3,
            user_counts={"alice": 3},
        )
        for guid in ("DS-SHARED", "REPORT-SOLO")
    ]

    workunits = list(source._usage_bucket_workunits(buckets))

    assert list(_aspects_by_urn(workunits, DatasetUsageStatisticsClass)) == [
        _SHARED_URN,
        _SOLO_URN,
    ]
    assert not _aspects_by_urn(workunits, ChartUsageStatisticsClass)


def test_clearing_project_state_lets_a_later_project_re_register_the_dataset() -> None:
    source = _per_report_source()
    source.client = _SharedReportClient()  # type: ignore[assignment]
    first = list(source._process_project(_PROJECT))
    assert _SHARED_URN in _aspects_by_urn(first, DatasetPropertiesClass)

    source._reset_project_caches()
    second = list(source._process_project(_PROJECT))

    shared = _aspects_by_urn(second, DatasetPropertiesClass)[_SHARED_URN]
    assert shared.customProperties["microstrategyDashboardCount"] == "2"


def test_run_installs_the_sqlglot_filter_only_while_running() -> None:
    source = _source()
    sqlglot_logger = logging.getLogger("sqlglot")
    filters_before = list(sqlglot_logger.filters)
    seen_during_run: List[int] = []

    def list_projects() -> List[Project]:
        seen_during_run.append(len(sqlglot_logger.filters))
        return []

    with (
        mock.patch.object(source.client, "login"),
        mock.patch.object(source.client, "close"),
        mock.patch.object(source.client, "list_projects", side_effect=list_projects),
    ):
        assert list(source.get_workunits_internal()) == []

    assert seen_during_run == [len(filters_before) + 1]
    assert sqlglot_logger.filters == filters_before


# One metric's model endpoint always fails.
class _MetricModelClient:
    def __init__(self) -> None:
        self.metric_model_calls: List[str] = []

    def get_metric_model(
        self, project_id: str, metric_id: str, stop: object = None
    ) -> Dict[str, object]:
        self.metric_model_calls.append(metric_id)
        if metric_id.upper() == "M-BROKEN":
            raise MicroStrategyAPIError("404 Not Found", status_code=404)
        return {"expression": {"text": f"Sum({metric_id})"}}

    def search_metrics(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            MicroStrategyObject.model_validate({"id": metric_id, "name": metric_id})
            for metric_id in ("m-net", "M-BROKEN", "M-CATALOG")
        )


def test_semantic_model_reuses_metric_models_from_dashboard_enrichment() -> None:
    source = _source({"emit_semantic_model_entities": True})
    client = _MetricModelClient()
    install_client(source, client)
    dataset = DatasetObject.model_validate(
        {
            "id": "ds-1",
            "name": "Sales Cube",
            "availableObjects": {
                "metrics": [
                    {"id": "M-NET", "name": "Net Sales"},
                    {"id": "M-BROKEN", "name": "Broken"},
                ]
            },
        }
    )

    source._enrich_dataset_metric_expressions("project-1", dataset)
    source._enrich_dataset_metric_expressions("project-1", dataset)
    workunits = list(
        source._process_project_semantic_model(
            _PROJECT,
            _FixedTablesLineage(
                source,
                [
                    {
                        "physicalTable": {"tableName": "T1"},
                        "facts": [
                            {"information": {"objectId": "F1", "name": "Revenue"}}
                        ],
                    }
                ],
            ),
        )
    )

    # M-NET is fetched once for both passes; the failed M-BROKEN is retried by
    # the semantic model and, with no graph to confirm it exists, skipped, and
    # only the metric the dashboards never saw is new.
    assert client.metric_model_calls == ["M-NET", "M-BROKEN", "M-BROKEN", "M-CATALOG"]
    assert not source.report.failures
    assert "MicroStrategy Metric Model Unavailable" in {
        warning.title for warning in source.report.warnings
    }
    assert source.report.semantic_model_metrics_emitted == 2
    metric_urns = {
        workunit.get_urn()
        for workunit in workunits
        if workunit.get_urn().startswith("urn:li:metric:")
    }
    assert metric_urns == {
        "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,M-NET)",
        "urn:li:metric:(urn:li:dataPlatform:microstrategy,project-1,M-CATALOG)",
    }


class _ObjectInfoClient(_ReportSearchClient):
    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        self.object_info_calls.append(object_id)
        return MicroStrategyObject.model_validate(
            {"id": object_id, "name": object_id, "type": "3", "subtype": "768"}
        )

    def get_report_object(self, project_id: str, report_id: str) -> MicroStrategyObject:
        return self.get_object_info(project_id, report_id, 3)


def test_report_object_lookups_are_shared_across_datasets_links_and_search() -> None:
    source = _report_scope_source({"extract_independent_reports": True})
    client = _ObjectInfoClient()
    install_client(source, client)

    dataset_object = source._dataset_object_info("project-1", "report-9")
    linked_object = source._linked_report_object("project-1", "REPORT-9")
    list(
        source._process_project_reports(
            _PROJECT, _LazyProjectLineage(source, "project-1", [])
        )
    )
    searched_link = source._linked_report_object("project-1", "report-1")
    searched_dataset = source._dataset_object_info("project-1", "REPORT-2")

    # One lookup for the dataset and its link; searched reports need none.
    assert client.object_info_calls == ["report-9"]
    assert linked_object == dataset_object
    assert searched_link is not None and searched_link.name == "Linked Report"
    assert searched_dataset is not None and searched_dataset.name == "Unlinked Report"
    assert source.report.dataset_object_lookups == 1


class _FailsFirstObjectInfoClient(_ObjectInfoClient):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures_left = failures

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> MicroStrategyObject:
        if self.failures_left > 0:
            self.failures_left -= 1
            self.object_info_calls.append(object_id)
            raise RuntimeError("lookup failed")
        return super().get_object_info(project_id, object_id, object_type)


def test_failed_dataset_lookup_does_not_hide_a_linked_report() -> None:
    source = _report_scope_source({"extract_independent_reports": True})
    client = _FailsFirstObjectInfoClient(failures=1)
    install_client(source, client)

    assert source._dataset_object_info("project-1", "report-9") is None
    linked_object = source._linked_report_object("project-1", "REPORT-9")

    assert client.object_info_calls == ["report-9", "REPORT-9"]
    assert linked_object is not None and linked_object.id == "REPORT-9"
    assert source.report.dataset_object_lookup_failures == 1


def test_failed_linked_report_lookup_does_not_hide_a_dataset() -> None:
    source = _report_scope_source({"extract_independent_reports": True})
    client = _FailsFirstObjectInfoClient(failures=1)
    install_client(source, client)

    assert source._linked_report_object("project-1", "report-9") is None
    dataset_object = source._dataset_object_info("project-1", "REPORT-9")

    assert client.object_info_calls == ["report-9", "REPORT-9"]
    assert dataset_object is not None and dataset_object.id == "REPORT-9"
    assert "Failed to Fetch Linked Report" in {
        warning.title for warning in source.report.warnings
    }


def test_linked_report_lookup_warns_after_a_failed_dataset_lookup() -> None:
    source = _report_scope_source({"extract_independent_reports": True})
    client = _FailsFirstObjectInfoClient(failures=2)
    install_client(source, client)

    assert source._dataset_object_info("project-1", "report-9") is None
    assert source._linked_report_object("project-1", "report-9") is None
    assert source._linked_report_object("project-1", "report-9") is None

    assert client.object_info_calls == ["report-9", "report-9"]
    assert [warning.title for warning in source.report.warnings].count(
        "Failed to Fetch Linked Report"
    ) == 1


def test_report_v2_definition_is_fetched_once_for_derived_metrics_and_reports() -> None:
    source = _source(
        {
            "extract_warehouse_lineage": False,
            "extract_visualization_details": False,
            "extract_dashboard_dependencies": False,
            "extract_metric_expressions": False,
            "extract_model_lineage": False,
            "extract_reports": True,
        }
    )
    client = _ModelEmptyClient()
    install_client(source, client)

    _run_dashboards(source)
    shared = source._fetch_report_definition(
        "project-1",
        MicroStrategyObject.model_validate(
            {"id": "DS-SHARED", "name": "Retail Sales Yesterday"}
        ),
    )
    other = source._fetch_report_definition(
        "project-1",
        MicroStrategyObject.model_validate({"id": "rep-2", "name": "Other"}),
    )

    # The derived-metric fallback's v2 payload serves the report pass too.
    assert client.v2_calls == ["ds-shared", "rep-2"]
    assert shared is not None and shared.available_objects
    assert other is not None
    assert source.report.report_definition_api_failures == 0


def _record_spill_stores(
    created: List[FileBackedDict[object]],
) -> Callable[..., FileBackedDict[object]]:
    real_factory: Callable[..., FileBackedDict[object]] = spill_cache.FileBackedDict

    def recording_factory(**kwargs: object) -> FileBackedDict[object]:
        store = real_factory(**kwargs)
        created.append(store)
        return store

    return recording_factory


def _fill_every_spill_cache(source: MicroStrategySource) -> None:
    report_key = object_cache_key("project-1", "R-1")
    source._metric_models.put(object_cache_key("project-1", "M-1"), {})
    source._type3_objects.put(
        report_key,
        _ReportObjectLookup(
            report_object=MicroStrategyObject.model_validate({"id": "R-1", "name": "R"})
        ),
    )
    source._report_sql_statements.put(report_key, _ReportSqlOutcome(sql="select 1"))
    source._report_v2_payloads.put(report_key, {})
    source._report_derived_metrics.put(report_key, [])
    source.lineage.warehouse_lineage_from_sql(
        "select * from fact_sales",
        WarehouseLineageContext(platform="snowflake", env="PROD"),
        [],
    )


@pytest.mark.parametrize(
    "error, expected_failures",
    [
        (RuntimeError("project blew up"), ["Failed to Process Project"]),
        (
            MicroStrategyAuthError("session lost"),
            ["MicroStrategy Authentication Lost"],
        ),
        (ConfigurationError("parser missing"), []),
    ],
)
def test_project_failure_still_closes_every_spill_cache(
    error: Exception, expected_failures: List[str]
) -> None:
    source = _source()
    created: List[FileBackedDict[object]] = []

    def failing_project(project: Project) -> Iterator[MetadataWorkUnit]:
        _fill_every_spill_cache(source)
        raise error
        yield

    with (
        mock.patch.object(
            spill_cache, "FileBackedDict", side_effect=_record_spill_stores(created)
        ),
        mock.patch.object(source.client, "login"),
        mock.patch.object(source.client, "close"),
        mock.patch.object(source.client, "list_projects", return_value=[_PROJECT]),
        mock.patch.object(source, "_process_project", side_effect=failing_project),
    ):
        if isinstance(error, ConfigurationError):
            with pytest.raises(ConfigurationError):
                list(source.get_workunits_internal())
        else:
            assert list(source.get_workunits_internal()) == []

    assert len(created) == 6
    assert all(store.closed for store in created)
    assert [failure.title for failure in source.report.failures] == expected_failures
    assert source.report.cache_project_resets == 1


def test_closing_the_run_early_still_closes_every_spill_cache() -> None:
    source = _source()
    created: List[FileBackedDict[object]] = []

    def project_with_workunits(project: Project) -> Iterator[MetadataWorkUnit]:
        _fill_every_spill_cache(source)
        yield from source.mapper.gen_project_container(project, source_warehouses=[])

    with (
        mock.patch.object(
            spill_cache, "FileBackedDict", side_effect=_record_spill_stores(created)
        ),
        mock.patch.object(source.client, "login"),
        mock.patch.object(source.client, "close") as client_close,
        mock.patch.object(source.client, "list_projects", return_value=[_PROJECT]),
        mock.patch.object(
            source, "_process_project", side_effect=project_with_workunits
        ),
    ):
        run = source.get_workunits_internal()
        assert isinstance(run, GeneratorType)
        next(run)
        run.close()

    assert len(created) == 6
    assert all(store.closed for store in created)
    assert source.report.cache_project_resets == 1
    client_close.assert_called_once()


def test_a_failing_cache_close_does_not_skip_the_rest_of_cleanup() -> None:
    source = _source()
    created: List[FileBackedDict[object]] = []

    def project(project: Project) -> Iterator[MetadataWorkUnit]:
        _fill_every_spill_cache(source)
        return iter([])

    with (
        mock.patch.object(
            spill_cache, "FileBackedDict", side_effect=_record_spill_stores(created)
        ),
        mock.patch.object(source.client, "login"),
        mock.patch.object(source.client, "close") as client_close,
        mock.patch.object(source.client, "list_projects", return_value=[_PROJECT]),
        mock.patch.object(source, "_process_project", side_effect=project),
        mock.patch.object(
            source._type3_objects, "close", side_effect=OSError("disk error")
        ),
        pytest.raises(OSError),
    ):
        list(source.get_workunits_internal())

    with pytest.raises(RuntimeError):
        source._metric_models.get(object_cache_key("project-1", "M-1"))
    client_close.assert_called_once()


def test_cached_only_dataset_emission_uses_a_stored_report_object() -> None:
    source = _per_report_source()
    source._shared_report_dataset(
        "project-1",
        _SHARING_DASHBOARD,
        DatasetObject.model_validate({"id": "ds-shared", "name": "Shared Report"}),
    )
    source._remember_searched_report(
        "project-1",
        MicroStrategyObject.model_validate(
            {
                "id": "DS-SHARED",
                "name": "Shared Report",
                "type": "3",
                "ancestors": [{"id": "f-1", "name": "Shared Reports"}],
            }
        ),
    )

    with mock.patch.object(
        source.client,
        "get_object_info",
        side_effect=AssertionError("no lookup after the session is lost"),
    ):
        workunits = list(
            source._gen_report_dataset_workunits("project-1", cached_only=True)
        )

    folder_names = {
        properties.name
        for properties in _aspects_by_urn(workunits, ContainerPropertiesClass).values()
    }
    assert "Shared Reports" in folder_names
    assert source.report.report_datasets_emitted == 1


def test_cached_only_dataset_emission_with_workers_makes_no_lookups() -> None:
    source = _per_report_source({"max_workers": 4})
    for dataset_id in ("ds-a", "ds-b"):
        source._shared_report_dataset(
            "project-1",
            _SHARING_DASHBOARD,
            DatasetObject.model_validate({"id": dataset_id, "name": dataset_id}),
        )
    lookups: List[str] = []

    def record_lookup(
        project_id: str, object_id: str, object_type: int
    ) -> Optional[MicroStrategyObject]:
        lookups.append(object_id)
        return None

    with mock.patch.object(source.client, "get_object_info", side_effect=record_lookup):
        list(source._gen_report_dataset_workunits("project-1", cached_only=True))

    assert lookups == []
    assert source.report.report_datasets_emitted == 2


class _SecondDossierWithoutWarehouseClient(_SharedReportClient):
    def __init__(self) -> None:
        super().__init__(second="dossier", with_sql=True)

    def list_project_datasources(self, project_id: str) -> List[Datasource]:
        return []

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, Any]:
        response = super().get_dossier_definition(project_id, dossier_id)
        if dossier_id == "dash-2":
            for dataset in response["result"]["definition"]["datasets"]:
                dataset.pop("sourceWarehouse")
        return response


def test_per_report_dossier_reads_its_sql_view_when_a_shared_dataset_brings_a_warehouse() -> (
    None
):
    source = _per_report_source({"extract_warehouse_lineage": True, "max_workers": 4})
    client = _SecondDossierWithoutWarehouseClient()
    install_client(source, client)

    list(source._process_project(_PROJECT))

    assert client.sql_view_reads == ["dash-1", "dash-2"]


def test_a_failed_report_v2_payload_is_not_cached() -> None:
    source = _source()
    calls: List[str] = []

    def definition(project_id: str, report_id: str) -> Dict[str, object]:
        calls.append(report_id)
        if len(calls) == 1:
            raise MicroStrategyAPIError("busy", status_code=503)
        return {"id": report_id}

    with mock.patch.object(
        source.client, "get_report_definition", side_effect=definition
    ):
        with pytest.raises(MicroStrategyAPIError):
            source._report_v2_payload("project-1", "rep-1")
        assert source._report_v2_payload("project-1", "rep-1") == {"id": "rep-1"}
        assert source._report_v2_payload("project-1", "REP-1") == {"id": "rep-1"}

    assert calls == ["rep-1", "rep-1"]


def test_an_empty_metric_model_is_cached_and_not_fetched_again() -> None:
    source = _source()

    with mock.patch.object(
        source.client, "get_metric_model", return_value={}
    ) as get_metric_model:
        assert source._get_metric_model("project-1", "M-1") == {}
        assert source._get_metric_model("project-1", "m-1") == {}

    get_metric_model.assert_called_once()
    assert source._cached_metric_model("project-1", "M-1") == {}


def test_mixed_warehouse_project_resolves_model_lineage_per_table(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    client = ModelTablesClient(
        [
            model_table("ora_orders", "fact-1", "aaa111"),
            model_table("snow_orders", "fact-2", "BBB222"),
            model_table("unlinked_orders", "fact-3", None),
            model_table("unknown_orders", "fact-4", "ZZZ999"),
        ]
    )
    install_client(source, client)
    lineage_context = _LazyProjectLineage(source, "project-1", mixed_datasources)

    index = lineage_context.model_lineage_index

    assert index is not None
    assert lineage_context.warehouse_context is None
    assert "primaryDataSource" in client.fields_requested[0]
    assert index.fact_field_urns(["fact-1"]) == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:oracle,"
        "ora_db.ora.ora_orders,PROD),amount)"
    ]
    assert index.fact_field_urns(["fact-2"]) == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "snow_db.snow.snow_orders,PROD),amount)"
    ]
    assert index.fact_field_urns(["fact-3", "fact-4"]) == []
    assert source.report.model_tables_resolved_by_datasource == 2
    assert source.report.model_tables_without_warehouse_context == 2


def test_mixed_warehouse_project_warning_names_platforms_and_mapping(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    install_client(source, ModelTablesClient([]))
    lineage_context = _LazyProjectLineage(source, "project-1", mixed_datasources)

    assert lineage_context.warehouse_context is None

    warning = next(
        w for w in source.report.warnings if "Several Source Warehouses" in str(w.title)
    )
    context = str(warning.context)
    assert "project_id=project-1" in context
    assert "oracle:1" in context
    assert "snow_flake:1" in context
    assert "datasource_platform_mapping" in context


@pytest.mark.parametrize(
    "datasources, expected_title",
    [
        (
            [{"id": "CCC333", "name": "legacy_ds", "database": {"type": "unknown_db"}}],
            "MicroStrategy Project Has No Supported Source Warehouse",
        ),
        ([], "MicroStrategy Model Lineage Unavailable"),
    ],
)
def test_project_without_warehouse_context_warns_once(
    datasources: List[Dict[str, object]], expected_title: str
) -> None:
    source = _source({"extract_model_lineage": True})
    install_client(source, ModelTablesClient([]))
    lineage_context = _LazyProjectLineage(
        source,
        "project-1",
        [Datasource.model_validate(datasource) for datasource in datasources],
    )

    assert lineage_context.model_lineage_index is None
    assert [str(w.title) for w in source.report.warnings] == [expected_title]


def test_unmapped_second_datasource_resolves_model_lineage_per_table(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    client = ModelTablesClient(
        [
            model_table("ora_orders", "fact-1", "AAA111"),
            model_table("legacy_orders", "fact-2", "CCC333"),
            model_table("unlinked_orders", "fact-3", None),
        ]
    )
    install_client(source, client)
    unmapped = Datasource.model_validate(
        {"id": "CCC333", "name": "legacy_ds", "database": {"type": "unknown_db"}}
    )
    lineage_context = _LazyProjectLineage(
        source, "project-1", [mixed_datasources[0], unmapped]
    )

    index = lineage_context.model_lineage_index

    assert index is not None
    assert lineage_context.warehouse_context is None
    assert index.fact_field_urns(["fact-1"]) == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:oracle,"
        "ora_db.ora.ora_orders,PROD),amount)"
    ]
    assert index.fact_field_urns(["fact-2", "fact-3"]) == []
    assert source.report.model_tables_resolved_by_datasource == 1
    assert source.report.model_tables_without_warehouse_context == 2


def test_single_warehouse_project_keeps_project_level_context(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    client = ModelTablesClient([model_table("orders", "fact-1", None)])
    install_client(source, client)
    lineage_context = _LazyProjectLineage(source, "project-1", mixed_datasources[:1])

    index = lineage_context.model_lineage_index

    assert index is not None
    assert "primaryDataSource" not in client.fields_requested[0]
    assert index.fact_field_urns(["fact-1"]) == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:oracle,"
        "ora_db.ora.orders,PROD),amount)"
    ]
    assert source.report.model_tables_without_warehouse_context == 0


def _connected_oracle_warehouse(schema: Optional[str] = None) -> Dict[str, object]:
    database: Dict[str, object] = {"type": "oracle", "name": "ORA_DB"}
    if schema:
        database["schema"] = schema
    return {
        "id": "AAA111",
        "name": "oracle_ds",
        "database": database,
        "connection": {"id": "conn-ora", "name": "oracle_connection"},
    }


def _connected_oracle_datasources(
    mixed_datasources: List[Datasource], schema: Optional[str] = None
) -> List[Datasource]:
    return [
        Datasource.model_validate(_connected_oracle_warehouse(schema)),
        *mixed_datasources[1:],
    ]


def _dashboard_with_fact_metric(schema: Optional[str] = None) -> DashboardDefinition:
    dashboard = _dashboard(_connected_oracle_warehouse(schema))
    dashboard.datasets[0].metric_enrichments["METRIC-1"] = MetricEnrichment(
        fact_ids=["fact-1"]
    )
    return dashboard


def test_dataset_sql_view_lineage_uses_connection_enriched_model_urns(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    client = ConnectionModelTablesClient([model_table("orders", "fact-1", "AAA111")])
    install_client(source, client)
    lineage_context = _LazyProjectLineage(
        source, "project-1", _connected_oracle_datasources(mixed_datasources)
    )
    index = lineage_context.model_lineage_index
    assert index is not None
    model_table_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:oracle,ora_db.conn_schema.orders,PROD)"
    )
    assert index.fact_field_urns(["fact-1"]) == [
        f"urn:li:schemaField:({model_table_urn},amount)"
    ]
    dashboard = _dashboard_with_fact_metric()

    source._attach_dataset_warehouse_upstreams(
        [{"id": "ds-1", "sqlStatement": "select amount from orders"}],
        dashboard,
        lineage_context.warehouse_context,
        project_id="project-1",
        entries={},
    )
    dataset = dashboard.datasets[0]
    source.mapper.attach_dataset_model_lineage(dataset, index)

    assert dataset.warehouse_upstream_urns == [model_table_urn]
    assert f"urn:li:schemaField:({model_table_urn},amount)" in [
        urn for urns in dataset.field_warehouse_upstreams.values() for urn in urns
    ]
    assert client.connection_requests == ["conn-ora"]


def test_report_dataset_in_mixed_project_uses_its_own_warehouse_context(
    mixed_datasources: List[Datasource],
) -> None:
    source = _per_report_source(
        {
            "extract_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_model_lineage": True,
            "extract_report_definitions": False,
        }
    )
    install_client(
        source, ReportSqlModelTablesClient([model_table("orders", "fact-1", "AAA111")])
    )
    lineage_context = _LazyProjectLineage(
        source, "project-1", _connected_oracle_datasources(mixed_datasources)
    )
    dataset = source._shared_report_dataset(
        "project-1",
        _SHARING_DASHBOARD,
        _dashboard_with_fact_metric().datasets[0],
    )
    report_object = MicroStrategyObject.model_validate(
        {"id": "ds-1", "name": "Sales Cube", "type": "3"}
    )

    _read_report_as_dataset(source, report_object, lineage_context)

    assert lineage_context.warehouse_context is None
    table_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:oracle,ora_db.conn_schema.orders,PROD)"
    )
    assert dataset.warehouse_upstream_urns == [table_urn]
    assert f"urn:li:schemaField:({table_urn},amount)" in [
        urn for urns in dataset.field_warehouse_upstreams.values() for urn in urns
    ]


def test_failed_connection_lookup_keeps_unenriched_context_and_is_cached(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    client = ConnectionModelTablesClient(
        [model_table("orders", "fact-1", "AAA111")], fail_connection=True
    )
    install_client(source, client)
    lineage_context = _LazyProjectLineage(
        source, "project-1", _connected_oracle_datasources(mixed_datasources, "ORA")
    )
    index = lineage_context.model_lineage_index
    assert index is not None
    dashboard = _dashboard_with_fact_metric(schema="ORA")

    source._attach_dataset_warehouse_upstreams(
        [{"id": "ds-1", "sqlStatement": "select amount from orders"}],
        dashboard,
        lineage_context.warehouse_context,
        project_id="project-1",
        entries={},
    )
    dataset = dashboard.datasets[0]
    source.mapper.attach_dataset_model_lineage(dataset, index)

    table_urn = "urn:li:dataset:(urn:li:dataPlatform:oracle,ora_db.ora.orders,PROD)"
    field_urn = f"urn:li:schemaField:({table_urn},amount)"
    assert dataset.warehouse_upstream_urns == [table_urn]
    assert index.fact_field_urns(["fact-1"]) == [field_urn]
    assert field_urn in [
        urn for urns in dataset.field_warehouse_upstreams.values() for urn in urns
    ]
    assert client.connection_requests == ["conn-ora"]
    assert source.report.warehouse_lineage_api_failures == 1
    assert lineage_context.lineage_degraded
    assert [
        w
        for w in source.report.warnings
        if w.title == "Failed to Fetch Datasource Connection"
    ]


def _warehouse(
    datasource_id: str,
    name: str,
    db_type: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    connection_id: Optional[str] = None,
) -> Datasource:
    database_ref: Dict[str, object] = {"type": db_type}
    if database:
        database_ref["name"] = database
    if schema:
        database_ref["schema"] = schema
    datasource: Dict[str, object] = {
        "id": datasource_id,
        "name": name,
        "database": database_ref,
    }
    if connection_id:
        datasource["connection"] = {"id": connection_id, "name": connection_id}
    return Datasource.model_validate(datasource)


def _field_urn(table_urn: str) -> str:
    return f"urn:li:schemaField:({table_urn},amount)"


def test_same_platform_datasources_resolve_to_their_own_connection_database() -> None:
    source = _source({"extract_model_lineage": True})
    client = ConnectionModelTablesClient(
        [
            model_table("orders", "fact-1", "AAA111"),
            model_table("orders", "fact-2", "BBB222"),
        ],
        connections={
            "conn-a": {
                "database": {"type": "snow_flake"},
                "connectionString": "DATABASE=DB_ONE;SCHEMA=SC_ONE",
            },
            "conn-b": {
                "database": {"type": "snow_flake"},
                "connectionString": "DATABASE=DB_TWO;SCHEMA=SC_TWO",
            },
        },
    )
    install_client(source, client)
    lineage_context = _LazyProjectLineage(
        source,
        "project-1",
        [
            _warehouse("AAA111", "wh_a", "snow_flake", connection_id="conn-a"),
            _warehouse("BBB222", "wh_b", "snow_flake", connection_id="conn-b"),
        ],
    )

    index = lineage_context.model_lineage_index

    assert index is not None
    assert lineage_context.warehouse_context is None
    assert index.fact_field_urns(["fact-1"]) == [
        _field_urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db_one.sc_one.orders,PROD)"
        )
    ]
    assert index.fact_field_urns(["fact-2"]) == [
        _field_urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db_two.sc_two.orders,PROD)"
        )
    ]
    assert source.report.model_tables_resolved_by_datasource == 2


def test_datasources_with_one_connection_enriched_context_count_as_one_warehouse() -> (
    None
):
    source = _source({"extract_model_lineage": True})
    client = ConnectionModelTablesClient(
        [model_table("orders", "fact-1", None)],
        connections={
            "conn-a": {"connectionString": "DATABASE=DB_SAME;SCHEMA=SC_SAME"},
            "conn-b": {"connectionString": "DATABASE=DB_SAME;SCHEMA=SC_SAME"},
        },
    )
    install_client(source, client)
    lineage_context = _LazyProjectLineage(
        source,
        "project-1",
        [
            _warehouse("AAA111", "wh_a", "oracle", "DB_A", connection_id="conn-a"),
            _warehouse("BBB222", "wh_b", "oracle", "DB_B", connection_id="conn-b"),
        ],
    )

    index = lineage_context.model_lineage_index

    assert index is not None
    assert lineage_context.warehouse_context is not None
    assert "primaryDataSource" not in client.fields_requested[0]
    assert index.fact_field_urns(["fact-1"]) == [
        _field_urn(
            "urn:li:dataset:(urn:li:dataPlatform:oracle,db_same.sc_same.orders,PROD)"
        )
    ]


def test_datasources_split_only_by_platform_instance_or_env_resolve_per_table() -> None:
    source = _source(
        {
            "extract_model_lineage": True,
            "datasource_platform_mapping": {
                "wh_a": {"platform_instance": "inst_a"},
                "wh_b": {"env": "DEV"},
            },
        }
    )
    install_client(
        source,
        ModelTablesClient(
            [
                model_table("orders", "fact-1", "AAA111"),
                model_table("orders", "fact-2", "BBB222"),
            ]
        ),
    )
    lineage_context = _LazyProjectLineage(
        source,
        "project-1",
        [
            _warehouse("AAA111", "wh_a", "oracle", "DB", "SC"),
            _warehouse("BBB222", "wh_b", "oracle", "DB", "SC"),
        ],
    )

    index = lineage_context.model_lineage_index

    assert index is not None
    assert lineage_context.warehouse_context is None
    assert index.fact_field_urns(["fact-1"]) == [
        _field_urn(
            "urn:li:dataset:(urn:li:dataPlatform:oracle,inst_a.db.sc.orders,PROD)"
        )
    ]
    assert index.fact_field_urns(["fact-2"]) == [
        _field_urn("urn:li:dataset:(urn:li:dataPlatform:oracle,db.sc.orders,DEV)")
    ]


def test_platform_mapping_resolves_an_unrecognized_datasource_per_table(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source(
        {
            "extract_model_lineage": True,
            "datasource_platform_mapping": {"legacy_ds": {"platform": "databricks"}},
        }
    )
    install_client(
        source,
        ModelTablesClient(
            [
                model_table("orders", "fact-1", "CCC333"),
                model_table("ora", "fact-2", "AAA111"),
            ]
        ),
    )
    lineage_context = _LazyProjectLineage(
        source,
        "project-1",
        [
            mixed_datasources[0],
            _warehouse("CCC333", "legacy_ds", "unknown_db", "CAT", "SCH"),
        ],
    )

    index = lineage_context.model_lineage_index

    assert index is not None
    assert index.fact_field_urns(["fact-1"]) == [
        _field_urn(
            "urn:li:dataset:(urn:li:dataPlatform:databricks,cat.sch.orders,PROD)"
        )
    ]
    assert len(index.fact_field_urns(["fact-2"])) == 1
    assert source.report.model_tables_without_warehouse_context == 0


def test_unresolved_table_warning_is_raised_once_per_project(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    install_client(
        source,
        ModelTablesClient(
            [model_table("a", "fact-1", None), model_table("b", "fact-2", "ZZZ999")]
        ),
    )

    for project_id in ("project-1", "project-2"):
        assert (
            _LazyProjectLineage(source, project_id, mixed_datasources).model_tables
            is not None
        )

    warnings = [
        w
        for w in source.report.warnings
        if "Logical Tables Without Source Warehouse" in str(w.title)
    ]
    contexts = [c for w in warnings for c in w.context]
    assert len(contexts) == 2
    assert "project_id=project-1" in contexts[0]
    assert "project_id=project-2" in contexts[1]
    assert source.report.model_tables_without_warehouse_context == 4


def test_index_and_semantic_model_reads_count_each_table_once(
    mixed_datasources: List[Datasource],
) -> None:
    source = _source({"extract_model_lineage": True})
    install_client(
        source,
        ModelTablesClient(
            [model_table("a", "fact-1", "AAA111"), model_table("b", "fact-2", None)]
        ),
    )
    lineage_context = _LazyProjectLineage(source, "project-1", mixed_datasources)

    assert lineage_context.model_lineage_index is not None
    lineage_context.model_tables_retrying_failure()
    lineage_context.model_tables_retrying_failure()

    assert source.report.model_tables_resolved_by_datasource == 1
    assert source.report.model_tables_without_warehouse_context == 1


def test_model_tables_are_paged_until_the_reported_total() -> None:
    source = _source({"page_size": 2})
    client = ModelTablesClient(
        [model_table(f"t{i}", f"fact-{i}", None) for i in range(5)]
    )
    install_client(source, client)

    tables = source.fetch_project_model_tables("project-1")

    assert tables is not None
    assert len(tables) == 5
    assert client.offsets_requested == [0, 2, 4]


def test_connection_lookup_failure_is_cached_per_project_and_warned_again_for_the_next() -> (
    None
):
    source = _source({"extract_model_lineage": True})
    client = ConnectionModelTablesClient([], fail_connection=True)
    install_client(source, client)
    datasource = _warehouse("AAA111", "wh", "oracle", "DB", connection_id="conn-1")

    for project_id in ("project-1", "project-1", "project-2"):
        context = source.datasource_warehouse_context(project_id, datasource)
        assert context is not None
        assert context.database == "DB"

    assert client.connection_requests == ["conn-1", "conn-1"]
    assert source.report.warehouse_lineage_api_failures == 2


def test_failed_connection_lookup_without_a_database_skips_the_datasource() -> None:
    source = _source({"extract_model_lineage": True})
    install_client(source, ConnectionModelTablesClient([], fail_connection=True))
    datasource = _warehouse("AAA111", "wh", "snow_flake", connection_id="conn-1")

    assert source.datasource_warehouse_context("project-1", datasource) is None
    assert source.report.datasources_skipped_without_database == 1


class _PromptedReportClient:
    """A report whose SQL view is refused with 406 until its prompts are
    answered."""

    def __init__(self, prompts: List[ReportPrompt]) -> None:
        self.prompts = prompts
        self.answered: List[List[ReportPrompt]] = []
        self.deleted: List[str] = []

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        return "instance-1"

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        if not self.answered:
            raise MicroStrategyAPIError("406 Not Acceptable", status_code=406)
        return SqlView.model_validate({"sqlStatement": "select a from t"})

    def get_report_instance_prompts(
        self, project_id: str, report_id: str, instance_id: str
    ) -> List[ReportPrompt]:
        return self.prompts

    def answer_report_prompts_with_defaults(
        self,
        project_id: str,
        report_id: str,
        instance_id: str,
        prompts: List[ReportPrompt],
    ) -> None:
        self.answered.append(prompts)

    def delete_report_instance(
        self, project_id: str, report_id: str, instance_id: str
    ) -> bool:
        self.deleted.append(instance_id)
        return True


_DEFAULTED_PROMPT = ReportPrompt.model_validate(
    {
        "key": "P1",
        "id": "prompt-1",
        "type": "ELEMENTS",
        "required": True,
        "defaultAnswer": [{"id": "h1;elem", "name": "Elem"}],
    }
)


def test_prompted_report_is_skipped_without_the_flag() -> None:
    source = _source()
    client = _PromptedReportClient([_DEFAULTED_PROMPT])
    source.client = client  # type: ignore[assignment]

    outcome = source._report_sql_outcome("project-1", "report-1", "ctx")

    assert outcome.sql is None
    assert outcome.execution is None
    assert client.answered == []
    assert client.deleted == ["instance-1"]
    assert source.report.prompted_report_sql_views == 1
    assert source.report.prompted_reports_answered == 0


def test_prompted_report_is_read_after_answering_its_defaults() -> None:
    source = _source({"answer_report_prompts_with_defaults": True})
    client = _PromptedReportClient([_DEFAULTED_PROMPT])
    source.client = client  # type: ignore[assignment]

    outcome = source._report_sql_outcome("project-1", "report-1", "ctx")

    assert outcome.sql == "select a from t"
    assert client.answered == [[_DEFAULTED_PROMPT]]
    assert client.deleted == ["instance-1"]
    assert source.report.prompted_reports_answered == 1


def test_prompted_report_with_a_required_prompt_without_default_is_skipped() -> None:
    source = _source({"answer_report_prompts_with_defaults": True})
    client = _PromptedReportClient(
        [
            ReportPrompt.model_validate(
                {"key": "P1", "type": "VALUE", "required": True, "name": "Region"}
            )
        ]
    )
    source.client = client  # type: ignore[assignment]

    assert source._report_sql_outcome("project-1", "report-1", "ctx").sql is None
    assert client.answered == []
    assert client.deleted == ["instance-1"]
    assert source.report.prompted_reports_without_defaults == 1


def test_answering_defaults_sends_use_default_per_prompt() -> None:
    client = MicroStrategyClient(
        MicroStrategyConfig.model_validate(
            {"base_url": "https://mstr.example.com/MicroStrategyLibrary"}
        ),
        MicroStrategyReport(),
    )
    with mock.patch.object(client, "_request") as request:
        client.answer_report_prompts_with_defaults(
            "project-1", "report-1", "instance-1", [_DEFAULTED_PROMPT]
        )
    method, path = request.call_args.args[:2]
    assert (method, path) == (
        "PUT",
        "/api/reports/report-1/instances/instance-1/prompts/answers",
    )
    assert request.call_args.kwargs["json"] == {
        "prompts": [
            {"useDefault": True, "key": "P1", "id": "prompt-1", "type": "ELEMENTS"}
        ]
    }
