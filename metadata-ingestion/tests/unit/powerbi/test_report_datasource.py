"""Unit tests for `ReportDatasource.from_raw`.

Covers the branches not reached by integration tests: skipping rows that the
PowerBI API returns without a usable `datasourceType` or `server`.
"""

from unittest.mock import MagicMock

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    AnalysisServicesModel,
    Page,
    PowerBIDataset,
    Report,
    ReportDatasource,
    ReportType,
    Table,
    Workspace,
    analysis_services_models_from_scan,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    StatusClass,
    SubTypesClass,
)


def test_from_raw_happy_path() -> None:
    ds = ReportDatasource.from_raw(
        {
            "datasourceType": "Sql",
            "connectionDetails": {
                "server": "sales-sqlserver.internal",
                "database": "MarketingDB",
            },
        }
    )
    assert ds == ReportDatasource(
        datasource_type="Sql",
        server="sales-sqlserver.internal",
        database="MarketingDB",
    )


def test_from_raw_returns_none_when_datasource_type_missing() -> None:
    assert (
        ReportDatasource.from_raw(
            {"connectionDetails": {"server": "host", "database": "db"}}
        )
        is None
    )


def test_from_raw_returns_none_when_server_missing() -> None:
    assert (
        ReportDatasource.from_raw(
            {"datasourceType": "Sql", "connectionDetails": {"database": "db"}}
        )
        is None
    )


def test_from_raw_allows_missing_database() -> None:
    ds = ReportDatasource.from_raw(
        {"datasourceType": "Sql", "connectionDetails": {"server": "host"}}
    )
    assert ds is not None
    assert ds.database is None


def test_from_raw_handles_missing_connection_details() -> None:
    assert ReportDatasource.from_raw({"datasourceType": "Sql"}) is None


def test_powerbi_dataset_id_for_pbi_dataset_binding() -> None:
    ds = ReportDatasource.from_raw(
        {
            "datasourceType": "AnalysisServices",
            "connectionDetails": {
                "server": "pbiazure://api.powerbi.com/",
                "database": "sobe_wowvirtualserver-a8149dea-aa76-44e7-b6a2-cb888b71443f",
            },
        }
    )
    assert ds is not None
    assert ds.powerbi_dataset_id == "a8149dea-aa76-44e7-b6a2-cb888b71443f"


def test_powerbi_dataset_id_none_for_external_datasource() -> None:
    ds = ReportDatasource.from_raw(
        {
            "datasourceType": "Sql",
            "connectionDetails": {"server": "host", "database": "db"},
        }
    )
    assert ds is not None
    assert ds.powerbi_dataset_id is None


def test_powerbi_dataset_id_none_for_analysisservices_without_pbi_catalog() -> None:
    # On-prem AS server, not a Power BI dataset binding.
    ds = ReportDatasource.from_raw(
        {
            "datasourceType": "AnalysisServices",
            "connectionDetails": {"server": "on-prem-as", "database": "AdventureWorks"},
        }
    )
    assert ds is not None
    assert ds.powerbi_dataset_id is None


def test_analysis_services_models_from_scan() -> None:
    scan = {
        "datasources": [
            {
                "datasourceType": "AnalysisServices",
                "datasourceId": "ds-aas",
                "connectionDetails": {
                    "server": "asazure://northeurope.asazure.windows.net/finance-server",
                    "database": "finance-model",
                },
            },
            {
                "datasourceType": "AnalysisServices",
                "datasourceId": "ds-pbi",
                "connectionDetails": {
                    "server": "pbiazure://api.powerbi.com/",
                    "database": "sobe_wowvirtualserver-a8149dea-aa76-44e7-b6a2-cb888b71443f",
                },
            },
            {
                "datasourceType": "Sql",
                "datasourceId": "ds-sql",
                "connectionDetails": {"server": "sql.internal", "database": "sales"},
            },
        ]
    }
    dataset = {
        "datasourceUsages": [
            {"datasourceInstanceId": "ds-aas"},
            {"datasourceInstanceId": "ds-pbi"},
            {"datasourceInstanceId": "ds-sql"},
        ]
    }
    assert analysis_services_models_from_scan(scan, dataset) == [
        AnalysisServicesModel(
            server="asazure://northeurope.asazure.windows.net/finance-server",
            database="finance-model",
            datasource_type="AnalysisServices",
        )
    ]


def test_analysis_services_models_from_table_usages_and_initial_catalog() -> None:
    scan = {
        "datasources": [
            {
                "datasourceType": "AnalysisServices",
                "datasourceId": "ds-aas",
                "connectionDetails": {
                    "server": "asazure://northeurope.asazure.windows.net/finance-server",
                    "initialCatalog": "finance-model",
                },
            }
        ]
    }
    dataset = {
        "tables": [
            {"name": "Sales", "datasourceUsages": [{"datasourceInstanceId": "ds-aas"}]}
        ]
    }
    assert analysis_services_models_from_scan(scan, dataset) == [
        AnalysisServicesModel(
            server="asazure://northeurope.asazure.windows.net/finance-server",
            database="finance-model",
            datasource_type="AnalysisServices",
        )
    ]


def _finance_model() -> AnalysisServicesModel:
    return AnalysisServicesModel(
        server="asazure://northeurope.asazure.windows.net/finance-server",
        database="finance-model",
        datasource_type="AnalysisServices",
    )


def _finance_mapper(*, graph: object) -> Mapper:
    config = PowerBiDashboardSourceConfig.parse_obj(
        {
            "client_id": "foo",
            "client_secret": "bar",
            "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
            "convert_lineage_urns_to_lowercase": False,
        }
    )
    ctx = MagicMock()
    ctx.graph = graph
    return Mapper(
        ctx=ctx,
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )


def _finance_table() -> Table:
    dataset = PowerBIDataset(
        id="dataset-1",
        name="Finance",
        description="",
        webUrl=None,
        workspace_id="workspace-1",
        workspace_name="Finance",
        parameters={},
        tables=[],
        tags=[],
        analysis_services_models=[_finance_model()],
    )
    return Table(name="Sales", full_name="Finance.Sales", dataset=dataset)


def test_extract_lineage_adds_analysis_services_model() -> None:
    mapper = _finance_mapper(graph=None)
    mcps = mapper.extract_lineage(
        _finance_table(), "urn:li:dataset:downstream", MagicMock()
    )
    assert len(mcps) == 1
    upstreams = mcps[0].aspect.upstreams
    assert [upstream.dataset for upstream in upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:azure-analysis-services,"
        "finance-model,PROD)"
    ]


def test_missing_analysis_services_model_is_hidden_dataset() -> None:
    mapper = _finance_mapper(graph=None)
    mapper.extract_lineage(_finance_table(), "urn:li:dataset:downstream", MagicMock())
    workunits = list(mapper.hidden_analysis_services_model_workunits())
    assert workunits
    assert all(workunit.is_primary_source is False for workunit in workunits)
    aspects = [workunit.metadata.aspect for workunit in workunits]
    assert StatusClass(removed=True) in aspects
    assert (
        DatasetPropertiesClass(
            name="finance-model",
            customProperties={
                "server": "asazure://northeurope.asazure.windows.net/finance-server",
                "datasourceType": "AnalysisServices",
            },
        )
        in aspects
    )
    assert SubTypesClass(
        typeNames=[DatasetSubTypes.CUBE, DatasetSubTypes.SEMANTIC_MODEL]
    ) in aspects


def test_existing_analysis_services_model_is_not_replaced() -> None:
    class _Graph:
        def exists(self, urn: str) -> bool:
            return True

        def get_aspect(self, urn: str, aspect_type: object) -> StatusClass:
            return StatusClass(removed=False)

    mapper = _finance_mapper(graph=_Graph())
    mapper.extract_lineage(_finance_table(), "urn:li:dataset:downstream", MagicMock())
    assert list(mapper.hidden_analysis_services_model_workunits()) == []


def test_hidden_analysis_services_model_is_refreshed_with_connection_details() -> None:
    class _Graph:
        def exists(self, urn: str) -> bool:
            return True

        def get_aspect(self, urn: str, aspect_type: object) -> StatusClass:
            return StatusClass(removed=True)

    mapper = _finance_mapper(graph=_Graph())
    mapper.extract_lineage(_finance_table(), "urn:li:dataset:downstream", MagicMock())
    aspects = [
        workunit.metadata.aspect
        for workunit in mapper.hidden_analysis_services_model_workunits()
    ]
    assert (
        DatasetPropertiesClass(
            name="finance-model",
            customProperties={
                "server": "asazure://northeurope.asazure.windows.net/finance-server",
                "datasourceType": "AnalysisServices",
            },
        )
        in aspects
    )
    assert SubTypesClass(
        typeNames=[DatasetSubTypes.CUBE, DatasetSubTypes.SEMANTIC_MODEL]
    ) in aspects


def test_live_connection_without_tables_links_the_report_to_the_model() -> None:
    mapper = _finance_mapper(graph=None)
    dataset = PowerBIDataset(
        id="dataset-1",
        name="Finance",
        description="",
        webUrl=None,
        workspace_id="workspace-1",
        workspace_name="Finance",
        parameters={},
        tables=[],
        tags=[],
        analysis_services_models=[_finance_model()],
    )
    report = Report(
        id="report-1",
        name="Finance",
        type=ReportType.PowerBIReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=dataset.id,
        dataset=dataset,
        pages=[Page(id="page-1", displayName="Overview", name="Overview", order=0)],
        users=[],
        tags=[],
    )
    workspace = Workspace(
        id="workspace-1",
        name="Finance",
        type="Workspace",
        dashboards={},
        reports={},
        datasets={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )
    model_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:azure-analysis-services,"
        "finance-model,PROD)"
    )
    workunits = list(mapper.report_to_datahub_work_units(report, workspace))
    dashboards = [
        workunit.metadata.aspect
        for workunit in workunits
        if isinstance(workunit.metadata.aspect, DashboardInfoClass)
    ]
    charts = [
        workunit.metadata.aspect
        for workunit in workunits
        if isinstance(workunit.metadata.aspect, ChartInfoClass)
    ]
    assert [edge.destinationUrn for edge in dashboards[0].datasetEdges] == [model_urn]
    assert charts[0].inputs == [model_urn]
    hidden = list(mapper.hidden_analysis_services_model_workunits())
    assert StatusClass(removed=True) in [
        workunit.metadata.aspect for workunit in hidden
    ]


def test_admin_resolver_report_datasources_is_empty() -> None:
    # The admin API has no /reports/{id}/datasources endpoint; the resolver
    # returns an empty list so admin_apis_only mode degrades gracefully.
    resolver = AdminAPIResolver.__new__(AdminAPIResolver)
    assert resolver.get_report_datasources(workspace=MagicMock(), report_id="r") == []


def test_admin_resolver_reads_dataset_datasources_from_admin_endpoint() -> None:
    resolver = AdminAPIResolver.__new__(AdminAPIResolver)
    resolver._admin_base_url = "https://api.powerbi.com/v1.0/myorg/admin"
    resolver.get_authorization_header = MagicMock(return_value={})  # type: ignore[method-assign]
    session = MagicMock()
    session.get.return_value.json.return_value = {
        "value": [{"datasourceType": "AnalysisServices"}]
    }
    resolver._request_session = session

    rows = resolver.get_dataset_datasources(workspace_id="w", dataset_id="d")

    assert rows == [{"datasourceType": "AnalysisServices"}]
    assert (
        session.get.call_args.args[0]
        == "https://api.powerbi.com/v1.0/myorg/admin/datasets/d/datasources"
    )
