"""Unit tests for `ReportDatasource.from_raw`.

Covers the branches not reached by integration tests: skipping rows that the
PowerBI API returns without a usable `datasourceType` or `server`.
"""

from unittest.mock import MagicMock

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    ReportDatasource,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
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


def test_admin_resolver_report_datasources_is_empty() -> None:
    # The admin API has no /reports/{id}/datasources endpoint; the resolver
    # returns an empty list so admin_apis_only mode degrades gracefully.
    resolver = AdminAPIResolver.__new__(AdminAPIResolver)
    assert resolver.get_report_datasources(workspace=MagicMock(), report_id="r") == []
