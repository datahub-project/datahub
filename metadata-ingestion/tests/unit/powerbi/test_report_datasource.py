"""Unit tests for `ReportDatasource.from_raw`.

Covers the branches not reached by integration tests: skipping rows that the
PowerBI API returns without a usable `datasourceType` or `server`.
"""

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    ReportDatasource,
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
