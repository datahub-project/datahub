from typing import Tuple
from unittest.mock import MagicMock

from datahub.ingestion.source.flink.catalog import (
    FlinkCatalogExtractor,
    _map_flink_type,
)
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.report import FlinkSourceReport
from datahub.ingestion.source.flink.sql_gateway_client import FlinkColumnSchema


def _extractor() -> Tuple[FlinkCatalogExtractor, MagicMock, FlinkSourceReport]:
    config = FlinkSourceConfig.model_validate(
        {
            "connection": {
                "rest_api_url": "http://localhost:8081",
                "sql_gateway_url": "http://localhost:8083",
            },
            "include_catalog_metadata": True,
        }
    )
    sql_client = MagicMock()
    report = FlinkSourceReport()
    extractor = FlinkCatalogExtractor(
        config=config, sql_client=sql_client, report=report
    )
    return extractor, sql_client, report


class TestMapFlinkType:
    def test_strips_precision(self) -> None:
        assert _map_flink_type("VARCHAR(255)") == "VARCHAR"
        assert _map_flink_type("DECIMAL(10,2)") == "DECIMAL"
        assert _map_flink_type("TIMESTAMP(3)") == "TIMESTAMP"

    def test_maps_complex_types(self) -> None:
        assert _map_flink_type("ROW") == "STRUCT"
        assert _map_flink_type("TIMESTAMP_LTZ") == "TIMESTAMP"
        assert _map_flink_type("MULTISET") == "ARRAY"

    def test_unknown_type_passes_through(self) -> None:
        assert _map_flink_type("GEOMETRY") == "GEOMETRY"


class TestCatalogExtractorEdgeCases:
    def test_empty_catalog_emits_container_only(self) -> None:
        extractor, sql_client, report = _extractor()
        sql_client.get_catalogs.return_value = ["empty_catalog"]
        sql_client.get_databases.return_value = []

        wus = list(extractor.extract())
        assert report.catalogs_scanned == 1
        assert report.databases_scanned == 0
        assert report.tables_scanned == 0
        assert len(wus) > 0

    def test_table_with_no_columns(self) -> None:
        extractor, sql_client, report = _extractor()
        sql_client.get_catalogs.return_value = ["my_catalog"]
        sql_client.get_databases.return_value = ["my_db"]
        sql_client.get_tables.return_value = ["empty_table"]
        sql_client.get_table_schema.return_value = []

        wus = list(extractor.extract())
        assert report.tables_scanned == 1
        assert len(wus) > 0


class TestCatalogExtractorErrorRecovery:
    def test_get_catalogs_failure_returns_gracefully(self) -> None:
        extractor, sql_client, report = _extractor()
        sql_client.get_catalogs.side_effect = RuntimeError("Connection refused")

        assert list(extractor.extract()) == []
        assert report.catalogs_scanned == 0

    def test_table_failure_continues_to_next_table(self) -> None:
        extractor, sql_client, report = _extractor()
        sql_client.get_catalogs.return_value = ["my_catalog"]
        sql_client.get_databases.return_value = ["my_db"]
        sql_client.get_tables.return_value = ["good_table", "bad_table", "other_table"]

        def schema_side_effect(catalog: str, database: str, table: str) -> list:
            if table == "bad_table":
                raise RuntimeError("Permission denied")
            return [
                FlinkColumnSchema(name="id", type="INT", nullable=False, comment=None)
            ]

        sql_client.get_table_schema.side_effect = schema_side_effect

        list(extractor.extract())
        assert report.tables_scanned == 2
        assert report.catalogs_scanned == 1
        assert report.databases_scanned == 1
