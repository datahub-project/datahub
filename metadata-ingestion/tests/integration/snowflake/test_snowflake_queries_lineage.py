"""End-to-end lineage tests for SnowflakeQueriesSource with a mocked connection.

Each test injects one crafted audit log row and compares the emitted MCPs
against a golden file. Regenerate goldens with `pytest --update-golden-files`.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, cast
from unittest import mock

import pytest

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesSourceReport,
)
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import RowCountList

pytestmark = pytest.mark.integration_batch_1

# Time window for the audit log query. Must span the audit row's QUERY_START_TIME.
START_TIME = datetime(2026, 4, 30, 0, 0, 0, tzinfo=timezone.utc)
END_TIME = datetime(2026, 4, 30, 23, 59, 59, tzinfo=timezone.utc)


def _build_audit_row(
    *,
    query_id: str,
    query_text: str,
    direct_objects_accessed: list,
    objects_modified: list,
) -> dict:
    """Build one row in the shape returned by QueryLogQueryBuilder.build_enriched_query_log_query()."""
    return {
        "QUERY_ID": query_id,
        "QUERY_TEXT": query_text,
        "QUERY_START_TIME": datetime(2026, 4, 30, 11, 50, 0, tzinfo=timezone.utc),
        "QUERY_TYPE": "INSERT",
        "ROWS_INSERTED": 100,
        "ROWS_UPDATED": 0,
        "ROWS_DELETED": 0,
        "USER_NAME": "ETL_USER",
        "ROLE_NAME": "ETL_ROLE",
        "SESSION_ID": "session-001",
        "WAREHOUSE_NAME": "ETL_WH",
        "DATABASE_NAME": "PROD",
        "SCHEMA_NAME": "STAGE_DWH",
        "DEFAULT_DB": "PROD",
        "DEFAULT_SCHEMA": "STAGE_DWH",
        "ROOT_QUERY_ID": None,
        "QUERY_COUNT": 1,
        "QUERY_SECONDARY_FINGERPRINT": None,
        "QUERY_DURATION": 5000,
        "OBJECTS_MODIFIED": json.dumps(objects_modified),
        "DIRECT_OBJECTS_ACCESSED": json.dumps(direct_objects_accessed),
        "OBJECT_MODIFIED_BY_DDL": None,
    }


def _make_query_results_handler(
    audit_row: Dict[str, Any],
) -> Callable[[str], RowCountList]:
    """Return audit_row only for the access_history query; empty for everything else."""

    def _handler(query: str) -> RowCountList:
        if "snowflake.account_usage.access_history" in query:
            return RowCountList([audit_row])
        return RowCountList([])

    return _handler


def _run_pipeline(
    audit_row: Dict[str, Any],
    output_file: Path,
) -> Pipeline:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = _make_query_results_handler(audit_row)

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(
                    type="snowflake-queries",
                    config={
                        "connection": {
                            "account_id": "ABC12345.ap-south-1.aws",
                            "username": "TST_USR",
                            "password": "TST_PWD",
                        },
                        "window": {
                            "start_time": START_TIME.isoformat(),
                            "end_time": END_TIME.isoformat(),
                        },
                        "include_usage_statistics": False,
                        "include_query_usage_statistics": False,
                        "include_operations": False,
                    },
                ),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()
        return pipeline


def test_snowflake_queries_stream_upstream_multi_target_lineage(pytestconfig, tmp_path):
    """Stream upstream + multi-target INSERT ALL emits one PreparsedQuery per downstream."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"
    output_file = tmp_path / "snowflake_queries_stream_upstream_lineage.json"
    golden_file = (
        test_resources_dir / "snowflake_queries_stream_upstream_lineage_golden.json"
    )

    audit_row = _build_audit_row(
        query_id="stream-multi-target-001",
        query_text=(
            "INSERT ALL"
            " WHEN 1 = 1 THEN INTO stage_dwh.st_dw_customer_profile (a) VALUES (a)"
            " WHEN 1 = 1 THEN INTO raw_betler.st_dw_jsonschemavalidation_error (b) VALUES (b)"
            " SELECT a, b FROM raw_betler.s_dw_customer_profile_2_stage"
        ),
        direct_objects_accessed=[
            {
                "objectName": "PROD.RAW_BETLER.S_DW_CUSTOMER_PROFILE_2_STAGE",
                "objectDomain": "Stream",
                "columns": [{"columnName": "RECORD_CONTENT"}],
            }
        ],
        objects_modified=[
            {
                "objectName": "PROD.STAGE_DWH.ST_DW_CUSTOMER_PROFILE",
                "objectDomain": "Table",
                "columns": [
                    {
                        "columnName": "ADDRESS",
                        "directSources": [
                            {
                                "objectName": "PROD.RAW_BETLER.S_DW_CUSTOMER_PROFILE_2_STAGE",
                                "objectDomain": "Stream",
                                "columnName": "RECORD_CONTENT",
                            }
                        ],
                    }
                ],
            },
            {
                "objectName": "PROD.RAW_BETLER.ST_DW_JSONSCHEMAVALIDATION_ERROR",
                "objectDomain": "Table",
                "columns": [
                    {
                        "columnName": "ERROR_MESSAGE",
                        "directSources": [
                            {
                                "objectName": "PROD.RAW_BETLER.S_DW_CUSTOMER_PROFILE_2_STAGE",
                                "objectDomain": "Stream",
                                "columnName": "RECORD_CONTENT",
                            }
                        ],
                    }
                ],
            },
        ],
    )

    pipeline = _run_pipeline(audit_row, output_file)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            r"root\[\d+\]\['systemMetadata'\]",
        ],
    )

    report = cast(SnowflakeQueriesSourceReport, pipeline.source.get_report())
    assert report.queries_extractor is not None
    assert report.queries_extractor.num_stream_queries_clean_fast_path == 1
    assert report.queries_extractor.num_stream_queries_observed == 0


def test_snowflake_queries_multi_target_insert_all_table_upstream_lineage(
    pytestconfig, tmp_path
):
    """Multi-target INSERT ALL with a table upstream emits one PreparsedQuery per downstream."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"
    output_file = tmp_path / "snowflake_queries_multi_target_insert_all_lineage.json"
    golden_file = (
        test_resources_dir
        / "snowflake_queries_multi_target_insert_all_lineage_golden.json"
    )

    audit_row = _build_audit_row(
        query_id="table-multi-target-001",
        query_text=(
            "INSERT ALL"
            " WHEN amount > 100 THEN INTO stage_dwh.large_orders (id, amount) VALUES (id, amount)"
            " WHEN amount <= 100 THEN INTO stage_dwh.small_orders (id, amount) VALUES (id, amount)"
            " SELECT id, amount FROM raw_betler.orders_source"
        ),
        direct_objects_accessed=[
            {
                "objectName": "PROD.RAW_BETLER.ORDERS_SOURCE",
                "objectDomain": "Table",
                "columns": [
                    {"columnName": "ID"},
                    {"columnName": "AMOUNT"},
                ],
            }
        ],
        objects_modified=[
            {
                "objectName": "PROD.STAGE_DWH.LARGE_ORDERS",
                "objectDomain": "Table",
                "columns": [
                    {
                        "columnName": "ID",
                        "directSources": [
                            {
                                "objectName": "PROD.RAW_BETLER.ORDERS_SOURCE",
                                "objectDomain": "Table",
                                "columnName": "ID",
                            }
                        ],
                    },
                    {
                        "columnName": "AMOUNT",
                        "directSources": [
                            {
                                "objectName": "PROD.RAW_BETLER.ORDERS_SOURCE",
                                "objectDomain": "Table",
                                "columnName": "AMOUNT",
                            }
                        ],
                    },
                ],
            },
            {
                "objectName": "PROD.STAGE_DWH.SMALL_ORDERS",
                "objectDomain": "Table",
                "columns": [
                    {
                        "columnName": "ID",
                        "directSources": [
                            {
                                "objectName": "PROD.RAW_BETLER.ORDERS_SOURCE",
                                "objectDomain": "Table",
                                "columnName": "ID",
                            }
                        ],
                    },
                    {
                        "columnName": "AMOUNT",
                        "directSources": [
                            {
                                "objectName": "PROD.RAW_BETLER.ORDERS_SOURCE",
                                "objectDomain": "Table",
                                "columnName": "AMOUNT",
                            }
                        ],
                    },
                ],
            },
        ],
    )

    pipeline = _run_pipeline(audit_row, output_file)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            r"root\[\d+\]\['systemMetadata'\]",
        ],
    )

    report = cast(SnowflakeQueriesSourceReport, pipeline.source.get_report())
    assert report.queries_extractor is not None
    assert report.queries_extractor.num_stream_queries_clean_fast_path == 0
    assert report.queries_extractor.num_stream_queries_observed == 0
