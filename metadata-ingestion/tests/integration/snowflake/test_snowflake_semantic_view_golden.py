from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from unittest import mock

import pytest

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import RowCountList, default_query_results

pytestmark = pytest.mark.integration_batch_1

# Semantic views had no end-to-end coverage at all: no golden file contained one,
# no fixture created one, and every test asserted on a hand-picked slice. That is
# structurally why casing bugs kept surviving here -- a change to one producer
# could alter a part of the output nothing asserted on.
#
# The fixture in common.py is deliberately awkward: a mixed-case logical table
# ("Orders"), a case-only column pair on it, a primary key on a quoted column, a
# join key whose dimension is spelled differently, and a derived metric that
# references another by its quoted name. Recorded with preserve_column_case on,
# because with it off those shapes collapse and the golden pins nothing new.


def _config(**overrides: Any) -> SnowflakeV2Config:
    config = SnowflakeV2Config(
        account_id="ABC12345.ap-south-1.aws",
        username="TST_USR",
        password="TST_PWD",  # type: ignore[arg-type]
        match_fully_qualified_names=True,
        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
        include_technical_schema=True,
        include_table_lineage=True,
        include_usage_stats=False,
        include_operational_stats=False,
        use_queries_v2=False,
        preserve_column_case=True,
        start_time=datetime(2022, 6, 6).replace(tzinfo=timezone.utc),
        end_time=datetime(2022, 6, 7, 7, 17).replace(tzinfo=timezone.utc),
        **overrides,
    )
    config.semantic_views.enabled = True
    config.semantic_views.emit_semantic_model_entities = True
    config.semantic_views.column_lineage = True
    return config


def _run_pipeline(
    config: SnowflakeV2Config,
    output_file: Path,
    query_results: Callable[[str], Any] = default_query_results,
) -> None:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=config),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.raise_from_status()


_IGNORE_PATHS = [
    r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
]


def test_snowflake_semantic_view_golden(pytestconfig: Any, tmp_path: Path) -> None:
    output_file = tmp_path / "snowflake_semantic_view_events.json"
    golden_file = (
        pytestconfig.rootpath
        / "tests/integration/snowflake/snowflake_semantic_view_golden.json"
    )

    _run_pipeline(_config(), output_file)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
        ignore_paths=_IGNORE_PATHS,
    )


# The same view with an ai_verified_queries block, shaped like GET_DDL renders
# it: one query against the logical table (`__Orders`, the Cortex Analyst
# convention) with a doubled-quote escape and every optional field, one against
# a fully-qualified physical table inside a CTE, and one with only SQL.
_DDL_WITH_VERIFIED_QUERIES = """create or replace semantic view SALES_MODEL
\ttables (
\t\t"Orders" as TEST_DB.TEST_SCHEMA.TABLE_1 primary key ("Order_Id"),
\t\tCUSTOMERS as TEST_DB.TEST_SCHEMA.TABLE_2 primary key (CUSTOMER_ID)
\t)
\tcomment='Sales semantic view'
\tai_verified_queries (
\t\tTOTAL_AMOUNT_BY_ORDER AS (
QUESTION 'What is the total amount per order, excluding ''void'' orders?'
VERIFIED_BY 'Sample Analyst'
VERIFIED_AT 1714497970
ONBOARDING_QUESTION true
SQL 'SELECT o."Order_Id", SUM(o.AMOUNT) FROM __Orders o WHERE o."Order_Id" <> ''void'' GROUP BY 1'),
\t\tCUSTOMER_COUNT AS (
QUESTION 'How many customers do we have?'
VERIFIED_BY 'Sample Analyst'
ONBOARDING_QUESTION false
SQL 'WITH c AS (SELECT CUSTOMER_ID FROM TEST_DB.TEST_SCHEMA.TABLE_2) SELECT COUNT(*) FROM c'),
\t\tSQL_ONLY AS (
SQL 'SELECT 1')
\t);"""


class _FetchableRows(RowCountList):
    # The GET_DDL reader calls cursor.fetchone(), which the shared fixture's rows
    # lack -- why no other golden carries a semantic view definition.
    def fetchone(self) -> Optional[Dict[str, str]]:
        return self[0] if self else None


def _query_results_with_verified_queries(query: str) -> Any:
    if query == SnowflakeQuery.get_semantic_view_ddl(
        "TEST_DB", "TEST_SCHEMA", "SALES_MODEL"
    ):
        return _FetchableRows([{"DDL": _DDL_WITH_VERIFIED_QUERIES}])
    return default_query_results(query)


def test_snowflake_semantic_view_verified_queries_golden(
    pytestconfig: Any, tmp_path: Path
) -> None:
    output_file = tmp_path / "snowflake_semantic_view_verified_queries_events.json"
    golden_file = (
        pytestconfig.rootpath
        / "tests/integration/snowflake/snowflake_semantic_view_verified_queries_golden.json"
    )

    config = _config()
    config.semantic_views.include_verified_queries = True
    _run_pipeline(config, output_file, _query_results_with_verified_queries)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
        ignore_paths=_IGNORE_PATHS,
    )
