"""SnowflakeSummarySource counts what a recipe would ingest.

The schema generator is stubbed (its enumeration is covered elsewhere); the
test checks that database filtering, per-schema object counts and the
query-history counts land in the report and that no metadata is emitted.
"""

import re
from datetime import datetime, timezone
from typing import List, Set
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeSchema,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_summary import (
    SnowflakeSummaryConfig,
    SnowflakeSummarySource,
)
from tests.unit.snowflake.conftest import (
    FakeCursor,
    database,
    fake_native_connection,
    schema,
    table,
    view,
)


def _window_bounds_ms(sql: str) -> Set[int]:
    return {
        int(float(ms)) for ms in re.findall(r"to_timestamp_ltz\(([\d.]+), 3\)", sql)
    }


def test_summary_counts_objects_per_schema_and_queries_in_window() -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    config = SnowflakeSummaryConfig(
        account_id="acct",
        username="u",
        password="p",
        start_time=start,
        end_time=end,
        database_pattern={"deny": ["DENIED"]},  # type: ignore[arg-type]
    )
    window_ms = {int(start.timestamp() * 1000), int(end.timestamp() * 1000)}

    def execute(sql: str) -> FakeCursor:
        # Both counts must be bounded by the configured window; any other query
        # shape is unexpected here.
        if "account_usage.query_history" in sql:
            assert _window_bounds_ms(sql) == window_ms
            return FakeCursor([{"CNT": 7}])
        if "account_usage.access_history" in sql:
            assert _window_bounds_ms(sql) == window_ms
            return FakeCursor([{"CNT": 2}])
        raise AssertionError(f"Unexpected query: {sql[:80]}")

    native = fake_native_connection(execute)

    def fetch_schemas(db: SnowflakeDatabase, db_name: str) -> None:
        db.schemas = [schema("S1"), schema("S2")]

    def fetch_tables(
        snowflake_schema: SnowflakeSchema, db_name: str, schema_name: str
    ) -> List[SnowflakeTable]:
        return [table("T1"), table("T2")] if schema_name == "S1" else []

    with patch("snowflake.connector.connect", return_value=native):
        source = SnowflakeSummarySource(PipelineContext(run_id="t"), config)

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_summary.SnowflakeSchemaGenerator",
        autospec=True,
    ) as generator_cls:
        generator = generator_cls.return_value
        generator.get_databases.return_value = [database("MAIN"), database("DENIED")]
        generator.fetch_schemas_for_database.side_effect = fetch_schemas
        generator.fetch_tables_for_schema.side_effect = fetch_tables
        generator.fetch_views_for_schema.return_value = [view("V1")]
        workunits = list(source.get_workunits_internal())

    assert workunits == []
    report = source.get_report()
    assert list(report.filtered) == ["DENIED.*"]
    assert report.schema_counters == {"MAIN": 2}
    assert report.object_counters["MAIN"] == {"S1": 3, "S2": 1}
    assert report.num_snowflake_queries == 7
    assert report.num_snowflake_mutations == 2
