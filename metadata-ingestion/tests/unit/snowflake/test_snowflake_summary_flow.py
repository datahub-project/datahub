"""SnowflakeSummarySource counts what a recipe would ingest.

The schema generator is stubbed (its enumeration is covered elsewhere); the
test checks that database filtering, per-schema object counts and the
query-history counts land in the report and that no metadata is emitted.
"""

from datetime import datetime, timezone
from typing import List
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


def test_summary_counts_objects_per_schema_and_queries_in_window() -> None:
    config = SnowflakeSummaryConfig(
        account_id="acct",
        username="u",
        password="p",
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
        database_pattern={"deny": ["DENIED"]},  # type: ignore[arg-type]
    )
    native = fake_native_connection(
        lambda sql: FakeCursor([{"CNT": 7}] if "query_history" in sql else [{"CNT": 2}])
    )

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
