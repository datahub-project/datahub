"""SnowflakeSummarySource counts what a recipe would ingest.

The schema generator is stubbed (its enumeration is covered elsewhere); the
test checks that database filtering, per-schema object counts and the
query-history counts land in the report and that no metadata is emitted.
"""

from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeSchema,
    SnowflakeTable,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_summary import (
    SnowflakeSummaryConfig,
    SnowflakeSummarySource,
)

CREATED = datetime(2024, 1, 1, tzinfo=timezone.utc)


class _Cursor(list):
    @property
    def rowcount(self) -> int:
        return len(self)


def _db(name: str) -> SnowflakeDatabase:
    return SnowflakeDatabase(name=name, created=CREATED, comment=None)


def _schema(name: str) -> SnowflakeSchema:
    return SnowflakeSchema(name=name, created=CREATED, last_altered=None, comment=None)


def _table(name: str) -> SnowflakeTable:
    return SnowflakeTable(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
    )


def _view(name: str) -> SnowflakeView:
    return SnowflakeView(
        name=name, comment=None, created=CREATED, last_altered=None, view_definition=""
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
    native = MagicMock()
    native.cursor.return_value.execute.side_effect = lambda sql: _Cursor(
        [{"CNT": 7}] if "query_history" in sql else [{"CNT": 2}]
    )

    def fetch_schemas(db: SnowflakeDatabase, db_name: str) -> None:
        db.schemas = [_schema("S1"), _schema("S2")]

    def fetch_tables(
        schema: SnowflakeSchema, db_name: str, schema_name: str
    ) -> List[SnowflakeTable]:
        return [_table("T1"), _table("T2")] if schema_name == "S1" else []

    with patch("snowflake.connector.connect", return_value=native):
        source = SnowflakeSummarySource(PipelineContext(run_id="t"), config)

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_summary.SnowflakeSchemaGenerator"
    ) as generator_cls:
        generator = generator_cls.return_value
        generator.get_databases.return_value = [_db("MAIN"), _db("DENIED")]
        generator.fetch_schemas_for_database.side_effect = fetch_schemas
        generator.fetch_tables_for_schema.side_effect = fetch_tables
        generator.fetch_views_for_schema.return_value = [_view("V1")]
        workunits = list(source.get_workunits_internal())

    assert workunits == []
    report = source.get_report()
    assert list(report.filtered) == ["DENIED.*"]
    assert report.schema_counters == {"MAIN": 2}
    assert report.object_counters["MAIN"] == {"S1": 3, "S2": 1}
    assert report.num_snowflake_queries == 7
    assert report.num_snowflake_mutations == 2
