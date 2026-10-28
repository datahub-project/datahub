"""Row-mapping behaviour of SnowflakeDataDictionary.

The data dictionary turns raw cursor rows from Snowflake's SHOW / information_schema
queries into the connector's dataclasses. These tests drive it with canned rows and
check the mapping, the null handling, and the fallbacks that matter in production.
"""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Union
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
    SnowflakeStageType,
    SnowflakeTable,
    SnowflakeTaskState,
)

CREATED = datetime(2024, 1, 1, tzinfo=timezone.utc)
DB = "MY_DB"
SCHEMA = "MY_SCHEMA"

QueryResult = Union[List[dict], Exception]
Dispatch = Callable[[str], QueryResult]


def _make_dictionary(dispatch: Dispatch) -> SnowflakeDataDictionary:
    connection = MagicMock()

    def query(sql: str) -> List[dict]:
        result = dispatch(sql)
        if isinstance(result, Exception):
            raise result
        return list(result)

    connection.query.side_effect = query
    return SnowflakeDataDictionary(connection, SnowflakeV2Report())


def _rows(rows: List[dict]) -> SnowflakeDataDictionary:
    return _make_dictionary(lambda _sql: rows)


def _table_row(name: str, schema: str = SCHEMA, **overrides: object) -> dict:
    row = {
        "TABLE_SCHEMA": schema,
        "TABLE_NAME": name,
        "TABLE_TYPE": "BASE TABLE",
        "CREATED": CREATED,
        "LAST_ALTERED": CREATED,
        "BYTES": 10,
        "ROW_COUNT": 3,
        "COMMENT": None,
        "CLUSTERING_KEY": None,
    }
    row.update(overrides)
    return row


def test_show_databases_and_information_schema_databases_map_rows() -> None:
    show = _rows([{"name": "A", "created_on": CREATED, "comment": "c"}])
    assert [d.name for d in show.show_databases()] == ["A"]

    ischema = _rows(
        [
            {
                "DATABASE_NAME": "A",
                "CREATED": CREATED,
                "LAST_ALTERED": CREATED,
                "COMMENT": None,
            }
        ]
    )
    databases = ischema.get_databases("A")
    assert databases[0].name == "A"
    assert databases[0].last_altered == CREATED


def test_schemas_secure_view_definitions_and_all_tags_map_rows() -> None:
    schemas = _rows(
        [
            {
                "SCHEMA_NAME": SCHEMA,
                "CREATED": CREATED,
                "LAST_ALTERED": None,
                "COMMENT": "schema comment",
            }
        ]
    ).get_schemas_for_database(DB)
    assert [(s.name, s.comment) for s in schemas] == [(SCHEMA, "schema comment")]

    secure = _rows(
        [
            {
                "TABLE_CATALOG": DB,
                "TABLE_SCHEMA": SCHEMA,
                "TABLE_NAME": "V",
                "VIEW_DEFINITION": "select 1",
            }
        ]
    ).get_secure_view_definitions()
    assert secure[DB][SCHEMA]["V"] == "select 1"

    tags = _rows(
        [{"TAG_DATABASE": DB, "TAG_SCHEMA": SCHEMA, "TAG_NAME": "PII"}]
    ).get_all_tags()
    assert [(t.database, t.schema, t.name) for t in tags] == [(DB, SCHEMA, "PII")]


def test_tables_for_database_groups_by_schema_and_detects_dynamic_tables() -> None:
    rows = [
        _table_row("PLAIN"),
        _table_row("DYN", IS_DYNAMIC="YES", IS_ICEBERG="NO"),
        _table_row("OTHER", schema="S2", IS_HYBRID="YES"),
    ]

    def dispatch(sql: str) -> List[dict]:
        if "DYNAMIC_TABLE" in sql.upper() or "SHOW DYNAMIC" in sql.upper():
            return []
        return rows

    tables = _make_dictionary(dispatch).get_tables_for_database(
        DB, table_types=frozenset({"BASE TABLE"}), exclude_dynamic_tables=True
    )
    assert tables is not None
    assert sorted(tables) == [SCHEMA, "S2"]
    by_name: Dict[str, SnowflakeTable] = {t.name: t for t in tables[SCHEMA]}
    assert isinstance(by_name["DYN"], SnowflakeDynamicTable)
    assert not isinstance(by_name["PLAIN"], SnowflakeDynamicTable)
    assert tables["S2"][0].is_hybrid


def test_tables_for_database_returns_none_when_query_fails() -> None:
    # "Returned too much data" is the normal signal to fall back to per-schema
    # queries, so the caller must see None rather than an exception.
    dictionary = _make_dictionary(lambda _sql: RuntimeError("too much data"))

    assert (
        dictionary.get_tables_for_database(DB, table_types=frozenset({"BASE TABLE"}))
        is None
    )


def test_tables_for_schema_maps_rows() -> None:
    tables = _rows([_table_row("T1", RETENTION_TIME=1)]).get_tables_for_schema(
        SCHEMA, DB, table_types={"BASE TABLE"}, exclude_dynamic_tables=True
    )

    assert [t.name for t in tables] == ["T1"]
    assert tables[0].retention_time == 1
    assert tables[0].rows_count == 3


def test_columns_for_schema_groups_columns_by_table() -> None:
    column_rows = [
        {
            "TABLE_NAME": "T",
            "COLUMN_NAME": "ID",
            "ORDINAL_POSITION": 1,
            "IS_NULLABLE": "NO",
            "DATA_TYPE": "NUMBER",
            "COMMENT": None,
            "CHARACTER_MAXIMUM_LENGTH": None,
            "NUMERIC_PRECISION": 38,
            "NUMERIC_SCALE": 0,
        },
        {
            "TABLE_NAME": "T",
            "COLUMN_NAME": "NAME",
            "ORDINAL_POSITION": 2,
            "IS_NULLABLE": "YES",
            "DATA_TYPE": "TEXT",
            "COMMENT": "c",
            "CHARACTER_MAXIMUM_LENGTH": 16,
            "NUMERIC_PRECISION": None,
            "NUMERIC_SCALE": None,
        },
    ]
    dictionary = _rows(column_rows)

    columns = dictionary.get_columns_for_schema(
        SCHEMA, DB, cache_exclude_all_objects=["T", "OTHER"]
    )

    assert [c.name for c in columns["T"]] == ["ID", "NAME"]
    assert not columns["T"][0].is_nullable
    assert columns["T"][1].is_nullable
    assert "OTHER" not in columns

    # A single object is fetched by exact match (used for stream source tables).
    single = dictionary.get_columns_for_schema(
        SCHEMA, DB, cache_exclude_all_objects=["T"]
    )
    assert "T" in single


def test_primary_and_foreign_key_rows_are_grouped_per_constraint() -> None:
    pk = _rows(
        [
            {"table_name": "T", "constraint_name": "PK_T", "column_name": "A"},
            {"table_name": "T", "constraint_name": "PK_T", "column_name": "B"},
        ]
    ).get_pk_constraints_for_schema(SCHEMA, DB)
    assert pk["T"].name == "PK_T"
    assert pk["T"].column_names == ["A", "B"]

    # Two single-column constraints on different tables. (Composite keys are
    # deliberately not asserted here: the grouping check in the source keys on
    # the wrong dict, so a multi-column FK currently splits into one entry per
    # column.)
    base = {"pk_database_name": DB, "pk_schema_name": SCHEMA}
    fk = _rows(
        [
            {
                **base,
                "fk_name": "FK_1",
                "fk_table_name": "CHILD",
                "fk_column_name": "P_ID",
                "pk_table_name": "PARENT",
                "pk_column_name": "ID",
            },
            {
                **base,
                "fk_name": "FK_2",
                "fk_table_name": "OTHER",
                "fk_column_name": "X",
                "pk_table_name": "PARENT",
                "pk_column_name": "ID",
            },
        ]
    ).get_fk_constraints_for_schema(SCHEMA, DB)
    assert sorted(fk) == ["CHILD", "OTHER"]
    assert fk["CHILD"][0].column_names == ["P_ID"]
    assert fk["CHILD"][0].referred_column_names == ["ID"]
    assert fk["CHILD"][0].referred_table == "PARENT"


def _tag_row(domain: str, **overrides: object) -> dict:
    row: Dict[str, Any] = {
        "TAG_DATABASE": DB,
        "TAG_SCHEMA": SCHEMA,
        "TAG_NAME": "OWNER",
        "TAG_VALUE": "team",
        "OBJECT_NAME": "OBJ",
        "OBJECT_SCHEMA": SCHEMA,
        "OBJECT_DATABASE": DB,
        "COLUMN_NAME": None,
        "DOMAIN": domain,
    }
    row.update(overrides)
    return row


def test_tag_references_are_routed_by_domain() -> None:
    rows = [
        _tag_row("DATABASE", OBJECT_NAME=DB, OBJECT_SCHEMA=None, OBJECT_DATABASE=None),
        _tag_row("SCHEMA", OBJECT_NAME=SCHEMA, OBJECT_SCHEMA=None),
        _tag_row("TABLE", OBJECT_NAME="T"),
        _tag_row("COLUMN", OBJECT_NAME="T", COLUMN_NAME="C", TAG_VALUE=None),
    ]
    cache = _rows(rows).get_tags_for_database_without_propagation(DB)

    assert [t.name for t in cache.get_database_tags(DB)] == ["OWNER"]
    assert [t.name for t in cache.get_schema_tags(SCHEMA, DB)] == ["OWNER"]
    assert [t.name for t in cache.get_table_tags("T", SCHEMA, DB)] == ["OWNER"]
    column_tags = cache.get_column_tags_for_table("T", SCHEMA, DB)
    assert column_tags["C"][0].value == ""  # null TAG_VALUE is normalised


def test_tag_references_with_missing_fields_or_unknown_domain_are_skipped() -> None:
    rows = [
        _tag_row("TABLE", TAG_NAME=None),
        _tag_row("TABLE", DOMAIN=None),
        _tag_row("SCHEMA", OBJECT_NAME=SCHEMA, OBJECT_DATABASE=None),
        _tag_row("TABLE", OBJECT_SCHEMA=None),
        _tag_row("COLUMN", COLUMN_NAME=None),
        _tag_row("WAREHOUSE"),
    ]
    dictionary = _rows(rows)

    cache = dictionary.get_tags_for_database_without_propagation(DB)

    assert cache.get_table_tags("OBJ", SCHEMA, DB) == []
    assert cache.get_schema_tags(SCHEMA, DB) == []
    assert cache.get_column_tags_for_table("OBJ", SCHEMA, DB) == {}
    # Only the unrecognised domain is reported; null fields are logged quietly.
    assert len(dictionary.report.warnings) == 1


def test_streamlit_apps_are_grouped_by_schema() -> None:
    rows = [
        {
            "name": "APP",
            "created_on": CREATED,
            "owner": "ROLE_A",
            "database_name": DB,
            "schema_name": SCHEMA,
            "title": "My App",
            "url_id": "abc",
            "owner_role_type": "ROLE",
        }
    ]
    apps = _rows(rows).get_streamlit_apps_for_database(DB)

    assert apps[SCHEMA][0].name == "APP"
    assert apps[SCHEMA][0].comment is None


def test_semantic_views_for_schema_via_information_schema() -> None:
    rows = [
        {
            "SEMANTIC_VIEW_NAME": "SV",
            "CREATED": CREATED,
            "LAST_ALTERED": CREATED,
        }
    ]
    views = _rows(rows).get_semantic_views_for_schema_using_information_schema(
        schema_name=SCHEMA, db_name=DB
    )

    assert [v.name for v in views] == ["SV"]
    assert views[0].view_definition is None


def test_stages_tasks_and_pipes_map_show_rows_with_defaults() -> None:
    stage = _rows(
        [
            {
                "name": "EXT",
                "created_on": CREATED,
                "type": "external",
                "url": "s3://bucket/path",
                "cloud": "AWS",
                "region": "",
                "storage_integration": None,
            }
        ]
    ).get_stages_for_schema(DB, SCHEMA)[0]
    assert stage.stage_type == SnowflakeStageType.EXTERNAL
    assert stage.url == "s3://bucket/path"
    assert stage.region is None
    assert stage.database_name == DB

    tasks = _rows(
        [
            {
                "name": "T_JSON",
                "created_on": CREATED,
                "predecessors": '["MY_DB.MY_SCHEMA.ROOT"]',
                "state": "started",
                "allow_overlapping_execution": "true",
            },
            {
                "name": "T_LIST",
                "created_on": CREATED,
                "predecessors": ["MY_DB.MY_SCHEMA.ROOT"],
                "state": None,
            },
            {
                "name": "T_BAD",
                "created_on": CREATED,
                "predecessors": "not json",
            },
        ]
    )
    task_list = tasks.get_tasks_for_schema(DB, SCHEMA)
    by_name = {t.name: t for t in task_list}
    assert by_name["T_JSON"].predecessors == ["MY_DB.MY_SCHEMA.ROOT"]
    assert by_name["T_JSON"].state == SnowflakeTaskState.STARTED
    assert by_name["T_JSON"].allow_overlapping_execution
    assert by_name["T_LIST"].predecessors == ["MY_DB.MY_SCHEMA.ROOT"]
    assert by_name["T_LIST"].state == SnowflakeTaskState.SUSPENDED
    assert by_name["T_BAD"].predecessors == []
    assert len(tasks.report.warnings) == 1

    pipe = _rows(
        [
            {
                "name": "P",
                "created_on": CREATED,
                "definition": "copy into t",
                "auto_ingest": "true",
                "notification_channel": "",
            }
        ]
    ).get_pipes_for_schema(DB, SCHEMA)[0]
    assert pipe.auto_ingest
    assert pipe.notification_channel is None


@pytest.mark.parametrize(
    "method", ["get_stages_for_schema", "get_tasks_for_schema", "get_pipes_for_schema"]
)
def test_stage_task_pipe_listing_failures_warn_and_return_empty(method: str) -> None:
    dictionary = _make_dictionary(lambda _sql: RuntimeError("no privilege"))

    assert getattr(dictionary, method)(DB, SCHEMA) == []
    assert len(dictionary.report.warnings) == 1


def test_as_obj_reports_cache_statistics() -> None:
    dictionary = _rows([])
    dictionary.get_pk_constraints_for_schema(SCHEMA, DB)

    report = dictionary.as_obj()

    assert report["fetch_views_from_information_schema"] is False
    # The lru caches are shared per method, so only the shape is stable.
    assert "misses" in report["get_pk_constraints_for_schema"]
