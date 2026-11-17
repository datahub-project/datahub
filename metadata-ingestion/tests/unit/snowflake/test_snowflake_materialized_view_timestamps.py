from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple
from unittest.mock import patch

import pytest

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SnowflakeShowKind
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import (
    SHOW_COMMAND_MAX_PAGE_SIZE,
    SHOW_STREAM_MAX_PAGE_SIZE,
    SnowflakeQuery,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OperationClass,
    OperationSourceTypeClass,
    OperationTypeClass,
    ViewPropertiesClass,
)
from tests.unit.snowflake.conftest import (
    QueryResult,
    as_mock,
    aspects,
    log_keys,
    make_source,
)

DB = "MY_DB"
SCHEMA = "MY_SCHEMA"
CREATED = datetime(2024, 1, 1, tzinfo=timezone.utc)
TABLE_LAST_ALTERED = datetime(2024, 3, 1, tzinfo=timezone.utc)
MV_CREATED_ON = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
VIEW_CREATED_ON = datetime(2024, 2, 1, 8, 0, tzinfo=timezone.utc)
REFRESHED_ON = datetime(2024, 3, 1, 6, 0, tzinfo=timezone.utc)
COMPACTED_ON = datetime(2024, 2, 20, 6, 0, tzinfo=timezone.utc)
RECREATED_ON = datetime(2024, 5, 1, tzinfo=timezone.utc)
LOWERCASING_WARNING = "URN lowercasing not applied"
LOOKUP_WARNING = (
    "Failed to get materialized view refresh times; "
    "materialized views keep their creation time"
)
CAP_WARNING = (
    "SHOW MATERIALIZED VIEWS returned its row limit; "
    "materialized views past it keep their creation time"
)
APPLY_WARNING = "Failed to apply materialized view refresh times"
FALLBACK_WARNING = "Database-wide SHOW failed; retrying per schema"


def _ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def _show_view_row(
    name: str,
    created_on: Optional[datetime],
    is_materialized: str,
    is_secure: str = "false",
    schema: str = SCHEMA,
) -> dict:
    kind = "materialized view" if is_materialized == "true" else "view"
    return {
        "schema_name": schema,
        "name": name,
        "created_on": created_on,
        "comment": None,
        "text": f"create {kind} {name} as select col_a from my_table",
        "is_secure": is_secure,
        "is_materialized": is_materialized,
    }


def _mv_row(
    refreshed_on: Optional[object],
    compacted_on: Optional[object],
    name: str = "MY_MV",
    schema: str = SCHEMA,
) -> dict:
    return {
        "schema_name": schema,
        "name": name,
        "created_on": MV_CREATED_ON,
        "refreshed_on": refreshed_on,
        "compacted_on": compacted_on,
        "behind_by": "0s",
        "invalid": "false",
    }


def _column_row(table_name: str) -> dict:
    return {
        "TABLE_NAME": table_name,
        "COLUMN_NAME": "COL_A",
        "ORDINAL_POSITION": 1,
        "IS_NULLABLE": "YES",
        "DATA_TYPE": "NUMBER",
        "COMMENT": None,
        "CHARACTER_MAXIMUM_LENGTH": None,
        "NUMERIC_PRECISION": 38,
        "NUMERIC_SCALE": 0,
    }


def _schema_rows(db: str, schemas: List[str]) -> Dict[str, QueryResult]:
    """Schemas of `db` that each hold MY_MV and MY_VIEW, and no tables or keys."""
    rows: Dict[str, QueryResult] = {
        SnowflakeQuery.schemas_for_database(db): [
            {
                "SCHEMA_NAME": schema,
                "CREATED": CREATED,
                "LAST_ALTERED": CREATED,
                "COMMENT": None,
            }
            for schema in schemas
        ]
    }
    for schema in schemas:
        rows[SnowflakeQuery.columns_for_schema(schema, db)] = [
            _column_row(name) for name in ("MY_MV", "MY_VIEW")
        ]
        rows[SnowflakeQuery.show_primary_keys_for_schema(schema, db)] = []
        rows[SnowflakeQuery.show_foreign_keys_for_schema(schema, db)] = []
    return rows


def _database_rows(db: str) -> Dict[str, QueryResult]:
    """The database-level queries of a database without tables or other objects."""
    return {
        SnowflakeQuery.get_databases(db): [
            {
                "DATABASE_NAME": db,
                "CREATED": CREATED,
                "LAST_ALTERED": CREATED,
                "COMMENT": None,
            }
        ],
        SnowflakeQuery.tables_for_database(
            db, table_types={"BASE TABLE", "EXTERNAL TABLE"}
        ): [],
        SnowflakeQuery.show_objects_for_database(
            SnowflakeShowKind.DYNAMIC_TABLES, db
        ): [],
        SnowflakeQuery.get_dynamic_table_graph_history(db): [],
        SnowflakeQuery.show_objects_for_database(
            SnowflakeShowKind.STREAMS, db, limit=SHOW_STREAM_MAX_PAGE_SIZE
        ): [],
        SnowflakeQuery.procedures_for_database(db): [],
    }


SHOW_VIEWS_ROWS = [
    _show_view_row("MY_MV", MV_CREATED_ON, is_materialized="true"),
    _show_view_row("MY_VIEW", VIEW_CREATED_ON, is_materialized="false"),
]

# One database on the default path: tables from information_schema, views from the
# database-wide SHOW VIEWS (fetch_views_from_information_schema=False).
QUERY_ROWS: Dict[str, QueryResult] = {
    SnowflakeQuery.show_databases(): [
        {"name": DB, "created_on": CREATED, "comment": None}
    ],
    **_database_rows(DB),
    SnowflakeQuery.schemas_for_database(DB): [
        {
            "SCHEMA_NAME": SCHEMA,
            "CREATED": CREATED,
            "LAST_ALTERED": CREATED,
            "COMMENT": None,
        }
    ],
    SnowflakeQuery.tables_for_database(
        DB, table_types={"BASE TABLE", "EXTERNAL TABLE"}
    ): [
        {
            "TABLE_SCHEMA": SCHEMA,
            "TABLE_NAME": "MY_TABLE",
            "TABLE_TYPE": "BASE TABLE",
            "CREATED": CREATED,
            "LAST_ALTERED": TABLE_LAST_ALTERED,
            "BYTES": 1024,
            "ROW_COUNT": 10,
            "COMMENT": None,
            "CLUSTERING_KEY": None,
        }
    ],
    SnowflakeQuery.show_external_tables(): [],
    SnowflakeQuery.show_objects_for_database(
        SnowflakeShowKind.VIEWS, DB
    ): SHOW_VIEWS_ROWS,
    SnowflakeQuery.columns_for_schema(SCHEMA, DB): [
        _column_row(name) for name in ("MY_TABLE", "MY_MV", "MY_VIEW")
    ],
    SnowflakeQuery.show_primary_keys_for_schema(SCHEMA, DB): [],
    SnowflakeQuery.show_foreign_keys_for_schema(SCHEMA, DB): [],
    # Turning operations on starts the usage extractor; an empty date range stops it there.
    SnowflakeQuery.get_access_history_date_range(): [],
}


def _show_mv_query(db: str = DB) -> str:
    return SnowflakeQuery.show_materialized_views_for_database(db)


def _run(
    extra: Dict[str, QueryResult], **config: object
) -> Tuple[SnowflakeV2Source, List[MetadataWorkUnit]]:
    source = make_source({**QUERY_ROWS, **extra}, **config)
    with patch.object(source, "_snowflake_clear_ocsp_cache"):
        workunits = list(source.get_workunits())
    return source, workunits


def _urn(
    source: SnowflakeV2Source, name: str, schema: str = SCHEMA, db: str = DB
) -> str:
    return source.identifiers.gen_dataset_urn(f"{db}.{schema}.{name}".lower())


def _last_modified(
    source: SnowflakeV2Source,
    workunits: Iterable[MetadataWorkUnit],
    name: str,
    schema: str = SCHEMA,
    db: str = DB,
) -> Optional[int]:
    props = aspects(workunits, DatasetPropertiesClass)[_urn(source, name, schema, db)]
    return props.lastModified.time if props.lastModified else None


def _new_warnings(source: SnowflakeV2Source) -> Set[str]:
    return log_keys(source.report.warnings) - {LOWERCASING_WARNING}


def _times_sent(source: SnowflakeV2Source, query: str) -> int:
    execute = as_mock(source.connection.native_connection()).cursor.return_value.execute
    return sum(call.args[0] == query for call in execute.call_args_list)


def test_lookup_statement() -> None:
    # Snowflake rejects LIMIT and FROM on this command, so the whole statement is pinned.
    assert _show_mv_query() == 'SHOW MATERIALIZED VIEWS IN DATABASE "MY_DB";'


@pytest.mark.parametrize(
    "refreshed_on, compacted_on, expected",
    [
        pytest.param(REFRESHED_ON, COMPACTED_ON, REFRESHED_ON, id="refresh_newest"),
        pytest.param(
            REFRESHED_ON,
            datetime(2024, 3, 5, 6, 0, tzinfo=timezone.utc),
            datetime(2024, 3, 5, 6, 0, tzinfo=timezone.utc),
            id="compaction_newest",
        ),
        pytest.param(None, None, MV_CREATED_ON, id="no_refresh_times"),
        pytest.param(
            datetime(2024, 1, 15, 10, 29, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 10, 29, tzinfo=timezone.utc),
            MV_CREATED_ON,
            id="refresh_older_than_creation",
        ),
        pytest.param(
            MV_CREATED_ON, MV_CREATED_ON, MV_CREATED_ON, id="refresh_at_creation"
        ),
    ],
)
def test_mv_last_modified_is_newest_change(
    refreshed_on: Optional[datetime],
    compacted_on: Optional[datetime],
    expected: datetime,
) -> None:
    source, workunits = _run({_show_mv_query(): [_mv_row(refreshed_on, compacted_on)]})

    assert not source.report.failures
    assert not _new_warnings(source)
    assert _last_modified(source, workunits, "my_mv") == _ms(expected)
    assert _last_modified(source, workunits, "my_view") == _ms(VIEW_CREATED_ON)
    mv_props = aspects(workunits, DatasetPropertiesClass)[_urn(source, "my_mv")]
    assert mv_props.created is not None
    assert mv_props.created.time == _ms(MV_CREATED_ON)
    assert aspects(workunits, ViewPropertiesClass)[_urn(source, "my_mv")].materialized


def test_no_mv_sends_no_refresh_lookup() -> None:
    plain_views = [
        _show_view_row("MY_VIEW", VIEW_CREATED_ON, is_materialized="false"),
        _show_view_row("MY_MV", MV_CREATED_ON, is_materialized="false"),
    ]
    source, _ = _run(
        {
            SnowflakeQuery.show_objects_for_database(
                SnowflakeShowKind.VIEWS, DB
            ): plain_views,
            _show_mv_query(): [_mv_row(REFRESHED_ON, None)],
        }
    )

    assert not source.report.failures
    assert not _new_warnings(source)
    assert _times_sent(source, _show_mv_query()) == 0


def test_technical_schema_off_sends_no_refresh_lookup() -> None:
    source, _ = _run(
        {_show_mv_query(): [_mv_row(REFRESHED_ON, None)]},
        include_technical_schema=False,
    )

    assert not source.report.failures
    assert _times_sent(source, _show_mv_query()) == 0


def test_mv_missing_from_lookup_keeps_created_on() -> None:
    source, workunits = _run(
        {_show_mv_query(): [_mv_row(REFRESHED_ON, None, name="OTHER_MV")]}
    )

    assert not _new_warnings(source)
    assert _last_modified(source, workunits, "my_mv") == _ms(MV_CREATED_ON)


def test_per_schema_path_uses_refresh_time() -> None:
    source, workunits = _run(
        {
            SnowflakeQuery.show_objects_for_database(
                SnowflakeShowKind.VIEWS, DB
            ): RuntimeError("database-wide SHOW VIEWS unavailable"),
            SnowflakeQuery.show_objects_for_schema(
                SnowflakeShowKind.VIEWS, DB, SCHEMA
            ): SHOW_VIEWS_ROWS,
            _show_mv_query(): [_mv_row(REFRESHED_ON, None)],
        }
    )

    assert not source.report.failures
    assert _new_warnings(source) == {FALLBACK_WARNING}
    assert _last_modified(source, workunits, "my_mv") == _ms(REFRESHED_ON)


def test_per_schema_path_looks_up_once_per_database() -> None:
    refreshed = {
        "S_A": datetime(2024, 3, 1, tzinfo=timezone.utc),
        "S_B": datetime(2024, 3, 2, tzinfo=timezone.utc),
        "S_C": datetime(2024, 3, 3, tzinfo=timezone.utc),
    }
    source, workunits = _run(
        {
            **_schema_rows(DB, list(refreshed)),
            SnowflakeQuery.show_objects_for_database(
                SnowflakeShowKind.VIEWS, DB
            ): RuntimeError("database-wide SHOW VIEWS unavailable"),
            **{
                SnowflakeQuery.show_objects_for_schema(
                    SnowflakeShowKind.VIEWS, DB, schema
                ): [
                    _show_view_row("MY_MV", MV_CREATED_ON, "true", schema=schema),
                    _show_view_row("MY_VIEW", VIEW_CREATED_ON, "false", schema=schema),
                ]
                for schema in refreshed
            },
            _show_mv_query(): [
                _mv_row(time, None, schema=schema) for schema, time in refreshed.items()
            ],
        }
    )

    assert not source.report.failures
    assert _new_warnings(source) == {FALLBACK_WARNING}
    assert _times_sent(source, _show_mv_query()) == 1
    for schema, time in refreshed.items():
        assert _last_modified(source, workunits, "my_mv", schema) == _ms(time)


def test_lookup_is_per_database() -> None:
    other_db, other_refreshed = "OTHER_DB", datetime(2024, 4, 1, tzinfo=timezone.utc)
    source, workunits = _run(
        {
            SnowflakeQuery.show_databases(): [
                {"name": name, "created_on": CREATED, "comment": None}
                for name in (DB, other_db)
            ],
            SnowflakeQuery.get_databases(DB): [
                {
                    "DATABASE_NAME": name,
                    "CREATED": CREATED,
                    "LAST_ALTERED": CREATED,
                    "COMMENT": None,
                }
                for name in (DB, other_db)
            ],
            **_database_rows(other_db),
            **_schema_rows(other_db, [SCHEMA]),
            SnowflakeQuery.show_objects_for_database(
                SnowflakeShowKind.VIEWS, other_db
            ): SHOW_VIEWS_ROWS,
            _show_mv_query(): [_mv_row(REFRESHED_ON, None)],
            _show_mv_query(other_db): [_mv_row(other_refreshed, None)],
        }
    )

    assert not source.report.failures
    assert not _new_warnings(source)
    assert _last_modified(source, workunits, "my_mv") == _ms(REFRESHED_ON)
    assert _last_modified(source, workunits, "my_mv", db=other_db) == _ms(
        other_refreshed
    )
    assert _times_sent(source, _show_mv_query()) == 1
    assert _times_sent(source, _show_mv_query(other_db)) == 1


def test_view_replaced_between_queries_keeps_created_on() -> None:
    # SHOW VIEWS lists the current objects; the lookup still describes the ones they replaced.
    source, workunits = _run(
        {
            SnowflakeQuery.show_objects_for_database(SnowflakeShowKind.VIEWS, DB): [
                _show_view_row("MY_MV", RECREATED_ON, is_materialized="true"),
                _show_view_row("MY_VIEW", VIEW_CREATED_ON, is_materialized="false"),
            ],
            _show_mv_query(): [
                _mv_row(REFRESHED_ON, None),
                _mv_row(REFRESHED_ON, None, name="MY_VIEW"),
            ],
        },
        include_operational_stats=True,
    )

    assert not _new_warnings(source)
    assert _last_modified(source, workunits, "my_mv") == _ms(RECREATED_ON)
    assert _last_modified(source, workunits, "my_view") == _ms(VIEW_CREATED_ON)
    assert _operations(source, workunits) == {}


def test_view_without_creation_time_gets_no_refresh_time() -> None:
    source, workunits = _run(
        {
            SnowflakeQuery.show_objects_for_database(SnowflakeShowKind.VIEWS, DB): [
                _show_view_row("MY_MV", None, is_materialized="true"),
                _show_view_row("MY_VIEW", VIEW_CREATED_ON, is_materialized="false"),
            ],
            _show_mv_query(): [_mv_row(REFRESHED_ON, None)],
        },
        include_operational_stats=True,
    )

    assert not _new_warnings(source)
    assert _last_modified(source, workunits, "my_mv") is None
    assert _operations(source, workunits) == {}


def test_secure_mv_without_definition_gets_refresh_time() -> None:
    secure_rows = [
        {
            **_show_view_row("MY_MV", MV_CREATED_ON, "true", is_secure="true"),
            "text": "",
        },
        _show_view_row("MY_VIEW", VIEW_CREATED_ON, is_materialized="false"),
    ]
    source, workunits = _run(
        {
            SnowflakeQuery.show_objects_for_database(
                SnowflakeShowKind.VIEWS, DB
            ): secure_rows,
            SnowflakeQuery.get_secure_view_definitions(): RuntimeError(
                "ACCOUNT_USAGE not readable"
            ),
            _show_mv_query(): [_mv_row(REFRESHED_ON, None)],
        }
    )

    assert not source.report.failures
    assert _last_modified(source, workunits, "my_mv") == _ms(REFRESHED_ON)


def _operations(
    source: SnowflakeV2Source, workunits: Iterable[MetadataWorkUnit]
) -> Dict[str, OperationClass]:
    return aspects(workunits, OperationClass)


def _operation_workunits(
    source: SnowflakeV2Source, workunits: Iterable[MetadataWorkUnit], name: str
) -> List[MetadataWorkUnit]:
    urn = _urn(source, name)
    return [
        wu
        for wu in workunits
        if wu.get_urn() == urn and wu.get_aspect_of_type(OperationClass) is not None
    ]


def test_mv_operation_at_refresh_time() -> None:
    source, workunits = _run(
        {_show_mv_query(): [_mv_row(REFRESHED_ON, COMPACTED_ON)]},
        include_operational_stats=True,
    )

    assert len(_operation_workunits(source, workunits, "my_mv")) == 1
    operation = _operations(source, workunits)[_urn(source, "my_mv")]
    assert operation.operationType == OperationTypeClass.UPDATE
    assert operation.sourceType == OperationSourceTypeClass.DATA_PLATFORM
    assert operation.actor is None
    assert operation.lastUpdatedTimestamp == _ms(REFRESHED_ON)
    assert operation.timestampMillis == operation.lastUpdatedTimestamp
    assert _urn(source, "my_view") not in _operations(source, workunits)


@pytest.mark.parametrize(
    "refreshed_on, operational_stats",
    [
        pytest.param(REFRESHED_ON, False, id="operational_stats_off"),
        pytest.param(None, True, id="no_refresh_times"),
        pytest.param(
            datetime(2024, 1, 15, 10, 29, tzinfo=timezone.utc),
            True,
            id="refresh_older_than_creation",
        ),
        pytest.param(MV_CREATED_ON, True, id="refresh_at_creation"),
    ],
)
def test_mv_operation_not_emitted(
    refreshed_on: Optional[datetime], operational_stats: bool
) -> None:
    source, workunits = _run(
        {_show_mv_query(): [_mv_row(refreshed_on, None)]},
        include_operational_stats=operational_stats,
    )

    assert _operations(source, workunits) == {}


def test_mv_operation_same_on_rerun() -> None:
    runs = []
    for _ in range(2):
        source, workunits = _run(
            {_show_mv_query(): [_mv_row(REFRESHED_ON, None)]},
            include_operational_stats=True,
        )
        (wu,) = _operation_workunits(source, workunits, "my_mv")
        runs.append((wu.id, wu.get_aspect_of_type(OperationClass)))

    assert runs[0] == runs[1]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(RuntimeError("SHOW MATERIALIZED VIEWS failed"), id="generic"),
        pytest.param(
            SnowflakePermissionError("Insufficient privileges"), id="permission"
        ),
    ],
)
def test_lookup_error_keeps_created_on(error: Exception) -> None:
    source, workunits = _run({_show_mv_query(): error})

    assert not source.report.failures
    assert _new_warnings(source) == {LOOKUP_WARNING}
    assert _last_modified(source, workunits, "my_mv") == _ms(MV_CREATED_ON)
    assert _last_modified(source, workunits, "my_view") == _ms(VIEW_CREATED_ON)


def test_one_unreadable_row_keeps_every_created_on() -> None:
    source, workunits = _run(
        {
            SnowflakeQuery.show_objects_for_database(
                SnowflakeShowKind.VIEWS, DB
            ): SHOW_VIEWS_ROWS
            + [
                _show_view_row(name, MV_CREATED_ON, "true")
                for name in ("BAD_MV", "LAST_MV")
            ],
            _show_mv_query(): [
                _mv_row(REFRESHED_ON, None),
                _mv_row("not a time", None, name="BAD_MV"),
                _mv_row(REFRESHED_ON, None, name="LAST_MV"),
            ],
        },
        include_operational_stats=True,
    )

    assert not source.report.failures
    assert _new_warnings(source) == {LOOKUP_WARNING}
    for name in ("my_mv", "bad_mv", "last_mv"):
        assert _last_modified(source, workunits, name) == _ms(MV_CREATED_ON)
    assert _operations(source, workunits) == {}


@pytest.mark.parametrize(
    "row_count, expected_warnings",
    [
        pytest.param(SHOW_COMMAND_MAX_PAGE_SIZE, {CAP_WARNING}, id="at_cap"),
        pytest.param(SHOW_COMMAND_MAX_PAGE_SIZE - 1, set(), id="below_cap"),
    ],
)
def test_lookup_result_cap(row_count: int, expected_warnings: Set[str]) -> None:
    rows = [_mv_row(REFRESHED_ON, None)] + [
        _mv_row(REFRESHED_ON, None, name=f"MV_{i:05d}") for i in range(row_count - 1)
    ]
    source, workunits = _run({_show_mv_query(): rows})

    assert not source.report.failures
    assert _new_warnings(source) == expected_warnings
    assert _last_modified(source, workunits, "my_mv") == _ms(REFRESHED_ON)


def test_helper_error_keeps_views() -> None:
    with patch.object(
        SnowflakeDataDictionary,
        "get_materialized_view_refresh_times",
        side_effect=RuntimeError("unexpected"),
    ):
        source, workunits = _run({})

    assert not source.report.failures
    assert _new_warnings(source) == {APPLY_WARNING}
    assert _last_modified(source, workunits, "my_mv") == _ms(MV_CREATED_ON)
    assert _last_modified(source, workunits, "my_view") == _ms(VIEW_CREATED_ON)
