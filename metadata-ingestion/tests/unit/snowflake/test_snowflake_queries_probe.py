"""`datahub recipe probe` for snowflake-queries.

The verdict tests compare `probe filter` with the predicate ingestion itself
applies to every object a query-log row names: SqlParsingAggregator drops a
temporary table, then asks SnowflakeQueriesExtractor.is_allowed_table.
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeConnectionError
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesExtractor,
    SnowflakeQueriesSourceConfig,
    SnowflakeQueriesSourceReport,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)

_CONNECTION = {
    "account_id": "my_account",
    "username": "my_user",
    "authentication_type": "KEY_PAIR_AUTHENTICATOR",
    "private_key": "my_private_key",
}


def _recipe(**filters: Any) -> Dict[str, object]:
    return {"connection": dict(_CONNECTION), **filters}


class _StubConnection:
    def __init__(self, rows: List[Dict[str, object]]) -> None:
        self.rows = rows
        self.issued: List[str] = []
        self.closed = False

    def query(self, sql: str) -> Iterator[Dict[str, object]]:
        self.issued.append(sql)
        return iter(self.rows)

    def close(self) -> None:
        self.closed = True


def _connect_with(
    monkeypatch: pytest.MonkeyPatch, connection: Optional[_StubConnection]
) -> List[SnowflakeConnectionConfig]:
    seen: List[SnowflakeConnectionConfig] = []

    def _get_connection(self: SnowflakeConnectionConfig) -> _StubConnection:
        seen.append(self)
        if connection is None:
            raise RuntimeError("login refused")
        return connection

    monkeypatch.setattr(SnowflakeConnectionConfig, "get_connection", _get_connection)
    return seen


def test_sql_runs_over_the_recipes_connection_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StubConnection([{"DATABASE_NAME": "ANALYTICS"}])
    seen = _connect_with(monkeypatch, connection)

    result = run_probe_method(
        "snowflake-queries",
        _recipe(),
        "sql",
        {"query": "SELECT database_name FROM snowflake.account_usage.databases"},
    )

    assert [config.account_id for config in seen] == ["my_account"]
    assert isinstance(result.result, dict)
    assert result.result["rows"] == [["ANALYTICS"]]
    assert connection.closed


def test_a_refused_connection_exits_3(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect_with(monkeypatch, None)

    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method(
            "snowflake-queries",
            _recipe(),
            "sql",
            {"query": "SELECT database_name FROM snowflake.account_usage.databases"},
        )
    assert "RuntimeError" in str(info.value)
    assert "login refused" not in str(info.value)


def test_the_query_log_itself_is_refused_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # query_history holds the SQL users ran, literals and all: the catalog
    # scope this source shares with `snowflake` leaves it out.
    connection = _StubConnection([])
    _connect_with(monkeypatch, connection)

    with pytest.raises(ProbeArgumentError):
        run_probe_method(
            "snowflake-queries",
            _recipe(),
            "sql",
            {"query": "SELECT query_text FROM snowflake.account_usage.query_history"},
        )
    assert not any("query_history" in sql.lower() for sql in connection.issued)


def _ingestion_keeps(
    recipe: Dict[str, object], database: str, schema: str, table: str
) -> bool:
    """What the aggregator decides for this object, through the extractor's
    own callbacks (SqlParsingAggregator.is_allowed_table)."""
    config = SnowflakeQueriesSourceConfig.model_validate(recipe)
    report = SnowflakeQueriesSourceReport()
    # Built uninitialised: __init__ opens the audit-log store and reads the
    # time window, and these two callbacks touch neither.
    extractor = SnowflakeQueriesExtractor.__new__(SnowflakeQueriesExtractor)
    extractor.config = config
    extractor.filters = SnowflakeFilter(
        filter_config=config, structured_reporter=report
    )
    # The standalone source never passes a discovered-tables list.
    extractor.discovered_tables = None
    name = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    ).get_dataset_identifier(table_name=table, schema_name=schema, db_name=database)
    return not extractor.is_temp_table(name) and extractor.is_allowed_table(name)


def _probe_verdict(
    recipe: Dict[str, object], kind: str, database: str, schema: str, table: str
) -> Tuple[bool, Optional[str]]:
    result = check_filters(
        source_type="snowflake-queries",
        config_dict=dict(recipe),
        kind=kind,
        parent_path=[database, schema],
        names=[table],
    )
    verdict = result.results[0]
    return verdict.included, verdict.excluded_by


_OBJECTS = [
    ("ANALYTICS", "PUBLIC", "ORDERS"),
    ("ANALYTICS", "PUBLIC", "CUSTOMERS"),
    ("ANALYTICS", "STAGING", "ORDERS"),
    ("ANALYTICS", "PUBLIC", "MODEL__DBT_TMP"),
    ("RAW", "FIVETRAN_SYNC_STAGING", "EVENTS"),
    ("OTHER_DB", "PUBLIC", "ORDERS"),
    ("SNOWFLAKE", "ACCOUNT_USAGE", "QUERY_HISTORY"),
]

_RECIPES = {
    "defaults": _recipe(),
    "database_pattern": _recipe(database_pattern={"allow": ["^ANALYTICS$"]}),
    "schema_pattern": _recipe(schema_pattern={"deny": ["^STAGING$"]}),
    "schema_pattern_qualified": _recipe(
        schema_pattern={"allow": [r"^ANALYTICS\.PUBLIC$"]},
        match_fully_qualified_names=True,
    ),
    "table_pattern": _recipe(table_pattern={"allow": [r"ANALYTICS\.PUBLIC\.ORD"]}),
    # Ingestion folds the identifier to lower case before matching, which only
    # shows once the pattern stops ignoring case.
    "case_sensitive_folded": _recipe(
        table_pattern={"allow": [r"analytics\.public\..*"], "ignoreCase": False}
    ),
    "case_sensitive_containers": _recipe(
        database_pattern={"allow": ["^analytics$"], "ignoreCase": False},
        schema_pattern={"allow": [r"^analytics\.public$"], "ignoreCase": False},
        match_fully_qualified_names=True,
    ),
    "case_sensitive_kept": _recipe(
        convert_urns_to_lowercase=False,
        table_pattern={"allow": [r"ANALYTICS\.PUBLIC\..*"], "ignoreCase": False},
    ),
    # Never read by this source: every object is judged as a table.
    "view_pattern_ignored": _recipe(view_pattern={"deny": [".*"]}),
    "no_temporary_tables": _recipe(temporary_tables_pattern=[]),
}


@pytest.mark.parametrize("kind", [DatasetSubTypes.TABLE, DatasetSubTypes.VIEW])
@pytest.mark.parametrize("recipe_name", sorted(_RECIPES))
def test_probe_filter_agrees_with_ingestion(recipe_name: str, kind: str) -> None:
    recipe = _RECIPES[recipe_name]
    disagreements = []
    for database, schema, table in _OBJECTS:
        expected = _ingestion_keeps(recipe, database, schema, table)
        included, excluded_by = _probe_verdict(recipe, kind, database, schema, table)
        if included != expected:
            disagreements.append((database, schema, table, expected, excluded_by))
    assert not disagreements
    # Vacuous agreement proves nothing: each recipe keeps something.
    assert any(_ingestion_keeps(recipe, *obj) for obj in _OBJECTS)


@pytest.mark.parametrize(
    "recipe, obj, excluded_by",
    [
        (
            _recipe(),
            ("ANALYTICS", "PUBLIC", "MODEL__DBT_TMP"),
            "temporary_tables_pattern",
        ),
        (
            _recipe(),
            ("SNOWFLAKE", "ACCOUNT_USAGE", "QUERY_HISTORY"),
            "database_pattern",
        ),
        (
            _recipe(schema_pattern={"deny": ["^STAGING$"]}),
            ("ANALYTICS", "STAGING", "ORDERS"),
            "schema_pattern",
        ),
        (
            _recipe(table_pattern={"deny": [r".*\.CUSTOMERS$"]}),
            ("ANALYTICS", "PUBLIC", "CUSTOMERS"),
            "table_pattern",
        ),
    ],
)
def test_the_reason_names_the_rule_that_dropped_it(
    recipe: Dict[str, object], obj: Tuple[str, str, str], excluded_by: str
) -> None:
    assert _probe_verdict(recipe, DatasetSubTypes.TABLE, *obj) == (False, excluded_by)


def test_a_view_is_judged_against_table_pattern_and_says_so() -> None:
    result = check_filters(
        source_type="snowflake-queries",
        config_dict=_recipe(
            table_pattern={"deny": [r".*\.V_ORDERS$"]}, view_pattern={"allow": [".*"]}
        ),
        kind=DatasetSubTypes.VIEW,
        parent_path=["ANALYTICS", "PUBLIC"],
        names=["V_ORDERS"],
    )
    assert result.results[0].excluded_by == "table_pattern"
    assert any("view_pattern has no effect" in w for w in result.warnings)


def test_without_the_database_a_table_is_judged_on_its_bare_name_with_a_warning() -> (
    None
):
    result = check_filters(
        source_type="snowflake-queries",
        config_dict=_recipe(),
        kind=DatasetSubTypes.TABLE,
        parent_path=["PUBLIC"],
        names=["ORDERS"],
    )
    assert result.results[0].target == "ORDERS"
    assert result.warnings
