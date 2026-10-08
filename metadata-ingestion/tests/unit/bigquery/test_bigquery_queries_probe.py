"""`datahub recipe probe` for bigquery-queries.

The verdict tests compare `probe filter` with the predicate ingestion itself
applies to every table a query names: SqlParsingAggregator drops a temporary
table, then asks BigQueryQueriesExtractor.is_allowed_table.
"""

from typing import Any, Dict, List, Optional, Tuple

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeConnectionError
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
)
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_queries import (
    BigQueryQueriesSourceConfig,
    BigQueryQueriesSourceReport,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    BigQueryQueriesExtractor,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


class _Field:
    def __init__(self, name: str) -> None:
        self.name = name


class _RowIterator:
    def __init__(self, rows: List[Dict[str, object]]) -> None:
        self.schema = [_Field(name) for name in (rows[0] if rows else {})]
        self._rows = rows

    def __iter__(self) -> Any:
        return iter(self._rows)


class _Job:
    def __init__(self, rows: List[Dict[str, object]]) -> None:
        self._rows = rows

    # Mirrors google.cloud.bigquery.QueryJob.result's keywords.
    def result(
        self, max_results: Optional[int] = None, timeout: Optional[float] = None
    ) -> _RowIterator:
        return _RowIterator(self._rows)


class _StubClient:
    def __init__(self, rows: List[Dict[str, object]]) -> None:
        self.rows = rows
        self.queries: List[str] = []
        self.closed = False

    def query(self, query: str, job_config: object = None) -> _Job:
        self.queries.append(query)
        return _Job(self.rows)

    def close(self) -> None:
        self.closed = True


def _connect_with(
    monkeypatch: pytest.MonkeyPatch, client: Optional[_StubClient]
) -> List[BigQueryConnectionConfig]:
    seen: List[BigQueryConnectionConfig] = []

    def _get_client(self: BigQueryConnectionConfig) -> _StubClient:
        seen.append(self)
        if client is None:
            raise RuntimeError("credentials refused")
        return client

    monkeypatch.setattr(BigQueryConnectionConfig, "get_bigquery_client", _get_client)
    return seen


@pytest.fixture(autouse=True)
def _restore_shard_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        BigqueryTableIdentifier,
        "_BQ_SHARDED_TABLE_SUFFIX",
        BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX,
    )


_RECIPE: Dict[str, object] = {"connection": {"project_on_behalf": "billing-project"}}
_SQL = "SELECT table_name FROM sales.INFORMATION_SCHEMA.TABLES"


def test_sql_runs_over_the_recipes_connection_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _StubClient([{"table_name": "orders"}])
    seen = _connect_with(monkeypatch, client)

    result = run_probe_method("bigquery-queries", dict(_RECIPE), "sql", {"query": _SQL})

    assert [config.project_on_behalf for config in seen] == ["billing-project"]
    assert isinstance(result.result, dict)
    assert result.result["rows"] == [["orders"]]
    assert client.closed


def test_a_refused_connection_exits_3(monkeypatch: pytest.MonkeyPatch) -> None:
    _connect_with(monkeypatch, None)

    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method("bigquery-queries", dict(_RECIPE), "sql", {"query": _SQL})
    assert "RuntimeError" in str(info.value)
    assert "credentials refused" not in str(info.value)


def test_the_jobs_view_is_refused_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # JOBS holds the SQL text of every job, which is exactly what this source
    # reads during ingestion; the probe's catalog scope leaves it out.
    client = _StubClient([])
    _connect_with(monkeypatch, client)

    with pytest.raises(ProbeArgumentError):
        run_probe_method(
            "bigquery-queries",
            dict(_RECIPE),
            "sql",
            {"query": "SELECT query FROM `region-us`.INFORMATION_SCHEMA.JOBS"},
        )
    assert client.queries == []


def _ingestion_keeps(
    recipe: Dict[str, object], project: str, dataset: str, table: str
) -> bool:
    """What the aggregator decides for this table, through the extractor's
    own callbacks (SqlParsingAggregator.is_allowed_table)."""
    config = BigQueryQueriesSourceConfig.model_validate(recipe)
    # Built uninitialised: __init__ opens the audit-log store and reads the
    # time window, and these two callbacks touch neither.
    extractor = BigQueryQueriesExtractor.__new__(BigQueryQueriesExtractor)
    extractor.config = config
    extractor.filters = BigQueryFilter(config, BigQueryQueriesSourceReport())
    # The standalone source never passes a discovered-tables list.
    extractor.discovered_tables = None
    # BigQueryQueriesSource.__init__ builds this, which sets the shard suffix
    # every table name is printed with (process-wide; the autouse fixture
    # restores it).
    BigQueryIdentifierBuilder(config, BigQueryQueriesSourceReport())
    name = f"{project}.{dataset}.{table}"
    return not extractor.is_temp_table(name) and extractor.is_allowed_table(name)


def _probe_verdict(
    recipe: Dict[str, object],
    kind: str,
    parents: List[str],
    table: str,
) -> Tuple[bool, Optional[str]]:
    result = check_filters(
        source_type="bigquery-queries",
        config_dict=dict(recipe),
        kind=kind,
        parent_path=parents,
        names=[table],
    )
    verdict = result.results[0]
    return verdict.included, verdict.excluded_by


_OBJECTS = [
    ("proj-a", "sales", "orders"),
    ("proj-a", "sales", "events_20240101"),
    ("proj-a", "sales", "customers"),
    ("proj-a", "marketing", "leads"),
    ("proj-a", "_scratch", "t1"),
    ("proj-a", "tmp_work", "t2"),
    ("proj-b", "sales", "orders"),
]


def _recipe(**filters: Any) -> Dict[str, object]:
    return {**_RECIPE, **filters}


_RECIPES = {
    "defaults": _recipe(),
    "project_ids": _recipe(project_ids=["proj-a"]),
    # project_ids replaces project_id_pattern rather than narrowing it.
    "project_ids_over_pattern": _recipe(
        project_ids=["proj-b"], project_id_pattern={"deny": ["proj-b"]}
    ),
    "project_id_pattern": _recipe(project_id_pattern={"allow": ["^proj-a$"]}),
    # match_fully_qualified_names is on by default, and the validator turns
    # `^sales$` into `^.*\.sales$`.
    "dataset_pattern": _recipe(dataset_pattern={"allow": ["^sales$"]}),
    "dataset_pattern_bare": _recipe(
        dataset_pattern={"allow": ["^sales$"]}, match_fully_qualified_names=False
    ),
    "table_pattern": _recipe(table_pattern={"allow": [r"proj-a\.sales\.orders"]}),
    # Shards are judged under their normalized name, which carries a suffix
    # only once legacy sharded-table support is off.
    "sharded_legacy": _recipe(table_pattern={"allow": [r".*\.events$"]}),
    "sharded_suffixed": _recipe(
        enable_legacy_sharded_table_support=False,
        table_pattern={"allow": [r".*\.events_yyyymmdd$"]},
    ),
    # Never read by this source: every object is judged as a table.
    "view_pattern_ignored": _recipe(view_pattern={"deny": [".*"]}),
    "temp_prefix": _recipe(temp_table_dataset_prefix="tmp_"),
}


@pytest.mark.parametrize("kind", [DatasetSubTypes.TABLE, DatasetSubTypes.VIEW])
@pytest.mark.parametrize("recipe_name", sorted(_RECIPES))
def test_probe_filter_agrees_with_ingestion(recipe_name: str, kind: str) -> None:
    recipe = _RECIPES[recipe_name]
    disagreements = []
    for project, dataset, table in _OBJECTS:
        expected = _ingestion_keeps(recipe, project, dataset, table)
        # The probe takes the suffix from the recipe, not from whatever the
        # last-built source left on the class.
        BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = "_leaked"
        included, excluded_by = _probe_verdict(recipe, kind, [project, dataset], table)
        if included != expected:
            disagreements.append((project, dataset, table, expected, excluded_by))
    assert not disagreements
    # Vacuous agreement proves nothing: each recipe keeps something.
    assert any(_ingestion_keeps(recipe, *obj) for obj in _OBJECTS)


@pytest.mark.parametrize(
    "recipe, obj, excluded_by",
    [
        (_recipe(), ("proj-a", "_scratch", "t1"), "temp_table_dataset_prefix"),
        (_recipe(project_ids=["proj-a"]), ("proj-b", "sales", "orders"), "project_ids"),
        (
            _recipe(dataset_pattern={"allow": ["^sales$"]}),
            ("proj-a", "marketing", "leads"),
            "dataset_pattern",
        ),
        (
            _recipe(table_pattern={"deny": [r".*\.customers$"]}),
            ("proj-a", "sales", "customers"),
            "table_pattern",
        ),
    ],
)
def test_the_reason_names_the_rule_that_dropped_it(
    recipe: Dict[str, object], obj: Tuple[str, str, str], excluded_by: str
) -> None:
    project, dataset, table = obj
    assert _probe_verdict(recipe, DatasetSubTypes.TABLE, [project, dataset], table) == (
        False,
        excluded_by,
    )


def test_one_listed_project_qualifies_a_dataset_given_alone() -> None:
    result = check_filters(
        source_type="bigquery-queries",
        config_dict=_recipe(
            project_ids=["proj-a"], table_pattern={"allow": [r"proj-a\.sales\."]}
        ),
        kind=DatasetSubTypes.TABLE,
        parent_path=["sales"],
        names=["orders"],
    )
    assert result.results[0].target == "proj-a.sales.orders"
    assert result.results[0].included


def test_a_view_is_judged_against_table_pattern_and_says_so() -> None:
    result = check_filters(
        source_type="bigquery-queries",
        config_dict=_recipe(
            table_pattern={"deny": [r".*\.v_orders$"]}, view_pattern={"allow": [".*"]}
        ),
        kind=DatasetSubTypes.VIEW,
        parent_path=["proj-a", "sales"],
        names=["v_orders"],
    )
    assert result.results[0].excluded_by == "table_pattern"
    assert any("view_pattern has no effect" in w for w in result.warnings)
