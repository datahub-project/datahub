"""The SQL family's match targets, answered by SQLCommonConfig.probe_match_target.

The check_filters cases pin what a caller reads (echoed kind, target and
warnings) for each kind on a three-tier and a two-tier source. The direct
cases pin what the hook itself answers per kind.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import pytest

from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.verdicts import ClassifyContext
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.postgres.source import PostgresConfig
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

_PG: Dict[str, object] = {
    "host_port": "h:5432",
    "username": "u",
    "password": "p",
    "database": "db",
}
_MY: Dict[str, object] = {"host_port": "h:3306", "username": "u", "password": "p"}
_UC: Dict[str, object] = {
    "token": "t",
    "workspace_url": "https://w.cloud.databricks.com",
}
_NO_PARENT = (
    "no parent given, so these were judged on their bare names; this "
    "source filters on a qualified identifier, so pass the containing "
    "schema/database to get the verdict ingestion actually makes"
)
_UNITY_NEEDS_PARENT = (
    "unity-catalog matches catalog_pattern and schema_pattern against the full "
    "id (`[metastore.]catalog[.schema]`); pass the containing names with "
    "--parent, outermost first, for exact verdicts. Judged on the bare name "
    "instead."
)

_MALFORMED = (
    "could not build a complete identifier for 'orders' (got '{got}'); judged "
    "on its bare name instead"
)


@dataclass(frozen=True)
class _Case:
    source_type: str
    config: Dict[str, object]
    kind: str
    parent_path: List[str]
    name: str
    # What the result reports: the canonical kind, the target, the warnings.
    echoed: str
    target: str
    warnings: List[str] = field(default_factory=list)


_BQ: Dict[str, object] = {"project_ids": ["p1"]}
_KAFKA: Dict[str, object] = {"connection": {"bootstrap": "h:9092"}}
_CASES: List[_Case] = [
    _Case(
        "postgres",
        _PG,
        "Table",
        ["db", "public"],
        "orders",
        "Table",
        "db.public.orders",
    ),
    _Case("postgres", _PG, "Table", ["public"], "orders", "Table", "db.public.orders"),
    _Case("postgres", _PG, "Table", [], "orders", "Table", "orders", [_NO_PARENT]),
    _Case("postgres", _PG, "View", ["db", "public"], "v1", "View", "db.public.v1"),
    _Case(
        "postgres",
        _PG,
        "table",
        ["db", "public"],
        "orders",
        "Table",
        "db.public.orders",
    ),
    _Case("postgres", _PG, "Schema", ["db"], "public", "Schema", "public"),
    _Case("postgres", _PG, "Schema", [], "public", "Schema", "public"),
    _Case("postgres", _PG, "Database", [], "db", "Database", "db"),
    _Case(
        "postgres",
        _PG,
        "Table",
        ["db", ""],
        "orders",
        "Table",
        "orders",
        [_MALFORMED.format(got="db..orders")],
    ),
    # A quoted name may itself hold dots; ingestion matches the identifier as
    # built, so the probe does too.
    _Case(
        "postgres",
        _PG,
        "Table",
        ["db", "public"],
        "a..b",
        "Table",
        "db.public.a..b",
    ),
    _Case(
        "postgres",
        _PG,
        "Table",
        ["db", "public"],
        "orders.",
        "Table",
        "db.public.orders.",
    ),
    _Case("mysql", _MY, "Table", ["shop"], "orders", "Table", "shop.orders"),
    _Case("mysql", _MY, "Table", [], "orders", "Table", "orders", [_NO_PARENT]),
    _Case("mysql", _MY, "View", ["shop"], "v1", "View", "shop.v1"),
    _Case("mysql", _MY, "Database", [], "shop", "Database", "shop"),
    _Case(
        "mysql",
        _MY,
        "Table",
        [""],
        "orders",
        "Table",
        "orders",
        [_MALFORMED.format(got=".orders")],
    ),
    # A top-level kind has no container to qualify it with, and no warning.
    _Case("bigquery", _BQ, "Project", [], "p1", "Project", "p1"),
]


@pytest.mark.parametrize(
    "case", _CASES, ids=lambda c: f"{c.source_type}-{c.kind}-{c.parent_path}"
)
def test_each_sql_kind_is_matched_on_the_target_ingestion_uses(case: _Case) -> None:
    result = check_filters(
        source_type=case.source_type,
        config_dict=case.config,
        kind=case.kind,
        parent_path=case.parent_path,
        names=[case.name],
    )
    assert result.kind == case.echoed
    assert [r.target for r in result.results] == [case.target]
    assert result.warnings == case.warnings


def _declares_no(source_type: str, kind: str, declared: str) -> str:
    return (
        f"'{source_type}' declares no kind '{kind}' and no filter for it, so every "
        f"name is reported included. Kinds it does declare: {declared}"
    )


@pytest.mark.parametrize(
    "source_type, config, kind, parent_path, name, echoed, declared",
    [
        ("mysql", _MY, "schema", [], "shop", "Schema", "Database, Table, View"),
        ("mysql", _MY, "Schema", ["x"], "shop", "Schema", "Database, Table, View"),
        ("bigquery", _BQ, "database", [], "p1", "Database", "Schema"),
        ("kafka", _KAFKA, "table", [], "t1", "Table", "Topic"),
    ],
)
def test_a_miscased_kind_the_source_does_not_declare_reads_as_before(
    source_type: str,
    config: Dict[str, object],
    kind: str,
    parent_path: List[str],
    name: str,
    echoed: str,
    declared: str,
) -> None:
    """Table, View, Schema and Database are canonicalised for every source,
    so a miscased one is echoed in that spelling and judged on the bare name,
    with only the "declares no kind" warning."""
    result = check_filters(
        source_type=source_type,
        config_dict=config,
        kind=kind,
        parent_path=parent_path,
        names=[name],
    )
    assert result.to_dict() == {
        "source_type": source_type,
        "kind": echoed,
        "parent_path": parent_path,
        "pattern_field": None,
        "filtering": "unresolved",
        "tried": None,
        "results": [
            {"name": name, "target": name, "included": True, "excluded_by": None}
        ],
        "warnings": [_declares_no(source_type, echoed, declared)],
    }


def test_a_resolver_with_nothing_usable_is_judged_on_the_bare_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Blank(PostgresConfig):
        def probe_filter_target(
            self,
            schema: str,
            entity: str,
            warn: Callable[[str], None],
            database: Optional[str] = None,
        ) -> Optional[str]:
            return ""

    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: _Blank)
    result = check_filters(
        source_type="postgres",
        config_dict=_PG,
        kind="Table",
        parent_path=["db", "public"],
        names=["orders", "v"],
    )
    assert [r.target for r in result.results] == ["orders", "v"]
    assert result.warnings == [
        "the connector's identifier resolver returned nothing usable, so these "
        "were judged on their bare names; the verdict may not be the one "
        "ingestion makes"
    ]


def test_a_resolver_answering_with_a_non_string_is_judged_on_the_bare_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # As unusable as an empty answer: the connector did answer, so the shim
    # does not overrule it.
    class _NotAString(PostgresConfig):
        pass

    monkeypatch.setattr(_NotAString, "probe_filter_target", lambda self, **_kw: 42)
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: _NotAString)
    result = check_filters(
        source_type="postgres",
        config_dict=_PG,
        kind="Table",
        parent_path=["db", "public"],
        names=["orders"],
    )
    assert [r.target for r in result.results] == ["orders"]
    assert any("returned nothing usable" in w for w in result.warnings)


def _ctx(
    config: object,
    kind: str,
    name: str,
    parent_path: Tuple[str, ...],
    messages: List[str],
) -> ClassifyContext:
    return ClassifyContext(
        config=config,
        name=name,
        fqn=".".join([*parent_path, name]),
        pattern_field=None,
        parent_path=parent_path,
        warn=messages.append,
        kind=kind,
    )


@pytest.mark.parametrize(
    "config, kind, parent_path, target, warnings",
    [
        (PostgresConfig(**_PG), "Table", ("db", "public"), "db.public.orders", []),
        (PostgresConfig(**_PG), "View", ("public",), "db.public.orders", []),
        (PostgresConfig(**_PG), "Table", (), None, [_NO_PARENT]),
        (PostgresConfig(**_PG), "Schema", ("db",), None, []),
        (PostgresConfig(**_PG), "Database", (), None, []),
        (MySQLConfig(**_MY), "Table", ("shop",), "shop.orders", []),
        (MySQLConfig(**_MY), "Database", (), None, []),
        # A kind the container chain does not describe is no table or view.
        (PostgresConfig(**_PG), "Stored Procedure", ("db", "public"), None, []),
    ],
)
def test_the_sql_hook_answers_tables_and_views_and_leaves_containers_bare(
    config: SQLCommonConfig,
    kind: str,
    parent_path: Tuple[str, ...],
    target: Optional[str],
    warnings: List[str],
) -> None:
    messages: List[str] = []
    assert (
        config.probe_match_target(_ctx(config, kind, "orders", parent_path, messages))
        == target
    )
    assert messages == warnings


@pytest.mark.parametrize(
    "extra, kind, parent_path, target, warnings",
    [
        ({}, "Catalog", (), "my_cat", []),
        ({}, "Schema", ("my cat",), "my_cat.my_cat", []),
        ({"include_metastore": True}, "Schema", ("meta", "c"), "meta.c.my_cat", []),
        ({"catalogs": ["only cat"]}, "Schema", (), "only_cat.my_cat", []),
        ({}, "Schema", (), "my cat", [_UNITY_NEEDS_PARENT]),
        ({"include_metastore": True}, "Catalog", (), "my cat", [_UNITY_NEEDS_PARENT]),
        ({}, "Table", ("cat", "sch"), "cat.sch.my cat", []),
        ({}, "Table", (), None, [_NO_PARENT]),
    ],
)
def test_unity_containers_are_matched_on_their_ids(
    extra: Dict[str, object],
    kind: str,
    parent_path: Tuple[str, ...],
    target: Optional[str],
    warnings: List[str],
) -> None:
    config = UnityCatalogSourceConfig.model_validate({**_UC, **extra})
    messages: List[str] = []
    assert (
        config.probe_match_target(_ctx(config, kind, "my cat", parent_path, messages))
        == target
    )
    assert messages == warnings


def test_a_resolver_dropping_the_final_component_is_judged_on_the_bare_name() -> None:
    from datahub.ingestion.source.sql.sql_probe import _complete_target

    warnings: List[str] = []
    ctx = ClassifyContext(
        config=None,
        name="orders",
        fqn="db.public.orders",
        pattern_field="table_pattern",
        parent_path=("db", "public"),
        warn=warnings.append,
        kind="Table",
    )
    assert _complete_target("db.public.", ctx) is None
    assert warnings == [_MALFORMED.format(got="db.public.")]
