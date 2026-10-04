"""The SQL family's verdict rules, declared on SQLCommonConfig.

Each rule is driven through check_filters, the path `probe filter` takes, and
asserted on the whole result a caller reads: verdict, excluded_by, target and
warnings.
"""

from typing import Annotated, Dict, FrozenSet, List, Optional, Tuple, Type

import pytest
from pydantic import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Enables,
    Filters,
)
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.declarations import declared_kind_enablers
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.sql.postgres.source import PostgresConfig
from datahub.ingestion.source.sql.sql_config import (
    SQLCommonConfig,
    sql_structural_verdict,
)

_PG: Dict[str, object] = {
    "host_port": "h:5432",
    "username": "u",
    "password": "p",
    "database": "db",
}
_RS: Dict[str, object] = {
    "host_port": "h:5439",
    "database": "dev",
    "username": "u",
    "password": "p",
}
_SF: Dict[str, object] = {
    "account_id": "a",
    "username": "u",
    "password": "p",
    "warehouse": "w",
}
_NEEDS_PARENT = (
    "this source matches containers on a qualified name and could not tell "
    "which one you mean, so these were judged on their bare names and will "
    "mostly read as excluded; pass --parent to get the verdict ingestion "
    "actually makes"
)


_Row = Tuple[str, str, bool, Optional[str]]


def _rows(result: FilterCheckResult) -> List[_Row]:
    return [(r.name, r.target, r.included, r.excluded_by) for r in result.results]


def _register(monkeypatch: pytest.MonkeyPatch, config_cls: Type[ConfigModel]) -> None:
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: config_cls)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])


def test_sql_configs_declare_the_table_and_view_switches() -> None:
    assert declared_kind_enablers(SQLCommonConfig) == {
        "Table": "include_tables",
        "View": "include_views",
    }


@pytest.mark.parametrize(
    ("kind", "flag", "name", "target"),
    [
        (DatasetSubTypes.TABLE, "include_tables", "orders", "db.public.orders"),
        (DatasetSubTypes.VIEW, "include_views", "v1", "db.public.v1"),
    ],
)
def test_a_switched_off_kind_is_excluded_by_its_switch(
    kind: str, flag: str, name: str, target: str
) -> None:
    result = check_filters(
        source_type="postgres",
        config_dict={**_PG, flag: False},
        kind=str(kind),
        parent_path=["db", "public"],
        names=[name],
    )
    assert _rows(result) == [(name, target, False, flag)]
    assert result.warnings == []


def test_a_default_schema_is_excluded_before_the_qualified_match() -> None:
    result = check_filters(
        source_type="redshift",
        config_dict={**_RS, "match_fully_qualified_names": True},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["pg_catalog", "public"],
    )
    assert _rows(result) == [
        ("pg_catalog", "pg_catalog", False, "default_schema"),
        ("public", "dev.public", True, None),
    ]
    assert result.warnings == []


def test_a_default_database_is_excluded_whatever_its_case() -> None:
    result = check_filters(
        source_type="postgres",
        config_dict=_PG,
        kind=str(DatasetContainerSubTypes.DATABASE),
        parent_path=[],
        names=["template0", "TEMPLATE1", "db"],
    )
    assert _rows(result) == [
        ("template0", "template0", False, "default_database"),
        ("TEMPLATE1", "TEMPLATE1", False, "default_database"),
        ("db", "db", True, None),
    ]
    assert result.warnings == []


def test_a_qualified_schema_match_reports_the_qualified_target() -> None:
    result = check_filters(
        source_type="redshift",
        config_dict={
            **_RS,
            "match_fully_qualified_names": True,
            "schema_pattern": {"allow": [r"^dev\.public$"]},
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["public", "other"],
    )
    assert _rows(result) == [
        ("public", "dev.public", True, None),
        ("other", "dev.other", False, "schema_pattern"),
    ]
    assert result.warnings == []


def test_the_callers_parent_qualifies_a_schema_on_a_multi_project_recipe() -> None:
    result = check_filters(
        source_type="bigquery",
        config_dict={
            "project_ids": ["p1", "p2"],
            "dataset_pattern": {"allow": [r"^p1\.ds$"]},
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=["p1"],
        names=["ds", "other"],
    )
    assert _rows(result) == [
        ("ds", "p1.ds", True, None),
        ("other", "p1.other", False, "dataset_pattern"),
    ]
    assert result.warnings == []


def test_a_qualified_source_with_no_container_says_to_pass_a_parent() -> None:
    result = check_filters(
        source_type="bigquery",
        config_dict={
            "project_ids": ["p1", "p2"],
            "dataset_pattern": {"allow": [r"^p1\.ds$"]},
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["ds"],
    )
    assert _rows(result) == [("ds", "ds", False, "dataset_pattern")]
    assert result.warnings == [_NEEDS_PARENT]


def test_snowflake_summary_matches_schemas_the_way_snowflake_does() -> None:
    """Not a SQLCommonConfig, but it filters schemas through the same
    match_fully_qualified_names rule and has a probe provider, so it declares
    the SQL verdict rules itself."""
    config = {
        **_SF,
        "match_fully_qualified_names": True,
        "schema_pattern": {"allow": [r"^MYDB\.PUBLIC$"]},
    }
    qualified = check_filters(
        source_type="snowflake-summary",
        config_dict=config,
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=["MYDB"],
        names=["PUBLIC", "OTHER"],
    )
    assert _rows(qualified) == [
        ("PUBLIC", "MYDB.PUBLIC", True, None),
        ("OTHER", "MYDB.OTHER", False, "schema_pattern"),
    ]

    bare = check_filters(
        source_type="snowflake-summary",
        config_dict=config,
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["PUBLIC"],
    )
    assert _rows(bare) == [("PUBLIC", "PUBLIC", False, "schema_pattern")]
    assert bare.warnings == [_NEEDS_PARENT]


class _PinnedPostgres(PostgresConfig):
    """A subclass with a rule of its own, keeping the family's rules."""

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        if ctx.kind == DatasetContainerSubTypes.DATABASE and ctx.name == "template1":
            return Verdict.include()
        return sql_structural_verdict(self, ctx)


def test_a_subclass_override_can_keep_the_family_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _PinnedPostgres)
    result = check_filters(
        source_type="fake",
        config_dict=_PG,
        kind=str(DatasetContainerSubTypes.DATABASE),
        parent_path=[],
        names=["template0", "template1"],
    )
    assert _rows(result) == [
        ("template0", "template0", False, "default_database"),
        ("template1", "template1", True, None),
    ]


class _SchemasSwitch(RedshiftConfig):
    include_schemas: Annotated[bool, Enables(DatasetContainerSubTypes.SCHEMA)] = True


def test_a_kind_switch_stands_over_the_family_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _SchemasSwitch)
    result = check_filters(
        source_type="fake",
        config_dict={
            **_RS,
            "include_schemas": False,
            "match_fully_qualified_names": True,
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["pg_catalog", "public"],
    )
    assert _rows(result) == [
        ("pg_catalog", "pg_catalog", False, "include_schemas"),
        ("public", "public", False, "include_schemas"),
    ]


class _SqlLookingFields(ConfigModel):
    """The SQL family's field names on a config that declares no rule."""

    include_tables: bool = True
    match_fully_qualified_names: bool = True
    schema_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.SCHEMA)
    ] = Field(default=AllowDenyPattern.allow_all())
    table_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
        default=AllowDenyPattern.allow_all()
    )

    @classmethod
    def default_schemas(cls) -> FrozenSet[str]:
        return frozenset({"sys"})


def test_the_framework_applies_no_sql_rule_a_config_does_not_declare(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _SqlLookingFields)
    tables = check_filters(
        source_type="fake",
        config_dict={"include_tables": False},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=[],
        names=["orders"],
    )
    schemas = check_filters(
        source_type="fake",
        config_dict={},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["sys"],
    )
    assert _rows(tables) == [("orders", "orders", True, None)]
    assert _rows(schemas) == [("sys", "sys", True, None)]
    assert schemas.warnings == []


# --- configs outside SQLCommonConfig declare the rules they share ----------


def test_snowflake_queries_matches_schemas_on_the_qualified_name() -> None:
    config = {
        "connection": _SF,
        "match_fully_qualified_names": True,
        "schema_pattern": {"allow": [r"^MYDB\.PUBLIC$"]},
    }
    qualified = check_filters(
        source_type="snowflake-queries",
        config_dict=config,
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=["MYDB"],
        names=["PUBLIC", "orders"],
    )
    assert _rows(qualified) == [
        ("PUBLIC", "MYDB.PUBLIC", True, None),
        ("orders", "MYDB.orders", False, "schema_pattern"),
    ]

    bare = check_filters(
        source_type="snowflake-queries",
        config_dict=config,
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["PUBLIC"],
    )
    assert _rows(bare) == [("PUBLIC", "PUBLIC", False, "schema_pattern")]
    assert bare.warnings == [_NEEDS_PARENT]


def test_bigquery_queries_matches_datasets_on_the_qualified_name() -> None:
    result = check_filters(
        source_type="bigquery-queries",
        config_dict={
            "project_ids": ["p1"],
            "dataset_pattern": {"allow": [r"^p1\.analytics$"]},
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["analytics", "orders"],
    )
    assert _rows(result) == [
        ("analytics", "p1.analytics", True, None),
        ("orders", "p1.orders", False, "dataset_pattern"),
    ]
    assert result.warnings == []


@pytest.mark.parametrize(
    ("source_type", "config_dict", "kind", "flag"),
    [
        (
            "cube",
            {"api_url": "http://h/cubejs-api/v1", "api_token": "t"},
            DatasetSubTypes.VIEW,
            "include_views",
        ),
        # The subtype Cube emits a view's dataset with: include_views drops
        # those too.
        (
            "cube",
            {"api_url": "http://h/cubejs-api/v1", "api_token": "t"},
            DatasetSubTypes.SEMANTIC_MODEL,
            "include_views",
        ),
        (
            "cube",
            {"api_url": "http://h/cubejs-api/v1", "api_token": "t"},
            DatasetSubTypes.CUBE,
            "include_cubes",
        ),
        (
            "informix",
            {"host_port": "h:1", "server": "s", "database": "db"},
            DatasetSubTypes.TABLE,
            "include_tables",
        ),
        (
            "informix",
            {"host_port": "h:1", "server": "s", "database": "db"},
            DatasetSubTypes.VIEW,
            "include_views",
        ),
    ],
)
def test_a_switch_outside_the_sql_family_still_excludes_its_kind(
    source_type: str, config_dict: Dict[str, object], kind: str, flag: str
) -> None:
    result = check_filters(
        source_type=source_type,
        config_dict={**config_dict, flag: False},
        kind=str(kind),
        parent_path=[],
        names=["orders"],
    )
    assert _rows(result) == [("orders", "orders", False, flag)]
    assert result.warnings == []
