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
from datahub.ingestion.agent.verdicts import (
    ContainerUnknown,
    Verdict,
    VerdictContext,
)
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


def test_a_bigquery_dataset_is_not_reported_excluded_on_a_pattern_it_rewrote() -> None:
    """The config BigQuery ships, which no other test here exercises.

    `match_fully_qualified_names` defaults True and `project_ids` defaults
    empty -- BigQuery discovers projects -- so this is what a recipe written
    by hand looks like. BigQuery's own validator rewrites `^analytics$` to
    `^.*\\.analytics$`, which the bare name `analytics` cannot match, and the
    command reported `included: false` about a dataset every real run
    ingests. Every other BigQuery case in this file pins `project_ids`, which
    is why the default shape went unnoticed.
    """
    with pytest.raises(ContainerUnknown) as refusal:
        check_filters(
            source_type="bigquery",
            config_dict={"dataset_pattern": {"allow": ["^analytics$"]}},
            kind=str(DatasetContainerSubTypes.SCHEMA),
            parent_path=[],
            names=["analytics"],
        )
    message = str(refusal.value)
    assert "dataset_pattern" in message
    assert "--parent" in message
    assert "project_ids" in message
    # The listing that answers it, so the refusal is one hop rather than a
    # dead end the caller has to resolve out of band.
    assert "probe run projects" in message


def test_naming_the_project_answers_the_same_question() -> None:
    """The refusal is a request, not a wall: what it asks for resolves it."""
    result = check_filters(
        source_type="bigquery",
        config_dict={"dataset_pattern": {"allow": ["^analytics$"]}},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=["proj1"],
        names=["analytics", "other"],
    )
    assert _rows(result) == [
        ("analytics", "proj1.analytics", True, None),
        ("other", "proj1.other", False, "dataset_pattern"),
    ]
    assert result.warnings == []


def test_an_allow_all_pattern_answers_without_a_container() -> None:
    """The carve-out, and the reason refusing is not simply "always refuse".

    A recipe with no `dataset_pattern` includes every dataset whatever string
    the pattern is matched against, so the missing project cannot change the
    verdict -- only the `target` reported beside it, which is said. This is
    the "I have not written any filters yet, what is in here?" call, and
    refusing it would break the common case to fix the broken one.
    """
    result = check_filters(
        source_type="bigquery",
        config_dict={},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["analytics"],
    )
    assert _rows(result) == [("analytics", "analytics", True, None)]
    assert len(result.warnings) == 1
    assert "not qualified with the container" in result.warnings[0]


def test_a_pattern_that_filters_nothing_is_not_refused_for_being_spelled_oddly() -> (
    None
):
    """`^.*$` admits every name exactly as `.*` does.

    AllowDenyPattern.is_allow_all recognises only the bare `.*`, and documents
    its False as "unknown" rather than "restricted" -- so keying the carve-out
    on it alone refused a recipe that filters nothing, for want of a container
    that could not have changed its verdict.
    """
    result = check_filters(
        source_type="bigquery",
        config_dict={"dataset_pattern": {"allow": ["^.*$"]}},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["analytics"],
    )
    assert _rows(result) == [("analytics", "analytics", True, None)]


def test_a_deny_the_source_appended_does_not_cost_the_recipe_its_answer() -> None:
    """A deny list filters, and is still blind to the container.

    `validate_legacy_schema_pattern` appends `.*INFORMATION_SCHEMA$` to every
    Snowflake recipe, including one that sets no filter of its own. Keying the
    carve-out on "filters nothing" therefore refused a recipe the caller never
    filtered, for want of a container that could not have changed its verdict:
    a suffix anchored past a dot cannot reach back across one.
    """
    result = check_filters(
        source_type="snowflake",
        config_dict={
            "account_id": "abc",
            "username": "u",
            "password": "p",
            "warehouse": "w",
            "role": "r",
            "match_fully_qualified_names": True,
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["MY_SCHEMA"],
    )
    assert _rows(result) == [("MY_SCHEMA", "MY_SCHEMA", True, None)]


def test_an_allow_anchored_past_the_dot_answers_where_a_qualified_one_refuses() -> None:
    """The carve-out turns on whether the container could change the verdict,
    not on whether the pattern filters.

    `^.*_prod$` filters plenty and says the same thing about `ds_prod` in
    every project, so it is answerable. `^p1\\.ds$` names a project, so the
    one the caller did not give is exactly what decides it.
    """
    blind = check_filters(
        source_type="bigquery",
        config_dict={"dataset_pattern": {"allow": ["^.*_prod$"]}},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["ds_prod"],
    )
    assert _rows(blind) == [("ds_prod", "ds_prod", True, None)]

    with pytest.raises(ContainerUnknown):
        check_filters(
            source_type="bigquery",
            config_dict={"dataset_pattern": {"allow": [r"^p1\\.ds$"]}},
            kind=str(DatasetContainerSubTypes.SCHEMA),
            parent_path=[],
            names=["ds_prod"],
        )


def test_the_note_says_when_the_container_itself_is_filtered() -> None:
    """ "The verdict stands" is exact about the field deciding this kind, and
    was read as broader than that.

    With a filtering `project_id_pattern` and no `--parent`, the dataset is
    reported included -- correctly, since `dataset_pattern` filters nothing --
    and naming a project then excludes it by a different field. The note now
    says so, rather than leaving an assurance to cover a container nobody
    named.
    """
    result = check_filters(
        source_type="bigquery",
        config_dict={"project_id_pattern": {"allow": ["^prod-.*"]}},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["analytics"],
    )
    assert result.results[0].included
    assert any("project_id_pattern" in w for w in result.warnings), result.warnings

    # And naming one really does flip it, which is what the clause warns of.
    excluded = check_filters(
        source_type="bigquery",
        config_dict={"project_id_pattern": {"allow": ["^prod-.*"]}},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=["dev-x"],
        names=["analytics"],
    )
    assert excluded.results[0].excluded_by == "project_id_pattern"


def test_the_note_stays_plain_when_nothing_filters_the_container() -> None:
    """The common case the note exists for: a recipe with no filters at all
    must not read as doubtful."""
    result = check_filters(
        source_type="bigquery",
        config_dict={},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["analytics"],
    )
    assert len(result.warnings) == 1
    assert "though" not in result.warnings[0]


def test_a_single_pinned_project_still_needs_no_parent() -> None:
    """The Qualifier fallback must survive: one pinned project IS the
    container, so nothing is missing and nothing is said about it."""
    result = check_filters(
        source_type="bigquery",
        config_dict={
            "project_ids": ["p1"],
            "dataset_pattern": {"allow": ["^analytics$"]},
        },
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["analytics"],
    )
    assert _rows(result) == [("analytics", "p1.analytics", True, None)]
    assert result.warnings == []


def test_a_qualified_source_with_no_container_refuses_rather_than_guessing() -> None:
    """It used to answer, and answer wrongly: `ds` judged against a pattern
    written for `project.ds` reads as excluded, with the only correction in
    `warnings` -- beside a flat `included: false` that a reader takes at face
    value. A verdict the command cannot stand behind is the failure it exists
    to prevent, so it now declines and says what to pass."""
    with pytest.raises(ContainerUnknown) as refusal:
        check_filters(
            source_type="bigquery",
            config_dict={
                "project_ids": ["p1", "p2"],
                "dataset_pattern": {"allow": [r"^p1\.ds$"]},
            },
            kind=str(DatasetContainerSubTypes.SCHEMA),
            parent_path=[],
            names=["ds"],
        )
    assert "dataset_pattern" in str(refusal.value)
    assert "project_ids" in str(refusal.value)


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

    # Snowflake has no Qualifier field, so without --parent there is nothing
    # to qualify with and the qualified pattern cannot be judged at all.
    with pytest.raises(ContainerUnknown):
        check_filters(
            source_type="snowflake-summary",
            config_dict=config,
            kind=str(DatasetContainerSubTypes.SCHEMA),
            parent_path=[],
            names=["PUBLIC"],
        )


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

    # Snowflake has no Qualifier field, so without --parent there is nothing
    # to qualify with and the qualified pattern cannot be judged at all.
    with pytest.raises(ContainerUnknown):
        check_filters(
            source_type="snowflake-queries",
            config_dict=config,
            kind=str(DatasetContainerSubTypes.SCHEMA),
            parent_path=[],
            names=["PUBLIC"],
        )


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
