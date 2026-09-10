from typing import Dict, List

import pytest

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.verdicts import UNFILTERED, pattern_verdict
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

MYSQL_CONFIG: Dict[str, object] = {
    "host_port": "localhost:3306",
    "username": "u",
    "password": "p",
    "database": "mydb",
}

TABLES = ["orders", "users", "audit_log_v2"]

# Mode declares Dataset and Query levels that it offers no pattern for, which is
# the real case behind "a kind with no filter" rather than a contrived one.
MODE_CONFIG: Dict[str, object] = {
    "token": "t",
    "password": "p",
    "workspace": "w",
}


def _check(names: List[str], **kwargs: object) -> FilterCheckResult:
    return check_filters(
        source_type="mysql",
        config_dict=MYSQL_CONFIG,
        kind=str(DatasetSubTypes.TABLE),
        parent_path=["information_schema"],
        names=names,
        **kwargs,  # type: ignore[arg-type]
    )


def test_resolves_the_conventional_pattern_field_for_the_kind():
    result = _check(TABLES)
    assert result.pattern_field == "table_pattern"


def test_the_match_target_is_the_qualified_identifier_ingestion_uses():
    # MySQL inherits sql_common's get_identifier -> "schema.entity". A caller
    # reasoning about the bare name would judge every anchored pattern wrongly.
    result = _check(["orders"])
    assert result.results[0].target == "information_schema.orders"


def test_no_connection_is_needed():
    # MYSQL_CONFIG points at a host that need not exist: the whole point of
    # splitting filtering from fetching is that judging a name is offline.
    result = _check(TABLES)
    assert len(result.results) == 3


def test_a_deny_in_the_recipe_is_reported_against_the_field_that_decided():
    result = check_filters(
        source_type="mysql",
        config_dict={**MYSQL_CONFIG, "table_pattern": {"deny": [".*_v2$"]}},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=["information_schema"],
        names=TABLES,
    )
    by_name = {r.name: r for r in result.results}
    assert by_name["audit_log_v2"].included is False
    assert by_name["audit_log_v2"].excluded_by == "table_pattern"
    assert by_name["orders"].included is True


def test_try_deny_overrides_the_recipes_own_pattern():
    result = _check(TABLES, try_deny=["^information_schema\\.orders$"])
    by_name = {r.name: r.included for r in result.results}
    assert by_name["orders"] is False
    assert by_name["users"] is True


def test_an_anchored_bare_name_pattern_matches_nothing():
    # The empirical finding this command exists for: `^orders.*` looks precise
    # and silently matches nothing, because ingestion evaluates
    # "information_schema.orders". Without an oracle the caller cannot see this.
    result = _check(TABLES, try_allow=["^orders.*"])
    assert [r.included for r in result.results] == [False, False, False]


def test_the_same_pattern_qualified_matches():
    result = _check(TABLES, try_allow=["^information_schema\\.orders$"])
    by_name = {r.name: r.included for r in result.results}
    assert by_name["orders"] is True
    assert by_name["users"] is False


def test_a_kind_with_no_filter_reports_every_name_included():
    """The question is "would these be ingested", and where nothing filters them
    the answer is "yes, all of them". Refusing said something was wrong when
    nothing was. A null pattern_field is how the result says why."""
    result = check_filters(
        source_type="mode",
        config_dict=MODE_CONFIG,
        kind="Dataset",
        parent_path=[],
        names=["a", "b"],
    )
    assert result.pattern_field is None
    assert [v.included for v in result.results] == [True, True]
    assert all(v.excluded_by is None for v in result.results)
    # A level the source genuinely does not filter is not a problem to report.
    assert result.warnings == []


def test_an_unknown_kind_still_answers_but_says_it_is_unrecognised():
    """A misspelling is more likely than a level without a filter, and answering
    "all included" for one silently would be a wrong answer delivered
    confidently. It warns rather than raises because the kinds are not fully
    enumerable without a connection, so a strict check would refuse valid input."""
    result = check_filters(
        source_type="mysql",
        config_dict=MYSQL_CONFIG,
        kind="Nonsense",
        parent_path=["information_schema"],
        names=TABLES,
    )
    assert all(v.included for v in result.results)
    assert result.pattern_field is None
    assert len(result.warnings) == 1
    assert "no kind 'Nonsense'" in result.warnings[0]
    # And it must name the kinds that would have worked.
    assert "Table" in result.warnings[0]


def test_a_table_with_no_parent_is_judged_on_its_bare_name_with_a_warning():
    # Without the container the shim would build ".orders" for MySQL (or
    # "db..orders" for Postgres) -- a string ingestion never evaluates. Reporting
    # a confident verdict from that is worse than saying the target is degraded.
    result = check_filters(
        source_type="mysql",
        config_dict=MYSQL_CONFIG,
        kind=str(DatasetSubTypes.TABLE),
        parent_path=[],
        names=["orders"],
    )
    assert result.results[0].target == "orders"
    assert any("bare name" in w for w in result.warnings)


def test_the_warning_is_recorded_once_not_per_name():
    result = check_filters(
        source_type="mysql",
        config_dict=MYSQL_CONFIG,
        kind=str(DatasetSubTypes.TABLE),
        parent_path=[],
        names=["a", "b", "c"],
    )
    assert len(result.warnings) == 1


def test_a_source_whose_name_is_its_target_does_not_ask_for_a_parent():
    """The no-parent warning must only fire where a parent would change the answer.

    Kafka topics and Mode spaces are what their patterns match, so there is no
    container to pass. Warning anyway told an agent to distrust a correct verdict and
    go looking for one -- and the likeliest thing it learns from that is to ignore
    warnings, which is the worst outcome, because the SQL family's warning is real.
    """
    result = check_filters(
        source_type="kafka",
        config_dict={
            "connection": {"bootstrap": "broker:29092"},
            "topic_patterns": {"allow": ["^events.*"]},
        },
        kind="Topic",
        parent_path=[],
        names=["events.orders", "audit_log"],
    )
    assert result.warnings == []
    assert [(v.name, v.target, v.included) for v in result.results] == [
        ("events.orders", "events.orders", True),
        ("audit_log", "audit_log", False),
    ]


def test_a_source_that_filters_on_a_qualified_identifier_still_asks():
    # And the parent changes the verdict, which is why it is worth asking for: the
    # same pattern gives the opposite answer with and without it.
    mysql: Dict[str, object] = {
        "host_port": "h:3306",
        "username": "u",
        "password": "p",
        "database": "analytics",
        "table_pattern": {"allow": [r"^analytics\.ord.*"]},
    }
    without = check_filters(
        source_type="mysql",
        config_dict=mysql,
        kind="Table",
        parent_path=[],
        names=["orders"],
    )
    assert without.results[0].included is False
    assert any("qualified identifier" in w for w in without.warnings)

    with_parent = check_filters(
        source_type="mysql",
        config_dict=mysql,
        kind="Table",
        parent_path=["analytics"],
        names=["orders"],
    )
    assert with_parent.results[0].target == "analytics.orders"
    assert with_parent.results[0].included is True
    assert with_parent.warnings == []


def test_the_unfiltered_sentinel_is_an_include_not_a_field_name():
    """UNFILTERED is a marker, not an attribute: reading it off the config asks
    for "__unfiltered__" and raises. check_filters guards it before calling,
    but the sentinel and pattern_verdict are exported together and read as
    composable, so the guard belongs in both."""

    v = pattern_verdict(object(), UNFILTERED, "anything")
    assert v.included is True
    assert v.excluded_by is None


def test_soft_on_status_degrades_only_the_statuses_it_was_given():
    """The empty-vs-unread primitive, which had no test at all.

    The load-bearing branch is the one that does NOT degrade: if an unlisted
    status became a ProbeSoftError, a 500 or a dropped connection would be
    reported as "this space has no datasets" plus a warning, at exit 2 -- the
    exact confusion this interface exists to prevent, arriving as the caller's
    fault.
    """
    import pytest

    from datahub.ingestion.agent.verdicts import ProbeSoftError, soft_on_status

    class _Resp:
        def __init__(self, code):
            self.status_code = code

    class _HttpError(Exception):
        def __init__(self, code):
            self.response = _Resp(code)

    # A listed status is expected absence.
    with pytest.raises(ProbeSoftError, match="404"):
        with soft_on_status(403, 404, context="listing datasets"):
            raise _HttpError(404)

    # An unlisted status is a real failure and must propagate unchanged.
    with pytest.raises(_HttpError):
        with soft_on_status(403, 404, context="listing datasets"):
            raise _HttpError(500)

    # No .response at all -- a connection error or exhausted retries. The
    # duck-typed getattr chain must fall through to a re-raise, not swallow.
    with pytest.raises(ConnectionError):
        with soft_on_status(403, 404, context="listing datasets"):
            raise ConnectionError("connection reset")


# --- the container override is told which container ---------------------------


def _bq(parent, projects=("proj_a", "proj_b"), allow="^proj_a\\.analytics$"):
    from datahub.ingestion.agent.filter_check import check_filters

    return check_filters(
        source_type="bigquery",
        config_dict={
            "project_ids": list(projects),
            "dataset_pattern": {"allow": [allow]},
        },
        kind="Schema",
        parent_path=list(parent),
        names=["analytics", "staging"],
        try_allow=[],
        try_deny=[],
    ).to_dict()


def _verdicts(payload):
    return {v["name"]: (v["included"], v.get("target")) for v in payload["results"]}


def test_a_multi_project_recipe_is_answerable_with_a_parent():
    """The reason probe_schema_verdict_override receives the parent at all.

    BigQuery matches dataset_pattern against "project.dataset", and a recipe may
    name several projects. Without knowing which one the caller means, the
    override cannot build the qualified name and the verdict falls back to the
    bare one -- which, against a qualified pattern, excludes everything.
    """
    verdicts = _verdicts(_bq(["proj_a"]))
    assert verdicts["analytics"] == (True, "proj_a.analytics")
    assert verdicts["staging"] == (False, "proj_a.staging")


def test_a_single_project_recipe_still_needs_no_parent():
    """The fallback that existed before the parameter: with one project
    configured there is no ambiguity to resolve."""
    verdicts = _verdicts(_bq([], projects=("proj_a",), allow="^analytics$"))
    assert verdicts["analytics"] == (True, "proj_a.analytics")
    assert verdicts["staging"] == (False, "proj_a.staging")


def test_an_override_that_cannot_answer_says_so():
    """Several projects and no parent: the bare-name verdict stands, and every
    name reads as excluded. That is defensible only if the caller is told why --
    silence here is a confidently wrong answer, which is the failure this
    command exists to prevent."""
    payload = _bq([])
    assert all(not v["included"] for v in payload["results"])
    assert any("qualified name" in w for w in payload["warnings"]), payload["warnings"]


def test_the_warning_is_absent_when_the_override_could_answer():
    """It must not fire on every container verdict, or it stops being read."""
    with_parent = _bq(["proj_a"])
    single = _bq([], projects=("proj_a",), allow="^analytics$")
    for payload in (with_parent, single):
        assert not [w for w in payload["warnings"] if "qualified name" in w]


def test_redshift_ignores_the_parent_and_uses_its_own_database():
    """A Redshift recipe connects to one database, so self.database is the only
    qualifier ingestion ever uses. Honouring a different parent would answer
    about a database this recipe does not read."""
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    config = RedshiftConfig.model_validate(
        {
            "host_port": "redshift.example:5439",
            "database": "prod",
            "username": "u",
            "password": "p",
            "match_fully_qualified_names": True,
            "schema_pattern": {"allow": ["^prod\\.analytics$"]},
        }
    )
    match = config.probe_schema_verdict_override(
        schema="analytics", parent_path=("some_other_db",)
    )
    assert match is not None
    assert match.target == "prod.analytics"


@pytest.mark.parametrize("source_type", ["postgres", "mysql", "mssql"])
def test_schema_verdicts_work_on_a_source_that_inherits_the_base_hook(source_type):
    """The two connectors overriding probe_schema_verdict_override were the only
    ones any Schema-kind test touched, so widening the hook's signature broke
    every source that inherits the base and nothing noticed.

    It surfaced as exit 2 -- TypeError is in recipe_cli._USER_ERRORS -- telling
    the caller their input was wrong about a framework bug, which is the exact
    misdirection the exit-code contract exists to prevent.
    """
    configs: Dict[str, Dict[str, object]] = {
        "postgres": {
            "host_port": "h:5432",
            "username": "u",
            "password": "p",
            "database": "d",
        },
        "mysql": {"host_port": "h:3306", "username": "u", "password": "p"},
        "mssql": {"host_port": "h:1433", "username": "u", "password": "p"},
    }
    result = check_filters(
        source_type=source_type,
        config_dict=configs[source_type],
        kind="Schema",
        parent_path=[],
        names=["public", "information_schema"],
        try_allow=[],
        try_deny=[],
    ).to_dict()
    assert result["pattern_field"] == "schema_pattern"
    assert result["filtering"] == "by_pattern"
    results = result["results"]
    assert isinstance(results, list)
    assert [v["name"] for v in results] == ["public", "information_schema"]


# --- a kind switched off wholesale is not a pattern question ----------------
#
# `probe filter --kind View` reported "included: true" for a view that
# `include_views: false` guarantees ingestion will never emit. The pattern was
# consulted and answered honestly; the flag that overrules it was not read at
# all, so the command whose only job is verdicts gave one ingestion does not
# make.


def test_a_view_is_excluded_when_the_recipe_switched_views_off():
    result = check_filters(
        source_type="mysql",
        config_dict={**MYSQL_CONFIG, "include_views": False},
        kind=str(DatasetSubTypes.VIEW),
        parent_path=["information_schema"],
        names=["some_view"],
    )
    verdict = result.results[0]
    assert verdict.included is False
    # Named, so a caller editing the recipe changes the line that decided --
    # reporting view_pattern here would send them to edit a pattern that had
    # no say.
    assert verdict.excluded_by == "include_views"


def test_a_table_is_excluded_when_the_recipe_switched_tables_off():
    result = check_filters(
        source_type="mysql",
        config_dict={**MYSQL_CONFIG, "include_tables": False},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=["information_schema"],
        names=["orders"],
    )
    assert result.results[0].included is False
    assert result.results[0].excluded_by == "include_tables"


def test_the_flag_for_one_kind_does_not_decide_the_other():
    """include_views must not suppress tables, and vice versa. The two share a
    lookup, so a mistake there would silently exclude everything."""
    result = check_filters(
        source_type="mysql",
        config_dict={**MYSQL_CONFIG, "include_views": False},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=["information_schema"],
        names=["orders"],
    )
    assert result.results[0].included is True


def test_the_flag_defaults_to_on_so_ordinary_recipes_are_unaffected():
    """Both default true, and a config that has no such field at all (Kafka,
    Mode) must fall through to the pattern rather than be reported excluded by
    a flag it does not have -- hence getattr with a True default."""
    result = check_filters(
        source_type="mysql",
        config_dict=MYSQL_CONFIG,
        kind=str(DatasetSubTypes.VIEW),
        parent_path=["information_schema"],
        names=["some_view"],
    )
    assert result.results[0].included is True
    assert result.results[0].excluded_by is None


# --- the hypothetical has to actually be the hypothetical ------------------


def test_try_allow_reaches_a_source_that_decides_structurally():
    """`structural or (...)` short-circuited the pattern branch, and the
    structural rule reads the pattern off the config itself -- so --try-allow
    was ignored by every source declaring probe_schema_verdict_override.
    On BigQuery that is every schema query, since match_fully_qualified_names
    defaults True. `tried` still echoed the hypothetical, so the result
    claimed to have applied what it ignored."""
    config = {
        "host_port": "h:5439",
        "database": "dev",
        "username": "u",
        "password": "p",
        "match_fully_qualified_names": True,
        "schema_pattern": {"allow": ["^nomatch$"]},
    }
    unchanged = check_filters(
        source_type="redshift",
        config_dict=config,
        kind="Schema",
        parent_path=[],
        names=["public"],
    )
    assert unchanged.results[0].included is False

    hypothetical = check_filters(
        source_type="redshift",
        config_dict=config,
        kind="Schema",
        parent_path=[],
        names=["public"],
        try_allow=[".*"],
    )
    assert hypothetical.results[0].included is True, (
        "--try-allow was echoed in `tried` but not applied"
    )


def test_try_deny_alone_keeps_the_recipes_allow_list():
    """`allow=['.*'] if not try_allow` discarded the recipe's own allow list,
    so "what if I added this deny" was answered against an allow-all. Every
    name outside the recipe's allow flipped to included, and the caller reads
    that as the deny being harmless."""
    result = check_filters(
        source_type="mysql",
        config_dict={**MYSQL_CONFIG, "database_pattern": {"allow": ["^analytics$"]}},
        kind=str(DatasetContainerSubTypes.DATABASE),
        parent_path=[],
        names=["other_db"],
        try_deny=["^zzz"],
    )
    assert result.tried == {"allow": ["^analytics$"], "deny": ["^zzz"]}
    assert result.results[0].included is False
    assert result.results[0].excluded_by == "database_pattern"


def test_try_allow_alone_keeps_the_recipes_deny_list():
    """The mirror image, so neither half can regress on its own."""
    result = check_filters(
        source_type="mysql",
        config_dict={**MYSQL_CONFIG, "database_pattern": {"deny": ["^secret_db$"]}},
        kind=str(DatasetContainerSubTypes.DATABASE),
        parent_path=[],
        names=["secret_db", "analytics"],
        try_allow=[".*"],
    )
    assert result.tried == {"allow": [".*"], "deny": ["^secret_db$"]}
    by_name = {v.name: v for v in result.results}
    assert by_name["secret_db"].included is False
    assert by_name["analytics"].included is True
