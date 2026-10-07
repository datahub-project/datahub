from typing import Dict, List, Mapping, Optional, Sequence

import pytest

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters

_BASE: Dict[str, object] = {"aws_region": "us-east-1"}
_OTHER_ACCOUNT = "222222222222"


def _judge(
    recipe: Mapping[str, object],
    kind: str,
    names: Sequence[str],
    parent: Sequence[str] = (),
    attributes: Optional[List[Mapping[str, str]]] = None,
    try_deny: Optional[Sequence[str]] = None,
) -> FilterCheckResult:
    return check_filters(
        source_type="glue",
        config_dict={**_BASE, **recipe},
        kind=kind,
        parent_path=list(parent),
        names=list(names),
        attributes=attributes,
        try_deny=try_deny,
    )


def _verdicts(result: FilterCheckResult) -> Dict[str, str]:
    """name -> excluded_by, or "" when included."""
    return {r.name: "" if r.included else (r.excluded_by or "") for r in result.results}


def test_table_pattern_is_matched_against_database_dot_table() -> None:
    result = _judge(
        {"table_pattern": {"allow": [r"^sales\.orders$"]}},
        "Table",
        ["orders", "refunds"],
        parent=["sales"],
    )

    assert [r.target for r in result.results] == ["sales.orders", "sales.refunds"]
    assert _verdicts(result) == {"orders": "", "refunds": "table_pattern"}
    assert result.warnings == []


def test_a_bare_table_name_pattern_excludes_as_ingestion_does() -> None:
    result = _judge(
        {"table_pattern": {"allow": ["^orders$"]}},
        "Table",
        ["orders"],
        parent=["sales"],
    )

    assert _verdicts(result) == {"orders": "table_pattern"}


def test_views_are_filtered_by_table_pattern() -> None:
    result = _judge(
        {"table_pattern": {"deny": [r"^sales\.v_.*"]}},
        "View",
        ["v_orders"],
        parent=["sales"],
    )

    assert result.pattern_field == "table_pattern"
    assert _verdicts(result) == {"v_orders": "table_pattern"}


def test_a_table_in_a_denied_database_is_excluded_by_the_database() -> None:
    result = _judge(
        {"database_pattern": {"deny": ["^scratch$"]}},
        "Table",
        ["t1"],
        parent=["scratch"],
    )

    assert _verdicts(result) == {"t1": "database_pattern"}
    assert result.excluded_by_container


def test_databases_are_matched_on_the_bare_name() -> None:
    result = _judge(
        {"database_pattern": {"allow": ["^sales$"]}}, "Database", ["sales", "ops"]
    )

    assert _verdicts(result) == {"sales": "", "ops": "database_pattern"}


@pytest.mark.parametrize("switch", [False, None])
def test_jobs_are_excluded_when_transforms_are_off(switch: Optional[bool]) -> None:
    result = _judge({"extract_transforms": switch}, "Job", ["nightly_load"])

    assert result.filtering == "unfiltered"
    assert _verdicts(result) == {"nightly_load": "extract_transforms"}


def test_jobs_are_included_by_default() -> None:
    result = _judge({}, "Job", ["nightly_load"])

    assert _verdicts(result) == {"nightly_load": ""}
    assert result.warnings == []


def test_an_ignored_resource_link_database_is_excluded_from_run_attributes() -> None:
    result = _judge(
        {"ignore_resource_links": True},
        "Database",
        ["sales", "shared_link"],
        attributes=[
            {"catalog_id": "123456789012", "resource_link": "false"},
            {"catalog_id": "123456789012", "resource_link": "true"},
        ],
    )

    assert _verdicts(result) == {"sales": "", "shared_link": "ignore_resource_links"}


def test_bare_names_warn_that_resource_links_could_not_be_judged() -> None:
    result = _judge({"ignore_resource_links": True}, "Database", ["shared_link"])

    assert _verdicts(result) == {"shared_link": ""}
    assert any("--from-run" in w for w in result.warnings)


def test_no_warning_when_no_rule_needs_an_attribute() -> None:
    result = _judge({}, "Database", ["sales"])

    assert result.warnings == []


def test_a_database_of_another_catalog_is_excluded_by_catalog_id() -> None:
    result = _judge(
        {"catalog_id": _OTHER_ACCOUNT},
        "Database",
        ["mine", "foreign", "unknown_owner"],
        attributes=[
            {"catalog_id": _OTHER_ACCOUNT, "resource_link": "false"},
            {"catalog_id": "333333333333", "resource_link": "false"},
            {"catalog_id": "", "resource_link": "false"},
        ],
    )

    assert _verdicts(result) == {
        "mine": "",
        "foreign": "catalog_id",
        "unknown_owner": "",
    }


def test_try_deny_reaches_the_database_override() -> None:
    result = _judge(
        {"catalog_id": _OTHER_ACCOUNT},
        "Database",
        ["sales"],
        attributes=[{"catalog_id": "333333333333", "resource_link": "false"}],
        try_deny=["^sales$"],
    )

    # database_pattern is checked before the catalog rule, as in get_all_databases.
    assert _verdicts(result) == {"sales": "database_pattern"}


def test_a_table_level_resource_link_is_excluded_when_ignored() -> None:
    attrs = {
        "resource_link": "true",
        "database_resource_link": "false",
        "database_catalog_id": "",
    }
    result = _judge(
        {"ignore_resource_links": True},
        "Table",
        ["shared_orders"],
        parent=["sales"],
        attributes=[attrs],
    )

    assert _verdicts(result) == {"shared_orders": "ignore_resource_links"}


def test_tables_of_an_ignored_resource_link_database_are_excluded() -> None:
    attrs = {
        "resource_link": "false",
        "database_resource_link": "true",
        "database_catalog_id": "",
    }
    result = _judge(
        {"ignore_resource_links": True},
        "Table",
        ["t1"],
        parent=["shared_link"],
        attributes=[attrs],
    )

    assert _verdicts(result) == {"t1": "ignore_resource_links"}


def test_tables_of_a_foreign_catalog_database_are_excluded() -> None:
    attrs = {
        "resource_link": "false",
        "database_resource_link": "false",
        "database_catalog_id": "333333333333",
    }
    result = _judge(
        {"catalog_id": _OTHER_ACCOUNT},
        "Table",
        ["orders"],
        parent=["sales"],
        attributes=[attrs],
    )

    assert _verdicts(result) == {"orders": "catalog_id"}


def test_resource_links_are_kept_when_not_ignored() -> None:
    attrs = {
        "resource_link": "true",
        "database_resource_link": "true",
        "database_catalog_id": "",
    }
    result = _judge(
        {}, "Table", ["shared_orders"], parent=["shared_link"], attributes=[attrs]
    )

    assert _verdicts(result) == {"shared_orders": ""}
