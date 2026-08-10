from typing import Dict, List

import pytest

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.source.common.subtypes import DatasetSubTypes

MYSQL_CONFIG: Dict[str, object] = {
    "host_port": "localhost:3306",
    "username": "u",
    "password": "p",
    "database": "mydb",
}

TABLES = ["orders", "users", "audit_log_v2"]


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


def test_a_kind_the_config_has_no_pattern_for_is_a_clear_error():
    with pytest.raises(ValueError, match="Nonsense"):
        check_filters(
            source_type="mysql",
            config_dict=MYSQL_CONFIG,
            kind="Nonsense",
            parent_path=["information_schema"],
            names=TABLES,
        )
