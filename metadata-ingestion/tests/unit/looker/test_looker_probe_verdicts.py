from typing import Any, Dict, List, Mapping, Optional, Sequence

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters

_BASE: Dict[str, Any] = {
    "base_url": "https://looker.example.com",
    "client_id": "probe-client-id",
    "client_secret": "probe-client-secret-value",
}
# extract_independent_looks is refused without stateful ingestion.
_LOOKS_ON: Dict[str, Any] = {
    "extract_independent_looks": True,
    "stateful_ingestion": {"enabled": True},
}


def _judge(
    kind: str,
    names: List[str],
    parent: Sequence[str] = (),
    attributes: Optional[Sequence[Mapping[str, str]]] = None,
    try_allow: Optional[Sequence[str]] = None,
    **recipe: Any,
) -> FilterCheckResult:
    return check_filters(
        source_type="looker",
        config_dict={**_BASE, **recipe},
        kind=kind,
        parent_path=list(parent),
        names=names,
        attributes=attributes,
        try_allow=try_allow,
    )


def _reasons(result: FilterCheckResult) -> Dict[str, Optional[str]]:
    return {v.name: v.excluded_by for v in result.results}


def _dash(
    deleted: bool = False,
    personal: bool = False,
    path: Optional[str] = "Shared/Sales",
    path_allowed: Optional[bool] = True,
) -> Dict[str, str]:
    # The shape listing_from_run hands over: scalars as strings, None dropped.
    attrs = {"deleted": str(deleted).lower(), "folder_personal": str(personal).lower()}
    if path is not None:
        attrs["folder_path"] = path
    if path_allowed is not None:
        attrs["folder_path_allowed"] = str(path_allowed).lower()
    return attrs


def test_dashboard_pattern_matches_the_id_and_is_the_reported_field() -> None:
    result = _judge(
        "Dashboard",
        ["1", "2"],
        attributes=[_dash(), _dash()],
        dashboard_pattern={"deny": ["^2$"]},
    )
    assert result.pattern_field == "dashboard_pattern"
    assert result.filtering == "by_pattern"
    assert _reasons(result) == {"1": None, "2": "dashboard_pattern"}


def test_dashboard_rules_apply_in_ingestion_order() -> None:
    result = _judge(
        "Dashboard",
        ["deleted", "personal", "archived", "kept", "no-folder"],
        attributes=[
            _dash(deleted=True),
            _dash(personal=True, path=None),
            _dash(path="Shared/Archive"),
            _dash(),
            _dash(path=None, path_allowed=None),
        ],
        skip_personal_folders=True,
        folder_path_pattern={"deny": ["^Shared/Archive"]},
    )
    assert _reasons(result) == {
        "deleted": "include_deleted",
        "personal": "skip_personal_folders",
        "archived": "folder_path_pattern",
        "kept": None,
        "no-folder": None,
    }


def test_a_deleted_dashboard_is_kept_when_include_deleted_is_set() -> None:
    result = _judge(
        "Dashboard", ["4"], attributes=[_dash(deleted=True)], include_deleted=True
    )
    assert _reasons(result) == {"4": None}


def test_a_listed_folder_path_is_rejudged_against_the_recipe_being_checked() -> None:
    # The run recorded allowed=true, but this recipe denies the path: the
    # path wins, because it is what ingestion matches.
    result = _judge(
        "Dashboard",
        ["1"],
        attributes=[_dash(path="Shared/Sales", path_allowed=True)],
        folder_path_pattern={"deny": ["^Shared/Sales"]},
    )
    assert _reasons(result) == {"1": "folder_path_pattern"}


def test_a_withheld_personal_path_falls_back_to_the_recorded_flag() -> None:
    result = _judge(
        "Dashboard",
        ["3"],
        attributes=[_dash(personal=True, path=None, path_allowed=False)],
        folder_path_pattern={"deny": ["^Users/"]},
    )
    assert _reasons(result) == {"3": "folder_path_pattern"}


def test_a_bare_dashboard_name_is_judged_on_the_pattern_and_says_what_was_not() -> None:
    result = _judge("Dashboard", ["1"], skip_personal_folders=True)
    assert _reasons(result) == {"1": None}
    assert any("--from-run" in w for w in result.warnings)


def test_try_allow_reaches_the_dashboard_pattern_through_the_override() -> None:
    result = _judge(
        "Dashboard", ["1", "2"], attributes=[_dash(), _dash()], try_allow=["^1$"]
    )
    assert _reasons(result) == {"1": None, "2": "dashboard_pattern"}


def test_a_chart_is_judged_on_its_element_id_then_its_parse_then_its_type() -> None:
    result = _judge(
        "Look",
        ["11", "12", "13", "14"],
        parent=["1"],
        attributes=[
            {"type": "vis", "has_query": "true"},
            {"type": "text", "has_query": "false"},
            {"type": "text", "has_query": "true"},
            {"type": "vis", "has_query": "true"},
        ],
        chart_pattern={"deny": ["^14$"]},
    )
    assert result.pattern_field == "chart_pattern"
    assert _reasons(result) == {
        "11": None,
        "12": "element_has_no_query",
        "13": "element_type",
        "14": "chart_pattern",
    }


def test_charts_under_a_denied_dashboard_are_excluded_by_the_dashboard_pattern() -> (
    None
):
    result = _judge(
        "Look",
        ["21"],
        parent=["2"],
        attributes=[{"type": "vis", "has_query": "true"}],
        dashboard_pattern={"deny": ["^2$"]},
    )
    assert _reasons(result) == {"21": "dashboard_pattern"}


def test_standalone_looks_need_the_switch() -> None:
    result = _judge("Look", ["101"], attributes=[{"deleted": "false"}])
    assert _reasons(result) == {"101": "extract_independent_looks"}


def test_standalone_looks_ignore_chart_pattern_and_say_so() -> None:
    result = _judge(
        "Look",
        ["101", "102", "103", "104"],
        attributes=[
            {"deleted": "false", "has_query": "true", "folder_personal": "false"},
            {"deleted": "false", "has_query": "true", "folder_personal": "true"},
            {"deleted": "false", "has_query": "false", "folder_personal": "false"},
            {"deleted": "true", "has_query": "true", "folder_personal": "false"},
        ],
        chart_pattern={"deny": [".*"]},
        skip_personal_folders=True,
        **_LOOKS_ON,
    )
    assert _reasons(result) == {
        "101": None,
        "102": "skip_personal_folders",
        "103": "look_has_no_query",
        "104": "include_deleted",
    }
    assert any("chart_pattern" in w for w in result.warnings)
    assert any("on a dashboard" in w for w in result.warnings)


def test_a_look_whose_query_could_not_be_read_is_excluded_with_a_warning() -> None:
    # `looks` writes has_query null when its read-back failed, and
    # listing_from_run drops nulls; ingestion skips a look whose read raises.
    result = _judge(
        "Look",
        ["101", "107"],
        attributes=[
            {"deleted": "false", "has_query": "true", "folder_personal": "false"},
            {"deleted": "false", "folder_personal": "false"},
        ],
        **_LOOKS_ON,
    )
    assert _reasons(result) == {"101": None, "107": "look_query_unreadable"}
    assert any("could not be read" in w for w in result.warnings)


def test_used_explores_only_reports_explores_and_models_by_rule_with_a_warning() -> (
    None
):
    for kind, name in (("Explore", "orders"), ("LookML Model", "sales")):
        result = _judge(kind, [name])
        assert result.filtering == "by_rule"
        assert result.pattern_field == "emit_used_explores_only"
        assert _reasons(result) == {name: "emit_used_explores_only"}
        assert any("queries it" in w for w in result.warnings)


def test_every_explore_and_non_empty_model_is_included_when_not_used_only() -> None:
    explores = _judge(
        "Explore", ["orders", "unused"], parent=["sales"], emit_used_explores_only=False
    )
    assert _reasons(explores) == {"orders": None, "unused": None}
    models = _judge(
        "LookML Model",
        ["sales", "empty"],
        attributes=[{"explore_count": "3"}, {"explore_count": "0"}],
        emit_used_explores_only=False,
    )
    assert _reasons(models) == {"sales": None, "empty": "model_has_no_explores"}


def test_a_traced_explore_is_judged_on_whether_kept_content_queries_it() -> None:
    result = _judge(
        "Explore",
        ["orders", "unused", "untraced"],
        parent=["sales"],
        attributes=[{"used": "true"}, {"used": "false"}, {}],
    )
    assert _reasons(result) == {
        "orders": None,
        "unused": "emit_used_explores_only",
        "untraced": "emit_used_explores_only",
    }
    assert any("--trace-charts" in w for w in result.warnings)


def test_a_fully_traced_listing_carries_no_undetermined_warning() -> None:
    result = _judge(
        "LookML Model",
        ["sales", "empty"],
        attributes=[{"used": "true"}, {"used": "false"}],
    )
    assert _reasons(result) == {"sales": None, "empty": "emit_used_explores_only"}
    assert result.warnings == []


def test_a_traced_look_on_a_kept_dashboard_is_not_a_standalone_chart() -> None:
    look = {"deleted": "false", "has_query": "true", "folder_personal": "false"}
    result = _judge(
        "Look",
        ["105", "101"],
        attributes=[
            {**look, "on_kept_dashboard": "true"},
            {**look, "on_kept_dashboard": "false"},
        ],
        **_LOOKS_ON,
    )
    assert _reasons(result) == {"105": "on_a_kept_dashboard", "101": None}
    assert not any("on a dashboard" in w for w in result.warnings)


def _element(**dashboard: str) -> Dict[str, str]:
    facts = {
        "type": "vis",
        "has_query": "true",
        "dashboard_deleted": "false",
        "dashboard_folder_personal": "false",
        "dashboard_folder_path": "Shared/Sales",
        "dashboard_folder_path_allowed": "true",
    }
    facts.update(dashboard)
    return facts


def test_charts_of_a_dashboard_ingestion_drops_are_excluded_by_its_rule() -> None:
    cases = {
        "deleted": (_element(dashboard_deleted="true"), "include_deleted"),
        "personal": (
            _element(
                dashboard_folder_personal="true",
                dashboard_folder_path="",
                dashboard_folder_path_allowed="false",
            ),
            "skip_personal_folders",
        ),
        "archived": (
            _element(dashboard_folder_path="Shared/Archive"),
            "folder_path_pattern",
        ),
        "kept": (_element(), None),
    }
    for name, (attributes, reason) in cases.items():
        if name == "personal":
            attributes.pop("dashboard_folder_path")
        result = _judge(
            "Look",
            ["11"],
            parent=["1"],
            attributes=[attributes],
            skip_personal_folders=True,
            folder_path_pattern={"deny": ["^Shared/Archive"]},
        )
        assert _reasons(result) == {"11": reason}, name
        # The listing carried the dashboard's facts, so nothing is undetermined.
        assert result.warnings == [], name
