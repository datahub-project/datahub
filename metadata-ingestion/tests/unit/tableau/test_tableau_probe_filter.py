from typing import Dict, Sequence

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters

_BASE: Dict[str, object] = {
    "connect_uri": "https://tableau.example.com",
    "site": "my-site",
}


def _judge(
    kind: str, names: Sequence[str], parents: Sequence[str] = (), **config: object
) -> FilterCheckResult:
    return check_filters(
        source_type="tableau",
        config_dict={**_BASE, **config},
        kind=kind,
        parent_path=list(parents),
        names=list(names),
    )


def test_projects_are_matched_on_their_path() -> None:
    by_parent = _judge(
        "Project", ["EMEA"], ["Sales"], project_path_pattern={"allow": ["^Sales/EMEA$"]}
    )
    by_path = _judge(
        "Project", ["Sales/EMEA"], project_path_pattern={"allow": ["^Sales/EMEA$"]}
    )
    for result in (by_parent, by_path):
        assert result.pattern_field == "project_path_pattern"
        assert (result.results[0].target, result.results[0].included) == (
            "Sales/EMEA",
            True,
        )


def test_a_child_of_a_selected_project_is_included() -> None:
    result = _judge(
        "Project", ["Sales/APAC"], project_path_pattern={"allow": ["^Sales$"]}
    )
    assert result.results[0].included


def test_an_excluded_parent_does_not_exclude_an_allowed_child() -> None:
    result = _judge(
        "Project", ["EMEA"], ["Sales"], project_path_pattern={"allow": ["^Sales/EMEA$"]}
    )
    assert result.results[0].included
    assert not result.excluded_by_container


def test_a_workbook_follows_its_project() -> None:
    result = _judge(
        "Workbook",
        ["Revenue"],
        ["Sales/Archive"],
        project_path_pattern={"deny": ["^Sales/Archive$"]},
    )
    assert result.filtering == "unfiltered"
    assert (result.results[0].included, result.results[0].excluded_by) == (
        False,
        "project_path_pattern",
    )
    assert result.excluded_by_container


def test_try_allow_is_judged_against_project_path_pattern() -> None:
    # The recipe allows Sales; the hypothetical allows only Marketing.
    result = check_filters(
        source_type="tableau",
        config_dict={**_BASE, "project_path_pattern": {"allow": ["^Sales$"]}},
        kind="Project",
        parent_path=[],
        names=["Sales", "Marketing"],
        try_allow=["^Marketing$"],
    )
    assert [r.included for r in result.results] == [False, True]


def test_readmission_through_a_readmitted_parent_warns_about_listing_order() -> None:
    result = _judge(
        "Project", ["Sales/EMEA/UK"], project_path_pattern={"allow": ["^Sales$"]}
    )
    assert result.results[0].included
    assert any("listing order" in w for w in result.warnings)


def test_a_separator_inside_a_name_is_reported() -> None:
    result = _judge("Project", ["Q1/Q2"], project_path_pattern={"allow": ["^Q1/Q2$"]})
    assert result.results[0].included
    assert any("separator" in w for w in result.warnings)


def test_site_name_pattern_applies_only_with_multiple_sites() -> None:
    multi = _judge(
        "Site",
        ["Finance", "Sandbox"],
        ingest_multiple_sites=True,
        site_name_pattern={"deny": ["^Sandbox$"]},
    )
    assert [r.included for r in multi.results] == [True, False]
    assert multi.pattern_field == "site_name_pattern"

    single = _judge("Site", ["my-site", "other-site"])
    assert [(r.included, r.excluded_by) for r in single.results] == [
        (True, None),
        (False, "ingest_multiple_sites"),
    ]
    assert any("ingest_multiple_sites" in w for w in single.warnings)


def test_only_a_recipe_using_project_pattern_is_warned_about_it() -> None:
    # The Project verdict is computed for the --parent too, after the pattern
    # has matched something; a used allow-all pattern must still read as unset.
    path_only = _judge(
        "Workbook",
        ["Revenue"],
        ["Sales/Archive"],
        project_path_pattern={"deny": ["^Sales/Archive$"]},
    )
    assert not any("deprecated" in w for w in path_only.warnings)

    legacy = _judge("Project", ["Sales"], project_pattern={"allow": ["^Sales$"]})
    assert any("deprecated" in w for w in legacy.warnings)
