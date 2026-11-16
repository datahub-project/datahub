from typing import Dict, List, Optional

from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauProject,
    TableauSiteSource,
)
from datahub.ingestion.source.tableau.tableau_selection import (
    project_segments,
    project_selection,
)


def _config(**overrides: object) -> TableauConfig:
    return TableauConfig.model_validate(
        {"connect_uri": "https://tableau.example.com", **overrides}
    )


def test_a_child_of_an_allowed_project_is_readmitted_through_the_hierarchy() -> None:
    selection = project_selection(
        _config(project_path_pattern={"allow": ["^Sales$"]}), ["Sales", "EMEA"]
    )
    assert (selection.included, selection.excluded_by, selection.target) == (
        True,
        None,
        "Sales/EMEA",
    )


def test_the_hierarchy_does_not_readmit_an_explicitly_denied_child() -> None:
    config = _config(
        project_path_pattern={"allow": ["^Sales$"], "deny": ["^Sales/Archive$"]}
    )
    selection = project_selection(config, ["Sales", "Archive"])
    assert (selection.included, selection.excluded_by) == (
        False,
        "project_path_pattern",
    )


def test_without_extract_project_hierarchy_a_child_needs_its_own_match() -> None:
    config = _config(
        project_path_pattern={"allow": ["^Sales$"]}, extract_project_hierarchy=False
    )
    assert not project_selection(config, ["Sales", "EMEA"]).included


def test_an_allowed_child_is_included_under_an_excluded_parent() -> None:
    config = _config(project_path_pattern={"allow": ["^Sales/EMEA$"]})
    assert not project_selection(config, ["Sales"]).included
    assert project_selection(config, ["Sales", "EMEA"]).included


def test_the_deprecated_project_pattern_matches_the_bare_name() -> None:
    config = _config(project_pattern={"allow": ["^EMEA$"]})
    assert project_selection(config, ["Sales", "EMEA"]).included
    apac = project_selection(config, ["Sales", "APAC"])
    assert (apac.included, apac.excluded_by) == (False, "project_pattern")


def test_the_legacy_projects_list_is_judged_as_project_pattern() -> None:
    config = _config(projects=["Sales"])
    assert project_selection(config, ["Sales"]).included
    assert project_selection(config, ["Marketing"]).excluded_by == "project_pattern"


def test_a_custom_separator_builds_the_target() -> None:
    config = _config(
        project_path_separator="|", project_path_pattern={"allow": [r"^Sales\|EMEA$"]}
    )
    selection = project_selection(config, ["Sales", "EMEA"])
    assert (selection.included, selection.target) == (True, "Sales|EMEA")


def test_readmission_through_a_readmitted_parent_is_flagged() -> None:
    config = _config(project_path_pattern={"allow": ["^Sales$"]})
    assert not project_selection(config, ["Sales", "EMEA"]).depends_on_rescued_parent
    assert project_selection(config, ["Sales", "EMEA", "UK"]).depends_on_rescued_parent


def test_segments_split_parents_and_name_on_the_separator() -> None:
    config = _config()
    assert project_segments(config, "EMEA", ["Sales"]) == ["Sales", "EMEA"]
    assert project_segments(config, "Sales/EMEA", []) == ["Sales", "EMEA"]


def test_an_empty_separator_judges_the_name_as_one_segment() -> None:
    config = _config(project_path_separator="")
    assert project_segments(config, "Sales/EMEA", []) == ["Sales/EMEA"]


def _project(
    pid: str, name: str, parent: Optional[str], path: List[str]
) -> TableauProject:
    return TableauProject(
        id=pid,
        name=name,
        description=None,
        parent_id=parent,
        parent_name=None,
        path=path,
    )


def test_selection_agrees_with_the_ingestion_registry() -> None:
    """The probe and ingestion must give one answer for the same projects.
    Depth <= 2, because deeper re-admission depends on listing order in
    ingestion (see depends_on_rescued_parent)."""
    projects: Dict[str, TableauProject] = {
        "1": _project("1", "Sales", None, ["Sales"]),
        "2": _project("2", "EMEA", "1", ["Sales", "EMEA"]),
        "3": _project("3", "Archive", "1", ["Sales", "Archive"]),
        "4": _project("4", "Marketing", None, ["Marketing"]),
        "5": _project("5", "Q3", "4", ["Marketing", "Q3"]),
    }
    config = _config(
        project_path_pattern={
            "allow": ["^Sales$", "^Marketing/Q3$"],
            "deny": ["^Sales/Archive$"],
        }
    )
    site = TableauSiteSource.__new__(TableauSiteSource)
    site.config = config
    site._init_tableau_project_registry(projects)

    for pid, project in projects.items():
        assert project_selection(config, project.path).included == (
            pid in site.tableau_project_registry
        ), project.path
