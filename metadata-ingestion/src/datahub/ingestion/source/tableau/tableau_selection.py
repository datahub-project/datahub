"""Which Tableau projects ingestion selects, as pure functions of the recipe.

Shared by TableauSiteSource (ingestion) and TableauConfig's probe hooks
(`probe filter`, which has no connection). The two must never disagree, so
neither restates the rule.
"""

from dataclasses import dataclass
from typing import List, Optional, Protocol, Sequence

from datahub.configuration.common import AllowDenyPattern

PROJECT_PATTERN = "project_pattern"
PROJECT_PATH_PATTERN = "project_path_pattern"


class ProjectFilterConfig(Protocol):
    # A Protocol so this module never imports tableau.py, which imports it.
    project_pattern: AllowDenyPattern
    project_path_pattern: AllowDenyPattern
    project_path_separator: str
    extract_project_hierarchy: bool


@dataclass(frozen=True)
class ProjectSelection:
    included: bool
    excluded_by: Optional[str]
    # The separator-joined path, the string project_path_pattern is matched on.
    target: str
    # Re-admitted through a parent that was itself only re-admitted.
    # _init_tableau_project_registry re-admits in one pass in API listing order,
    # so ingestion includes such a project only if it happened to visit the
    # parent first.
    depends_on_rescued_parent: bool


def is_project_allowed(config: ProjectFilterConfig, name: str, path: str) -> bool:
    # project_pattern is deprecated but still honoured, on the name OR the path.
    return (
        config.project_pattern.allowed(name) or config.project_pattern.allowed(path)
    ) and config.project_path_pattern.allowed(path)


def is_project_denied(config: ProjectFilterConfig, name: str, path: str) -> bool:
    """An explicit deny, as opposed to not being allowed.

    extract_project_hierarchy re-admits a child of a selected project unless it
    is denied outright. With allow ["A"] and deny ["B"], a sibling C of B under
    A is not in the allow list but is not denied either, so it is still
    ingested. Checking allowed() alone would drop it.
    """
    return config.project_pattern.denied(name) or config.project_path_pattern.denied(
        path
    )


def project_segments(
    config: ProjectFilterConfig, name: str, parent_path: Sequence[str]
) -> List[str]:
    """The project names from the root, however the caller spelled them.

    Accepts `--parent Sales --name EMEA` and `--name Sales/EMEA` alike, because
    `projects` and `workbooks` address a project by its joined path. A name that
    itself contains the separator is ambiguous here. It is equally ambiguous to
    project_path_pattern, which matches the same joined string.
    """
    separator = config.project_path_separator
    parts = [*parent_path, name]
    if not separator:
        return parts
    return [segment for part in parts for segment in part.split(separator)]


def _excluded_by(config: ProjectFilterConfig, path: str) -> str:
    if not config.project_path_pattern.allowed(path):
        return PROJECT_PATH_PATTERN
    return PROJECT_PATTERN


def project_selection(
    config: ProjectFilterConfig, segments: Sequence[str]
) -> ProjectSelection:
    """Ingestion's verdict for the project at the end of `segments`, walking
    _init_tableau_project_registry's rule down from the root: allowed on its
    own, or re-admitted under an included parent unless explicitly denied."""
    if not segments:
        raise ValueError("a project path needs at least one project name")
    separator = config.project_path_separator
    included = rescued = depends = False
    parent_included = parent_rescued = False
    path = ""
    for depth in range(1, len(segments) + 1):
        name = segments[depth - 1]
        path = separator.join(segments[:depth])
        if is_project_allowed(config, name, path):
            included, rescued = True, False
        elif (
            config.extract_project_hierarchy
            and parent_included
            and not is_project_denied(config, name, path)
        ):
            included, rescued = True, True
        else:
            included, rescued = False, False
        depends = rescued and parent_rescued
        parent_included, parent_rescued = included, rescued
    return ProjectSelection(
        included=included,
        excluded_by=None if included else _excluded_by(config, path),
        target=path,
        depends_on_rescued_parent=depends,
    )
