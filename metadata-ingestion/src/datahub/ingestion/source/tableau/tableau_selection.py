"""Which Tableau projects ingestion selects, as pure functions of the recipe.

Shared by TableauSiteSource (ingestion) and TableauConfig's probe hooks
(`probe filter`, which has no connection). The two must never disagree, so
neither restates the rule.
"""

from dataclasses import dataclass
from typing import Callable, List, Optional, Protocol, Sequence

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    Verdict,
    VerdictContext,
    pattern_verdict,
)

PROJECT_PATTERN = "project_pattern"
PROJECT_PATH_PATTERN = "project_path_pattern"
INGEST_MULTIPLE_SITES = "ingest_multiple_sites"
# Not a recipe field: ingestion skips a site that is not Active outright.
SITE_STATE = "site_state"
ACTIVE_SITE_STATE = "Active"


class ProjectFilterConfig(Protocol):
    # A Protocol so this module never imports tableau.py, which imports it.
    project_pattern: AllowDenyPattern
    project_path_pattern: AllowDenyPattern
    project_path_separator: str
    extract_project_hierarchy: bool


class SiteFilterConfig(Protocol):
    ingest_multiple_sites: bool
    site: str
    site_name_pattern: AllowDenyPattern


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
        raise ProbeArgumentError("a project path needs at least one project name")
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


def probe_project_verdict(
    config: ProjectFilterConfig,
    name: str,
    parent_path: Sequence[str],
    warn: Callable[[str], None],
) -> Verdict:
    """`probe filter`'s Project verdict: project_selection, plus the caveats a
    caller needs to read it the way ingestion will act on it."""
    segments = project_segments(config, name, parent_path)
    selection = project_selection(config, segments)
    literal = [*parent_path, name]
    if (
        len(segments) > len(literal)
        and project_selection(config, literal).included != selection.included
    ):
        # Splitting on the separator is right for the documented usage
        # (`--name Sales/EMEA`), and then both readings agree on the path
        # pattern. They part only through the per-segment rules: the hierarchy
        # re-admission and project_pattern's bare-name match. Warn just then,
        # so the warning is never noise an agent learns to skip.
        warn(
            f"read '{config.project_path_separator}' in '{name}' as the project "
            "path separator; this only matters if a project name itself "
            f"contains '{config.project_path_separator}', and here it changes "
            "the verdict. Set project_path_separator to a character no project "
            "name uses"
        )
    # Compared field by field: AllowDenyPattern.__eq__ compares __dict__, which
    # also holds the regexes it caches once it has matched anything, so a used
    # allow-all pattern compares unequal to a fresh one.
    default = AllowDenyPattern.allow_all()
    if (config.project_pattern.allow, config.project_pattern.deny) != (
        default.allow,
        default.deny,
    ):
        warn(
            "this recipe filters projects with the deprecated project_pattern "
            "(or projects), which is matched on the bare name or the path; "
            "--try-allow/--try-deny replace project_path_pattern, not it"
        )
    if selection.depends_on_rescued_parent:
        warn(
            "included only through extract_project_hierarchy, via a parent that "
            "is itself included only that way; ingestion re-admits these in one "
            "pass in Tableau's API listing order, so whether it is ingested can "
            "depend on that order"
        )
    if not selection.included:
        warn(
            "an excluded project can still appear in DataHub as an empty "
            "container when an included project below it needs it for its "
            "browse path; its workbooks are not ingested"
        )
    return Verdict(selection.included, selection.excluded_by, selection.target)


def probe_site_verdict(
    config: SiteFilterConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    """site_name_pattern applies only with ingest_multiple_sites, and then
    only to Active sites; without it ingestion reads the recipe's one site,
    chosen by content URL (TableauSource.get_workunits_internal). A saved
    `probe run sites` supplies each site's content_url and state."""
    if config.ingest_multiple_sites:
        if ctx.structural is not None:
            return None
        by_name = pattern_verdict(config, ctx.pattern_field, ctx.target)
        if not by_name.included:
            return by_name
        state = ctx.attributes.get("state")
        if state is None:
            ctx.warn(
                "sites whose state is not Active are skipped whatever "
                "site_name_pattern says, and no state was given here; save "
                "`probe run sites --report-to` and pass it with `probe filter "
                "--from-run` to judge it"
            )
        elif state != ACTIVE_SITE_STATE:
            return Verdict.exclude(SITE_STATE)
        return by_name
    content_url = ctx.attributes.get("content_url")
    if content_url is None:
        ctx.warn(
            "ingest_multiple_sites is off, so site_name_pattern is not applied: "
            f"ingestion reads only the site whose content URL is '{config.site}'. "
            "Judge a site by its content_url here"
        )
        content_url = ctx.name
    if content_url == config.site:
        return Verdict.include()
    return Verdict.exclude(INGEST_MULTIPLE_SITES)
