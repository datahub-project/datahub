import logging
import re
from typing import Annotated, FrozenSet, Iterator, List, Optional, Protocol

from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import GoogleAuthError
from google.cloud.resourcemanager_v3 import ProjectsClient
from pydantic import BaseModel, Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

logger = logging.getLogger(__name__)

# GCP project IDs: 6-30 chars, lowercase letters/digits/hyphens, start with letter, end with letter/digit
GCP_PROJECT_ID_PATTERN = re.compile(r"^[a-z][-a-z0-9]{4,28}[a-z0-9]$")
# GCP label format: key:value, both lowercase letters/digits/underscores/hyphens
GCP_LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*:[a-z0-9_-]+$")


class GCPValidationError(ValueError):
    """Raised when GCP configuration values fail format validation."""

    pass


def validate_project_id_list(
    project_ids: List[str], *, allow_empty: bool = True
) -> None:
    """Validate a list of GCP project IDs for format, duplicates, and empty values."""
    if not project_ids:
        if not allow_empty:
            raise GCPValidationError(
                "project_ids cannot be an empty list. "
                "Either specify project IDs or omit the field to use auto-discovery."
            )
        return

    empty_values = [pid for pid in project_ids if not pid.strip()]
    if empty_values:
        raise GCPValidationError(
            "project_ids contains empty values. "
            "Remove empty strings or omit project_ids to use auto-discovery."
        )

    seen: set = set()
    duplicates: List[str] = []
    for pid in project_ids:
        if pid in seen:
            duplicates.append(pid)
        else:
            seen.add(pid)
    if duplicates:
        raise GCPValidationError(f"project_ids contains duplicates: {duplicates}")

    invalid = [pid for pid in project_ids if not GCP_PROJECT_ID_PATTERN.match(pid)]
    if invalid:
        raise GCPValidationError(
            f"Invalid project_ids format: {invalid}. "
            "Must be 6-30 chars, lowercase letters/numbers/hyphens, "
            "start with letter, end with letter or number."
        )


def validate_project_label_list(project_labels: List[str]) -> None:
    """Validate a list of GCP project labels for format and duplicates."""
    if not project_labels:
        return

    empty_values = [label for label in project_labels if not label.strip()]
    if empty_values:
        raise GCPValidationError("project_labels contains empty values.")

    seen: set = set()
    duplicates: List[str] = []
    for label in project_labels:
        if label in seen:
            duplicates.append(label)
        else:
            seen.add(label)
    if duplicates:
        raise GCPValidationError(f"project_labels contains duplicates: {duplicates}")

    invalid = [label for label in project_labels if not GCP_LABEL_PATTERN.match(label)]
    if invalid:
        raise GCPValidationError(
            f"Invalid project_labels format: {invalid}. "
            "Must be 'key:value' format with lowercase letters, digits, underscores, or hyphens. "
            "Example: env:prod"
        )


class GcpProject(BaseModel):
    id: str
    name: str


class GcpProjectFilterConfig(ConfigModel):
    project_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Explicit list of GCP project ids to ingest. Overrides project_id_pattern."
        ),
    )
    project_labels: List[str] = Field(
        default_factory=list,
        description=(
            "Filter projects by labels in `key:value` format. Applied before project_id_pattern."
        ),
    )
    # Declared here rather than per connector: every consumer filters GCP
    # projects on this field (is_project_allowed), and "Project" is the
    # container subtype they emit. A connector redeclaring the field would drop
    # the annotation, since pydantic v2 replaces inherited field metadata.
    project_id_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.BIGQUERY_PROJECT)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny pattern for GCP project ids.",
    )


class ProjectFilterProtocol(Protocol):
    """Protocol for filter configs that support project filtering."""

    project_ids: List[str]
    project_id_pattern: AllowDenyPattern


PROJECT_IDS = "project_ids"
PROJECT_ID_PATTERN = "project_id_pattern"


def project_rejected_by(
    filter_config: ProjectFilterProtocol, project_id: str
) -> Optional[str]:
    """The field that excludes a GCP project, or None when it is allowed.

    An explicit project_ids list replaces project_id_pattern outright; only
    without one does the pattern decide. The one statement of that
    precedence, for ingestion (is_project_allowed) and `probe filter`.
    """
    if filter_config.project_ids:
        return None if project_id in filter_config.project_ids else PROJECT_IDS
    if filter_config.project_id_pattern.allowed(project_id):
        return None
    return PROJECT_ID_PATTERN


def is_project_allowed(filter_config: ProjectFilterProtocol, project_id: str) -> bool:
    """Whether a GCP project passes project_ids, else project_id_pattern
    (project_rejected_by). Works with any config carrying both fields,
    GcpProjectFilterConfig and BigQueryFilterConfig included."""
    return project_rejected_by(filter_config, project_id) is None


def iter_projects(
    projects_client: ProjectsClient, query: Optional[str] = None
) -> Iterator[GcpProject]:
    """Projects from Resource Manager `search_projects`, one page at a time, so
    a caller that needs only the first few stops the pager early."""
    pages = (
        projects_client.search_projects(query=query)
        if query is not None
        else projects_client.search_projects()
    )
    for project in pages:
        if getattr(project, "project_id", None):
            display_name = getattr(project, "display_name", None)
            yield GcpProject(
                id=project.project_id,
                name=display_name if display_name else project.project_id,
            )


def iter_projects_by_labels(
    labels: FrozenSet[str], projects_client: Optional[ProjectsClient] = None
) -> Iterator[GcpProject]:
    if projects_client is None:
        projects_client = ProjectsClient()
    labels_query = " OR ".join([f"labels.{label}" for label in labels])
    return iter_projects(projects_client, labels_query)


def iter_all_projects(
    projects_client: Optional[ProjectsClient] = None,
) -> Iterator[GcpProject]:
    if projects_client is None:
        projects_client = ProjectsClient()
    return iter_projects(projects_client)


def iter_candidate_projects(
    filter_config: GcpProjectFilterConfig,
    projects_client: Optional[ProjectsClient] = None,
) -> Iterator[GcpProject]:
    """The projects resolve_gcp_projects chooses from, before
    project_id_pattern: exactly project_ids when set (Resource Manager is not
    called), else the projects carrying any of project_labels, else every
    project the credential can see. Lazy, so a caller wanting the first few
    stops the pager early."""
    if filter_config.project_ids:
        return iter(GcpProject(id=pid, name=pid) for pid in filter_config.project_ids)
    if filter_config.project_labels:
        return iter_projects_by_labels(
            frozenset(filter_config.project_labels), projects_client
        )
    return iter_all_projects(projects_client)


def resolve_gcp_projects(
    filter_config: GcpProjectFilterConfig,
    report: SourceReport,
    projects_client: Optional[ProjectsClient] = None,
) -> List[GcpProject]:
    """
    Resolve a list of GCP project ids based on filter configuration.

    Precedence:
      1) project_ids (explicit)
      2) project_labels (via Cloud Resource Manager search)
      3) search all visible projects, then apply project_id_pattern
    """
    try:
        if filter_config.project_ids:
            resolved = [
                GcpProject(id=pid, name=pid) for pid in filter_config.project_ids
            ]
            report.info(
                title="GCP Project Filter",
                message="Resolved GCP projects via explicit project_ids",
                context=str([p.id for p in resolved]),
            )
            return resolved

        if filter_config.project_labels:
            labeled = list(iter_candidate_projects(filter_config, projects_client))
            allowed = [p for p in labeled if is_project_allowed(filter_config, p.id)]
            if not allowed:
                report.warning(
                    title="Project Filter",
                    message="No projects matched provided labels after applying project_id_pattern.",
                )
            report.info(
                title="GCP Project Filter",
                message="Resolved GCP projects via project_labels and project_id_pattern",
                context=str([p.id for p in allowed]),
            )
            return allowed

        all_projects = list(iter_candidate_projects(filter_config, projects_client))
        allowed = [p for p in all_projects if is_project_allowed(filter_config, p.id)]
        if not allowed:
            report.failure(
                title="No GCP projects resolved",
                message=(
                    "Could not resolve any GCP projects. Ensure Resource Manager permissions or adjust filters."
                ),
            )
        report.info(
            title="GCP Project Filter",
            message="Resolved GCP projects via project_id_pattern",
            context=str([p.id for p in allowed]),
        )
        return allowed
    except (GoogleAPICallError, GoogleAuthError) as e:
        logger.error("Failed to resolve GCP projects", exc_info=True)
        report.failure(
            title="Failed to resolve GCP projects",
            message="Error while resolving GCP projects via Resource Manager",
            exc=e,
        )
        return []
