import logging
from typing import FrozenSet, List, Optional, Protocol

from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import GoogleAuthError
from google.cloud.resourcemanager_v3 import ProjectsClient
from pydantic import BaseModel, Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.source import SourceReport

logger = logging.getLogger(__name__)


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
    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny pattern for GCP project ids.",
    )


class ProjectFilterProtocol(Protocol):
    """Protocol for filter configs that support project filtering."""

    project_ids: List[str]
    project_id_pattern: AllowDenyPattern


def is_project_allowed(filter_config: ProjectFilterProtocol, project_id: str) -> bool:
    """
    Check if a GCP project is allowed based on filter configuration.

    This function works with any config that has project_ids and project_id_pattern fields,
    including GcpProjectFilterConfig and BigQueryFilterConfig.

    Args:
        filter_config: Configuration containing project_ids list or project_id_pattern
        project_id: The GCP project ID to check

    Returns:
        True if the project is allowed, False otherwise

    Logic:
        - If project_ids is specified, checks if project_id is in the list
        - Otherwise, checks if project_id matches the project_id_pattern
    """
    if filter_config.project_ids:
        return project_id in filter_config.project_ids
    return filter_config.project_id_pattern.allowed(project_id)


def _search_projects_by_labels(
    labels: FrozenSet[str], projects_client: Optional[ProjectsClient] = None
) -> List[GcpProject]:
    """
    Search for GCP projects matching any of the provided labels.

    Note: GCP API errors are caught by the calling function (resolve_gcp_projects)
    which wraps all project resolution logic in a try-except block.
    """
    if projects_client is None:
        projects_client = ProjectsClient()
    labels_query = " OR ".join([f"labels.{label}" for label in labels])
    projects: List[GcpProject] = []

    for project in projects_client.search_projects(query=labels_query):
        if getattr(project, "project_id", None):
            display_name = getattr(project, "display_name", None)
            projects.append(
                GcpProject(
                    id=project.project_id,
                    name=display_name if display_name else project.project_id,
                )
            )

    return projects


def _list_all_projects(
    projects_client: Optional[ProjectsClient] = None,
) -> List[GcpProject]:
    if projects_client is None:
        projects_client = ProjectsClient()
    projects: List[GcpProject] = []
    for project in projects_client.list_projects():
        if getattr(project, "project_id", None):
            display_name = getattr(project, "display_name", None)
            projects.append(
                GcpProject(
                    id=project.project_id,
                    name=display_name if display_name else project.project_id,
                )
            )
    return projects


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
      3) list all projects, then apply project_id_pattern
    """
    try:
        if filter_config.project_ids:
            return [GcpProject(id=pid, name=pid) for pid in filter_config.project_ids]

        if filter_config.project_labels:
            labeled = _search_projects_by_labels(
                frozenset(filter_config.project_labels), projects_client
            )
            allowed = [p for p in labeled if is_project_allowed(filter_config, p.id)]
            if not allowed:
                report.warning(
                    title="Project Filter",
                    message="No projects matched provided labels after applying project_id_pattern.",
                )
            return allowed

        all_projects = _list_all_projects(projects_client)
        allowed = [p for p in all_projects if is_project_allowed(filter_config, p.id)]
        if not allowed:
            report.failure(
                title="No GCP projects resolved",
                message=(
                    "Could not resolve any GCP projects. Ensure Resource Manager permissions or adjust filters."
                ),
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
