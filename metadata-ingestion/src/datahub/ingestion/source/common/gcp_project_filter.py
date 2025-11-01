import logging
from typing import FrozenSet, List, Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.source import SourceReport

logger = logging.getLogger(__name__)


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


class GcpProjectFilter:
    def __init__(
        self, filter_config: GcpProjectFilterConfig, report: SourceReport
    ) -> None:
        self.filter_config = filter_config
        self.report = report

    def is_project_allowed(self, project_id: str) -> bool:
        if self.filter_config.project_ids:
            return project_id in self.filter_config.project_ids
        return self.filter_config.project_id_pattern.allowed(project_id)


def _search_projects_by_labels(labels: FrozenSet[str]) -> List[str]:
    from google.cloud import resourcemanager_v3

    projects_client = resourcemanager_v3.ProjectsClient()
    labels_query = " OR ".join([f"labels.{label}" for label in labels])
    project_ids: List[str] = []

    for project in projects_client.search_projects(query=labels_query):
        # project.project_id is the string id we want
        if getattr(project, "project_id", None):
            project_ids.append(project.project_id)

    return project_ids


def _list_all_projects() -> List[str]:
    from google.cloud import resourcemanager_v3

    projects_client = resourcemanager_v3.ProjectsClient()
    project_ids: List[str] = []
    for project in projects_client.list_projects():
        if getattr(project, "project_id", None):
            project_ids.append(project.project_id)
    return project_ids


def resolve_gcp_projects(
    filter_config: GcpProjectFilterConfig, report: Optional[SourceReport] = None
) -> List[str]:
    """
    Resolve a list of GCP project ids based on filter configuration.

    Precedence:
      1) project_ids (explicit)
      2) project_labels (via Cloud Resource Manager search)
      3) list all projects, then apply project_id_pattern
    """
    try:
        if filter_config.project_ids:
            return list(filter_config.project_ids)

        if filter_config.project_labels:
            labeled = _search_projects_by_labels(
                frozenset(filter_config.project_labels)
            )
            allowed = [
                p for p in labeled if filter_config.project_id_pattern.allowed(p)
            ]
            if not allowed and report:
                report.report_failure(
                    "metadata-extraction",
                    "No projects matched provided labels after applying project_id_pattern.",
                )
            return allowed

        # No explicit ids or labels; list all projects and apply pattern
        all_projects = _list_all_projects()
        allowed = [
            p for p in all_projects if filter_config.project_id_pattern.allowed(p)
        ]
        if not allowed and report:
            report.failure(
                title="No GCP projects resolved",
                message=(
                    "Could not resolve any GCP projects. Ensure Resource Manager permissions or adjust filters."
                ),
            )
        return allowed
    except Exception as e:
        logger.error("Failed to resolve GCP projects", exc_info=True)
        if report:
            report.failure(
                title="Failed to resolve GCP projects",
                message="Error while resolving GCP projects via Resource Manager",
                exc=e,
            )
        return []
