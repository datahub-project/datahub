import json
import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional

from google.api_core import retry
from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.cloud import resourcemanager_v3
from google.oauth2.credentials import Credentials

from datahub.configuration.common import AllowDenyPattern

logger = logging.getLogger(__name__)


@contextmanager
def temporary_credentials_file(credentials_dict: Dict[str, Any]) -> Iterator[str]:
    """
    Context manager for temporary GCP credentials file.

    Creates a temporary JSON credentials file that is automatically cleaned up
    when exiting the context, even on exceptions. This is more reliable than
    atexit for containerized environments where SIGKILL may bypass cleanup.

    Args:
        credentials_dict: Dictionary containing GCP service account credentials

    Yields:
        Path to the temporary credentials file

    Example:
        with temporary_credentials_file(creds_dict) as cred_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred_path
            # ... do work ...
        # File is automatically deleted here
    """
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".json",
        prefix="gcp_creds_",
    )

    try:
        json.dump(credentials_dict, temp_file)
        temp_file.flush()
        temp_file.close()
        logger.debug("Created temporary credentials file: %s", temp_file.name)
        yield temp_file.name
    finally:
        try:
            os.unlink(temp_file.name)
            logger.debug("Cleaned up temporary credentials file: %s", temp_file.name)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(
                "Failed to cleanup credentials file %s: %s", temp_file.name, e
            )


PERMISSION_DENIED_ERROR_MSG = (
    "Permission denied when listing GCP projects. "
    "Ensure the service account has the 'resourcemanager.projects.list' permission "
    "(included in roles/browser or roles/viewer at organization/folder level), "
    "or specify project_ids explicitly in the configuration."
)


def _format_transient_error(error_type: str, exc: Exception, debug_cmd: str) -> str:
    if error_type == "timeout":
        return (
            f"GCP API request timed out: {exc}. "
            "The Resource Manager API did not respond in time. "
            "Possible causes: (1) Network latency, (2) GCP service degradation, "
            "(3) Too many projects to list. "
            f"Action: Retry the ingestion or specify project_ids explicitly. Debug: {debug_cmd}"
        )
    return (
        f"GCP service temporarily unavailable: {exc}. "
        "The Resource Manager API is experiencing issues. "
        "Action: Wait a few minutes and retry the ingestion. "
        f"Check GCP status: https://status.cloud.google.com. Debug: {debug_cmd}"
    )


class GCPProjectDiscoveryError(Exception):
    pass


def _handle_discovery_error(
    exc: GoogleAPICallError,
    context: str,
    debug_cmd: str,
) -> GCPProjectDiscoveryError:
    if isinstance(exc, PermissionDenied):
        return GCPProjectDiscoveryError(PERMISSION_DENIED_ERROR_MSG)
    if isinstance(exc, DeadlineExceeded):
        return GCPProjectDiscoveryError(
            _format_transient_error("timeout", exc, debug_cmd)
        )
    if isinstance(exc, ServiceUnavailable):
        return GCPProjectDiscoveryError(
            _format_transient_error("unavailable", exc, debug_cmd)
        )
    return GCPProjectDiscoveryError(
        f"GCP API error {context}: {exc}. Debug: {debug_cmd}"
    )


def _validate_and_filter_projects(
    projects: List["GCPProject"],
    pattern: AllowDenyPattern,
    source_description: str,
) -> List["GCPProject"]:
    filtered = _filter_projects_by_pattern(projects, pattern)

    if not filtered:
        raise GCPProjectDiscoveryError(
            f"Found {len(projects)} project(s) via {source_description}, but all were "
            "excluded by project_id_pattern. Adjust your allow/deny patterns."
        )

    logger.info(
        "Found %d projects via %s, %d match project_id_pattern",
        len(projects),
        source_description,
        len(filtered),
    )
    return filtered


@dataclass
class GCPProject:
    id: str
    name: str


def get_projects_client(
    credentials: Optional[Credentials] = None,
) -> resourcemanager_v3.ProjectsClient:
    return resourcemanager_v3.ProjectsClient(credentials=credentials)


def _is_rate_limit_error(exc: BaseException) -> bool:
    if isinstance(exc, ResourceExhausted):
        logger.debug("Rate limit hit for projects API, retrying...")
        return True

    if isinstance(exc, GoogleAPICallError):
        exc_message_lower = str(exc).lower()
        if "quota" in exc_message_lower or "rate limit" in exc_message_lower:
            logger.debug("Rate limit hit (via error message), retrying...")
            return True

    return False


def _filter_projects_by_pattern(
    projects: List[GCPProject],
    pattern: AllowDenyPattern,
) -> List[GCPProject]:
    filtered = []
    excluded = []
    for p in projects:
        if pattern.allowed(p.id):
            filtered.append(p)
        else:
            excluded.append(p.id)
    if excluded:
        logger.info(
            "Filtered out %d project(s) by project_id_pattern: %s",
            len(excluded),
            ", ".join(excluded[:10]) + ("..." if len(excluded) > 10 else ""),
        )
    return filtered


def _search_projects_with_retry(
    client: resourcemanager_v3.ProjectsClient,
    query: str,
) -> Iterable[GCPProject]:
    search_with_retry = retry.Retry(
        predicate=_is_rate_limit_error,
        initial=1.0,
        maximum=60.0,
        multiplier=2.0,
        timeout=300.0,
    )(client.search_projects)

    request = resourcemanager_v3.SearchProjectsRequest(query=query)
    for project in search_with_retry(request=request):
        if not project.project_id:
            logger.error(
                "GCP returned project without project_id (display_name=%s). "
                "This is unexpected - please report this issue.",
                project.display_name or "unknown",
            )
            continue
        yield GCPProject(
            id=project.project_id,
            name=project.display_name or project.project_id,
        )


def list_all_accessible_projects(
    client: resourcemanager_v3.ProjectsClient,
) -> Iterable[GCPProject]:
    debug_cmd = "gcloud projects list --filter='lifecycleState:ACTIVE'"
    try:
        yield from _search_projects_with_retry(client, "state:ACTIVE")
    except GoogleAPICallError as e:
        raise _handle_discovery_error(e, "when listing all projects", debug_cmd) from e


def get_projects_from_explicit_list(
    project_ids: List[str],
    project_id_pattern: Optional[AllowDenyPattern] = None,
) -> List[GCPProject]:
    pattern = project_id_pattern or AllowDenyPattern.allow_all()
    projects = [GCPProject(id=pid, name=pid) for pid in project_ids]
    filtered = _filter_projects_by_pattern(projects, pattern)
    logger.info(
        "Using %d projects from explicit list (filtered from %d configured)",
        len(filtered),
        len(project_ids),
    )
    return filtered


def get_projects_by_labels(
    labels: List[str],
    client: resourcemanager_v3.ProjectsClient,
    project_id_pattern: Optional[AllowDenyPattern] = None,
) -> List[GCPProject]:
    if not labels:
        raise GCPProjectDiscoveryError(
            "project_labels cannot be empty. Provide at least one label."
        )

    pattern = project_id_pattern or AllowDenyPattern.allow_all()

    label_queries = []
    for label in labels:
        if ":" in label:
            label_queries.append(f"labels.{label}")
        else:
            label_queries.append(f"labels.{label}:*")
    query = f"state:ACTIVE AND ({' OR '.join(label_queries)})"

    logger.debug("Discovering projects with labels query: %s", query)

    first_label = labels[0] if labels else "LABEL"
    debug_cmd = f"gcloud projects list --filter='labels.{first_label}:*'"

    try:
        projects = list(_search_projects_with_retry(client, query))

        if not projects:
            raise GCPProjectDiscoveryError(
                f"No projects match labels {labels}. Verify: "
                "(1) labels exist on target projects, "
                "(2) service account has 'resourcemanager.projects.list' permission, "
                "(3) projects are in ACTIVE state. "
                f"Debug: {debug_cmd}"
            )

        return _validate_and_filter_projects(
            projects, pattern, f"label search {labels}"
        )

    except GoogleAPICallError as e:
        raise _handle_discovery_error(
            e, f"when searching projects by labels {labels}", debug_cmd
        ) from e


def get_projects(
    project_ids: Optional[List[str]] = None,
    project_labels: Optional[List[str]] = None,
    project_id_pattern: Optional[AllowDenyPattern] = None,
    client: Optional[resourcemanager_v3.ProjectsClient] = None,
    credentials: Optional[Credentials] = None,
) -> List[GCPProject]:
    pattern = project_id_pattern or AllowDenyPattern.allow_all()

    if project_ids:
        return get_projects_from_explicit_list(project_ids, pattern)

    if client is None:
        client = get_projects_client(credentials)

    if project_labels:
        return get_projects_by_labels(project_labels, client, pattern)

    logger.info(
        "No project_ids or project_labels specified. Discovering all accessible GCP projects..."
    )

    discovered_projects = list(list_all_accessible_projects(client))

    if not discovered_projects:
        raise GCPProjectDiscoveryError(
            "No projects discovered via auto-discovery. Verify: "
            "(1) service account has 'resourcemanager.projects.list' permission, "
            "(2) there are ACTIVE projects accessible to the service account. "
            "Debug: gcloud projects list --filter='lifecycleState:ACTIVE'"
        )

    return _validate_and_filter_projects(discovered_projects, pattern, "auto-discovery")
