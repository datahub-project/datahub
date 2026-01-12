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


class GCPProjectDiscoveryError(Exception):
    """Exception for project discovery failures with rich context and debug commands."""

    @classmethod
    def permission_denied(cls, debug_cmd: str) -> "GCPProjectDiscoveryError":
        """Permission denied when listing projects."""
        return cls(
            "Permission denied when listing GCP projects. "
            "Ensure the service account has the 'resourcemanager.projects.list' permission "
            "(included in roles/browser or roles/viewer at organization/folder level), "
            f"or specify project_ids explicitly in the configuration. Debug: {debug_cmd}"
        )

    @classmethod
    def timeout(cls, exc: Exception, debug_cmd: str) -> "GCPProjectDiscoveryError":
        """API request timed out."""
        return cls(
            f"GCP API request timed out: {exc}. "
            "The Resource Manager API did not respond in time. "
            "Possible causes: (1) Network latency, (2) GCP service degradation, "
            "(3) Too many projects to list. "
            f"Action: Retry the ingestion or specify project_ids explicitly. Debug: {debug_cmd}"
        )

    @classmethod
    def service_unavailable(
        cls, exc: Exception, debug_cmd: str
    ) -> "GCPProjectDiscoveryError":
        """GCP service temporarily unavailable."""
        return cls(
            f"GCP service temporarily unavailable: {exc}. "
            "The Resource Manager API is experiencing issues. "
            "Action: Wait a few minutes and retry the ingestion. "
            f"Check GCP status: https://status.cloud.google.com. Debug: {debug_cmd}"
        )

    @classmethod
    def rate_limit_exceeded(
        cls, exc: Exception, debug_cmd: str
    ) -> "GCPProjectDiscoveryError":
        """Rate limit exceeded."""
        return cls(
            f"Rate limit exceeded: {exc}. "
            "Too many API requests. Retry after a few minutes. "
            f"Debug: {debug_cmd}"
        )

    @classmethod
    def no_projects_found(
        cls,
        source: str,
        debug_cmd: str,
        include_label_check: bool = False,
    ) -> "GCPProjectDiscoveryError":
        """No projects discovered from the specified source."""
        checks = []
        if include_label_check:
            checks.append("labels exist on target projects")
        checks.extend(
            [
                "service account has 'resourcemanager.projects.list' permission",
                "there are ACTIVE projects accessible to the service account",
            ]
        )
        checks_str = ", ".join(f"({i + 1}) {check}" for i, check in enumerate(checks))
        return cls(
            f"No projects discovered via {source}. Verify: {checks_str}. Debug: {debug_cmd}"
        )

    @classmethod
    def all_filtered_out(
        cls,
        total_found: int,
        source: str,
        pattern: AllowDenyPattern,
    ) -> "GCPProjectDiscoveryError":
        """All discovered projects were filtered out by pattern."""
        return cls(
            f"Found {total_found} project(s) via {source}, but all were "
            f"excluded by project_id_pattern (allow: {pattern.allow}, deny: {pattern.deny}). "
            "Adjust your allow/deny patterns."
        )

    @classmethod
    def config_error(cls, message: str, debug_cmd: str) -> "GCPProjectDiscoveryError":
        """Configuration validation error."""
        return cls(f"Configuration error: {message}. Debug: {debug_cmd}")

    @classmethod
    def api_error(
        cls, exc: Exception, context: str, debug_cmd: str
    ) -> "GCPProjectDiscoveryError":
        """Generic GCP API error."""
        return cls(f"GCP API error {context}: {exc}. Debug: {debug_cmd}")


def _handle_discovery_error(
    exc: Exception,
    context: str,
    debug_cmd: str,
) -> GCPProjectDiscoveryError:
    """Unified error handler for project discovery failures."""
    if isinstance(exc, PermissionDenied):
        return GCPProjectDiscoveryError.permission_denied(debug_cmd)
    if isinstance(exc, DeadlineExceeded):
        return GCPProjectDiscoveryError.timeout(exc, debug_cmd)
    if isinstance(exc, ServiceUnavailable):
        return GCPProjectDiscoveryError.service_unavailable(exc, debug_cmd)
    if isinstance(exc, ResourceExhausted):
        return GCPProjectDiscoveryError.rate_limit_exceeded(exc, debug_cmd)
    if isinstance(exc, ValueError):
        return GCPProjectDiscoveryError.config_error(str(exc), debug_cmd)
    return GCPProjectDiscoveryError.api_error(exc, context, debug_cmd)


def _validate_pattern_before_discovery(
    pattern: AllowDenyPattern,
    discovery_method: str,
) -> None:
    """
    Validate project_id_pattern for common mistakes BEFORE making API calls.
    Raises GCPProjectDiscoveryError if pattern is obviously broken.
    """
    deny_all_patterns = {".*", ".+", "^.*$", "^.+$"}

    if pattern.deny:
        has_deny_all = any(d in deny_all_patterns for d in pattern.deny)
        has_default_allow = pattern.allow == [".*"]
        if has_deny_all and has_default_allow:
            raise GCPProjectDiscoveryError(
                f"Invalid project_id_pattern: deny pattern {pattern.deny} blocks ALL projects. "
                f"Either remove the deny pattern or add explicit allow patterns. "
                f"Failing fast to avoid expensive {discovery_method} API calls."
            )

    if pattern.allow and pattern.allow != [".*"]:
        for allow_pattern in pattern.allow:
            if "*" in allow_pattern and ".*" not in allow_pattern:
                logger.warning(
                    "project_id_pattern allow '%s' looks like glob syntax. "
                    "AllowDenyPattern uses regex. Did you mean '%s'?",
                    allow_pattern,
                    allow_pattern.replace("*", ".*"),
                )

    if pattern.allow and pattern.allow != [".*"] and pattern.deny:
        has_deny_all = any(d in deny_all_patterns for d in pattern.deny)
        if has_deny_all:
            logger.warning(
                "project_id_pattern has deny pattern %s which overrides allow patterns %s. "
                "Deny patterns take precedence - this may filter out all projects.",
                pattern.deny,
                pattern.allow,
            )


def _validate_pattern_against_explicit_list(
    pattern: AllowDenyPattern,
    project_ids: List[str],
) -> None:
    """
    Validate pattern against explicit project ID list.
    Fail fast if pattern will filter out ALL explicitly configured projects.
    """
    if not project_ids:
        return

    would_match = [pid for pid in project_ids if pattern.allowed(pid)]

    if not would_match:
        ids_preview = ", ".join(project_ids[:5])
        if len(project_ids) > 5:
            ids_preview += "..."
        raise GCPProjectDiscoveryError(
            f"project_id_pattern excludes ALL {len(project_ids)} explicitly configured project_ids: "
            f"{ids_preview}. Pattern allow={pattern.allow}, deny={pattern.deny}. "
            "Adjust your patterns or remove project_id_pattern to use all configured projects."
        )

    filtered_out = len(project_ids) - len(would_match)
    if filtered_out > 0:
        excluded = [pid for pid in project_ids if not pattern.allowed(pid)]
        excluded_preview = ", ".join(excluded[:5])
        if len(excluded) > 5:
            excluded_preview += "..."
        logger.warning(
            "project_id_pattern will exclude %d of %d explicitly configured project_ids: %s",
            filtered_out,
            len(project_ids),
            excluded_preview,
        )


def _validate_and_filter_projects(
    projects: List["GCPProject"],
    pattern: AllowDenyPattern,
    source_description: str,
) -> List["GCPProject"]:
    filtered = _filter_projects_by_pattern(projects, pattern)

    if not filtered:
        raise GCPProjectDiscoveryError.all_filtered_out(
            total_found=len(projects),
            source=source_description,
            pattern=pattern,
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
    _validate_pattern_against_explicit_list(pattern, project_ids)

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
    first_label = labels[0] if labels else "LABEL"
    debug_cmd = f"gcloud projects list --filter='labels.{first_label}:*'"

    if not labels:
        raise GCPProjectDiscoveryError.config_error(
            "project_labels cannot be empty. Provide at least one label", debug_cmd
        )

    pattern = project_id_pattern or AllowDenyPattern.allow_all()
    _validate_pattern_before_discovery(pattern, "label search")

    label_queries = []
    for label in labels:
        if ":" in label:
            label_queries.append(f"labels.{label}")
        else:
            label_queries.append(f"labels.{label}:*")
    query = f"state:ACTIVE AND ({' OR '.join(label_queries)})"

    logger.debug("Discovering projects with labels query: %s", query)

    try:
        projects = list(_search_projects_with_retry(client, query))

        if not projects:
            raise GCPProjectDiscoveryError.no_projects_found(
                source=f"label search {labels}",
                debug_cmd=debug_cmd,
                include_label_check=True,
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

    _validate_pattern_before_discovery(pattern, "auto-discovery")

    logger.info(
        "No project_ids or project_labels specified. Discovering all accessible GCP projects..."
    )

    debug_cmd = "gcloud projects list --filter='lifecycleState:ACTIVE'"
    discovered_projects = list(list_all_accessible_projects(client))

    if not discovered_projects:
        raise GCPProjectDiscoveryError.no_projects_found(
            source="auto-discovery",
            debug_cmd=debug_cmd,
        )

    return _validate_and_filter_projects(discovered_projects, pattern, "auto-discovery")
