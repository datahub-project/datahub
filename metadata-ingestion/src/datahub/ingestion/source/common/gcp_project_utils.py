import json
import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, TypeVar

from google.api_core import retry
from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
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


@contextmanager
def with_temporary_credentials(credentials_path: str) -> Iterator[None]:
    """Temporarily set GOOGLE_APPLICATION_CREDENTIALS, restoring on exit."""
    original = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        yield
    finally:
        if original is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original
        else:
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)


def is_gcp_transient_error(exc: Exception) -> bool:
    """Check if a GCP API error is transient and should be retried."""
    if isinstance(exc, ResourceExhausted):
        return True
    if isinstance(exc, (DeadlineExceeded, ServiceUnavailable)):
        return True
    if isinstance(exc, GoogleAPICallError):
        code = getattr(exc, "code", None)
        if code is not None and code >= 500:
            return True
    return False


T = TypeVar("T")


def _is_transient_error_predicate(exc: BaseException) -> bool:
    """Retry predicate wrapper for is_gcp_transient_error."""
    if isinstance(exc, Exception) and is_gcp_transient_error(exc):
        logger.debug("Transient GCP error, retrying: %s", exc)
        return True
    return False


def gcp_api_retry(timeout: float = 300.0) -> retry.Retry:
    """Standard retry configuration for GCP API calls."""
    return retry.Retry(
        predicate=_is_transient_error_predicate,
        initial=1.0,
        maximum=60.0,
        multiplier=2.0,
        timeout=timeout,
    )


def call_with_retry(
    func: Callable[..., T], *args: Any, timeout: float = 300.0, **kwargs: Any
) -> T:
    """Execute a GCP API call with standard retry logic."""
    return gcp_api_retry(timeout)(func)(*args, **kwargs)


class GCPProjectDiscoveryError(Exception):
    """Exception for project discovery failures."""

    pass


def get_gcp_error_type(exc: Exception) -> str:
    """Get a user-friendly error type name for a GCP exception."""
    if isinstance(exc, PermissionDenied):
        return "Permission denied"
    if isinstance(exc, DeadlineExceeded):
        return "API timeout"
    if isinstance(exc, ServiceUnavailable):
        return "Service unavailable"
    if isinstance(exc, ResourceExhausted):
        return "Rate limit exceeded"
    if isinstance(exc, NotFound):
        return "Not found"
    return "API error"


def build_gcp_error_message(exc: Exception, debug_cmd: str) -> str:
    """Build user-friendly error message with debug command."""
    error_type = get_gcp_error_type(exc)
    return f"{error_type}: {exc}. Debug: {debug_cmd}"


def _handle_discovery_error(
    exc: GoogleAPICallError,
    debug_cmd: str,
) -> GCPProjectDiscoveryError:
    """Convert GoogleAPICallError to GCPProjectDiscoveryError with helpful context."""
    if isinstance(exc, PermissionDenied):
        return GCPProjectDiscoveryError(
            "Permission denied when listing GCP projects. "
            "Ensure the service account has 'resourcemanager.projects.list' permission, "
            f"or specify project_ids explicitly. Debug: {debug_cmd}"
        )
    base_msg = build_gcp_error_message(exc, debug_cmd)
    if isinstance(exc, (DeadlineExceeded, ServiceUnavailable)):
        return GCPProjectDiscoveryError(
            f"{base_msg} Retry or specify project_ids explicitly."
        )
    return GCPProjectDiscoveryError(base_msg)


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


def _filter_and_validate(
    projects: List["GCPProject"],
    pattern: AllowDenyPattern,
    source: str,
) -> List["GCPProject"]:
    """Filter projects by pattern and raise if all filtered out."""
    filtered = _filter_projects_by_pattern(projects, pattern)
    if not filtered:
        raise GCPProjectDiscoveryError(
            f"Found {len(projects)} projects via {source}, but all were excluded by "
            f"project_id_pattern (allow: {pattern.allow}, deny: {pattern.deny})."
        )
    logger.info(
        "Found %d projects via %s, %d match pattern",
        len(projects),
        source,
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
            "Filtered out %d projects by project_id_pattern: %s",
            len(excluded),
            ", ".join(excluded[:10]) + ("..." if len(excluded) > 10 else ""),
        )
    return filtered


def _search_projects_with_retry(
    client: resourcemanager_v3.ProjectsClient,
    query: str,
) -> Iterable[GCPProject]:
    search_with_retry = gcp_api_retry()(client.search_projects)
    request = resourcemanager_v3.SearchProjectsRequest(query=query)
    for project in search_with_retry(request=request):
        if not project.project_id:
            raise GCPProjectDiscoveryError(
                f"GCP API returned project without project_id (display_name={project.display_name or 'unknown'}). "
                "This indicates a serious API issue. Please report this to the DataHub team."
            )
        yield GCPProject(
            id=project.project_id,
            name=project.display_name or project.project_id,
        )


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
        raise GCPProjectDiscoveryError(
            f"project_labels cannot be empty. Provide at least one label. Debug: {debug_cmd}"
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
            raise GCPProjectDiscoveryError(
                f"No projects found with labels {labels}. "
                f"Verify labels exist and service account has access. Debug: {debug_cmd}"
            )

        return _filter_and_validate(projects, pattern, f"label search {labels}")

    except GoogleAPICallError as e:
        raise _handle_discovery_error(e, debug_cmd) from e


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
    logger.info("Discovering all accessible GCP projects...")

    debug_cmd = "gcloud projects list --filter='lifecycleState:ACTIVE'"
    try:
        discovered_projects = list(_search_projects_with_retry(client, "state:ACTIVE"))
    except GoogleAPICallError as e:
        raise _handle_discovery_error(e, debug_cmd) from e

    if not discovered_projects:
        raise GCPProjectDiscoveryError(
            f"No projects discovered. Verify service account has access. Debug: {debug_cmd}"
        )

    return _filter_and_validate(discovered_projects, pattern, "auto-discovery")
