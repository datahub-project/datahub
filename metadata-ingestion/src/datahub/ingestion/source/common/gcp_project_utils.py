import json
import logging
import os
import re
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, TypeVar

from google.api_core import retry
from google.api_core.exceptions import (
    DeadlineExceeded,
    FailedPrecondition,
    GoogleAPICallError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.cloud import resourcemanager_v3
from google.oauth2.credentials import Credentials

from datahub.configuration.common import AllowDenyPattern

logger = logging.getLogger(__name__)

GCP_PROJECT_ID_PATTERN = re.compile(r"^[a-z][-a-z0-9]{4,28}[a-z0-9]$")
LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*(?::[a-z0-9_-]+)?$")

RETRY_INITIAL_DELAY = 1.0
RETRY_MAX_DELAY = 60.0
RETRY_MULTIPLIER = 2.0
RETRY_DEFAULT_TIMEOUT = 300.0


class GCPValidationError(ValueError):
    """Base exception for GCP validation errors."""

    pass


def validate_project_id_format(project_id: str) -> None:
    if not GCP_PROJECT_ID_PATTERN.match(project_id):
        raise GCPValidationError(
            f"Invalid project ID format: '{project_id}'. "
            "Must be 6-30 characters, lowercase letters, digits, and hyphens. "
            "Must start with a letter and end with a letter or digit."
        )


def validate_project_id_list(
    project_ids: List[str], *, allow_empty: bool = True
) -> None:
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

    seen: set[str] = set()
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


def validate_project_label_format(label: str) -> None:
    if not LABEL_PATTERN.match(label):
        raise GCPValidationError(
            f"Invalid project_labels format: '{label}'. "
            "Must be 'key:value' format. Example: env:prod"
        )
    if ":" not in label:
        raise GCPValidationError(
            f"Invalid project_labels format: '{label}'. "
            "Must be 'key:value' format. Example: env:prod"
        )


def validate_project_label_list(project_labels: List[str]) -> None:
    if not project_labels:
        return

    empty_values = [label for label in project_labels if not label.strip()]
    if empty_values:
        raise GCPValidationError("project_labels contains empty values")

    seen: set[str] = set()
    duplicates: List[str] = []
    for label in project_labels:
        if label in seen:
            duplicates.append(label)
        else:
            seen.add(label)
    if duplicates:
        raise GCPValidationError(f"project_labels contains duplicates: {duplicates}")

    for label in project_labels:
        validate_project_label_format(label)


@contextmanager
def temporary_credentials_file(credentials_dict: Dict[str, Any]) -> Iterator[str]:
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

        os.chmod(temp_file.name, 0o600)

        logger.debug("Created temporary credentials file: %s", temp_file.name)
        yield temp_file.name
    finally:
        try:
            os.unlink(temp_file.name)
            logger.debug("Cleaned up temporary credentials file: %s", temp_file.name)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(
                "SECURITY: Failed to cleanup credentials file %s - credentials may be leaked on disk! Error: %s",
                temp_file.name,
                e,
                exc_info=True,
            )


@contextmanager
def gcp_credentials_context(
    credentials_dict: Optional[Dict[str, Any]],
) -> Iterator[None]:
    """
    Context manager for GCP credential setup/teardown.

    Handles the common pattern of conditionally setting up credentials:
    - If credentials_dict is None: yields immediately (uses ADC)
    - If credentials_dict provided: creates temp file, sets env var, cleans up

    Args:
        credentials_dict: GCP service account credentials dict, or None for ADC

    Example:
        with gcp_credentials_context(config.get_credentials_dict()):
            yield from super().get_workunits()
    """
    if credentials_dict is None:
        yield
    else:
        with (
            temporary_credentials_file(credentials_dict) as cred_path,
            with_temporary_credentials(cred_path),
        ):
            yield


@contextmanager
def with_temporary_credentials(credentials_path: str) -> Iterator[None]:
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
    if isinstance(exc, Exception) and is_gcp_transient_error(exc):
        logger.debug("Transient GCP error, retrying: %s", exc)
        return True
    return False


def gcp_api_retry(timeout: float = RETRY_DEFAULT_TIMEOUT) -> retry.Retry:
    return retry.Retry(
        predicate=_is_transient_error_predicate,
        initial=RETRY_INITIAL_DELAY,
        maximum=RETRY_MAX_DELAY,
        multiplier=RETRY_MULTIPLIER,
        timeout=timeout,
    )


def call_with_retry(
    func: Callable[..., T],
    *args: Any,
    timeout: float = RETRY_DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> T:
    return gcp_api_retry(timeout)(func)(*args, **kwargs)


class GCPProjectDiscoveryError(Exception):
    pass


def get_gcp_error_type(exc: Exception) -> str:
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
    if isinstance(exc, (InvalidArgument, FailedPrecondition)):
        return "Configuration error"
    return "API error"


def _handle_discovery_error(
    exc: GoogleAPICallError,
    debug_cmd: str,
    discovery_method: str = "project discovery",
) -> GCPProjectDiscoveryError:
    error_type = get_gcp_error_type(exc)
    error_msg = f"{error_type}: {exc}. Debug: {debug_cmd}"
    if isinstance(exc, PermissionDenied):
        if "label" in discovery_method.lower() or discovery_method == "auto-discovery":
            return GCPProjectDiscoveryError(
                f"Permission denied: {discovery_method} requires organization-level permissions.\n\n"
                f"Your service account needs 'resourcemanager.projects.list' permission at the "
                f"organization or folder level to list and filter projects.\n\n"
                f"To fix:\n"
                f"  1. Grant the 'Browser' role at org/folder level:\n"
                f"     gcloud organizations add-iam-policy-binding ORGANIZATION_ID \\\n"
                f"       --member='serviceAccount:SERVICE_ACCOUNT_EMAIL' \\\n"
                f"       --role='roles/browser'\n\n"
                f"  2. Or use explicit project_ids instead (doesn't require org-level permissions):\n"
                f"     project_ids:\n"
                f"       - your-project-1\n"
                f"       - your-project-2\n\n"
                f"Debug: {debug_cmd}"
            )
        return GCPProjectDiscoveryError(
            f"Permission denied when listing GCP projects. {error_msg}"
        )
    return GCPProjectDiscoveryError(error_msg)


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
            has_glob_star = "*" in allow_pattern and ".*" not in allow_pattern
            has_glob_question = (
                "?" in allow_pattern
                and ".?" not in allow_pattern
                and "\\?" not in allow_pattern
                and "[?" not in allow_pattern
                and "??" in allow_pattern
            )
            if has_glob_star or has_glob_question:
                suggested = allow_pattern.replace("*", ".*").replace("?", ".?")
                logger.warning(
                    "project_id_pattern allow '%s' looks like glob syntax. "
                    "AllowDenyPattern uses regex. Did you mean '%s'?",
                    allow_pattern,
                    suggested,
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
    for project in projects:
        if pattern.allowed(project.id):
            filtered.append(project)
        else:
            excluded.append(project.id)
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
                "This indicates a serious API issue. Please report at https://github.com/datahub-project/datahub/issues"
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
                f"No projects found with labels {labels}.\n\n"
                f"Possible causes:\n"
                f"  1. No projects have these labels - Verify labels exist:\n"
                f"     {debug_cmd}\n"
                f"  2. Missing org-level permissions - Label-based discovery requires "
                f"'resourcemanager.projects.list' at organization/folder level.\n"
                f"     If you only have project-level permissions, use explicit project_ids instead.\n\n"
                f"Debug: {debug_cmd}"
            )

        return _filter_and_validate(projects, pattern, f"label search {labels}")

    except GoogleAPICallError as e:
        raise _handle_discovery_error(
            e, debug_cmd, discovery_method="label-based discovery"
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
    logger.info("Discovering all accessible GCP projects...")

    debug_cmd = "gcloud projects list --filter='lifecycleState:ACTIVE'"
    try:
        discovered_projects = list(_search_projects_with_retry(client, "state:ACTIVE"))
    except GoogleAPICallError as e:
        raise _handle_discovery_error(
            e, debug_cmd, discovery_method="auto-discovery"
        ) from e

    if not discovered_projects:
        raise GCPProjectDiscoveryError(
            f"No projects discovered. Verify service account has access. Debug: {debug_cmd}"
        )

    return _filter_and_validate(discovered_projects, pattern, "auto-discovery")
