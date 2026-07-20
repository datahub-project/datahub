"""Resolve ``github:owner/repo[@version]`` specs to pip-installable URLs.

Uses the GitHub Releases API to find wheel assets attached to releases.
Falls back to ``git+https://`` if no wheel is found.
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import requests

logger = logging.getLogger(__name__)

_GITHUB_SPEC_RE = re.compile(
    r"^github:(?P<owner>[^/]+)/(?P<repo>[^@]+?)(?:@(?P<version>.+))?$"
)

GITHUB_API_BASE = "https://api.github.com"


@dataclass(frozen=True)
class GitHubSpec:
    owner: str
    repo: str
    version: Optional[str]  # None means "latest release"

    def __post_init__(self) -> None:
        if not self.owner or not self.owner.strip():
            raise ValueError("GitHubSpec.owner must not be empty")
        if not self.repo or not self.repo.strip():
            raise ValueError("GitHubSpec.repo must not be empty")

    @staticmethod
    def parse(spec: str) -> Optional["GitHubSpec"]:
        m = _GITHUB_SPEC_RE.match(spec)
        if m is None:
            return None
        return GitHubSpec(
            owner=m.group("owner"),
            repo=m.group("repo"),
            version=m.group("version"),
        )


@dataclass(frozen=True)
class _ResolvedBase:
    """Common fields and validation shared by all resolved specs."""

    download_url: str
    version: str

    def __post_init__(self) -> None:
        cls_name = type(self).__name__
        if not self.download_url:
            raise ValueError(f"{cls_name}.download_url must not be empty")
        if not self.version:
            raise ValueError(f"{cls_name}.version must not be empty")


@dataclass(frozen=True)
class ResolvedWheel(_ResolvedBase):
    """A resolved GitHub spec that points to a .whl release asset."""

    asset_api_url: Optional[str] = None  # GitHub API URL for authenticated download
    asset_filename: Optional[str] = None  # Original filename of the wheel


@dataclass(frozen=True)
class ResolvedGitSource(_ResolvedBase):
    """A resolved GitHub spec that uses git+https:// install."""


def _resolve_github_token() -> Optional[str]:
    """Return a GitHub token from the environment or the ``gh`` CLI."""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token

    # Fall back to the gh CLI's stored credentials
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        logger.debug("gh CLI auth token fallback failed", exc_info=True)

    return None


def _github_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Accept": "application/vnd.github+json"}
    token = _resolve_github_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _find_wheel_asset(assets: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the first asset whose name ends with .whl."""
    return next(
        (asset for asset in assets if asset.get("name", "").endswith(".whl")),
        None,
    )


def resolve_github_spec(spec: str) -> Union[ResolvedWheel, ResolvedGitSource]:
    """Resolve a ``github:owner/repo[@version]`` spec to a downloadable URL.

    Raises ``ValueError`` if the spec cannot be resolved.
    """
    parsed = GitHubSpec.parse(spec)
    if parsed is None:
        raise ValueError(f"Invalid GitHub plugin spec: {spec}")

    headers = _github_headers()
    repo_path = f"{parsed.owner}/{parsed.repo}"

    if parsed.version:
        url = f"{GITHUB_API_BASE}/repos/{repo_path}/releases/tags/{parsed.version}"
        not_found_msg = f"Release '{parsed.version}' not found for {repo_path}"
    else:
        url = f"{GITHUB_API_BASE}/repos/{repo_path}/releases/latest"
        not_found_msg = (
            f"No releases found for {repo_path}. "
            "Ensure the repository has at least one published release."
        )

    has_token = "Authorization" in headers
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 404:
        if not has_token:
            not_found_msg += (
                " If this is a private repository, set the GITHUB_TOKEN"
                " environment variable or authenticate with `gh auth login`."
            )
        raise ValueError(not_found_msg)
    resp.raise_for_status()
    release = resp.json()

    tag = release.get("tag_name", parsed.version or "unknown")
    version = tag.lstrip("v")

    wheel = _find_wheel_asset(release.get("assets", []))
    if wheel is not None:
        return ResolvedWheel(
            download_url=wheel["browser_download_url"],
            version=version,
            asset_api_url=wheel.get("url"),
            asset_filename=wheel.get("name"),
        )

    # Fallback: use git+https:// at the release tag
    logger.info(
        "No .whl asset found in release %s; falling back to git+https:// install",
        tag,
    )
    git_url = f"git+https://github.com/{parsed.owner}/{parsed.repo}.git@{tag}"
    return ResolvedGitSource(
        download_url=git_url,
        version=version,
    )


def download_wheel(resolved: ResolvedWheel) -> str:
    """Download a wheel asset to a temp file, returning the local path.

    Uses the GitHub API with authentication so private repo assets
    can be fetched. Falls back to the browser_download_url for
    public repos or when no API URL is available.
    """
    headers = _github_headers()

    # Prefer the API URL with Accept: octet-stream (works for private repos)
    if resolved.asset_api_url:
        headers["Accept"] = "application/octet-stream"
        download_url = resolved.asset_api_url
    else:
        download_url = resolved.download_url

    resp = requests.get(download_url, headers=headers, timeout=120, stream=True)
    if resp.status_code == 403:
        raise ValueError(
            f"Access denied downloading wheel from {download_url}. "
            "Ensure GITHUB_TOKEN is set for private repositories."
        )
    if resp.status_code == 404:
        raise ValueError(
            f"Wheel asset not found at {download_url}. "
            "The release may have been deleted or the asset removed."
        )
    resp.raise_for_status()

    filename = resolved.asset_filename or "plugin.whl"
    tmp_dir = tempfile.mkdtemp(prefix="datahub-plugin-")
    local_path = os.path.join(tmp_dir, filename)

    try:
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
    except (OSError, requests.RequestException) as e:
        logger.warning("Failed to write wheel to %s: %s", local_path, e, exc_info=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    logger.info("Downloaded wheel to %s", local_path)
    return local_path
