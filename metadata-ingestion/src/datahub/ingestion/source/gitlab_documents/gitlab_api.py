from __future__ import annotations

import base64
import logging
import time
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import quote, urlparse

import requests

from datahub.ingestion.source.github_documents.github_api import (
    MAX_FILE_SIZE_BYTES,
    GitHubFileInfo,
    _matches_filters,
)

logger = logging.getLogger(__name__)

DEFAULT_GITLAB_URL = "https://gitlab.com"
TREE_PAGE_SIZE = 100
MAX_RETRY_AFTER_SECONDS = 60


def parse_project_path(raw: str, base_url: Optional[str]) -> Tuple[str, str]:
    """Return ``(base_url, project_path)`` from a project path or URL.

    A URL sets the instance when ``base_url`` is not given. GitLab instances can
    live under a relative path (``https://host/gitlab``), so a given ``base_url``
    is stripped as a prefix rather than assuming the host root.
    """
    raw = raw.strip().rstrip("/")
    base = (base_url or DEFAULT_GITLAB_URL).rstrip("/")
    if raw.startswith(("http://", "https://")):
        if base_url and raw.startswith(f"{base}/"):
            path = raw[len(base) + 1 :]
        elif base_url:
            raise ValueError(
                f"Repository URL {raw} is not on the configured base_url {base}."
            )
        else:
            parsed = urlparse(raw)
            base = f"{parsed.scheme}://{parsed.netloc}"
            path = parsed.path.strip("/")
    else:
        path = raw.strip("/")

    # Links copied from the GitLab UI carry a /-/tree/<branch> style suffix.
    path = path.split("/-/", 1)[0]
    if path.endswith(".git"):
        path = path[: -len(".git")]
    segments = path.split("/")
    if len(segments) < 2 or not all(segments):
        raise ValueError(
            f"Invalid GitLab project: {raw}. Use 'group/project' "
            "(nested groups allowed) or the project's URL."
        )
    return base, path


class GitLabApiClient:
    def __init__(
        self, token: str, base_url: str = DEFAULT_GITLAB_URL, timeout_seconds: int = 30
    ) -> None:
        self._token = token
        self._api_base = f"{base_url.rstrip('/')}/api/v4"
        self._session = requests.Session()
        self._timeout = timeout_seconds
        self._last_api_error: Optional[str] = None
        self._index_file_paths: Set[str] = set()

    @property
    def index_file_paths(self) -> Set[str]:
        return self._index_file_paths

    @property
    def last_api_error(self) -> Optional[str]:
        return self._last_api_error

    def _project_url(self, project_path: str) -> str:
        return f"{self._api_base}/projects/{quote(project_path, safe='')}"

    def get_latest_commit_sha(self, owner_repo: str, branch: str) -> str:
        url = (
            f"{self._project_url(owner_repo)}/repository/branches/"
            f"{quote(branch, safe='')}"
        )
        response = self._get(url)
        node = _json_or_none(response)
        if isinstance(node, dict) and isinstance(node.get("commit"), dict):
            return node["commit"].get("id") or "unknown"
        return "unknown"

    def list_matching_files(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        extensions: List[str],
    ) -> Tuple[List[GitHubFileInfo], bool]:
        # The whole tree is listed, not just path_prefix, because OKF bundle
        # discovery needs index.md files above the prefix.
        url: Optional[str] = f"{self._project_url(owner_repo)}/repository/tree"
        params: Optional[Dict[str, Union[str, int]]] = {
            "ref": branch,
            "recursive": "true",
            "per_page": TREE_PAGE_SIZE,
        }
        ext_set = {ext.lower() for ext in extensions}
        normalized_prefix = path_prefix.strip("/")
        matching: List[GitHubFileInfo] = []
        self._index_file_paths = set()

        while url:
            response = self._get(url, params=params)
            items = _json_or_none(response)
            if not isinstance(items, list):
                detail = self._last_api_error or "GitLab API request failed"
                raise RuntimeError(
                    f"Could not list branch '{branch}' in project {owner_repo}: {detail}"
                )
            for item in items:
                if item.get("type") != "blob":
                    continue
                path = item.get("path", "")
                if path == "index.md" or path.endswith("/index.md"):
                    self._index_file_paths.add(path)
                if not _matches_filters(path, normalized_prefix, ext_set):
                    continue
                blob_id = item.get("id")
                matching.append(
                    GitHubFileInfo(
                        path=path,
                        size=None,
                        sha=blob_id if isinstance(blob_id, str) else None,
                    )
                )
            assert response is not None
            # The next link already carries the query string (offset or keyset).
            url = response.links.get("next", {}).get("url")
            params = None
        # GitLab pages through the full tree, so it is never truncated.
        return matching, False

    def fetch_file_content(
        self, owner_repo: str, file_path: str, branch: str
    ) -> Optional[str]:
        url = (
            f"{self._project_url(owner_repo)}/repository/files/"
            f"{quote(file_path, safe='')}"
        )
        response = self._get(url, params={"ref": branch})
        node = _json_or_none(response)
        if not isinstance(node, dict):
            detail = self._last_api_error or "GitLab API request failed"
            logger.warning("Failed to fetch file content for %s: %s", file_path, detail)
            return None

        size = node.get("size")
        if isinstance(size, int) and size > MAX_FILE_SIZE_BYTES:
            logger.warning(
                "File %s is %s bytes (max %s), skipping",
                file_path,
                size,
                MAX_FILE_SIZE_BYTES,
            )
            return None

        content = node.get("content")
        if not isinstance(content, str):
            return None
        if node.get("encoding") == "base64":
            return base64.b64decode(content).decode("utf-8", errors="replace")
        return content

    def _get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Union[str, int]]] = None,
        allow_retry: bool = True,
    ) -> Optional[requests.Response]:
        try:
            response = self._session.get(
                url,
                params=params,
                headers={"PRIVATE-TOKEN": self._token},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            self._last_api_error = f"GitLab API request failed: {exc}"
            logger.warning("%s for %s", self._last_api_error, url)
            return None

        if response.status_code == 200:
            self._last_api_error = None
            return response

        if allow_retry and response.status_code == 429:
            sleep_seconds = _retry_after_seconds(response)
            logger.warning(
                "GitLab rate limit exceeded for %s; retrying after %s seconds",
                url,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            return self._get(url, params=params, allow_retry=False)

        self._last_api_error = _describe_http_error(response)
        logger.warning("%s for %s", self._last_api_error, url)
        return None


def _json_or_none(response: Optional[requests.Response]) -> object:
    if response is None:
        return None
    try:
        return response.json()
    except ValueError:
        # A proxy or SSO login page can answer 200 with HTML.
        return None


def _retry_after_seconds(response: requests.Response) -> int:
    header = response.headers.get("Retry-After", "")
    if header.isdigit():
        return min(max(int(header), 1), MAX_RETRY_AFTER_SECONDS)
    return 1


def _describe_http_error(response: requests.Response) -> str:
    status = response.status_code
    if status == 401:
        return (
            "GitLab authentication failed (HTTP 401). "
            "Verify gitlab_token is valid and has not expired."
        )
    if status == 403:
        return (
            "GitLab access denied (HTTP 403). "
            "Verify the token has the read_api scope and at least Reporter access."
        )
    if status == 404:
        # GitLab answers 404, not 403, for private projects the token cannot see.
        return (
            "GitLab resource not found (HTTP 404). Check the project path and "
            "branch, and that the token can see the project."
        )
    if status == 429:
        return "GitLab API rate limit exceeded (HTTP 429)."
    return f"GitLab API returned HTTP {status}"
