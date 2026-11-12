from __future__ import annotations

import base64
import logging
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Protocol, Set, Tuple, Union
from urllib.parse import quote

import requests

from datahub.ingestion.source.repository_documents.client import (
    MAX_FILE_SIZE_BYTES,
    RepositoryDocumentsClient as RepositoryDocumentsClient,
    RepositoryFileInfo,
)
from datahub.ingestion.source.repository_documents.paths import (
    collect_intermediate_directories as collect_intermediate_directories,
    make_dir_source_id as _make_dir_source_id,
    make_file_source_id as _make_file_source_id,
    make_repo_source_id as _make_repo_source_id,
    matches_filters,
    normalize_document_id as normalize_document_id,
    resolve_parent_dir_source_id as _resolve_parent_dir_source_id,
)

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
REPO_URL_PATTERN = re.compile(r"https?://github\.com/([^/]+/[^/]+?)(?:\.git)?$")


class GitHubTokenProvider(Protocol):
    """Provides GitHub API bearer tokens (PAT, installation token, etc.)."""

    def get_token(self) -> str: ...


@dataclass(frozen=True)
class StaticTokenProvider:
    """OSS default: a fixed personal access token from recipe config."""

    token: str

    def get_token(self) -> str:
        return self.token


def parse_repo_identifier(raw: str) -> str:
    """Parse owner/repo from a URL or shorthand identifier."""
    raw = raw.strip().rstrip("/")
    if raw.startswith("http://") or raw.startswith("https://"):
        match = REPO_URL_PATTERN.match(raw)
        if not match:
            raise ValueError(
                f"Could not parse GitHub URL: {raw}. "
                "Expected format: https://github.com/owner/repo"
            )
        return match.group(1)

    parts = raw.split("/")
    if len(parts) == 2 and parts[0] and parts[1]:
        return raw

    raise ValueError(
        f"Invalid repository identifier: {raw}. "
        "Use 'owner/repo' or 'https://github.com/owner/repo'."
    )


class GitHubApiClient:
    def __init__(
        self,
        token_or_provider: Union[str, GitHubTokenProvider],
        timeout_seconds: int = 30,
    ) -> None:
        if isinstance(token_or_provider, str):
            self._token_provider: GitHubTokenProvider = StaticTokenProvider(
                token_or_provider
            )
        else:
            self._token_provider = token_or_provider
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

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token_provider.get_token()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def list_matching_files(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        extensions: List[str],
    ) -> Tuple[List[RepositoryFileInfo], bool]:
        # The source passes a resolved commit SHA; branch names remain supported
        # for connection checks and direct client callers.
        tree_url = (
            f"{GITHUB_API_BASE}/repos/{owner_repo}/git/trees/{quote(branch, safe='')}"
            "?recursive=true"
        )
        tree = self._get_json(tree_url)
        if tree is None or "tree" not in tree:
            detail = self._last_api_error or "GitHub API request failed"
            raise RuntimeError(
                f"Could not access branch '{branch}' in repository {owner_repo}: {detail}"
            )
        tree_truncated = bool(tree.get("truncated"))

        ext_set = {ext.lower() for ext in extensions}
        normalized_prefix = path_prefix.strip("/")

        matching: List[RepositoryFileInfo] = []
        self._index_file_paths = set()
        for item in tree["tree"]:
            if item.get("type") != "blob":
                continue
            path = item.get("path", "")
            if path == "index.md" or path.endswith("/index.md"):
                self._index_file_paths.add(path)
            if not matches_filters(path, normalized_prefix, ext_set):
                continue
            size = item.get("size")
            blob_sha = item.get("sha")
            matching.append(
                RepositoryFileInfo(
                    path=path,
                    size=size,
                    sha=blob_sha if isinstance(blob_sha, str) else None,
                )
            )
        return matching, tree_truncated

    def fetch_file_content(
        self, owner_repo: str, file_path: str, branch: str
    ) -> Optional[str]:
        encoded_path = quote(file_path, safe="")
        url = (
            f"{GITHUB_API_BASE}/repos/{owner_repo}/contents/{encoded_path}"
            f"?ref={quote(branch, safe='')}"
        )
        node = self._get_json(url)
        if node is None:
            detail = self._last_api_error or "GitHub API request failed"
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

        if node.get("content") and node.get("encoding") == "base64":
            encoded = node["content"].replace("\n", "").replace(" ", "")
            return base64.b64decode(encoded).decode("utf-8", errors="replace")

        download_url = node.get("download_url")
        if download_url:
            return self._get_text(download_url)

        return None

    def get_latest_commit_sha(self, owner_repo: str, branch: str) -> str:
        url = f"{GITHUB_API_BASE}/repos/{owner_repo}/branches/{quote(branch, safe='')}"
        node = self._get_json(url)
        if node and "commit" in node:
            return node["commit"].get("sha", "unknown")
        return "unknown"

    def _get_json(
        self, url: str, *, allow_rate_limit_retry: bool = True
    ) -> Optional[dict]:
        try:
            response = self._session.get(
                url, headers=self._auth_headers(), timeout=self._timeout
            )
            if response.status_code == 200:
                self._last_api_error = None
                payload = response.json()
                return payload if isinstance(payload, dict) else None

            if (
                allow_rate_limit_retry
                and response.status_code == 403
                and self._is_rate_limited(response)
            ):
                sleep_seconds = self._rate_limit_sleep_seconds(response)
                if sleep_seconds > 0:
                    logger.warning(
                        "GitHub rate limit exceeded for %s; retrying after %s seconds",
                        url,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
                    return self._get_json(url, allow_rate_limit_retry=False)

            self._last_api_error = self._describe_http_error(response)
            logger.warning("%s for %s", self._last_api_error, url)
            return None
        except requests.RequestException as exc:
            self._last_api_error = f"GitHub API request failed: {exc}"
            logger.warning("%s for %s", self._last_api_error, url)
            return None

    @staticmethod
    def _is_rate_limited(response: requests.Response) -> bool:
        remaining = response.headers.get("X-RateLimit-Remaining")
        if remaining is not None and remaining.isdigit() and int(remaining) == 0:
            return True
        message = response.text.lower()
        return "rate limit" in message or "api rate limit exceeded" in message

    @staticmethod
    def _rate_limit_sleep_seconds(response: requests.Response) -> int:
        reset_header = response.headers.get("X-RateLimit-Reset")
        if reset_header and reset_header.isdigit():
            return min(max(int(reset_header) - int(time.time()) + 1, 1), 60)
        return 1

    @staticmethod
    def _describe_http_error(response: requests.Response) -> str:
        status = response.status_code
        if status == 401:
            return (
                "GitHub authentication failed (HTTP 401). "
                "Verify github_token is valid and has repository read access."
            )
        if status == 403:
            if GitHubApiClient._is_rate_limited(response):
                reset_header = response.headers.get("X-RateLimit-Reset")
                if reset_header and reset_header.isdigit():
                    return (
                        "GitHub API rate limit exceeded (HTTP 403). "
                        f"Retry after epoch {reset_header}."
                    )
                return "GitHub API rate limit exceeded (HTTP 403)."
            return (
                "GitHub access denied (HTTP 403). "
                "Verify the token has permission to read this repository."
            )
        if status == 404:
            return "GitHub resource not found (HTTP 404)."
        return f"GitHub API returned HTTP {status}"

    def _get_text(self, url: str) -> Optional[str]:
        try:
            response = self._session.get(
                url, headers=self._auth_headers(), timeout=self._timeout
            )
            if response.status_code == 200:
                return response.text
            self._last_api_error = self._describe_http_error(response)
            logger.warning(
                "Failed to download raw content from %s: %s",
                url,
                self._last_api_error,
            )
            return None
        except requests.RequestException as exc:
            self._last_api_error = f"GitHub API request failed: {exc}"
            logger.warning("Failed to download raw content from %s: %s", url, exc)
            return None


# Kept so code built on the github-documents modules keeps importing.
GitHubFileInfo = RepositoryFileInfo
_matches_filters = matches_filters


def make_file_source_id(
    owner_repo: str, file_path: str, *, source_prefix: str = "github"
) -> str:
    return _make_file_source_id(owner_repo, file_path, source_prefix=source_prefix)


def make_dir_source_id(
    owner_repo: str, dir_path: str, *, source_prefix: str = "github"
) -> str:
    return _make_dir_source_id(owner_repo, dir_path, source_prefix=source_prefix)


def make_repo_source_id(owner_repo: str, *, source_prefix: str = "github") -> str:
    return _make_repo_source_id(owner_repo, source_prefix=source_prefix)


def resolve_parent_dir_source_id(
    owner_repo: str,
    full_path: str,
    path_prefix: str,
    *,
    repo_root_source_id: Optional[str] = None,
    source_prefix: str = "github",
) -> Optional[str]:
    return _resolve_parent_dir_source_id(
        owner_repo,
        full_path,
        path_prefix,
        repo_root_source_id=repo_root_source_id,
        source_prefix=source_prefix,
    )
