"""GitHub REST API helpers for the github-documents ingestion source."""

from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
MAX_FILE_SIZE_BYTES = 1_000_000
REPO_URL_PATTERN = re.compile(r"https?://github\.com/([^/]+/[^/]+?)(?:\.git)?$")


@dataclass(frozen=True)
class GitHubFileInfo:
    path: str
    size: Optional[int]


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


def make_file_source_id(owner_repo: str, file_path: str) -> str:
    repo_part = owner_repo.replace("/", ".")
    path_no_ext = file_path
    dot = path_no_ext.rfind(".")
    if dot > 0:
        path_no_ext = path_no_ext[:dot]
    path_part = path_no_ext.replace("/", ".")
    return f"github.{repo_part}.{path_part}"


def make_dir_source_id(owner_repo: str, dir_path: str) -> str:
    repo_part = owner_repo.replace("/", ".")
    path_part = dir_path.replace("/", ".")
    return f"github.{repo_part}.{path_part}._dir"


def normalize_document_id(source_id: str) -> str:
    """Normalize a source id into a stable document entity id (matches GMS import service)."""
    safe = re.sub(r"[^a-zA-Z0-9_.\-]", "-", source_id)
    safe = re.sub(r"-{2,}", "-", safe)
    safe = safe.strip("-").lower()
    return safe[:200]


class GitHubApiClient:
    def __init__(self, token: str, timeout_seconds: int = 30) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        self._timeout = timeout_seconds

    def list_matching_files(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        extensions: List[str],
    ) -> List[GitHubFileInfo]:
        tree_url = (
            f"{GITHUB_API_BASE}/repos/{owner_repo}/git/trees/{quote(branch, safe='')}"
            "?recursive=true"
        )
        tree = self._get_json(tree_url)
        if tree is None or "tree" not in tree:
            raise RuntimeError(
                f"Could not access branch '{branch}' in repository {owner_repo}"
            )

        ext_set = {ext.lower() for ext in extensions}
        normalized_prefix = path_prefix.strip("/")

        matching: List[GitHubFileInfo] = []
        for item in tree["tree"]:
            if item.get("type") != "blob":
                continue
            path = item.get("path", "")
            if not _matches_filters(path, normalized_prefix, ext_set):
                continue
            size = item.get("size")
            matching.append(GitHubFileInfo(path=path, size=size))
        return matching

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
            logger.warning("Failed to fetch file content for %s", file_path)
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
            return base64.b64decode(encoded).decode("utf-8")

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

    def _get_json(self, url: str) -> Optional[dict]:
        try:
            response = self._session.get(url, timeout=self._timeout)
            if response.status_code != 200:
                logger.warning(
                    "GitHub API returned HTTP %s for %s", response.status_code, url
                )
                return None
            payload = response.json()
            return payload if isinstance(payload, dict) else None
        except requests.RequestException as exc:
            logger.warning("GitHub API request failed for %s: %s", url, exc)
            return None

    def _get_text(self, url: str) -> Optional[str]:
        try:
            response = self._session.get(url, timeout=self._timeout)
            if response.status_code == 200:
                return response.text
            logger.warning(
                "Failed to download raw content from %s: HTTP %s",
                url,
                response.status_code,
            )
            return None
        except requests.RequestException as exc:
            logger.warning("Failed to download raw content from %s: %s", url, exc)
            return None


def _matches_filters(file_path: str, path_prefix: str, extensions: Set[str]) -> bool:
    if path_prefix and not file_path.startswith(path_prefix):
        return False
    dot = file_path.rfind(".")
    if dot < 0:
        return False
    ext = file_path[dot:].lower()
    return ext in extensions


def collect_intermediate_directories(
    files: List[GitHubFileInfo], path_prefix: str
) -> Set[str]:
    dirs: Set[str] = set()
    normalized_prefix = path_prefix.strip("/")

    for file in files:
        relative_path = _relativize(file.path, normalized_prefix)
        last_slash = relative_path.rfind("/")
        while last_slash > 0:
            relative_dir = relative_path[:last_slash]
            full_dir = _resolve_path(normalized_prefix, relative_dir)
            dirs.add(full_dir)
            last_slash = relative_dir.rfind("/")
    return dirs


def resolve_parent_dir_source_id(
    owner_repo: str, full_path: str, path_prefix: str
) -> Optional[str]:
    relative_path = _relativize(full_path, path_prefix.strip("/"))
    parent_dir = _parent_directory(relative_path)
    if parent_dir is None:
        return None
    return make_dir_source_id(
        owner_repo, _resolve_path(path_prefix.strip("/"), parent_dir)
    )


def _relativize(full_path: str, prefix: str) -> str:
    if not prefix:
        return full_path
    if full_path.startswith(prefix + "/"):
        return full_path[len(prefix) + 1 :]
    if full_path.startswith(prefix):
        return full_path[len(prefix) :]
    return full_path


def _parent_directory(relative_path: str) -> Optional[str]:
    last_slash = relative_path.rfind("/")
    if last_slash <= 0:
        return None
    return relative_path[:last_slash]


def _resolve_path(prefix: str, relative: str) -> str:
    if not prefix:
        return relative
    return f"{prefix}/{relative}"
