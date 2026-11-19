from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import quote, urlparse, urlsplit

import requests
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from datahub.ingestion.source.repository_documents.client import (
    MAX_FILE_SIZE_BYTES,
    RepositoryFileInfo,
)
from datahub.ingestion.source.repository_documents.paths import TreeSelection

logger = logging.getLogger(__name__)

DEFAULT_GITLAB_URL = "https://gitlab.com"
API_PATH = "/api/v4"
TREE_PAGE_SIZE = 100
MAX_RETRY_AFTER_SECONDS = 60
MAX_REQUEST_ATTEMPTS = 4
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class _RawFile:
    content: bytes


@dataclass(frozen=True)
class _TooLarge:
    """The file is larger than MAX_FILE_SIZE_BYTES, so it was not read."""


@dataclass(frozen=True)
class _Failed:
    """GitLab answered with an error, recorded in ``last_api_error``."""


_RawFileResult = Union[_RawFile, _TooLarge, _Failed]


class _RetryableResponse(Exception):
    def __init__(self, response: requests.Response) -> None:
        super().__init__(f"HTTP {response.status_code}")
        self.response = response


_exponential_wait = wait_exponential(multiplier=1, max=30)


def _wait_before_retry(state: RetryCallState) -> float:
    error = state.outcome.exception() if state.outcome else None
    if isinstance(error, _RetryableResponse) and error.response.status_code == 429:
        return _retry_after_seconds(error.response)
    return _exponential_wait(state)


# One policy for every request. A body can fail after the headers arrived
# (connection reset mid-body, broken compression), so those errors are
# transient too.
_retry_transient_errors = retry(
    retry=retry_if_exception_type(
        (
            _RetryableResponse,
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        )
    ),
    stop=stop_after_attempt(MAX_REQUEST_ATTEMPTS),
    wait=_wait_before_retry,
    reraise=True,
)


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
        self._api_base = f"{base_url.rstrip('/')}{API_PATH}"
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

    def get_latest_commit_sha(self, owner_repo: str, branch: str) -> Optional[str]:
        url = (
            f"{self._project_url(owner_repo)}/repository/branches/"
            f"{quote(branch, safe='')}"
        )
        response = self._get(url)
        node = self._json_or_none(response)
        if response is None or node is None:
            return None
        commit = node.get("commit") if isinstance(node, dict) else None
        commit_id = commit.get("id") if isinstance(commit, dict) else None
        if isinstance(commit_id, str) and commit_id:
            return commit_id
        self._record_unexpected_shape(response, "a branch with a commit id")
        return None

    def list_matching_files(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        extensions: List[str],
    ) -> Tuple[List[RepositoryFileInfo], bool]:
        # The whole tree is listed, not just path_prefix, because OKF bundle
        # discovery needs index.md files above the prefix.
        url: Optional[str] = f"{self._project_url(owner_repo)}/repository/tree"
        params: Optional[Dict[str, Union[str, int]]] = {
            "ref": branch,
            "recursive": "true",
            "per_page": TREE_PAGE_SIZE,
            # Offset pagination stops at a fixed number of entries on large
            # trees; keyset pagination does not. Older instances ignore it.
            "pagination": "keyset",
        }
        selection = TreeSelection.for_filters(path_prefix, extensions)
        self._index_file_paths = set()

        saw_pagination_header = False
        last_page_full = False
        while url:
            response = self._get(url, params=params)
            items = self._json_or_none(response)
            if (
                response is not None
                and items is not None
                and not isinstance(items, list)
            ):
                self._record_unexpected_shape(response, "a list of tree entries")
            if response is None or not isinstance(items, list):
                # A partial listing would let stale removal delete the files
                # on the pages that were not read, so fail the run instead.
                detail = self._last_api_error or "GitLab API request failed"
                raise RuntimeError(
                    f"Could not list branch '{branch}' in project {owner_repo}: {detail}"
                )
            for item in items:
                if not isinstance(item, dict) or item.get("type") != "blob":
                    continue
                blob_id = item.get("id")
                selection.add(
                    RepositoryFileInfo(
                        path=item.get("path", ""),
                        size=None,
                        sha=blob_id if isinstance(blob_id, str) else None,
                    )
                )
            if "Link" in response.headers or "X-Next-Page" in response.headers:
                saw_pagination_header = True
            last_page_full = len(items) >= TREE_PAGE_SIZE
            url, params = self._next_tree_page(response, url, params)

        # Keyset responses only send a Link header while more pages remain, and
        # never X-Next-Page, so a complete tree can end on a full page with no
        # pagination headers. Once pagination headers have been seen, a missing
        # next link means the listing is done. If none came back at all (a proxy
        # can strip them), a full single page is ambiguous, so ask for the next
        # offset page to find out.
        truncated = (
            last_page_full
            and not saw_pagination_header
            and self._tree_has_entries_after_first_page(owner_repo, branch)
        )
        self._index_file_paths = selection.index_paths
        return selection.files, truncated

    def _tree_has_entries_after_first_page(self, owner_repo: str, branch: str) -> bool:
        response = self._get(
            f"{self._project_url(owner_repo)}/repository/tree",
            params={
                "ref": branch,
                "recursive": "true",
                "per_page": TREE_PAGE_SIZE,
                "page": 2,
            },
        )
        items = self._json_or_none(response)
        if response is not None and items is not None and not isinstance(items, list):
            self._record_unexpected_shape(response, "a list of tree entries")
        if not isinstance(items, list):
            # Cannot confirm the listing is complete, so treat it as truncated
            # and keep stale removal from acting on it.
            return True
        return len(items) > 0

    def _next_tree_page(
        self,
        response: requests.Response,
        url: str,
        params: Optional[Dict[str, Union[str, int]]],
    ) -> Tuple[Optional[str], Optional[Dict[str, Union[str, int]]]]:
        next_url = response.links.get("next", {}).get("url")
        if next_url:
            # The link already carries the query string (offset or keyset).
            return self._on_configured_instance(next_url), None
        next_page = response.headers.get("X-Next-Page", "")
        if next_page.isdigit():
            return url, {**(params or {}), "page": int(next_page)}
        return None, None

    def _on_configured_instance(self, link: str) -> str:
        """The API link rebased onto the configured instance.

        Behind a TLS proxy, a self-managed instance's ``external_url`` can name
        another scheme, host or port than the one configured here, and GitLab
        builds next-page links from it. The token is sent with every request,
        so the link's API path is replayed on the configured instance instead
        of following the link's host.
        """
        if link.startswith(f"{self._api_base}/"):
            return link
        parsed = urlsplit(link)
        marker = parsed.path.find(f"{API_PATH}/")
        if marker < 0:
            raise RuntimeError(
                f"GitLab returned a next-page link that is not an API link: {link}"
            )
        rebased = f"{self._api_base}{parsed.path[marker + len(API_PATH) :]}"
        logger.debug("Rebased next-page link %s onto %s", link, self._api_base)
        return f"{rebased}?{parsed.query}" if parsed.query else rebased

    def fetch_file_content(
        self, owner_repo: str, file_path: str, branch: str
    ) -> Optional[str]:
        # The raw endpoint returns the bytes themselves, so one streamed GET
        # both applies the size limit and reads the file. The tree listing
        # carries no sizes.
        url = (
            f"{self._project_url(owner_repo)}/repository/files/"
            f"{quote(file_path, safe='')}/raw"
        )
        result: _RawFileResult
        try:
            result = self._get_raw_file_with_retry(url, branch)
        except (_RetryableResponse, requests.RequestException) as exc:
            # Includes a connection reset or timeout while reading the body.
            self._record_error(exc)
            result = _Failed()
        if isinstance(result, _Failed):
            detail = self._last_api_error or "GitLab API request failed"
            logger.warning("Failed to fetch file content for %s: %s", file_path, detail)
            return None
        if isinstance(result, _TooLarge):
            logger.warning(
                "File %s is over %s bytes, skipping", file_path, MAX_FILE_SIZE_BYTES
            )
            return None
        return result.content.decode("utf-8", errors="replace")

    @_retry_transient_errors
    def _get_raw_file_with_retry(self, url: str, ref: str) -> _RawFileResult:
        """The file, read here inside the retry because a streamed body can
        still fail after the response headers arrived."""
        response = self._session.get(
            url,
            params={"ref": ref},
            headers={"PRIVATE-TOKEN": self._token},
            timeout=self._timeout,
            stream=True,
            allow_redirects=False,
        )
        with response:
            if response.status_code in RETRYABLE_STATUS_CODES:
                logger.info(
                    "GitLab returned HTTP %s for %s; retrying",
                    response.status_code,
                    url,
                )
                raise _RetryableResponse(response)
            if response.status_code != 200:
                self._last_api_error = _describe_http_error(response)
                logger.warning("%s for %s", self._last_api_error, url)
                return _Failed()
            if _is_html(response):
                # A proxy or SSO login page can answer 200 with HTML. Raw files
                # are served as text/plain, so this is not the file.
                self._last_api_error = (
                    "GitLab returned an HTML page instead of the file. A proxy or "
                    "SSO login page may be intercepting API requests."
                )
                logger.warning("%s for %s", self._last_api_error, response.url)
                return _Failed()
            # A skip for size is not a failed read either, so clear any earlier
            # request's error.
            self._last_api_error = None
            content = _read_up_to(response, MAX_FILE_SIZE_BYTES)
            return _TooLarge() if content is None else _RawFile(content=content)

    def _record_error(
        self, exc: Union[_RetryableResponse, requests.RequestException]
    ) -> None:
        """Record a request that failed after its retries as ``last_api_error``."""
        if isinstance(exc, _RetryableResponse):
            self._last_api_error = _describe_http_error(exc.response)
        else:
            self._last_api_error = f"GitLab API request failed: {exc}"

    def _record_unexpected_shape(
        self, response: requests.Response, expected: str
    ) -> None:
        # A 200 cleared last_api_error, so without this the caller would only
        # see a generic failure.
        self._last_api_error = (
            f"GitLab returned an unexpected response shape (expected {expected}). "
            "A proxy may be answering for the API, or the API version differs."
        )
        logger.warning("%s for %s", self._last_api_error, response.url)

    def _json_or_none(self, response: Optional[requests.Response]) -> object:
        if response is None:
            return None
        try:
            return response.json()
        except ValueError:
            # A proxy or SSO login page can answer 200 with HTML. Record it as an
            # API error so callers do not mistake it for a missing file.
            content_type = response.headers.get("Content-Type") or "no content type"
            self._last_api_error = (
                f"GitLab returned a non-JSON response ({content_type}). A proxy or "
                "SSO login page may be intercepting API requests."
            )
            logger.warning("%s for %s", self._last_api_error, response.url)
            return None

    def _get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Union[str, int]]] = None,
    ) -> Optional[requests.Response]:
        try:
            return self._get_with_retry(url, params)
        except (_RetryableResponse, requests.RequestException) as exc:
            self._record_error(exc)
        logger.warning("%s for %s", self._last_api_error, url)
        return None

    @_retry_transient_errors
    def _get_with_retry(
        self, url: str, params: Optional[Dict[str, Union[str, int]]]
    ) -> Optional[requests.Response]:
        response = self._session.get(
            url,
            params=params,
            headers={"PRIVATE-TOKEN": self._token},
            timeout=self._timeout,
            # requests keeps custom headers such as PRIVATE-TOKEN on a redirect
            # to another host, so redirects are reported rather than followed.
            allow_redirects=False,
        )
        if response.status_code in RETRYABLE_STATUS_CODES:
            logger.info(
                "GitLab returned HTTP %s for %s; retrying", response.status_code, url
            )
            raise _RetryableResponse(response)
        if response.status_code == 200:
            self._last_api_error = None
            return response
        self._last_api_error = _describe_http_error(response)
        logger.warning("%s for %s", self._last_api_error, url)
        return None


def _is_html(response: requests.Response) -> bool:
    content_type = response.headers.get("Content-Type") or ""
    return content_type.lower().startswith("text/html")


def _read_up_to(response: requests.Response, limit: int) -> Optional[bytes]:
    """The body, or None once it is larger than ``limit`` bytes. Stops reading
    as soon as the limit is passed, so an oversized file is never downloaded."""
    declared = response.headers.get("Content-Length", "")
    if declared.isdigit() and int(declared) > limit:
        return None
    body = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        body.extend(chunk)
        if len(body) > limit:
            return None
    return bytes(body)


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
    if 300 <= status < 400:
        # Not followed: the token would be sent to wherever it points.
        return (
            f"GitLab redirected the request (HTTP {status}) to "
            f"{response.headers.get('Location', 'an unknown location')}. A proxy "
            "or SSO login may be intercepting API requests; set base_url to the "
            "address that serves the API."
        )
    return f"GitLab API returned HTTP {status}"
