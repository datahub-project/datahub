import json
import logging
import os
import platform
import shutil
import stat
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.hex_v2.constants import (
    HEX_CLI_CACHE_DIR,
    HEX_CLI_PAGE_SIZE_MAX,
    HEX_CLI_RELEASE_ASSETS,
    HEX_CLI_VERSION,
)
from datahub.ingestion.source.hex_v2.model import (
    Analytics,
    Category,
    Component,
    DataConnection,
    Owner,
    Project,
    RunRecord,
    Status,
)

logger = logging.getLogger(__name__)


@dataclass
class HexCliReport(SourceReport):
    list_projects_calls: int = 0
    list_projects_items: int = 0
    get_project_calls: int = 0
    get_project_failures: int = 0
    list_connections_calls: int = 0
    export_project_calls: int = 0
    export_project_failures: int = 0
    run_list_calls: int = 0
    run_list_failures: int = 0
    unknown_connection_types: List[str] = field(default_factory=list)


class HexCliClient:
    """
    Thin wrapper around the Hex CLI binary.

    All data fetching goes through subprocess calls to `hex ...`.

    Auth note: `--version draft` conflicts with the CLI's global `--version` flag.
    Never pass `--version` to `hex project export` — the default is already draft.
    """

    def __init__(
        self,
        token: str,
        workspace_name: str,
        report: HexCliReport,
        hex_cli_path: str = "hex",
        timeout_seconds: int = 120,
        page_size: int = 25,
        auto_install: bool = True,
    ):
        self._token = token
        self._workspace_name = workspace_name
        self._report = report
        self._timeout = timeout_seconds
        self._page_size = min(page_size, HEX_CLI_PAGE_SIZE_MAX)
        # Deterministic profile name per workspace so we don't clobber user profiles
        safe_ws = workspace_name[:20].replace("/", "-").replace(" ", "-")
        self._profile = f"datahub-{safe_ws}"
        self._authed = False
        # REST session for endpoints the CLI doesn't expose (e.g. sharing/collections)
        self._api_base_url = "https://app.hex.tech/api/v1"
        # Resolve the CLI binary path, downloading v{HEX_CLI_VERSION} if necessary
        self._hex_cli_path = _ensure_hex_binary(
            configured_path=hex_cli_path, auto_install=auto_install
        )
        self._session = self._make_session()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def ensure_auth(self) -> None:
        """Bootstrap CLI auth using the configured token."""
        env = {**os.environ, "HEX_CLI_LOGIN_TOKEN": self._token}
        result = subprocess.run(
            [
                self._hex_cli_path,
                "auth",
                "login",
                "--token-from-env",
                "--insecure-storage",
                "--profile",
                self._profile,
            ],
            capture_output=True,
            text=True,
            timeout=self._timeout,
            env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Hex CLI auth failed (exit {result.returncode}): "
                f"{result.stdout.strip()} {result.stderr.strip()}"
            )
        self._authed = True
        logger.debug("Hex CLI auth succeeded for profile %s", self._profile)

    def _make_session(self) -> requests.Session:
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {self._token}"
        retry = Retry(total=3, status_forcelist=[429], backoff_factor=2)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        if not self._authed:
            self.ensure_auth()
        cmd = [self._hex_cli_path, "--profile", self._profile, *args]
        logger.debug("Running: %s", " ".join(cmd))
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=self._timeout,
        )

    # ------------------------------------------------------------------
    # Sharing / collections (REST only — CLI --json omits sharing data)
    # ------------------------------------------------------------------

    def fetch_collections_for_projects(
        self, project_ids: List[str]
    ) -> Dict[str, List[str]]:
        """
        Return a map of project_id → list[collection_name] by calling the
        REST API with includeSharing=true.

        The CLI's `project list --include-sharing --json` does NOT include sharing
        data in its JSON output. We fall back to a single paginated REST call here.
        """
        result: Dict[str, List[str]] = {pid: [] for pid in project_ids}
        id_set = set(project_ids)
        after: Optional[str] = None

        while True:
            params: Dict[str, object] = {
                "includeSharing": True,
                "limit": self._page_size,
            }
            if after:
                params["after"] = after
            try:
                resp = self._session.get(
                    f"{self._api_base_url}/projects",
                    params=params,
                    timeout=self._timeout,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                self._report.warning(
                    title="Failed to fetch project sharing/collections",
                    message="REST call for sharing info failed; collections will be omitted",
                    exc=e,
                )
                return result

            for item in data.get("values", []):
                pid = item.get("id")
                if pid not in id_set:
                    continue
                collections = [
                    c["collection"]["name"]
                    for c in (item.get("sharing") or {}).get("collections") or []
                ]
                result[pid] = collections

            after = (data.get("pagination") or {}).get("after")
            if not after:
                break

        return result

    # ------------------------------------------------------------------
    # Project listing
    # ------------------------------------------------------------------

    def list_projects(
        self,
        include_components: bool = True,
        include_archived: bool = False,
        include_trashed: bool = False,
    ) -> Generator[Union[Project, Component], None, None]:
        """Yield all projects (and optionally components) via paginated CLI calls."""
        args = [
            "project",
            "list",
            "--json",
            "--limit",
            str(self._page_size),
        ]
        if include_components:
            args.append("--include-components")
        if include_archived:
            args.append("--include-archived")
        if include_trashed:
            args.append("--include-trashed")

        after: Optional[str] = None
        while True:
            page_args = args + (["--after", after] if after else [])
            result = self._run(*page_args)
            self._report.list_projects_calls += 1

            if result.returncode != 0:
                self._report.failure(
                    title="Error listing projects",
                    message="CLI returned non-zero exit code listing projects",
                    context=result.stdout[:500],
                )
                return

            try:
                data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                self._report.failure(
                    title="Error parsing project list response",
                    message="JSON decode error listing projects",
                    exc=e,
                    context=result.stdout[:500],
                )
                return

            for item in data.get("projects", []):
                self._report.list_projects_items += 1
                yield _parse_project_list_item(item)

            after = (data.get("pagination") or {}).get("after")
            if not after:
                break

    # ------------------------------------------------------------------
    # Project get (rich metadata)
    # ------------------------------------------------------------------

    def get_project(self, project_id: str) -> Optional[Union[Project, Component]]:
        """Fetch full metadata for a single project or component."""
        result = self._run("project", "get", project_id, "--json")
        self._report.get_project_calls += 1

        if result.returncode != 0:
            self._report.get_project_failures += 1
            self._report.warning(
                title="Failed to get project metadata",
                message=f"CLI returned exit {result.returncode} for project {project_id}",
                context=result.stdout[:500],
            )
            return None

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            self._report.get_project_failures += 1
            self._report.warning(
                title="Failed to parse project metadata",
                message=f"JSON decode error for project {project_id}",
                exc=e,
            )
            return None

        return _parse_project_get(data)

    # ------------------------------------------------------------------
    # Connections
    # ------------------------------------------------------------------

    def list_connections(self) -> Dict[str, DataConnection]:
        """Return a map of connection_id → DataConnection."""
        result = self._run("connection", "list", "--json")
        self._report.list_connections_calls += 1

        if result.returncode != 0:
            self._report.warning(
                title="Failed to list data connections",
                message="CLI returned non-zero exit listing connections; lineage platform resolution will fall back to default",
                context=result.stdout[:500],
            )
            return {}

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            self._report.warning(
                title="Failed to parse connections response",
                message="JSON decode error listing connections",
                exc=e,
            )
            return {}

        connections: Dict[str, DataConnection] = {}
        for item in data.get("connections", []):
            conn = DataConnection(
                id=item["id"],
                name=item.get("name", ""),
                connection_type=item.get("connection_type", ""),
                description=item.get("description"),
            )
            connections[conn.id] = conn

        return connections

    # ------------------------------------------------------------------
    # Project YAML export
    # ------------------------------------------------------------------

    def export_project_yaml(self, project_id: str) -> Optional[str]:
        """
        Export project YAML and return its content as a string.

        Important: do NOT pass --version — it conflicts with the global CLI
        --version flag. The default export version is already draft.
        """
        self._report.export_project_calls += 1

        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            tmp_path = f.name

        try:
            result = self._run("project", "export", project_id, "-o", tmp_path)

            file_size = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
            if result.returncode != 0 or file_size == 0:
                self._report.export_project_failures += 1
                self._report.warning(
                    title="Failed to export project YAML",
                    message=f"Export failed for project {project_id} (exit {result.returncode}, file_size={file_size})",
                    context=result.stdout[:300],
                )
                return None

            with open(tmp_path) as f:
                return f.read()
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ------------------------------------------------------------------
    # Run history
    # ------------------------------------------------------------------

    def get_latest_run(self, project_id: str) -> Optional[RunRecord]:
        """Return the most recent run for a project, or None if unavailable."""
        result = self._run("run", "list", project_id, "--json", "--limit", "1")
        self._report.run_list_calls += 1

        # Exit 1 with "An unknown error occurred." is normal when a project has no
        # runs or the account lacks run-history permissions — treat as no runs.
        if result.returncode != 0:
            self._report.run_list_failures += 1
            logger.debug(
                "run list returned exit %s for project %s (likely no runs): %s",
                result.returncode,
                project_id,
                result.stdout[:100],
            )
            return None

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            self._report.run_list_failures += 1
            return None

        runs = data.get("runs", [])
        if not runs:
            return None

        return _parse_run_record(runs[0])


# ------------------------------------------------------------------
# Parse helpers
# ------------------------------------------------------------------


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    # Hex timestamps are always UTC ISO-8601 with 'Z'
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_project_list_item(data: dict) -> Union[Project, Component]:
    """Parse a minimal project-list item (only id, title, updated_at)."""
    # project list only returns id/title/updated_at — type unknown until get
    return Project(
        id=data["id"],
        title=data.get("title", ""),
        description=None,
        last_edited_at=_parse_iso(data.get("updated_at")),
    )


def _parse_project_get(data: dict) -> Union[Project, Component]:
    """Parse the rich response from `hex project get <id> --json`."""
    status = Status(name=data["status"]["name"]) if data.get("status") else None

    categories = [
        Category(name=c["name"], description=c.get("description"))
        for c in (data.get("categories") or [])
    ]

    creator = Owner(email=data["creator"]["email"]) if data.get("creator") else None
    owner = Owner(email=data["owner"]["email"]) if data.get("owner") else None

    analytics: Optional[Analytics] = None
    if data.get("analytics") and data["analytics"].get("appViews"):
        av = data["analytics"]["appViews"]
        analytics = Analytics(
            appviews_all_time=av.get("allTime"),
            appviews_last_7_days=av.get("lastSevenDays"),
            appviews_last_14_days=av.get("lastFourteenDays"),
            appviews_last_30_days=av.get("lastThirtyDays"),
            last_viewed_at=_parse_iso(data["analytics"].get("lastViewedAt")),
        )

    kwargs = dict(
        id=data["id"],
        title=data.get("title", ""),
        description=data.get("description"),
        created_at=_parse_iso(data.get("createdAt")),
        last_edited_at=_parse_iso(data.get("lastEditedAt")),
        last_published_at=_parse_iso(data.get("lastPublishedAt")),
        status=status,
        categories=categories,
        creator=creator,
        owner=owner,
        analytics=analytics,
    )

    if data.get("type") == "COMPONENT":
        return Component(**kwargs)
    return Project(**kwargs)


def _parse_run_record(data: dict) -> Optional[RunRecord]:
    start_time = _parse_iso(data.get("start_time"))
    if not start_time:
        return None
    return RunRecord(
        run_id=data["run_id"],
        status=data.get("status", "UNKNOWN"),
        start_time=start_time,
        elapsed_seconds=data.get("elapsed_seconds"),
    )


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ------------------------------------------------------------------
# Hex CLI binary resolution + auto-install
# ------------------------------------------------------------------

_CACHED_CLI_PATH: Optional[str] = None


def _ensure_hex_binary(configured_path: str, auto_install: bool) -> str:
    """
    Return a path to a working `hex` binary at pinned version HEX_CLI_VERSION.

    Resolution order:
    1. If `configured_path` points to a working binary at the right version, use it.
    2. If a cached binary exists in HEX_CLI_CACHE_DIR, use it.
    3. If `auto_install` is True, download and cache the pinned release, then use it.
    4. If nothing works, raise RuntimeError with an actionable message.
    """
    global _CACHED_CLI_PATH
    if _CACHED_CLI_PATH:
        return _CACHED_CLI_PATH

    # 1. Try configured path (typically "hex" on PATH)
    resolved = shutil.which(configured_path) or (
        configured_path if os.path.isfile(configured_path) else None
    )
    if resolved and _is_correct_version(resolved):
        logger.debug("Using Hex CLI at %s (version %s)", resolved, HEX_CLI_VERSION)
        _CACHED_CLI_PATH = resolved
        return resolved

    # 2. Try cached download
    cached = HEX_CLI_CACHE_DIR / HEX_CLI_VERSION / "hex"
    if cached.is_file() and _is_correct_version(str(cached)):
        logger.debug("Using cached Hex CLI at %s", cached)
        _CACHED_CLI_PATH = str(cached)
        return str(cached)

    # 3. Auto-download
    if not auto_install:
        raise RuntimeError(
            f"Hex CLI v{HEX_CLI_VERSION} not found. Install it from "
            f"https://github.com/hex-inc/hex-cli/releases/tag/v{HEX_CLI_VERSION} "
            f"or set auto_install_hex_cli: true in your recipe to download automatically."
        )

    downloaded = _download_hex_cli()
    _CACHED_CLI_PATH = downloaded
    return downloaded


def _is_correct_version(path: str) -> bool:
    """Return True if the binary at `path` is HEX_CLI_VERSION."""
    try:
        r = subprocess.run(
            [path, "--version"], capture_output=True, text=True, timeout=10
        )
        return HEX_CLI_VERSION in (r.stdout + r.stderr)
    except Exception:
        return False


def _download_hex_cli() -> str:
    """Download the pinned Hex CLI release for the current platform, cache, and return path."""
    system = platform.system()
    machine = platform.machine()

    asset = HEX_CLI_RELEASE_ASSETS.get((system, machine))
    if not asset:
        raise RuntimeError(
            f"No Hex CLI release available for {system}/{machine}. "
            f"Install manually from https://github.com/hex-inc/hex-cli/releases/tag/v{HEX_CLI_VERSION}"
        )

    url = f"https://github.com/hex-inc/hex-cli/releases/download/v{HEX_CLI_VERSION}/{asset}"
    logger.info(
        "Downloading Hex CLI v%s for %s/%s from %s",
        HEX_CLI_VERSION,
        system,
        machine,
        url,
    )

    dest_dir = HEX_CLI_CACHE_DIR / HEX_CLI_VERSION
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_binary = dest_dir / "hex"

    with tempfile.NamedTemporaryFile(suffix=".tar.xz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        with tarfile.open(tmp_path, "r:xz") as tar:
            # Archive layout: hex-<platform>/hex  (e.g. hex-aarch64-apple-darwin/hex)
            hex_member = next(
                (
                    m
                    for m in tar.getmembers()
                    if m.name.split("/")[-1] == "hex" and m.isfile()
                ),
                None,
            )
            if hex_member is None:
                raise RuntimeError(f"Could not find 'hex' binary inside {asset}")
            extracted = tar.extractfile(hex_member)
            if extracted is None:
                raise RuntimeError(f"Failed to extract 'hex' binary from {asset}")
            with open(dest_binary, "wb") as out:
                out.write(extracted.read())

        # Make executable
        dest_binary.chmod(
            dest_binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        )

        logger.info("Hex CLI v%s installed to %s", HEX_CLI_VERSION, dest_binary)
        return str(dest_binary)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
