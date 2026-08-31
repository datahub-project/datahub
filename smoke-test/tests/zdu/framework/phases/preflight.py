"""Phase -1 — PreflightPhase.

Fail-fast checks for common setup mistakes BEFORE Phase 0 spends 5+ minutes
building images against a stack that can't actually receive writes:

1. GMS reachable on ``config.gms_url`` (otherwise: "stack not running").
2. ``config.gms_token`` non-empty (otherwise: "run datahub init").
3. ``DATAHUB_LOCAL_COMMON_ENV`` points at a file containing the master HEAD
   authz bypass (otherwise: seed phase will hit HTTP 403 against
   ``PrivilegeConstraintsValidator`` on current master).

Each failure prints a one-line remediation that the developer can copy-paste.
Skippable via ``ZDU_SKIP_PHASES=preflight`` for users who know better.
"""

from __future__ import annotations

import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from .base import ConfiguredPhase, PhaseResult
from ..config import ZDUTestConfig
from ..context import TestContext

log = logging.getLogger(__name__)

# Both env vars must be set to bypass master HEAD's auth + privilege validators.
_REQUIRED_BYPASS_KEYS: tuple[str, ...] = (
    "AUTH_POLICIES_ENABLED=false",
    "REST_API_AUTHORIZATION_ENABLED=false",
    # METADATA_SERVICE_AUTH_ENABLED=false bypasses bearer-token auth — required
    # because clean_build's MySQL wipe invalidates stateful PERSONAL tokens
    # and master HEAD's createAccessToken regression prevents minting new ones.
    "METADATA_SERVICE_AUTH_ENABLED=false",
    # Note: ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED is NOT
    # checked here. It's only passed via env_overrides to the explicit
    # Phase 4 / Phase 8 system-update one-shot containers — see comment in
    # docker/profiles/zdu-test.env for the full rationale.
)


class PreflightPhase(ConfiguredPhase):
    name = "preflight"

    def __init__(self, project_dir: str) -> None:
        self._project_dir = project_dir

    def run(self, ctx: TestContext, config: ZDUTestConfig) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        failures: list[str] = []

        # When SetupOldStackPhase will nuke + redeploy, the stack may be
        # currently down — that's exactly the case the nuke is here to fix.
        # Skip the reachability check so preflight doesn't block recovery.
        if not config.clean_build:
            if msg := self._check_gms_reachable(config.gms_url):
                failures.append(msg)

        # Same logic for the token — refresh_token regenerates it after
        # redeploy, so missing/stale token at preflight time is fine.
        if not config.refresh_token:
            if msg := self._check_token_set(config.gms_token):
                failures.append(msg)

        if msg := self._check_authz_bypass_env():
            failures.append(msg)

        duration_s = time.monotonic() - t0
        if failures:
            log.error("[preflight] %d check(s) failed:", len(failures))
            for f in failures:
                log.error("[preflight]   • %s", f)
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration_s,
                error="\n".join(failures),
                details={"failed_checks": len(failures)},
            )
        log.info("[preflight] all checks passed")
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration_s,
            details={"checks": 3},
        )

    def _check_gms_reachable(self, gms_url: str) -> str | None:
        try:
            with urllib.request.urlopen(f"{gms_url}/health", timeout=5) as resp:
                if resp.status == 200:
                    return None
                return (
                    f"GMS at {gms_url}/health returned HTTP {resp.status} — "
                    f"expected 200. Restart the stack with "
                    f"`scripts/dev/datahub-dev.sh start`."
                )
        except urllib.error.URLError as e:
            return (
                f"GMS at {gms_url}/health is not reachable ({e.reason}) — "
                f"start the stack first: `scripts/dev/datahub-dev.sh start`"
            )

    def _check_token_set(self, gms_token: str | None) -> str | None:
        if not gms_token:
            return (
                "No GMS bearer token found. Run "
                "`datahub init --username datahub --password datahub` to "
                "issue a personal access token (writes ~/.datahubenv, which "
                "the framework reads automatically). DATAHUB_GMS_TOKEN env "
                "var is honored if set but no longer required."
            )
        return None

    def _check_authz_bypass_env(self) -> str | None:
        env_file = os.environ.get("DATAHUB_LOCAL_COMMON_ENV")
        if not env_file or env_file == "empty.env":
            return (
                "DATAHUB_LOCAL_COMMON_ENV is not set. Master HEAD's "
                "PrivilegeConstraintsValidator rejects seed-phase writes "
                "unless GMS is started with AUTH_POLICIES_ENABLED=false + "
                "REST_API_AUTHORIZATION_ENABLED=false. Re-run with "
                "DATAHUB_LOCAL_COMMON_ENV=zdu-test.env "
                "(see docker/profiles/zdu-test.env)."
            )
        file_path = Path(self._project_dir) / env_file
        if not file_path.is_file():
            return (
                f"DATAHUB_LOCAL_COMMON_ENV={env_file} but no such file at "
                f"{file_path}. Use the tracked override file "
                f"docker/profiles/zdu-test.env."
            )
        content = file_path.read_text()
        missing = [k for k in _REQUIRED_BYPASS_KEYS if k not in content]
        if missing:
            return (
                f"{file_path} is missing required bypass keys: {missing}. "
                f"See docker/profiles/zdu-test.env for the canonical form."
            )
        return None
