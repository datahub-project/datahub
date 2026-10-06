"""Phase 0 (combined) — SetupOldStackPhase.

Replaces separate BuildImagesPhase + NukeAndRedeployPhase. Single phase
that brings the stack to the canonical "production-like OLD" starting
state every test run wants:

    A. build OLD + NEW images (always — same logic as BuildImagesPhase)
    B. nuke + redeploy on OLD (default ON, opt-out via ZDU_SKIP_NUKE=1)
    C. refresh access token (default ON, opt-out via ZDU_SKIP_TOKEN_REFRESH=1)

Why combined: A, B, C are interdependent — B needs A's OLD tag, C needs
B's redeployed stack to be healthy. Splitting them across phases meant
runner-level conditionals that didn't reflect this dependency. One phase
makes the contract explicit ("starts test → ends with healthy OLD stack
+ valid token") and lets the orchestrator decide internally which
sub-steps fire.

Sub-step B (nuke) runs only when build_images=True AND old_image_tag !=
"debug" — i.e., only when Phase A actually produced a distinct OLD tag.
Without that, nuking the dev stack and bringing it back on `debug` is
destructive without buying any signal.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .base import ConfiguredPhase, PhaseResult
from .build_images import BuildImagesPhase
from .nuke_and_redeploy import NukeAndRedeployPhase
from ..config import ZDUTestConfig
from ..context import TestContext

log = logging.getLogger(__name__)


class SetupOldStackPhase(ConfiguredPhase):
    name = "setup_old_stack"

    def __init__(
        self,
        build_phase: BuildImagesPhase,
        nuke_phase: NukeAndRedeployPhase,
        gms_url: str,
        datahub_init_username: str = "datahub",
        datahub_init_password: str = "datahub",
    ) -> None:
        self._build = build_phase
        self._nuke = nuke_phase
        self._gms_url = gms_url
        self._username = datahub_init_username
        self._password = datahub_init_password

    def run(self, ctx: TestContext, config: ZDUTestConfig) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        sub_results: dict[str, dict] = {}

        # ── Sub-step A: build images ──────────────────────────────────────
        log.info("[setup] sub-step A — build_images")
        build_result = self._build.run(ctx, config)
        sub_results["build_images"] = {
            "status": build_result.status,
            "details": build_result.details,
        }
        if build_result.status == "failed":
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                error=f"build_images failed: {build_result.error}",
                details={"sub_results": sub_results},
            )

        # ── Sub-step B: nuke + redeploy on OLD ────────────────────────────
        # The phase itself does its own gating on clean_build + old_image_tag,
        # but we still short-circuit here to keep the sub_results clean and
        # the log clearer about what was skipped vs what ran.
        if not config.clean_build:
            log.info("[setup] sub-step B — nuke skipped (clean_build=False)")
            sub_results["nuke_and_redeploy"] = {
                "status": "skipped",
                "reason": "clean_build=False",
            }
        else:
            log.info("[setup] sub-step B — nuke_and_redeploy")
            nuke_result = self._nuke.run(ctx, config)
            sub_results["nuke_and_redeploy"] = {
                "status": nuke_result.status,
                "details": nuke_result.details,
            }
            if nuke_result.status == "failed":
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"nuke_and_redeploy failed: {nuke_result.error}",
                    details={"sub_results": sub_results},
                )

        # ── Sub-step C: refresh access token ──────────────────────────────
        # Best-effort. Since the nuke preserves MySQL, the existing
        # ~/.datahubenv token (if any) is still valid against the redeployed
        # stack — refresh is a defensive belt+suspenders. If `datahub init`
        # hits an authz regression (e.g. canGeneratePersonalAccessToken
        # returning false under AUTH_POLICIES_ENABLED=false on master HEAD),
        # log a warning and proceed with whatever token is already present.
        if not config.refresh_token:
            log.info("[setup] sub-step C — token refresh skipped (refresh_token=False)")
            sub_results["refresh_token"] = {
                "status": "skipped",
                "reason": "refresh_token=False",
            }
        else:
            log.info("[setup] sub-step C — refresh_token via `datahub init`")
            token_result = self._refresh_token(config)
            sub_results["refresh_token"] = token_result
            if token_result["status"] == "failed":
                log.warning(
                    "[setup] token refresh failed (%s) — continuing with "
                    "existing ~/.datahubenv token if present. Phase 4 seed "
                    "will surface the underlying auth issue if this token "
                    "is also invalid.",
                    token_result.get("error", "unknown"),
                )

        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={"sub_results": sub_results},
        )

    def _refresh_token(self, config: ZDUTestConfig) -> dict:
        """Run `datahub init` and load the new token into config.gms_token.

        Why subprocess instead of direct HTTP: the CLI handles endpoint
        discovery, server-config negotiation, and writing ~/.datahubenv with
        the right format — re-implementing that in the framework would be
        churn we don't need. Smoke-test venv ships with the CLI.
        """
        datahub_bin = self._find_datahub_cli()
        if datahub_bin is None:
            return {
                "status": "failed",
                "error": (
                    "datahub CLI not found on PATH. Ensure smoke-test venv is "
                    "activated or install acryl-datahub."
                ),
            }
        # The flag is `--host` (not `--gms-server`). Quickstart defaults host
        # to http://localhost:8080 — passing it explicitly is harmless and
        # makes the framework work against staging/custom URLs.
        cmd = [
            datahub_bin,
            "init",
            "--username",
            self._username,
            "--password",
            self._password,
            "--host",
            self._gms_url,
        ]
        # /health returns 200 well before the GraphQL endpoint is wired up
        # (the auth token mint goes through /api/graphql). Retry with backoff
        # so the redeployed stack has a chance to finish wiring its beans
        # before we mint a token.
        t_start = time.monotonic()
        last_error = ""
        result = None
        for attempt in range(1, 11):
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
            except subprocess.TimeoutExpired:
                last_error = "`datahub init` timed out after 60s"
                continue
            if result.returncode == 0:
                break
            last_error = (
                f"rc={result.returncode}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
            log.info(
                "[setup] `datahub init` attempt %d failed (%s); retrying in 5s",
                attempt,
                last_error.split("\n", 1)[0][:100],
            )
            time.sleep(5)
        if result is None or result.returncode != 0:
            return {
                "status": "failed",
                "error": f"`datahub init` failed after retries: {last_error}",
            }
        # Read the freshly-written token back from ~/.datahubenv and put it
        # on config.gms_token so the runner can refresh DataHubClient.
        token = self._read_datahubenv_token()
        if token is None:
            return {
                "status": "failed",
                "error": (
                    "datahub init succeeded but no token: line found in "
                    "~/.datahubenv afterwards"
                ),
            }
        config.gms_token = token
        log.info(
            "[setup] token refreshed in %.1fs (jti unchanged inspection skipped)",
            time.monotonic() - t_start,
        )
        return {
            "status": "passed",
            "duration_s": round(time.monotonic() - t_start, 2),
            "token_length": len(token),
        }

    @staticmethod
    def _find_datahub_cli() -> str | None:
        """Locate the datahub CLI binary.

        Prefers the smoke-test venv (sibling of the active python) so we use
        the same version that `from datahub import ...` would. Falls back to
        PATH lookup.
        """
        # sys.executable is .../smoke-test/venv/bin/python → datahub is in
        # the same directory.
        import sys

        candidate = Path(sys.executable).parent / "datahub"
        if candidate.is_file():
            return str(candidate)
        return shutil.which("datahub")

    @staticmethod
    def _read_datahubenv_token() -> str | None:
        path = Path.home() / ".datahubenv"
        if not path.is_file():
            return None
        try:
            for line in path.read_text().splitlines():
                stripped = line.strip()
                if stripped.startswith("token:"):
                    value = stripped.split(":", 1)[1].strip()
                    return value or None
        except OSError:
            return None
        return None
