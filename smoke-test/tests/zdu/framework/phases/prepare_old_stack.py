"""Phase 0.5 — PrepareOldStackPhase.

Ensures the running Compose stack matches ``config.old_image_tag`` before
``seed`` / ``discovery`` run. Mirrors ``RollingRestartPhase`` but runs
INTO the OLD image (vs OLD→NEW) and fires BEFORE any test traffic so
service ordering is not as sensitive.

Skipped when ``ZDU_SKIP_PREPARE_OLD_STACK=1`` or when ``config.old_image_tag``
is the default ``debug`` (i.e., Phase 0 BuildImagesPhase didn't run).
"""

from __future__ import annotations

import logging
import time
import urllib.error
import urllib.request
from datetime import datetime

from ._shared import read_token_passthrough
from .base import ConfiguredPhase, PhaseResult
from ..config import ZDUTestConfig
from ..constants import PER_SERVICE_VERSION_KEY, TOKEN_SERVICE_KEYS
from ..context import PrepareOldStackResult, TestContext
from ..docker_compose import DockerComposeClient

log = logging.getLogger(__name__)

# Local alias kept for diff-clarity during the centralization pass.
_SERVICE_VERSION_ENV = PER_SERVICE_VERSION_KEY


def _compose_env(
    service: str, tag: str, passthrough: dict[str, str] | None = None
) -> dict[str, str]:
    env: dict[str, str] = {"DATAHUB_VERSION": tag}
    if (per_svc := _SERVICE_VERSION_ENV.get(service)) is not None:
        env[per_svc] = tag
    if passthrough:
        env.update(passthrough)
    return env


class PrepareOldStackPhase(ConfiguredPhase):
    name = "prepare_old_stack"

    def __init__(
        self,
        docker: DockerComposeClient,
        services_to_restart: tuple[str, ...],
        gms_service: str,
        health_url: str,
        timeout_s: int = 180,
    ) -> None:
        self._docker = docker
        self._services_to_restart = services_to_restart
        self._gms_service = gms_service
        self._health_url = health_url
        self._timeout_s = timeout_s

    def run(self, ctx: TestContext, config: ZDUTestConfig) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        if not config.prepare_old_stack:
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={"reason": "ZDU_SKIP_PREPARE_OLD_STACK set or config disabled"},
            )

        old_tag = config.old_image_tag
        if not old_tag or old_tag == "debug":
            # Phase 0 didn't produce a tag → ctx.image_build is None.
            # Otherwise the dev explicitly opted-in to the 'debug' tag — either
            # way we can't enforce a specific OLD without risking a restart loop.
            reason = (
                "old_image_tag is 'debug' — no specific OLD tag to enforce. "
                "Phase 0 was skipped or ZDU_OLD_IMAGE_TAG=debug was set explicitly. "
                "Skipping OLD-stack restart."
                if ctx.image_build is None
                else "old_image_tag is 'debug' (explicit) — skipping restart"
            )
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={"reason": reason},
            )

        current = self._docker.get_all_service_images()
        log.info(
            "[prepare-old-stack] current images: %s",
            {k: v for k, v in current.items() if k in self._services_to_restart},
        )

        # Only consider services that are actually running. The debug compose
        # profile collapses MAE + MCE consumers into the GMS process; trying to
        # recreate them as separate services raises "no such service" from
        # Compose. Skipping absent services lets the same _services_to_restart
        # tuple work across debug and full production-shape profiles.
        absent = [svc for svc in self._services_to_restart if svc not in current]
        if absent:
            log.info(
                "[prepare-old-stack] services not in current stack, skipping: %s",
                absent,
            )

        mismatched = [
            svc
            for svc in self._services_to_restart
            if svc in current and old_tag not in current[svc]
        ]

        if not mismatched:
            log.info("[prepare-old-stack] all services already on OLD (%s)", old_tag)
            ctx.prepare_old_stack = PrepareOldStackResult(
                old_image_tag=old_tag,
                current_images=current,
                services_inspected=list(self._services_to_restart),
                recreated_services=[],
                health_check_passed=True,
                duration_s=time.monotonic() - t0,
            )
            return PhaseResult(
                phase_name=self.name,
                status="passed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={
                    "old_image_tag": old_tag,
                    "recreated_services": [],
                    "reason": "all services already on OLD",
                },
            )

        log.info(
            "[prepare-old-stack] recreating %d service(s) onto %s: %s",
            len(mismatched),
            old_tag,
            mismatched,
        )
        passthrough = read_token_passthrough(
            self._docker, self._gms_service, purpose="prepare_old_stack"
        )
        missing = [k for k in TOKEN_SERVICE_KEYS if not passthrough.get(k)]
        if missing:
            duration_s = time.monotonic() - t0
            ctx.prepare_old_stack = PrepareOldStackResult(
                old_image_tag=old_tag,
                current_images=current,
                services_inspected=list(self._services_to_restart),
                recreated_services=[],
                health_check_passed=False,
                duration_s=duration_s,
            )
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration_s,
                error=(
                    f"refusing to recreate services — required passthrough env "
                    f"missing from {self._gms_service}: {missing}. "
                    f"Without these, system-update-debug (cascade-recreated by "
                    f"Compose) crashes on Spring init. Start the stack via "
                    f"scripts/dev/datahub-dev.sh start so the secrets are "
                    f"loaded before running ZDU."
                ),
                details={"missing_passthrough_keys": missing},
            )

        for service in mismatched:
            log.info("[prepare-old-stack] recreate %s @ %s", service, old_tag)
            self._docker.recreate_service(
                service=service,
                compose_env=_compose_env(service, old_tag, passthrough=passthrough),
                timeout_s=self._timeout_s,
            )

        healthy = self._wait_for_health(self._health_url, self._timeout_s)
        duration_s = time.monotonic() - t0
        ctx.prepare_old_stack = PrepareOldStackResult(
            old_image_tag=old_tag,
            current_images=current,
            services_inspected=list(self._services_to_restart),
            recreated_services=mismatched,
            health_check_passed=healthy,
            duration_s=duration_s,
        )
        if not healthy:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration_s,
                error=(
                    f"GMS health check did not pass after recreating {len(mismatched)} "
                    f"service(s) onto {old_tag} within {self._timeout_s}s"
                ),
                details={"recreated_services": mismatched, "old_image_tag": old_tag},
            )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration_s,
            details={
                "old_image_tag": old_tag,
                "recreated_services": mismatched,
                "current_images_before": current,
            },
        )

    def _wait_for_health(self, url: str, timeout_s: int) -> bool:
        log.info("[prepare-old-stack] polling %s/health (timeout %ds)", url, timeout_s)
        deadline = time.monotonic() + timeout_s
        last_http_code: int | None = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{url}/health", timeout=5) as resp:
                    if resp.status == 200:
                        return True
            except urllib.error.HTTPError as e:
                # Endpoint reachable but returning error (e.g., 503 during boot).
                # Log only on transition so we don't spam the 180s loop.
                if last_http_code != e.code:
                    log.info(
                        "[prepare-old-stack] /health returned %s — retrying", e.code
                    )
                    last_http_code = e.code
            except urllib.error.URLError:
                pass  # connection refused / DNS — expected during restart
            time.sleep(2)
        return False
