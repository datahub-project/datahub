"""Phase 11 — CleanupPhase.

Restores GMS to the image tag it was running BEFORE the test started, so
the developer's dev session isn't left on a Plan-14-built ``zdu-new-*``
image. Reads the pre-test tag from ``ctx.prepare_old_stack.current_images``
(captured by Phase 0.5 before recreation).

If the pre-test tag is itself a ``zdu-{old,new}-*`` test artifact (i.e.
the user ran two ZDU tests back-to-back without their normal dev workflow
in between), restore to the canonical ``rest_tag`` (default ``debug``)
instead — that's the user's true "rest" state.

Skippable via ``ZDU_SKIP_CLEANUP=1`` (or by adding ``cleanup`` to
``ZDU_SKIP_PHASES``). Useful when the user wants to inspect the post-test
state of the NEW image, or in CI where the stack is torn down at run end.
"""

from __future__ import annotations

import logging
import time
import urllib.error
import urllib.request
from datetime import datetime

from ._shared import read_token_passthrough
from .base import Phase, PhaseResult
from ..context import TestContext
from ..docker_compose import DockerComposeClient

log = logging.getLogger(__name__)


def _extract_tag(image_ref: str) -> str | None:
    """Pull the tag out of ``acryldata/datahub-gms:debug`` → ``"debug"``.

    Returns None for digest-form refs we can't restore from (``sha256:...``).
    """
    if not image_ref or ":" not in image_ref:
        return None
    if image_ref.startswith("sha256:"):
        return None
    return image_ref.rsplit(":", 1)[1]


def _is_zdu_test_artifact(tag: str) -> bool:
    """True when ``tag`` looks like an image this framework built itself
    (``zdu-old-<sha>`` / ``zdu-new-<sha>``). Used to detect when the
    "pre-test" snapshot is itself residue from a prior ZDU run, so cleanup
    can fall back to the canonical rest_tag instead of restoring to garbage.
    """
    return tag.startswith("zdu-old-") or tag.startswith("zdu-new-")


class CleanupPhase(Phase):
    name = "cleanup"

    def __init__(
        self,
        docker: DockerComposeClient,
        gms_service: str,
        health_url: str,
        timeout_s: int = 180,
        rest_tag: str = "debug",
    ) -> None:
        self._docker = docker
        self._gms_service = gms_service
        self._health_url = health_url
        self._timeout_s = timeout_s
        self._rest_tag = rest_tag

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        if ctx.prepare_old_stack is None:
            log.info(
                "[cleanup] ctx.prepare_old_stack is None — nothing to restore "
                "(Phase 0.5 either skipped or didn't run). Skipping."
            )
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={"reason": "no prepare_old_stack snapshot to restore from"},
            )

        pre_test_images = ctx.prepare_old_stack.current_images
        pre_test_gms_image = pre_test_images.get(self._gms_service, "")
        captured_tag = _extract_tag(pre_test_gms_image)

        if captured_tag is None:
            # Pre-test image was a digest (sha256:...) — can't derive a tag.
            # Fall back to the canonical rest_tag so we still leave the user
            # on a sane image (rather than skipping and abandoning them on
            # whatever the test left running).
            original_tag = self._rest_tag
            tag_source = "rest_tag fallback (pre-test image was digest)"
        elif _is_zdu_test_artifact(captured_tag):
            # Pre-test snapshot is itself ZDU-test residue (back-to-back
            # runs without an intervening `datahub-dev.sh start`). Restoring
            # to it would leave the user on yet another test artifact, not
            # their canonical dev image. Use rest_tag instead.
            original_tag = self._rest_tag
            tag_source = (
                f"rest_tag fallback (pre-test tag {captured_tag!r} "
                f"is a zdu-* test artifact)"
            )
        else:
            original_tag = captured_tag
            tag_source = "captured pre-test tag"

        # Check whether GMS is already on the original tag (e.g. nothing to do
        # because Phase 6 rolling_restart never ran or already restored).
        current_image = self._docker.get_all_service_images().get(self._gms_service, "")
        if original_tag in current_image:
            log.info(
                "[cleanup] GMS already on original tag %s — nothing to restore",
                original_tag,
            )
            return PhaseResult(
                phase_name=self.name,
                status="passed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={
                    "original_tag": original_tag,
                    "restored": False,
                    "reason": "already on original tag",
                },
            )

        log.info(
            "[cleanup] restoring %s to %s (source: %s)",
            self._gms_service,
            original_tag,
            tag_source,
        )
        passthrough = read_token_passthrough(
            self._docker, self._gms_service, purpose="cleanup"
        )
        compose_env: dict[str, str] = {
            "DATAHUB_VERSION": original_tag,
            "DATAHUB_GMS_VERSION": original_tag,
            # Restore the default env-file lookup so AUTH_POLICIES_ENABLED
            # comes back to its default (true). Don't leave the dev stack
            # with the test override loaded.
            "DATAHUB_LOCAL_COMMON_ENV": "empty.env",
            **passthrough,
        }
        self._docker.recreate_service(
            service=self._gms_service,
            compose_env=compose_env,
            timeout_s=self._timeout_s,
        )

        healthy = self._wait_for_health(self._health_url, self._timeout_s)
        duration_s = time.monotonic() - t0
        if not healthy:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration_s,
                error=(
                    f"GMS did not come back healthy after restoring to "
                    f"{original_tag} within {self._timeout_s}s"
                ),
                details={
                    "original_tag": original_tag,
                    "restored": True,
                    "tag_source": tag_source,
                },
            )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration_s,
            details={
                "original_tag": original_tag,
                "restored": True,
                "tag_source": tag_source,
            },
        )

    def _wait_for_health(self, url: str, timeout_s: int) -> bool:
        log.info("[cleanup] polling %s/health (timeout %ds)", url, timeout_s)
        t_start = time.monotonic()
        deadline = t_start + timeout_s
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{url}/health", timeout=5) as resp:
                    if resp.status == 200:
                        log.info(
                            "[cleanup] GMS healthy at %s/health after %.1fs",
                            url,
                            time.monotonic() - t_start,
                        )
                        return True
            except (urllib.error.HTTPError, urllib.error.URLError):
                pass
            time.sleep(2)
        log.error(
            "[cleanup] GMS did not become healthy at %s/health within %ds",
            url,
            timeout_s,
        )
        return False
