"""Phase 0.25 — NukeAndRedeployPhase (G19c).

Opt-in phase that wipes the Compose stack to a clean state, then redeploys
from scratch on the OLD image. Solves the "ES retains stale mappings" problem
that otherwise prevents SystemUpdateBlocking from seeing real mapping diffs
between OLD and NEW images.

The diagnostic context: GMS at boot only writes mappings for indices that
don't yet exist (ESIndexBuilder.buildIndex line 504). For existing indices
it leaves mappings alone — only SystemUpdateBlocking actually applies updates.
So if a previous test run left ES with NEW-shape mappings, the current run's
OLD GMS doesn't reset them, and Phase 4's diff comes up empty — the test
silently reports "no mapping change in this run" even when OLD/NEW PDLs
genuinely differ.

This phase puts ES (and MySQL, Kafka, etc.) back to a green-field state so
GMS-OLD at first-boot writes OLD-shape mappings. SystemUpdateBlocking then
sees the real diff and exercises the reindex path.

Opt-in via ``ZDU_CLEAN_BUILD=1`` (or ``--clean-build``). Default OFF because
it adds ~60-180s for full cold-boot and destroys all stack data.

Token-service secrets are captured from the running GMS BEFORE the nuke and
restored via compose_env on the redeploy, so the developer's ~/.datahubenv
token still verifies after the stack comes back up.
"""

from __future__ import annotations

import logging
import os
import pathlib
import time
import urllib.error
import urllib.request
from datetime import datetime

from .base import ConfiguredPhase, PhaseResult
from ..config import ZDUTestConfig
from ..constants import REPO_ROOT, TOKEN_SERVICE_KEYS
from ..context import TestContext
from ..docker_compose import DockerComposeClient
from ..host_mounts import worktree_mount_env

# Persisted dev secrets file maintained by docker/build.gradle's
# resolveTokenServiceSecrets. Mirrors the Gradle precedence so we end up with
# the same JWT signing key the user's datahub-dev.sh workflow uses across runs.
_LOCAL_SECRETS_FILE = REPO_ROOT / "docker" / ".local-secrets.env"

log = logging.getLogger(__name__)

# Local alias kept for diff clarity — TOKEN_SERVICE_KEYS is the canonical name.
_PRESERVED_KEYS: tuple[str, ...] = TOKEN_SERVICE_KEYS


def _read_local_secrets_file(path: pathlib.Path) -> dict[str, str]:
    """Parse a KEY=VALUE env file. Returns {} on any failure (missing,
    unreadable, malformed) — caller decides whether to log a warning.
    """
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    try:
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            key = key.strip()
            value = value.strip()
            if key in _PRESERVED_KEYS and value:
                out[key] = value
    except OSError:
        return {}
    return out


class NukeAndRedeployPhase(ConfiguredPhase):
    name = "nuke_and_redeploy"

    def __init__(
        self,
        docker: DockerComposeClient,
        gms_service: str,
        health_url: str,
        down_timeout_s: int = 120,
        up_timeout_s: int = 120,
        health_timeout_s: int = 300,
    ) -> None:
        self._docker = docker
        self._gms_service = gms_service
        self._health_url = health_url
        self._down_timeout_s = down_timeout_s
        self._up_timeout_s = up_timeout_s
        self._health_timeout_s = health_timeout_s

    def run(self, ctx: TestContext, config: ZDUTestConfig) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        if not config.clean_build:
            log.info("[nuke] clean_build=False — skipping")
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={"reason": "ZDU_CLEAN_BUILD not set"},
            )

        if config.old_image_tag == "debug":
            # No Phase 0 build happened — there's no distinct OLD tag to
            # redeploy onto. Nuking the dev stack and bringing it back on
            # 'debug' is destructive without buying any test signal.
            log.warning(
                "[nuke] old_image_tag=debug — refusing to nuke without a "
                "distinct OLD tag from Phase 0 build_images"
            )
            return PhaseResult(
                phase_name=self.name,
                status="skipped",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={
                    "reason": "no distinct OLD tag (Phase 0 build_images did "
                    "not run or produced the default debug tag)"
                },
            )

        log.info(
            "[nuke] capturing token-service secrets from running %s "
            "(so redeployed stack keeps the same signing key)",
            self._gms_service,
        )
        preserved = self._docker.get_service_env(
            self._gms_service, list(_PRESERVED_KEYS)
        )
        # When the stack is already down (e.g. previous run's nuke crashed
        # mid-way, or this is a first-ever run), docker inspect returns
        # nothing. Fall back to docker/.local-secrets.env — the persisted
        # file Gradle's build maintains for the same purpose. Without this,
        # the redeployed stack starts with a blank signing key and
        # system-update-debug crashes Spring init with "signingKey must be
        # set and not be empty".
        if set(preserved.keys()) != set(_PRESERVED_KEYS):
            from_file = _read_local_secrets_file(_LOCAL_SECRETS_FILE)
            if from_file:
                log.info(
                    "[nuke] running GMS env missing token-service keys; "
                    "loaded from %s instead",
                    _LOCAL_SECRETS_FILE,
                )
                preserved = {**from_file, **preserved}
        if set(preserved.keys()) != set(_PRESERVED_KEYS):
            log.warning(
                "[nuke] could not resolve all token-service keys "
                "(got %s); redeployed stack will generate fresh keys and "
                "the existing ~/.datahubenv token may stop verifying. "
                "Run `datahub init` if so.",
                sorted(preserved.keys()),
            )

        # Wipe BOTH OpenSearch and MySQL volumes. Two reasons:
        #   1. ES wipe forces GMS-OLD at boot to create indices with OLD-shape
        #      mappings (`ESIndexBuilder.buildIndex` line 504), which makes
        #      SystemUpdateBlocking see a real diff when NEW introduces
        #      TEXT_PARTIAL or @Searchable annotations.
        #   2. MySQL wipe ensures seed phase writes aren't idempotent no-ops
        #      against stale data from previous runs. Without this, GMS's
        #      ConditionalWriteValidator sees same-data UPSERTs and suppresses
        #      MCL emission → ES never gets populated by seed → reindex sees
        #      empty source index → TC-015 etc. fail with "document missing".
        # The stale ~/.datahubenv token gets ignored anyway because
        # METADATA_SERVICE_AUTH_ENABLED=false (set in compose_env below), so
        # we don't need MySQL's stateful token records to be valid.
        # Pass SHORT volume names; DockerComposeClient prefixes with the live
        # compose project name. Hardcoding ``datahub_*`` here was broken on
        # any machine where the clone directory wasn't named ``datahub``
        # (compose project name defaults to the dir name, so the prefix
        # differed and the wipe silently no-op'd).
        log.info("[nuke] wiping containers + MySQL + OpenSearch volumes")
        self._docker.down_and_wipe(
            timeout_s=self._down_timeout_s,
            wipe_volumes=["osdata", "mysqldata"],
        )

        # G20c — point the dev host mounts at the OLD worktree's build outputs
        # so the redeployed GMS / system-update containers run with OLD jars +
        # PDL resources, not whatever the dev's working tree last built. When
        # the worktree isn't present (skip-build flow), fall back to the YAML
        # defaults — the dev tree mount is still better than a broken stack.
        mount_env = worktree_mount_env(
            REPO_ROOT,
            config.build_images_root,
            "old",
        )
        if mount_env:
            log.info(
                "[nuke] pinning host mounts to OLD worktree: %s",
                sorted(mount_env.keys()),
            )
        compose_env = {
            # Pin GMS specifically to OLD via the per-service var (NOT
            # DATAHUB_VERSION — that one cascades to all services including
            # frontend/actions whose images don't exist at the OLD tag).
            "DATAHUB_GMS_VERSION": config.old_image_tag,
            # Carry DATAHUB_LOCAL_COMMON_ENV through so the redeployed GMS
            # honors the same authz-bypass file (zdu-test.env) that
            # preflight already validated.
            **(
                {"DATAHUB_LOCAL_COMMON_ENV": v}
                if (v := os.environ.get("DATAHUB_LOCAL_COMMON_ENV"))
                else {}
            ),
            **(mount_env or {}),
            # METADATA_SERVICE_AUTH_ENABLED must be set as a SHELL env var
            # (not just in zdu-test.env), because the compose YAML reads it
            # via ${METADATA_SERVICE_AUTH_ENABLED:-true} at parse time —
            # that substitution comes from the parent process env, not
            # from env_file injections. zdu-test.env's value (loaded as
            # container env) loses to the YAML's `environment:` declaration.
            "METADATA_SERVICE_AUTH_ENABLED": "false",
            # G20a's reindex flags (ELASTICSEARCH_BUILD_INDICES_INCREMENTAL_REINDEX_ENABLED
            # and ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX) reach GMS via
            # zdu-test.env (loaded as env_file by the compose YAML's
            # ${DATAHUB_LOCAL_COMMON_ENV} substitution), so they don't need to
            # be in compose_env here. Compose_env feeds YAML ${...} interpolation,
            # not container env — the JVM reads from env_file at boot.
            **preserved,
        }
        log.info(
            "[nuke] redeploying stack — GMS on OLD (%s), other services on default debug tag",
            config.old_image_tag,
        )
        # Bring up the FULL profile (no `services=` arg). Reason: GMS's
        # depends_on chain doesn't include kafka-broker, so `up -d
        # datahub-gms-debug` would skip Kafka — and system-update-debug
        # then crashes constructing its Kafka producer. Bringing up the
        # whole profile pulls in kafka-broker (+ frontend, actions) which
        # use `:debug` tag defaults and exist locally. The DATAHUB_GMS_VERSION
        # override only affects GMS — frontend/actions stay on debug.
        self._docker.up_stack(
            compose_env=compose_env,
            services=None,
            timeout_s=self._up_timeout_s,
        )

        log.info(
            "[nuke] waiting for GMS health at %s (timeout %ds)",
            self._health_url,
            self._health_timeout_s,
        )
        healthy = self._wait_for_health(self._health_url, self._health_timeout_s)
        duration_s = time.monotonic() - t0
        if not healthy:
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=duration_s,
                error=(
                    f"GMS did not become healthy at {self._health_url}/health "
                    f"within {self._health_timeout_s}s after nuke + redeploy"
                ),
                details={
                    "old_image_tag": config.old_image_tag,
                    "preserved_keys": sorted(preserved.keys()),
                },
            )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=duration_s,
            details={
                "old_image_tag": config.old_image_tag,
                "preserved_keys": sorted(preserved.keys()),
                "wiped": True,
            },
        )

    def _wait_for_health(self, url: str, timeout_s: int) -> bool:
        t_start = time.monotonic()
        deadline = t_start + timeout_s
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{url}/health", timeout=5) as resp:
                    if resp.status == 200:
                        log.info(
                            "[nuke] GMS healthy at %s/health after %.1fs",
                            url,
                            time.monotonic() - t_start,
                        )
                        return True
            except (urllib.error.HTTPError, urllib.error.URLError):
                pass
            time.sleep(3)
        log.error(
            "[nuke] GMS did not become healthy at %s/health within %ds",
            url,
            timeout_s,
        )
        return False
