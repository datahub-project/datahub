from __future__ import annotations

import os
import pathlib
import time
from dataclasses import dataclass, field
from typing import Any

from .constants import REPO_ROOT

# Absolute path to docker/profiles/ regardless of pytest invocation directory.
_DEFAULT_PROJECT_DIR = str(REPO_ROOT / "docker" / "profiles")


def _resolve_gms_token() -> str | None:
    """Resolve the GMS bearer token, falling back to ``~/.datahubenv``.

    Precedence:
      1. ``DATAHUB_GMS_TOKEN`` env var (explicit override)
      2. ``token:`` line in ``~/.datahubenv`` (written by ``datahub init``)
      3. None — preflight will fail with a remediation message

    The ``.datahubenv`` fallback exists because requiring developers to
    ``export DATAHUB_GMS_TOKEN=<long jwt>`` opens a copy-paste typo path
    that preflight's truthiness check can't catch (a malformed token still
    passes ``if not gms_token``). Reading the file the standard DataHub CLI
    already maintains eliminates that failure mode entirely.
    """
    if v := os.environ.get("DATAHUB_GMS_TOKEN"):
        return v
    datahubenv = pathlib.Path.home() / ".datahubenv"
    if not datahubenv.is_file():
        return None
    try:
        for line in datahubenv.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("token:"):
                value = stripped.split(":", 1)[1].strip()
                return value or None
    except OSError:
        return None
    return None


_TRUTHY = ("1", "true", "True", "yes")


def _apply_bool_flags(kwargs: dict[str, Any]) -> None:
    """Apply env-var-driven bool field overrides into ``kwargs`` in place.

    Extracted from ``ZDUTestConfig.from_env`` to keep its cyclomatic
    complexity below ruff's C901 threshold. The flag semantics are:

    * ``ZDU_SKIP_BUILD_IMAGES`` / ``ZDU_BUILD_IMAGES`` — toggle Phase 0.
      Skip wins (the explicit opt-out beats the legacy opt-in).
    * ``ZDU_SKIP_PREPARE_OLD_STACK`` — toggle Phase 0.5.
    * ``ZDU_SKIP_NUKE`` / ``ZDU_FORCE_NUKE`` — toggle SetupOldStack nuke.
      Force wins (CI can set both to override developer's local skip).
    * ``ZDU_SKIP_TOKEN_REFRESH`` — toggle SetupOldStack token refresh.
    """
    if os.environ.get("ZDU_SKIP_BUILD_IMAGES") in _TRUTHY:
        kwargs["build_images"] = False
    elif os.environ.get("ZDU_BUILD_IMAGES") in _TRUTHY:
        # Backward-compat: ZDU_BUILD_IMAGES=1 was the Plan-13 opt-in. Now
        # it's the default, so this is a no-op but keep accepting it.
        kwargs["build_images"] = True
    if os.environ.get("ZDU_SKIP_PREPARE_OLD_STACK") in _TRUTHY:
        kwargs["prepare_old_stack"] = False
    if os.environ.get("ZDU_SKIP_NUKE") in _TRUTHY:
        kwargs["clean_build"] = False
    if os.environ.get("ZDU_FORCE_NUKE") in _TRUTHY:
        kwargs["clean_build"] = True
    if os.environ.get("ZDU_SKIP_TOKEN_REFRESH") in _TRUTHY:
        kwargs["refresh_token"] = False
    # urn_sync_enabled: explicit "false" / "0" disables; default stays True
    if os.environ.get("ZDU_URN_SYNC_ENABLED") in ("0", "false", "False", "no"):
        kwargs["urn_sync_enabled"] = False


@dataclass
class GapSimulationConfig:
    """Tracks which intermediate mutator versions are intentionally absent."""

    enabled: bool = True
    missing_versions: list[int] = field(default_factory=lambda: [3])


@dataclass
class ZDUTestConfig:
    # ── Docker ──────────────────────────────────────────────────────────────
    project_dir: str = field(default_factory=lambda: _DEFAULT_PROJECT_DIR)
    compose_files: list[str] = field(default_factory=lambda: ["docker-compose.yml"])
    compose_profiles: list[str] = field(default_factory=lambda: ["debug"])
    rebuild_command: str = "scripts/dev/datahub-dev.sh rebuild --wait"
    rebuild_cwd: str = field(default_factory=lambda: str(REPO_ROOT))
    gms_service: str = "datahub-gms-debug"
    upgrade_service: str = "system-update-debug"

    # ── Two-image-tag testing (F-3) ──────────────────────────────────────────
    # Image tags consumed by the parameterized Compose YAMLs in docker/profiles/.
    # The OLD tag is what runs in the stack at seed time; the NEW tag is what
    # Phase 4's system-update-debug container uses, and what Phase 6 (future
    # RollingRestartPhase) will swap services to. Default ``debug`` matches the
    # compose YAML fallback so a stack started with the regular dev workflow
    # works without setting these env vars.
    old_image_tag: str = "debug"
    new_image_tag: str = "debug"

    # ── BuildImages (Plan 13/14) ─────────────────────────────────────────────
    # Default-on: BuildImagesPhase runs every test and ensures OLD/NEW images
    # are built deterministically from git refs. Opt out via ZDU_SKIP_BUILD_IMAGES=1
    # when iterating on Python-only changes (uses cached debug images).
    build_images: bool = True
    build_old_ref: str = "master"
    # Persistent worktree root for OLD/NEW image builds.
    build_images_root: str = "smoke-test/build/zdu-images"

    # ── PrepareOldStack (Plan 14) ────────────────────────────────────────────
    # Default-on: ensure the running Compose stack matches config.old_image_tag
    # before seed/discovery run. Opt out via ZDU_SKIP_PREPARE_OLD_STACK=1.
    prepare_old_stack: bool = True

    # ── SetupOldStack: nuke + token-refresh (G19c) ───────────────────────────
    # Default-ON: SetupOldStackPhase will (a) build images, (b) `down -v` +
    # redeploy on the OLD image, (c) refresh the access token. Wipes ES + MySQL
    # + Kafka so SystemUpdateBlocking's mapping-diff actually fires when OLD
    # and NEW image PDLs differ — without this, ES retains stale mappings from
    # the previous run and TC-101 / TC-103 never see a real reindex.
    #
    # Opt-out via ZDU_SKIP_NUKE=1 or --no-clean-build (e.g. for Python-only
    # iteration against an already-prepared stack). Setting ZDU_FORCE_NUKE=1
    # overrides ZDU_SKIP_NUKE.
    #
    # Implementation detail: nuke only runs when build_images=True AND
    # old_image_tag != "debug" (i.e., when Phase 0 actually produced a distinct
    # OLD tag to redeploy onto). Without that, nuking the dev stack and
    # bringing it back on 'debug' is destructive without buying any signal.
    clean_build: bool = True

    # Default-ON: after redeploy, run `datahub init --username datahub
    # --password datahub` to refresh the access token in ~/.datahubenv and
    # update config.gms_token in-memory. Guards against signing-key drift
    # across nuke (partial-capture failures, fully-down stacks) — every run
    # ends up with a token that verifies against whatever signing key the
    # redeployed stack ended up using. Opt-out via ZDU_SKIP_TOKEN_REFRESH=1.
    refresh_token: bool = True

    # ── Cleanup (Phase 11) ──────────────────────────────────────────────────
    # Canonical "rest" image tag to restore GMS to at end of run. Used when
    # the pre-test snapshot was itself a ZDU test artifact (zdu-{old,new}-*)
    # — restoring to that would leave the user on residue instead of their
    # normal dev image. Override via ZDU_CLEANUP_REST_TAG (e.g. a release tag).
    cleanup_rest_tag: str = "debug"

    # ── DataHub connection ───────────────────────────────────────────────────
    gms_url: str = "http://localhost:8080"
    gms_token: str | None = None

    # ── Sweep ────────────────────────────────────────────────────────────────
    sweep_timeout_s: int = 600
    # Auto-generated on each run so MigrateAspectsStep's idempotency guard
    # never silently skips the sweep due to a prior SUCCEEDED result.
    upgrade_version: str = field(
        default_factory=lambda: f"zdu-test-{int(time.time() * 1000)}"
    )

    # ── Concurrent I/O ───────────────────────────────────────────────────────
    reader_workers: int = 3
    writer_workers: int = 2
    # Milliseconds the sweep sleeps between logging batch URNs and calling ingestProposal.
    # Set >0 to guarantee the concurrent writer beats the sweep write (deterministic race).
    pre_write_delay_ms: int = 500
    # Writers' synchronization strategy. ``True`` (default): block on BATCH_URNS
    # events from the sweep log so each write lands inside the sweep's
    # PRE_WRITE_DELAY_MS window — deterministic ConditionalWriteValidator
    # contention. ``False``: writers cycle through the IO pool on timers
    # (probabilistic overlap). Override via ZDU_URN_SYNC_ENABLED=0.
    urn_sync_enabled: bool = True

    # ── Elasticsearch connection ─────────────────────────────────────────────
    es_url: str = "http://localhost:9200"

    # ── MySQL connection ─────────────────────────────────────────────────────
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "datahub"
    mysql_password: str = "datahub"
    mysql_database: str = "datahub"

    # ── Test hooks (Java-side env vars, default 0 = no-op) ───────────────────
    reindex_delay_ms: int = 0
    catch_up_delay_ms: int = 0

    # ── Suite filter ─────────────────────────────────────────────────────────
    suites: list[str] = field(default_factory=list)

    # ── Phase 3 SnapshotT0 ────────────────────────────────────────────────────
    # Aspects whose schemaVersion histogram should be captured at T0.
    # Suite A targets "embed" and "globalTags"; future suites may extend.
    aspects_under_test: list[str] = field(
        default_factory=lambda: ["embed", "globalTags"]
    )

    # ── Gap simulation ──────────────────────────────────────────────────────
    gap_simulation: GapSimulationConfig = field(default_factory=GapSimulationConfig)

    # ── Runner behaviour ─────────────────────────────────────────────────────
    skip_phases: list[str] = field(default_factory=list)
    fail_fast: bool = True

    # ── Scenario filtering ───────────────────────────────────────────────────
    run_only_tc: list[int] = field(default_factory=list)
    skip_tc: list[int] = field(default_factory=list)

    # ── Output ───────────────────────────────────────────────────────────────
    # Resolved against REPO_ROOT so the same defaults work regardless of which
    # directory the test runner is invoked from (smoke-test/ vs. repo root).
    # Without this, running `cd smoke-test && python -m tests.zdu` produces a
    # nested smoke-test/smoke-test/build/... path while the docs and prior
    # report state live at smoke-test/build/...
    report_path: str = field(
        default_factory=lambda: str(REPO_ROOT / "smoke-test/build/zdu-test-report.json")
    )
    # Failure-bundle root directory. Bundles are written to
    # ``{build_dir}/zdu-failure-{timestamp}/`` on any phase or scenario FAIL.
    build_dir: str = field(default_factory=lambda: str(REPO_ROOT / "smoke-test/build"))

    @classmethod
    def from_env(cls) -> ZDUTestConfig:
        kwargs: dict[str, Any] = {}
        # gms_token has a fallback path beyond the env var — see
        # ``_resolve_gms_token``. Handled separately because the other entries
        # in this loop are pure env-var lookups.
        if v := _resolve_gms_token():
            kwargs["gms_token"] = v
        # String fields (direct assignment)
        for env_var, field_name in [
            ("DATAHUB_GMS_URL", "gms_url"),
            ("ZDU_PROJECT_DIR", "project_dir"),
            ("ZDU_GMS_SERVICE", "gms_service"),
            ("ZDU_UPGRADE_SERVICE", "upgrade_service"),
            ("ZDU_REBUILD_CWD", "rebuild_cwd"),
            ("ES_URL", "es_url"),
            ("MYSQL_HOST", "mysql_host"),
            ("MYSQL_USER", "mysql_user"),
            ("MYSQL_PASSWORD", "mysql_password"),
            ("MYSQL_DATABASE", "mysql_database"),
            ("ZDU_OLD_IMAGE_TAG", "old_image_tag"),
            ("ZDU_NEW_IMAGE_TAG", "new_image_tag"),
            ("ZDU_OLD_REF", "build_old_ref"),
            ("ZDU_BUILD_IMAGES_ROOT", "build_images_root"),
            ("ZDU_CLEANUP_REST_TAG", "cleanup_rest_tag"),
        ]:
            if v := os.environ.get(env_var):
                kwargs[field_name] = v
        # Integer fields
        for env_var, field_name in [
            ("ZDU_SWEEP_TIMEOUT", "sweep_timeout_s"),
            ("ZDU_READER_WORKERS", "reader_workers"),
            ("ZDU_WRITER_WORKERS", "writer_workers"),
            ("ZDU_PRE_WRITE_DELAY_MS", "pre_write_delay_ms"),
            ("MYSQL_PORT", "mysql_port"),
            ("ZDU_REINDEX_DELAY_MS", "reindex_delay_ms"),
            ("ZDU_CATCH_UP_DELAY_MS", "catch_up_delay_ms"),
        ]:
            if v := os.environ.get(env_var):
                kwargs[field_name] = int(v)
        _apply_bool_flags(kwargs)
        # Comma-separated fields
        if v := os.environ.get("ZDU_SKIP_PHASES"):
            kwargs["skip_phases"] = [p.strip() for p in v.split(",") if p.strip()]
        # Convenience: ZDU_SKIP_CLEANUP=1 → add "cleanup" to skip_phases.
        if os.environ.get("ZDU_SKIP_CLEANUP") in ("1", "true", "True", "yes"):
            existing = kwargs.get("skip_phases", [])
            if "cleanup" not in existing:
                kwargs["skip_phases"] = [*existing, "cleanup"]
        if v := os.environ.get("ZDU_SUITES"):
            kwargs["suites"] = [s.strip().lower() for s in v.split(",") if s.strip()]
        if v := os.environ.get("ZDU_ASPECTS_UNDER_TEST"):
            kwargs["aspects_under_test"] = [
                a.strip() for a in v.split(",") if a.strip()
            ]
        return cls(**kwargs)
