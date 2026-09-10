"""Ensure the datahub-upgrade JAR is up-to-date before running ZDU tests.

The ``system-update-debug`` Compose service mounts the host directory
``datahub-upgrade/build/libs/`` directly into ``/datahub/datahub-upgrade/bin/``.
A stale JAR there silently produces an empty mutator chain
(``MigrateAspects: no mutators registered — nothing to migrate``) and the
sweep hangs until ``sweep_timeout_s`` elapses.

This helper runs ``./gradlew :datahub-upgrade:bootJar`` before each test
session so the mounted JAR matches the current source. Gradle is incremental;
the call is a ~14s no-op when nothing changed and a real build otherwise.

Opt-out via the env var ``ZDU_SKIP_BOOTJAR=1`` (useful in CI where the JAR is
pre-built, or for power users iterating on Python-only changes).
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

_GRADLE_TASK = ":datahub-upgrade:bootJar"
_OPT_OUT_ENV = "ZDU_SKIP_BOOTJAR"

# Process-local guard: only run gradle once per Python process even if multiple
# ZDUTestRunner instances are constructed (e.g., conftest reuse + a CLI run).
_already_checked = False


def _find_repo_root(start: Path) -> Path | None:
    """Walk up looking for ``settings.gradle`` and ``./gradlew``."""
    for parent in (start, *start.parents):
        if (parent / "settings.gradle").exists() and (parent / "gradlew").exists():
            return parent
    return None


def ensure_upgrade_jar_fresh() -> None:
    """Run ``./gradlew :datahub-upgrade:bootJar`` from the repo root.

    Idempotent within a process (subsequent calls are no-ops). Logs and skips
    silently if the repo root cannot be located. Raises ``RuntimeError`` if
    the gradle build fails — callers should treat that as a hard test setup
    failure rather than continuing into the upgrade phase with a stale JAR.
    """
    global _already_checked
    if _already_checked:
        return

    if os.environ.get(_OPT_OUT_ENV) == "1":
        log.info("[upgrade-jar] %s=1 — skipping bootJar freshness check", _OPT_OUT_ENV)
        _already_checked = True
        return

    repo_root = _find_repo_root(Path(__file__).resolve())
    if repo_root is None:
        log.warning(
            "[upgrade-jar] could not locate repo root from %s; skipping bootJar check. "
            "Set %s=1 to silence this warning if running outside the repo.",
            Path(__file__),
            _OPT_OUT_ENV,
        )
        _already_checked = True
        return

    log.info(
        "[upgrade-jar] running %s from %s (set %s=1 to skip)",
        _GRADLE_TASK,
        repo_root,
        _OPT_OUT_ENV,
    )
    result = subprocess.run(
        ["./gradlew", _GRADLE_TASK, "-q"],
        cwd=repo_root,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"`./gradlew {_GRADLE_TASK}` failed (rc={result.returncode}). "
            f"Fix the build error or set {_OPT_OUT_ENV}=1 to skip."
        )
    _already_checked = True
    log.info("[upgrade-jar] bootJar up-to-date")


def reset_for_tests() -> None:
    """Reset the process-local guard. Test-only — do not call from production paths."""
    global _already_checked
    _already_checked = False
