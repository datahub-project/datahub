#!/usr/bin/env python3
"""Compare the smoke-test service env of the Docker quickstart with the Docker-free stack's.

Docker side: `docker compose config` for the smoke profile, with the inline env of
`quickstart_compose()` in run-quickstart.sh and its env-file slots, plus the CI GMS overlay that
docker-unified.yml appends, when there is one. Needs the Docker CLI, not a running daemon.
Docker-free side: `gms-lite-stack.py --print-env --job smoke-test --mode ci`.

Every difference must match a line in env-parity-allowlist.txt (`service KEY-GLOB  # reason`), and
every allowlist line must still match a difference. Keys the launcher pins (`source: protected`)
are not compared; they are listed in their own report section. Setting one in the smoke job layer
(smoke-test/env) has no effect, so that fails the check. Stdlib only.

    python3 smoke-test/check_env_parity.py [--report report.txt]
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

SMOKE_DIR = Path(__file__).resolve().parent
REPO_ROOT = SMOKE_DIR.parent
LAUNCHER = REPO_ROOT / "metadata-service/gms-lite/bin/gms-lite-stack.py"
JOB_DIR = SMOKE_DIR / "env"
ALLOWLIST = SMOKE_DIR / "env-parity-allowlist.txt"
CI_GMS_OVERLAY = SMOKE_DIR / "env/ci/gms.env"
DOCKER_WORKFLOW = REPO_ROOT / ".github/workflows/docker-unified.yml"
# Docker-free service -> the compose service that runs it in the quickstart-consumers profile.
SERVICES = {
    "gms": "datahub-gms-quickstart-consumers",
}
_QUICKSTART_FN = re.compile(r"^quickstart_compose\(\) \{\n.*?^\}\n", re.M | re.S)
# Fixed placeholders for values run-quickstart.sh generates per run, so both sides are stable.
_SHELL_PLACEHOLDERS = {
    "DATAHUB_VERSION": "head",
    "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "<per-run>",
    "DATAHUB_TOKEN_SERVICE_SALT": "<per-run>",
    "DATAHUB_SEARCH_IMAGE": "opensearchproject/opensearch",
    "DATAHUB_SEARCH_TAG": "3.7.0",
    "XPACK_SECURITY_ENABLED": "plugins.security.disabled=true",
    "ELASTICSEARCH_USE_SSL": "false",
    "USE_AWS_ELASTICSEARCH": "true",
}


@dataclass(frozen=True)
class AllowEntry:
    service: str
    pattern: str
    reason: str
    line: int


def _minimal_env() -> dict[str, str]:
    return {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}


def docker_envs() -> dict[str, dict[str, str]]:
    script = (SMOKE_DIR / "run-quickstart.sh").read_text()
    function = _QUICKSTART_FN.search(script)
    if function is None:
        raise SystemExit("quickstart_compose() not found in run-quickstart.sh")
    exports = "\n".join(
        line for line in script.splitlines() if line.startswith("export ")
    )
    with tempfile.TemporaryDirectory() as tmp:
        # docker-unified.yml appends the CI GMS values to smoke.gms.env before quickstart.
        gms_env = Path(tmp) / "smoke.gms.env"
        gms_env.write_text(
            "\n".join(
                path.read_text()
                for path in (SMOKE_DIR / "smoke.gms.env", CI_GMS_OVERLAY)
                if path.exists()
            )
        )
        bash = "\n".join(
            [
                "set -euo pipefail",
                f'DIR="{SMOKE_DIR}"',
                exports,
                f'export DATAHUB_LOCAL_GMS_ENV="{gms_env}"',
                function[0],
                "quickstart_compose config --format json",
            ]
        )
        result = subprocess.run(
            ["bash", "-c", bash],
            cwd=SMOKE_DIR,
            env={**_minimal_env(), **_SHELL_PLACEHOLDERS},
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        raise SystemExit(f"docker compose config failed:\n{result.stderr}")
    services = json.loads(result.stdout)["services"]
    return {
        service: {
            key: "" if value is None else str(value)
            for key, value in (services[compose_name].get("environment") or {}).items()
        }
        for service, compose_name in SERVICES.items()
    }


def docker_free_envs() -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    """(compared values, launcher-pinned values) per service."""
    result = subprocess.run(
        [
            sys.executable,
            str(LAUNCHER),
            "--print-env",
            "--processes",
            "1",
            "--job",
            "smoke-test",
            "--mode",
            "ci",
        ],
        env={**_minimal_env(), "PYTHONPATH": str(REPO_ROOT / "scripts/dev")},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"gms-lite-stack.py --print-env failed:\n{result.stderr}")
    report = json.loads(result.stdout)["gms-0"]
    envs: dict[str, dict[str, str]] = {}
    pinned: dict[str, dict[str, str]] = {}
    for service in SERVICES:
        envs[service], pinned[service] = {}, {}
        for key, entry in report[service].items():
            target = pinned if entry["source"] == "protected" else envs
            target[service][key] = entry["value"]
    return envs, pinned


def job_keys() -> dict[str, set[str]]:
    """Keys the smoke job layer sets for each service (CI mode, as the check resolves it)."""
    keys: dict[str, set[str]] = {}
    for service in SERVICES:
        keys[service] = set()
        for folder in (JOB_DIR, JOB_DIR / "ci"):
            for name in ("all", service):
                path = folder / f"{name}.env"
                if path.exists():
                    keys[service] |= {
                        line.split("=", 1)[0].removeprefix("export ").strip()
                        for line in path.read_text().splitlines()
                        if "=" in line and not line.strip().startswith("#")
                    }
    return keys


def job_sets_pinned(
    keys: dict[str, set[str]], pinned: dict[str, dict[str, str]]
) -> list[str]:
    """Job-layer keys that the launcher overrides with its pinned value."""
    return [
        f"{service} {key}: set in {JOB_DIR.relative_to(REPO_ROOT)}, but the launcher pins it"
        for service in SERVICES
        for key in sorted(keys[service] & set(pinned[service]))
    ]


def ci_overlay_drift() -> list[str]:
    """CI GMS values that docker-unified.yml does not append to smoke.gms.env as written."""
    if not CI_GMS_OVERLAY.exists():
        return []
    workflow = DOCKER_WORKFLOW.read_text()
    missing = []
    for raw in CI_GMS_OVERLAY.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and f'echo "{line}"' not in workflow:
            missing.append(
                f"gms {line}: in {CI_GMS_OVERLAY.relative_to(REPO_ROOT)} but not appended "
                f"by {DOCKER_WORKFLOW.relative_to(REPO_ROOT)}"
            )
    return missing


def read_allowlist(path: Path) -> list[AllowEntry]:
    entries = []
    for number, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        rule, _, reason = line.partition("#")
        parts = rule.split()
        if len(parts) != 2 or not reason.strip():
            raise SystemExit(
                f"{path.name}:{number}: expected 'service KEY-GLOB  # reason', got {raw!r}"
            )
        if parts[0] not in SERVICES:
            raise SystemExit(f"{path.name}:{number}: unknown service {parts[0]!r}")
        entries.append(AllowEntry(parts[0], parts[1], reason.strip(), number))
    return entries


def _normalise(value: str | None) -> str | None:
    if value is not None and value.lower() in ("true", "false"):
        return value.lower()
    return value


def compare(
    docker: dict[str, dict[str, str]],
    docker_free: dict[str, dict[str, str]],
    allowlist: list[AllowEntry],
    pinned: dict[str, dict[str, str]],
) -> tuple[list[str], list[str], list[AllowEntry], list[str]]:
    """(unlisted differences, allowed differences, allowlist entries matching nothing,
    differences on launcher-pinned keys)."""
    unlisted: list[str] = []
    allowed: list[str] = []
    pins: list[str] = []
    used: set[AllowEntry] = set()
    for service in SERVICES:
        left, right = docker[service], {**docker_free[service], **pinned[service]}
        for key in sorted(set(left) | set(right)):
            d, f = _normalise(left.get(key)), _normalise(right.get(key))
            if d == f:
                continue
            line = f"{service} {key}: docker={d!r} docker-free={f!r}"
            if key in pinned[service]:
                pins.append(line)
                continue
            matches = [
                e
                for e in allowlist
                if e.service == service and fnmatch.fnmatchcase(key, e.pattern)
            ]
            used.update(matches)
            (allowed if matches else unlisted).append(line)
    return unlisted, allowed, [e for e in allowlist if e not in used], pins


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--report", type=Path, help="also write the report here")
    args = parser.parse_args()

    docker_free, pinned = docker_free_envs()
    unlisted, allowed, stale, pins = compare(
        docker_envs(), docker_free, read_allowlist(ALLOWLIST), pinned
    )
    unlisted = ci_overlay_drift() + job_sets_pinned(job_keys(), pinned) + unlisted
    lines = [
        f"Env parity: {len(unlisted)} unlisted difference(s), {len(stale)} stale allowlist "
        f"line(s), {len(allowed)} allowed difference(s), {len(pins)} pinned by the launcher.",
    ]
    if unlisted:
        lines += [
            "",
            "Unlisted differences (align the values, or add an allowlist line with a reason):",
        ]
        lines += [f"  {line}" for line in unlisted]
    if stale:
        lines += ["", "Stale allowlist lines (they match no difference; remove them):"]
        lines += [
            f"  {ALLOWLIST.name}:{e.line}: {e.service} {e.pattern}" for e in stale
        ]
    if allowed:
        lines += ["", "Allowed differences:"]
        lines += [f"  {line}" for line in allowed]
    if pins:
        lines += ["", "Pinned by the launcher (protected keys; not compared):"]
        lines += [f"  {line}" for line in pins]
    report = "\n".join(lines) + "\n"
    print(report, end="")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report)
    return 1 if unlisted or stale else 0


if __name__ == "__main__":
    sys.exit(main())
