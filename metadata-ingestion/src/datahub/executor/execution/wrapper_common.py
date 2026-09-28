"""Shared utilities for wrapper scripts (run_ingest_with_masking, run_test_connection_with_masking).

These functions run inside the short-lived wrapper subprocess, NOT in the
long-lived executor process. They handle venv activation, secret masking
registration, envelope parsing, and datahub capability detection.
"""

import json
import os
import re
import resource
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import yaml

from datahub.masking.bootstrap import initialize_secret_masking
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled


def parse_bool_env(env_var: str, default: bool = True) -> bool:
    """Parse boolean from environment variable."""
    value = os.getenv(env_var, "").lower()
    if value in ("true", "1", "yes"):
        return True
    elif value in ("false", "0", "no"):
        return False
    return default


def setup_memory_limit() -> None:
    """Apply memory limit if EXECUTOR_TASK_MEMORY_LIMIT is set."""
    memory_limit = os.environ.get("EXECUTOR_TASK_MEMORY_LIMIT")
    if not memory_limit:
        return

    try:
        limit_bytes = int(memory_limit)
        print(f"Setting memory limit to {limit_bytes} bytes", file=sys.stderr)
        resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
    except Exception as e:
        print(f"Warning: Failed to set memory limit: {e}", file=sys.stderr)


def validate_venv(venv_path: str) -> tuple[Path, Path]:
    """Validate venv exists and return (python_path, datahub_path)."""
    venv_python = Path(venv_path) / "bin" / "python"
    venv_datahub = Path(venv_path) / "bin" / "datahub"

    if not venv_python.exists():
        print(f"ERROR: Python binary not found in venv: {venv_python}", file=sys.stderr)
        sys.exit(1)

    if not venv_datahub.exists():
        print(f"ERROR: DataHub CLI not found in venv: {venv_datahub}", file=sys.stderr)
        sys.exit(1)

    return venv_python, venv_datahub


def activate_venv(venv_path: str) -> None:
    """Activate virtual environment by setting PATH and VIRTUAL_ENV."""
    os.environ["VIRTUAL_ENV"] = venv_path
    os.environ["PATH"] = f"{venv_path}/bin:{os.environ.get('PATH', '')}"


def register_secrets_for_masking(secrets: dict[str, str]) -> None:
    """Register secrets with DataHub masking framework if enabled.

    Two switches govern masking, and they are not the same one:

      DATAHUB_ENABLE_SECRET_MASKING   read only here, by the wrapper
      DATAHUB_DISABLE_SECRET_MASKING  read by the library -- env_vars,
                                      secret_registry, bootstrap

    Opposite polarity, different scope, and each used to ignore the other.
    An operator who set DATAHUB_DISABLE_SECRET_MASKING=true to debug turned
    the library's masking off while the wrapper went on registering every
    secret into a registry that no longer masked; one who set
    DATAHUB_ENABLE_SECRET_MASKING=false silenced the wrapper while the
    library kept masking whatever else reached it. Either way, what the
    operator asked for and what they got were different things.

    Both are honoured, so the wrapper never feeds a registry the library
    has been told not to use. The library's switch still decides whether
    masking APPLIES; this only decides whether the wrapper feeds it.
    """
    if not parse_bool_env("DATAHUB_ENABLE_SECRET_MASKING", default=True):
        print(
            "Secret masking is DISABLED via DATAHUB_ENABLE_SECRET_MASKING=false",
            file=sys.stderr,
        )
        return

    if not is_masking_enabled():
        print(
            "Secret masking is DISABLED via DATAHUB_DISABLE_SECRET_MASKING; "
            "not registering secrets either",
            file=sys.stderr,
        )
        return

    if not secrets:
        return

    try:
        initialize_secret_masking()
        registry = SecretRegistry.get_instance()
        for name, value in secrets.items():
            if value:
                registry.register_secret(name, value)

        print(
            f"Secret masking enabled: registered {registry.get_count()} secret(s)",
            file=sys.stderr,
        )
    except Exception as e:
        print(
            f"Warning: Failed to initialize secret masking: {e}. Continuing without masking.",
            file=sys.stderr,
        )


ENVELOPE_MIN_CLI_VERSION = (1, 5, 0, 15)


def get_venv_datahub_version(venv_python: Path) -> Optional[tuple[int, ...]]:
    """Installed acryl-datahub version in the venv, None if undeterminable.

    Asks the venv's package metadata rather than the CLI's self-reported
    version string, which is unreliable (dev installs report "unavailable")."""
    try:
        result = subprocess.run(
            [
                str(venv_python),
                "-c",
                "from importlib.metadata import version; print(version('acryl-datahub'))",
            ],
            capture_output=True,
            text=True,
        )
        match = re.match(r"(?:\d+!)?(\d+(?:\.\d+)+)", result.stdout.strip())
        if match is None:
            return None
        return tuple(int(part) for part in match.group(1).split("."))
    except Exception as e:
        print(
            f"Warning: Failed to determine venv datahub version: {e}", file=sys.stderr
        )
        return None


def supports_stdin_envelope(venv_python: Path) -> bool:
    """Whether the venv's `datahub ingest -c -` understands the JSON secrets
    envelope (acryl-datahub >= 1.5.0.15); older CLIs parse it as a recipe."""
    version = get_venv_datahub_version(venv_python)
    return version is not None and version >= ENVELOPE_MIN_CLI_VERSION


def check_cli_flag_support(datahub_binary: Path, flag: str) -> bool:
    """Check if the datahub CLI supports a given flag on `ingest run`."""
    try:
        result = subprocess.run(
            [str(datahub_binary), "ingest", "run", "--help"],
            capture_output=True,
            text=True,
        )
        return flag in result.stdout
    except Exception as e:
        print(
            f"Warning: Failed to check --{flag} support: {e}",
            file=sys.stderr,
        )
        return False


def _resolve_element(element: Any, secrets: dict[str, str], pattern: re.Pattern) -> Any:  # type: ignore[type-arg]
    """Recursively resolve ${VAR} in a config element (str, dict, or list)."""
    if isinstance(element, str):

        def replace_match(match: re.Match) -> str:  # type: ignore[type-arg]
            var_name = match.group(1)
            if var_name in secrets:
                return secrets[var_name]
            return match.group(0)

        return pattern.sub(replace_match, element)
    elif isinstance(element, dict):
        return {k: _resolve_element(v, secrets, pattern) for k, v in element.items()}
    elif isinstance(element, list):
        return [_resolve_element(item, secrets, pattern) for item in element]
    return element


_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def read_stdin_envelope() -> tuple[str, dict]:
    """Read JSON envelope from stdin. Returns (raw_json, parsed_dict)."""
    raw = sys.stdin.read()
    if not raw:
        print("ERROR: No input received on stdin", file=sys.stderr)
        sys.exit(1)
    try:
        return raw, json.loads(raw)
    except json.JSONDecodeError as e:
        print(
            f"ERROR: Invalid JSON envelope on stdin: {e} (received {len(raw)} bytes)",
            file=sys.stderr,
        )
        sys.exit(1)


def build_datahub_stdin(recipe_yaml: str, secrets: dict[str, str]) -> str:
    """Resolve ${VAR} in recipe and return YAML to pipe to datahub's stdin.

    Parses YAML to a dict, resolves ${VAR} at the value level, then
    re-serializes to YAML. This ensures proper escaping of secret values
    that contain YAML-special characters (multi-line keys, colons, etc.).

    Secrets stay in memory — never written to env or disk.
    Only handles simple ${VAR} patterns. Advanced syntax like ${VAR:-default}
    is left as-is for datahub's EnvResolver to handle from os.environ.
    """
    recipe_dict = yaml.safe_load(recipe_yaml)
    resolved = _resolve_element(recipe_dict, secrets, _VAR_PATTERN)
    return yaml.dump(resolved)


# Carries the inherited venv-cache lock descriptor down the spawn chain.
# The executor hands the fd to the wrapper with pass_fds, which preserves the
# number, so the same value is valid in every process that inherits it.
VENV_LOCK_FD_ENV = "DATAHUB_VENV_LOCK_FD"


def _inherited_lock_fds() -> tuple[int, ...]:
    """The venv-cache lock descriptor to pass on, if this process has one.

    The datahub CLI is a GRANDCHILD: the executor spawns this wrapper, and
    the wrapper spawns the CLI. The lock has to reach the process that is
    actually importing out of the venv, because that is the one whose death
    should release it -- if the wrapper is SIGKILLed while the CLI keeps
    running (which is exactly why the CLI gets its own session), a lock held
    only by the wrapper would be released with a live interpreter still in
    the venv.

    A malformed or stale value is ignored rather than raised on: the lock is
    an optimisation and must never fail a run that could have worked.
    """
    raw = os.environ.get(VENV_LOCK_FD_ENV)
    if not raw:
        return ()
    try:
        fd = int(raw)
        os.fstat(fd)
    except (ValueError, OSError):
        print(
            f"WARNING: {VENV_LOCK_FD_ENV}={raw!r} is not an open descriptor; "
            "the venv cache entry will not be protected for this run",
            file=sys.stderr,
        )
        return ()
    return (fd,)


def run_datahub_subprocess(cmd: list[str], stdin_data: str) -> int:
    """Launch datahub CLI, pipe stdin_data, stream masked output. Returns exit code."""
    print(f"Executing: {' '.join(cmd)}", file=sys.stderr)

    # No registry named, for the reason bootstrap does not name one: a
    # filter given one captures it for life, and this one outlives the call.
    # Harmless here today -- this runs in the short-lived wrapper subprocess
    # (see the module docstring), where there is one task and no masking
    # scope to capture the wrong one -- but it is the same shape that pinned
    # the first task's scope onto every process-wide handler in the
    # executor. Left resolving per call so it cannot become that.
    masking_filter = SecretMaskingFilter()

    process: Optional[subprocess.Popen] = None

    def _reap_and_exit(signum: int, _frame: Any) -> None:
        # Our process group got a termination signal -- stop and REAP the datahub
        # child so it cannot orphan into a zombie under PID 1.
        #
        # Installed BEFORE the child is spawned. Registering it afterwards leaves a
        # window where the default disposition applies: a signal arriving there kills
        # this wrapper outright and the child it just spawned is never reaped.
        # `process` is still None for the part of that window before Popen returns,
        # hence the guard.
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, _reap_and_exit)

    process = subprocess.Popen(
        cmd,
        env=os.environ.copy(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        # Hand the venv-cache lock on to the CLI. Without this the
        # descriptor stops here, and a SIGKILLed wrapper would release the
        # lock while the CLI is still executing from the venv.
        pass_fds=_inherited_lock_fds(),
    )

    try:
        assert process.stdin is not None
        process.stdin.write(stdin_data)
        process.stdin.close()
    except BrokenPipeError:
        # Subprocess exited before consuming stdin (crash, bad binary, etc.)
        returncode = process.wait()
        print(
            f"ERROR: datahub process exited before reading recipe (exit code {returncode})",
            file=sys.stderr,
        )
        return returncode if returncode != 0 else 1

    if process.stdout:
        for line in process.stdout:
            masked_line = masking_filter.mask_text(line)
            print(masked_line, end="", flush=True)

    return process.wait()
