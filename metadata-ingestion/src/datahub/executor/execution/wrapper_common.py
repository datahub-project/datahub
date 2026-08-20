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
from datahub.masking.secret_registry import SecretRegistry


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
    """Register secrets with DataHub masking framework if enabled."""
    masking_enabled = parse_bool_env("DATAHUB_ENABLE_SECRET_MASKING", default=True)

    if not masking_enabled:
        print(
            "Secret masking is DISABLED via DATAHUB_ENABLE_SECRET_MASKING=false",
            file=sys.stderr,
        )
        return

    if not secrets:
        return

    try:
        initialize_secret_masking(force=True)
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


def run_datahub_subprocess(cmd: list[str], stdin_data: str) -> int:
    """Launch datahub CLI, pipe stdin_data, stream masked output. Returns exit code."""
    print(f"Executing: {' '.join(cmd)}", file=sys.stderr)

    registry = SecretRegistry.get_instance()
    masking_filter = SecretMaskingFilter(secret_registry=registry)

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
