"""Docker-free datahub-dev lifecycle. Stdlib only; imported lazily by datahub_dev."""

from __future__ import annotations

import errno
from collections import deque
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error

from extensions import datahub_local_extension as extensions
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, cast

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "metadata-service/gms-lite/bin"))
sys.path.insert(0, str(_REPO_ROOT / "metadata-ingestion/src/datahub/cli"))
from _opensearch_manager import (  # noqa: E402
    SharedOpenSearchManager,
    OPENSEARCH_READY_TIMEOUT_ENV,
    env_seconds,
    delete_prefixes,
    distribution_script,
    validate_major,
    index_prefix,
    request_json,
    selected_major,
)
from _profile_store import profile_path_in, write_profile_text  # noqa: E402

# Keep stack data separate from other local launchers sharing the instance registry.
LOCAL_DATA_ROOT = Path.home() / ".datahub/local/stacks"
OWNER = "datahub-dev-oss-local-v1"
INSTANCE_RECORD = "instance.json"
TIMINGS_FILE = "timings.json"
LOCAL_PORT_BASE = {"gms": 8080, "frontend": 9002}
# Measured on the embedded OpenSearch 3 node: 86 shards per OSS GMS instance.
# Prefer measurements from completed stacks as the entity registry grows.
DEFAULT_SHARDS_PER_INSTANCE = 86


def local_data_dir(worktree_id: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{6}", worktree_id):
        raise ValueError("Invalid worktree id")
    return LOCAL_DATA_ROOT / worktree_id


def local_ports(slot: int) -> dict[str, int]:
    return {name: port + slot * 1000 for name, port in LOCAL_PORT_BASE.items()}


def shared_opensearch_dir(major: str) -> Path:
    return Path.home() / f".datahub/local/opensearch-{validate_major(major)}"


def recorded_major(entry: Mapping[str, object]) -> str:
    # Before selection was introduced, every local stack ran OpenSearch 3.
    return validate_major(str(entry.get("opensearch_version", "3")))


def resolve_major(entry: Mapping[str, object] | None) -> str:
    return recorded_major(entry) if entry else selected_major()


def is_owned_entry(entry: Mapping[str, object]) -> bool:
    try:
        directory = shared_opensearch_dir(recorded_major(entry))
    except ValueError:
        return False
    return (
        entry.get("mode") == "local"
        and entry.get("local_owner") == OWNER
        and entry.get("opensearch_dir") == str(directory)
    )


def read_instance_record(worktree_id: str) -> dict[str, object] | None:
    try:
        record = json.loads((local_data_dir(worktree_id) / INSTANCE_RECORD).read_text())
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) and is_owned_entry(record) else None


def write_instance_record(worktree_id: str, entry: Mapping[str, object]) -> None:
    if not is_owned_entry(entry):
        raise RuntimeError("Refusing to record an unmanaged local stack")
    data_dir = local_data_dir(worktree_id)
    if data_dir.is_symlink() or (
        data_dir.exists()
        and any(data_dir.iterdir())
        and read_instance_record(worktree_id) is None
    ):
        raise RuntimeError(f"Refusing to adopt unrecognized data directory {data_dir}")
    data_dir.mkdir(parents=True, exist_ok=True)
    if read_instance_record(worktree_id) != entry:
        path = data_dir / INSTANCE_RECORD
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(dict(entry), indent=2) + "\n")
        temporary.replace(path)


def _require_owned(worktree_id: str) -> dict[str, object]:
    record = read_instance_record(worktree_id)
    if record is None or local_data_dir(worktree_id).is_symlink():
        raise RuntimeError(f"Refusing to change unrecognized local stack {worktree_id}")
    return record


def _log(message: str) -> None:
    print(f"[datahub-dev local] {message}", flush=True)


def _http_ok(url: str, timeout: float = 3.0) -> bool:
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError):
        return False


def _occupied_stack_port(
    ports: Mapping[str, int], processes: int, no_frontend: bool = False
) -> int | None:
    requested = list(range(ports["gms"], ports["gms"] + processes))
    if not no_frontend:
        requested.append(ports["frontend"])
    requested.extend(
        port for role, port in ports.items() if role not in ("gms", "frontend")
    )
    for port in requested:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                if error.errno == errno.EADDRINUSE:
                    return port
                raise
    return None


def _process_command(pid: int) -> str | None:
    try:
        result = subprocess.run(
            ["ps", "-ww", "-o", "command=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else ""


def process_identity(pid: int) -> str | None:
    try:
        result = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "lstart=,command="],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else ""


def starting_alive(entry: Mapping[str, object]) -> bool:
    marker = entry.get("starting")
    return (
        isinstance(marker, dict)
        and isinstance(marker.get("pid"), int)
        and bool(marker.get("identity"))
        and process_identity(marker["pid"]) == marker["identity"]
    )


def _owned_by(pid: int, data_dir: Path, unknown: bool = False) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    command = _process_command(pid)
    if command is None:
        return unknown
    try:
        args = shlex.split(command)
    except ValueError:
        return unknown
    # Require both the launcher and the exact data argument: a substring can name
    # another stack or an unrelated process inspecting this directory.
    launcher = any(arg.endswith("/gms-lite-stack.py") for arg in args) and any(
        args[i : i + 2] == ["--data-dir", str(data_dir)] for i in range(len(args) - 1)
    )
    frontend = (
        any(
            arg.endswith("/gradlew")
            or arg.endswith("/gradle/wrapper/gradle-wrapper.jar")
            or arg == "org.gradle.wrapper.GradleWrapperMain"
            for arg in args
        )
        and any(
            task in args
            for task in (
                ":datahub-web-react:yarnServe",
                ":datahub-web-react:yarnPreview",
            )
        )
        and f"-PfrontendStackFile={data_dir / 'stack.json'}" in args
        and "-PfrontendMode=lite" in args
    )
    if launcher or frontend:
        return True
    try:
        identities = json.loads((data_dir / "processes.json").read_text())
        expected = identities.get(str(pid))
    except (OSError, ValueError):
        return False
    # Match birth time as well as the full command, to reject a recycled PID running the same jar.
    return bool(expected) and process_identity(pid) == expected


def _load_pids(path: Path) -> dict[str, int]:
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if isinstance(v, int) and v > 0}


def _save_pids(path: Path, pids: dict[str, int]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(pids) + "\n")
    temporary.replace(path)


def recorded_pids(data_dir: Path) -> dict[str, int]:
    pids = _load_pids(data_dir / "pids.json")
    candidates: dict[str, int] = {}
    try:
        stack = json.loads((data_dir / "stack.json").read_text())
        candidates.update(
            {
                f"{role}/{name}": pid
                for role, instances in stack.get("servicePids", {}).items()
                for name, pid in instances.items()
                if isinstance(pid, int) and pid > 0
            }
        )
    except (OSError, ValueError):
        pass
    try:
        identities = json.loads((data_dir / "processes.json").read_text())
        candidates.update(
            {
                f"service-{pid}": int(pid)
                for pid in identities
                if str(pid).isdigit() and int(pid) > 0
            }
        )
    except (OSError, ValueError):
        pass
    for name, pid in candidates.items():
        if pid not in pids.values():
            pids[name] = pid
    return pids


def _launcher_alive(data_dir: Path) -> bool:
    pid = _load_pids(data_dir / "pids.json").get("launcher")
    # Unknown ownership blocks deletion, but never authorizes sending a signal.
    return pid is not None and _owned_by(pid, data_dir, unknown=True)


def stack_running(worktree_id: str, ports: Mapping[str, int] | None = None) -> bool:
    data_dir = local_data_dir(worktree_id)
    return any(
        _owned_by(pid, data_dir, unknown=True)
        for pid in recorded_pids(data_dir).values()
    ) or bool(ports and _http_ok(f"http://localhost:{ports['gms']}/health"))


def opensearch_manager(major: str) -> SharedOpenSearchManager:
    return SharedOpenSearchManager(
        shared_opensearch_dir(major), _REPO_ROOT, major=major
    )


def _stack_names(worktree_id: str, processes: int = 0) -> list[str]:
    data_dir = local_data_dir(worktree_id)
    names = {f"gms-{i}" for i in range(processes)}
    names.update(
        p.name for p in data_dir.glob("gms-*") if re.fullmatch(r"gms-[0-9]+", p.name)
    )
    record = read_instance_record(worktree_id) or {}
    options = record.get("local_options")
    if isinstance(options, dict):
        names.update(f"gms-{i}" for i in range(int(options.get("processes", 1))))
    return sorted(names)


def _prefixes(worktree_id: str, processes: int = 0) -> list[str]:
    return [
        index_prefix(local_data_dir(worktree_id), name)
        for name in _stack_names(worktree_id, processes)
    ]


@dataclass(frozen=True)
class ShardUsage:
    used: int
    limit: int

    @property
    def free(self) -> int:
        return self.limit - self.used


def shard_inventory(port: int) -> tuple[ShardUsage, dict[str, int]]:
    settings = cast(
        dict[str, dict[str, object]],
        request_json(
            port, "/_cluster/settings?include_defaults=true&flat_settings=true"
        ),
    )
    per_node = next(
        (
            int(settings[scope]["cluster.max_shards_per_node"])
            for scope in ("transient", "persistent", "defaults")
            if "cluster.max_shards_per_node" in settings.get(scope, {})
        ),
        # OpenSearch defaults to 1000 when no scope supplies the setting.
        1000,
    )
    health = cast(dict[str, object], request_json(port, "/_cluster/health"))
    rows = cast(
        list[dict[str, str]],
        request_json(
            port,
            "/_cat/indices?format=json&h=index,pri,rep,status&expand_wildcards=all",
        ),
    )
    shards = {
        row["index"]: int(row["pri"]) * (1 + int(row["rep"]))
        for row in rows
        if row.get("status") != "close"
    }
    return ShardUsage(
        sum(shards.values()), per_node * int(health["number_of_data_nodes"])
    ), shards


def shards_per_instance(
    registry: Mapping[str, Mapping[str, object]], shards: Mapping[str, int]
) -> int:
    measured = []
    for worktree_id, entry in registry.items():
        if not is_owned_entry(entry):
            continue
        # Only a ready stack supplies a complete measurement.
        try:
            stack = json.loads((local_data_dir(worktree_id) / "stack.json").read_text())
            names = stack["instances"]
        except (OSError, ValueError, KeyError):
            continue
        for name in names:
            prefix = index_prefix(local_data_dir(worktree_id), name)
            measured.append(
                sum(n for index, n in shards.items() if index.startswith(prefix + "_"))
            )
    return (
        max(measured, default=DEFAULT_SHARDS_PER_INSTANCE)
        or DEFAULT_SHARDS_PER_INSTANCE
    )


def shard_budget_error(
    usage: ShardUsage,
    shards: Mapping[str, int],
    worktree_id: str,
    processes: int,
    estimate: int,
) -> str | None:
    prefixes = [
        index_prefix(local_data_dir(worktree_id), f"gms-{i}") for i in range(processes)
    ]
    own = sum(
        n
        for name, n in shards.items()
        if any(name.startswith(p + "_") for p in prefixes)
    )
    needed = max(0, estimate * processes - own)
    if usage.free >= needed:
        return None
    return (
        f"shared OpenSearch has {usage.free} of {usage.limit} shards free; this stack needs about {needed} more. "
        "Run 'datahub-dev.sh instances clean' to reclaim stopped stacks whose worktrees are gone, "
        "or 'datahub-dev.sh nuke' in an unused worktree."
    )


def _prepare_shared_opensearch(
    worktree_id: str,
    processes: int,
    fresh: bool,
    registry: Mapping[str, Mapping[str, object]],
) -> str | None:
    record = _require_owned(worktree_id)
    manager = opensearch_manager(recorded_major(record))
    provider = record.get("local_extension")
    if isinstance(provider, dict):
        extension = extensions.load(provider["module"])
        assert extension is not None
        description = extension.describe(provider["options"])
        manager.env = dict(os.environ)
        if "opensearch-knn" in description.search_plugins:
            manager.env["OPENSEARCH_KNN_ENABLED"] = "true"
    try:
        port = manager.get_or_start()
        usage, shards = shard_inventory(port)
        if fresh:
            # H2 is also cleared; old search documents must not survive that wipe.
            delete_prefixes(port, _prefixes(worktree_id, processes))
            usage, shards = shard_inventory(port)
        return shard_budget_error(
            usage,
            shards,
            worktree_id,
            processes,
            shards_per_instance(
                {
                    key: entry
                    for key, entry in registry.items()
                    if is_owned_entry(entry) and recorded_major(entry) == manager.major
                },
                shards,
            ),
        )
    except (RuntimeError, OSError, ValueError, urllib.error.URLError) as error:
        return f"shared OpenSearch is not usable: {error}"


def profile_name(
    worktree_id: str, instance: str = "gms-0", default: str = "gms-0"
) -> str:
    return f"local-{worktree_id}" + (f"-{instance}" if instance != default else "")


def stack_test_env(stack: Mapping[str, object]) -> dict[str, str]:
    """Smoke-test and Playwright environment for a local stack's default instance.

    Every value comes from stack.json; nothing is inferred from the worktree id or slot.
    No DATAHUB_GMS_TOKEN: tests log in as the admin user, as on the Docker stack.
    """
    instance = cast(str, stack["default"])
    prefix = cast(dict[str, str], stack["indexPrefixes"])[instance]
    services = cast(dict[str, dict[str, str]], stack.get("services") or {})
    env = {
        "SMOKE_BACKEND": "local",
        "DATAHUB_GMS_URL": cast(dict[str, str], stack["instances"])[instance],
        # Actuator shares GMS's port here. Without this, a stack on 8080 (slot 0)
        # makes the tests assume Docker's separate management port, 4319.
        "DATAHUB_GMS_MANAGEMENT_URL": cast(dict[str, str], stack["instances"])[
            instance
        ],
        "ELASTICSEARCH_URL": f"http://localhost:{stack['opensearchPort']}",
        "INDEX_PREFIX": prefix,
        "ELASTICSEARCH_INDEX": f"{prefix}_datahub_usage_event",
        "ADMIN_USERNAME": "datahub",
        "ADMIN_PASSWORD": "datahub",
        "DATAHUB_ADMIN_USERNAME": "datahub",
        "DATAHUB_ADMIN_PASSWORD": "datahub",
        "DATAHUB_TELEMETRY_ENABLED": "false",
    }
    if stack.get("frontendPort"):
        env["DATAHUB_FRONTEND_URL"] = env["BASE_URL"] = (
            f"http://localhost:{stack['frontendPort']}"
        )
    if stack.get("systemClientId"):
        env["DATAHUB_SYSTEM_CLIENT_ID"] = cast(str, stack["systemClientId"])
        env["DATAHUB_SYSTEM_CLIENT_SECRET"] = cast(str, stack["systemClientSecret"])
    for role, key in (
        ("integrations", "DATAHUB_INTEGRATIONS_SERVICE_URL"),
        ("executor", "DATAHUB_EXECUTOR_URL"),
    ):
        if instance in services.get(role, {}):
            env[key] = services[role][instance]
    return env


def load_test_env(worktree_id: str) -> dict[str, str]:
    return stack_test_env(
        json.loads((local_data_dir(worktree_id) / "stack.json").read_text())
    )


def _profile_owner(path: Path) -> str | None:
    try:
        raw = json.loads(path.read_text())
        return raw.get("profile", {}).get("datahub_dev_owner")
    except (OSError, ValueError, AttributeError):
        return None


def refresh_profiles(worktree_id: str) -> None:
    _require_owned(worktree_id)
    data_dir = local_data_dir(worktree_id)
    stack = json.loads((data_dir / "stack.json").read_text())
    configs: dict[str, dict[str, object]] = {}
    for name, url in stack["instances"].items():
        profile = profile_name(worktree_id, name, stack["default"])
        path = profile_path_in(Path.home(), profile)
        if (
            path.is_symlink()
            or path.parent.is_symlink()
            or (path.exists() and _profile_owner(path) != str(data_dir))
        ):
            raise RuntimeError(f"Refusing to replace unmanaged CLI profile {profile}")
        configs[profile] = {
            "gms": {"server": url, "token": stack["adminTokens"].get(name)},
            "profile": {
                "description": f"Local DataHub {worktree_id} / {name}",
                "datahub_dev_owner": str(data_dir),
                "token_expires_at": stack["adminTokenExpiresAt"].get(name),
            },
        }
    for name, config in configs.items():
        # JSON is a YAML subset; the CLI and wrapper share paths and private atomic writes.
        write_profile_text(
            profile_path_in(Path.home(), name), json.dumps(config, indent=2) + "\n"
        )
    remove_profiles(worktree_id, keep=set(configs))


def remove_profiles(worktree_id: str, keep: set[str] | None = None) -> None:
    _require_owned(worktree_id)
    root = profile_path_in(Path.home(), profile_name(worktree_id)).parent.parent
    if not root.exists():
        return
    for directory in root.iterdir():
        name = directory.name
        if name in (keep or set()) or not (
            name == profile_name(worktree_id)
            or name.startswith(profile_name(worktree_id) + "-gms-")
        ):
            continue
        path = directory / ".datahubenv"
        if (
            directory.is_symlink()
            or path.is_symlink()
            or _profile_owner(path) != str(local_data_dir(worktree_id))
        ):
            continue
        path.unlink()
        if not any(directory.iterdir()):
            directory.rmdir()


def cmd_start_local(
    slot: int,
    ports: dict[str, int],
    worktree_id: str,
    repo_root: Path,
    *,
    processes: int = 1,
    skip_build: bool = False,
    fresh: bool = False,
    no_auth: bool = False,
    no_frontend: bool = False,
    registry_instances: Mapping[str, Mapping[str, object]] | None = None,
    timeout: int = 1200,
    verbose: bool = False,
) -> int:
    env_seconds(OPENSEARCH_READY_TIMEOUT_ENV, 900)
    env_seconds("GMS_LITE_GMS_HEALTH_TIMEOUT", 600)
    record = _require_owned(worktree_id)
    data_dir = local_data_dir(worktree_id)
    stack_file = data_dir / "stack.json"
    if _launcher_alive(data_dir):
        if (
            not fresh
            and stack_file.exists()
            and _http_ok(f"http://localhost:{ports['gms']}/health")
        ):
            refresh_profiles(worktree_id)
            _log(f"Local stack already running at http://localhost:{ports['gms']}")
            return 0
        _log(
            "ERROR: this local stack is running or still starting; stop it first with 'datahub-dev.sh stop'."
        )
        return 1
    occupied = _occupied_stack_port(ports, processes, no_frontend)
    if occupied is not None:
        _log(
            f"ERROR: port {occupied} is in use; stop the owning service before starting this stack"
        )
        return 1
    launcher = repo_root / "metadata-service/gms-lite/bin/gms-lite-stack.py"
    provider = record.get("local_extension")
    extension = (
        extensions.load(provider["module"]) if isinstance(provider, dict) else None
    )
    description = (
        extension.describe(provider["options"])
        if extension and isinstance(provider, dict)
        else extensions.Description()
    )
    artifacts = [
        repo_root / "metadata-service/war/build/libs/war.war",
        repo_root / "datahub-upgrade/build/libs/datahub-upgrade.jar",
        distribution_script(repo_root, recorded_major(record)),
    ]
    if not launcher.exists():
        _log(f"ERROR: launcher not found at {launcher}")
        return 1
    plugin_artifacts = all(
        any(repo_root.glob(pattern.format(major=recorded_major(record))))
        for pattern in description.artifact_globs
    )
    reuse = skip_build or (
        all(path.exists() for path in artifacts) and plugin_artifacts
    )
    if not reuse:
        result = subprocess.run(
            [
                str(repo_root / "gradlew"),
                ":metadata-service:war:bootJar",
                ":datahub-upgrade:bootJar",
                ":metadata-service:opensearch-embedded:installDist",
                f"-PopensearchVersion={recorded_major(record)}",
                *description.build_arguments,
                "-x",
                "test",
            ],
            cwd=repo_root,
        )
        if result.returncode:
            return result.returncode
    elif not all(path.exists() for path in artifacts) or not plugin_artifacts:
        _log(
            "ERROR: --skip-build requires GMS, upgrade and OpenSearch artifacts (including declared plugins); run without --skip-build"
        )
        return 1
    for build in description.builds:
        if build.artifact.exists():
            continue
        if not skip_build:
            result = subprocess.run(
                [str(repo_root / "gradlew"), build.task], cwd=repo_root
            )
            if result.returncode and build.required:
                return result.returncode
        if not build.artifact.exists():
            _log(
                f"{'ERROR' if build.required else 'WARNING'}: missing {build.artifact}; build {build.task}"
            )
            if build.required:
                return 1
    prepare_started = time.monotonic()
    error = _prepare_shared_opensearch(
        worktree_id, processes, fresh, registry_instances or {}
    )
    opensearch_ensure_s = round(time.monotonic() - prepare_started, 2)
    if error:
        _log(f"ERROR: {error}")
        return 1
    env_file = data_dir / "env-overrides.env"
    if fresh:
        shutil.rmtree(data_dir)
        write_instance_record(worktree_id, record)
    env_file.touch(mode=0o600, exist_ok=True)
    stack_file.unlink(missing_ok=True)
    timings_file = data_dir / TIMINGS_FILE
    timings_file.unlink(missing_ok=True)
    command = [
        sys.executable,
        str(launcher),
        "--processes",
        str(processes),
        "--port-base",
        str(ports["gms"]),
        "--data-dir",
        str(data_dir),
        "--frontend-port",
        str(ports["frontend"]),
        "--shared-opensearch-dir",
        str(shared_opensearch_dir(recorded_major(record))),
        "--opensearch",
        recorded_major(record),
        "--env-file",
        str(env_file),
        "--skip-build",
        "--heap",
        os.environ.get("GMS_LITE_HEAP", "1g"),
    ]
    if isinstance(provider, dict):
        command += [
            "--extension",
            provider["module"],
            "--extension-options",
            json.dumps(provider["options"]),
            "--extension-slot",
            str(slot),
        ]
    for key in ("job", "mode"):
        value = record.get("local_options", {}).get(key)
        if value is not None:
            command += [f"--{key}", str(value)]
    if no_auth:
        command.append("--no-auth")
    log_file = data_dir / "launcher.log"
    _log(f"Starting GMS-lite stack (data: {data_dir}); tail -f {log_file}")
    spawned_at = time.monotonic()
    with log_file.open("w") as output:
        process = subprocess.Popen(
            command,
            cwd=repo_root,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _save_pids(data_dir / "pids.json", {"launcher": process.pid})
    deadline = spawned_at + timeout
    log_offset = 0
    while time.monotonic() < deadline:
        if verbose:
            log_offset = _echo_new_log_lines(log_file, log_offset)
        measured = {
            "opensearch_ensure_s": opensearch_ensure_s,
            "stack_json_wait_s": round(time.monotonic() - spawned_at, 2),
        }
        if process.poll() is not None:
            _log(f"ERROR: launcher exited with {process.returncode}; see {log_file}")
            record_datahub_dev_timings(timings_file, measured)
            _log_tail(log_file)
            log_boot_timings(timings_file)
            return 1
        if stack_file.exists():
            if verbose:
                _echo_new_log_lines(log_file, log_offset)
            record_datahub_dev_timings(timings_file, measured)
            refresh_profiles(worktree_id)
            (data_dir / "env.applied").write_text(env_file.read_text())
            _log(
                f"GMS ready: http://localhost:{ports['gms']}; CLI profile: {profile_name(worktree_id)}"
            )
            return 0
        time.sleep(1)
    _log(f"ERROR: stack did not become ready within {timeout}s; see {log_file}")
    measured = {
        "opensearch_ensure_s": opensearch_ensure_s,
        "stack_json_wait_s": round(time.monotonic() - spawned_at, 2),
    }
    cmd_stop_local(worktree_id)
    record_datahub_dev_timings(timings_file, measured)
    _log_tail(log_file)
    log_boot_timings(timings_file)
    return 1


def _echo_new_log_lines(log_file: Path, offset: int) -> int:
    try:
        with log_file.open("rb") as stream:
            stream.seek(offset)
            chunk = stream.read()
    except OSError:
        return offset
    complete = chunk[: chunk.rfind(b"\n") + 1]
    for line in complete.decode(errors="replace").splitlines():
        _log(f"  | {line}")
    return offset + len(complete)


def _log_tail(log_file: Path) -> None:
    try:
        with log_file.open(errors="replace") as stream:
            lines = deque(stream, maxlen=40)
    except OSError:
        return
    _log(f"Last {len(lines)} lines of {log_file}:")
    for line in lines:
        _log(f"  | {line.rstrip()}")


def read_timings(path: Path) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def record_datahub_dev_timings(path: Path, values: Mapping[str, float]) -> None:
    timings = read_timings(path)
    if timings is None:
        return
    section = timings.get("datahub_dev")
    timings["datahub_dev"] = {
        **(section if isinstance(section, dict) else {}),
        **values,
    }
    try:
        path.write_text(json.dumps(timings, indent=2) + "\n")
    except OSError:
        pass


def log_boot_timings(path: Path) -> None:
    timings = read_timings(path)
    if timings is None:
        return
    try:
        phases = cast(dict[str, float], timings["phases"])
        section = cast(dict[str, float], timings.get("datahub_dev", {}))
        split = " ".join(
            f"{name}={float(value):.1f}s" for name, value in phases.items()
        )
        steps = " ".join(
            f"{name.removesuffix('_s')}={float(section[name]):.1f}s"
            for name in ("opensearch_ensure_s", "stack_json_wait_s", "frontend_ready_s")
            if name in section
        )
        _log(
            f"Boot timings: {split} total={float(timings['total_s']):.1f}s warm={timings['warm']} {steps}".rstrip()
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        # Diagnostics must not make an otherwise healthy stack unusable.
        pass


def launch_frontend(
    worktree_id: str,
    ports: Mapping[str, int],
    repo_root: Path,
    timeout: int = 900,
    *,
    preview: bool = False,
    build: bool = False,
) -> int:
    """Start the frontend in the background, record its PID and wait for /health.

    ``preview`` serves the built dist/ bundle (``build`` rebuilds it first) instead of HMR.
    """
    _require_owned(worktree_id)
    data_dir = local_data_dir(worktree_id)
    stack_file = data_dir / "stack.json"
    stack = json.loads(stack_file.read_text())
    port = ports["frontend"]
    if stack.get("frontendPort") != port:
        _log(
            f"ERROR: frontendPort does not match registry; restart the stack: {stack_file}"
        )
        return 1
    pids_file = data_dir / "pids.json"
    pids = _load_pids(pids_file)
    pid = pids.get("frontend")
    label = "preview" if preview else "HMR"
    if pid and _owned_by(pid, data_dir, unknown=True):
        task = "yarnPreview" if preview else "yarnServe"
        running = _process_command(pid) or ""
        if _owned_by(pid, data_dir) and _http_ok(f"http://localhost:{port}/health"):
            if task in running:
                _log(f"Frontend {label} already running: http://localhost:{port}")
                return 0
            _log(
                "ERROR: a frontend in the other mode is running; stop it first with 'datahub-dev.sh stop'"
            )
            return 1
        _log("ERROR: frontend is still starting or ownership is unknown; stop it first")
        return 1
    if _occupied_stack_port({"gms": port, "frontend": port}, 1, no_frontend=True):
        _log(f"ERROR: frontend port {port} is in use; stop its owning service first")
        return 1
    command = [
        str(repo_root / "gradlew"),
        "--no-daemon",
        *([":datahub-web-react:yarnBuild"] if preview and build else []),
        ":datahub-web-react:yarnPreview" if preview else ":datahub-web-react:yarnServe",
        f"-PfrontendPort={port}",
        "-PfrontendMode=lite",
        f"-PfrontendStackFile={stack_file}",
    ]
    log_file = data_dir / "frontend.log"
    _log(f"Starting frontend {label}: http://localhost:{port}; tail -f {log_file}")
    spawned_at = time.monotonic()
    with log_file.open("w") as output:
        process = subprocess.Popen(
            command,
            cwd=repo_root,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, "GMS_LITE_STACK_FILE": str(stack_file)},
        )
    # Record before dependency preparation so stop can interrupt a partially started frontend.
    _save_pids(pids_file, {**pids, "frontend": process.pid})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            _log(f"ERROR: frontend exited with {process.returncode}; see {log_file}")
            return 1
        if _http_ok(f"http://localhost:{port}/health", timeout=1):
            record_datahub_dev_timings(
                data_dir / TIMINGS_FILE,
                {"frontend_ready_s": round(time.monotonic() - spawned_at, 2)},
            )
            _log(f"Frontend {label} ready: http://localhost:{port}")
            return 0
        time.sleep(1)
    _log(f"ERROR: frontend did not become ready within {timeout}s; see {log_file}")
    cmd_stop_local(worktree_id, names={"frontend"})
    _log(
        "GMS left running; retry with 'datahub-dev.sh start --local' "
        "or shut down with 'datahub-dev.sh stop'."
    )
    return 1


def cmd_stop_local(worktree_id: str, *, names: set[str] | None = None) -> int:
    """Stop all or selected recorded processes only after proving ownership."""
    _require_owned(worktree_id)
    data_dir = local_data_dir(worktree_id)
    pids_file = data_dir / "pids.json"
    pids = _load_pids(pids_file)
    if names is None:
        pids = recorded_pids(data_dir)
    selected = {
        name: pid for name, pid in pids.items() if names is None or name in names
    }

    if not selected:
        _log("No local stack PIDs found; nothing to stop.")
        return 0

    # A saved PID may have exited long ago and been reused by an unrelated process;
    # signalling its process group would hit that process. Only signal our own.
    owned: dict[str, int] = {}
    for name, pid in selected.items():
        if _owned_by(pid, data_dir):
            owned[name] = pid
        else:
            _log(f"{name} (PID {pid}) is not running for this stack; skipping")

    for name, pid in owned.items():
        try:
            pgid = os.getpgid(pid)
            os.killpg(pgid, signal.SIGTERM)
            _log(f"Sent SIGTERM to {name} process group (PID {pid}, PGID {pgid})")
        except ProcessLookupError:
            _log(f"{name} (PID {pid}) already exited")
        except PermissionError:
            _log(f"Cannot kill {name} (PID {pid}): permission denied")

    # Extensions own independent groups; allow their reverse-order shutdown to finish
    # before killing the launcher, or those detached children would lose their supervisor.
    record = _require_owned(worktree_id)
    options = cast(dict[str, object], record.get("local_options", {}))
    roles = len(
        {
            role.rsplit("-", 1)[0]
            for role in cast(dict[str, int], record.get("ports", {}))
        }
    )
    shutdown_timeout = (
        max(25, 35 * int(str(options.get("processes", 1))) * roles)
        if names is None and record.get("local_extension")
        else 25
    )
    deadline = time.time() + shutdown_timeout
    while time.time() < deadline:
        if not any(_owned_by(pid, data_dir) for pid in owned.values()):
            break
        time.sleep(0.5)

    # Force-kill any stragglers (entire process group), re-checking ownership: a PID
    # that exited above may already have been reused.
    for name, pid in owned.items():
        if not _owned_by(pid, data_dir):
            continue
        try:
            pgid = os.getpgid(pid)
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    if any(_owned_by(pid, data_dir, unknown=True) for pid in selected.values()):
        _log(
            "ERROR: process ownership is unknown or it did not stop; PID file retained"
        )
        return 1
    remaining = {name: pid for name, pid in pids.items() if name not in selected}
    if remaining:
        _save_pids(pids_file, remaining)
    else:
        pids_file.unlink(missing_ok=True)
        if names is None:
            (data_dir / "processes.json").unlink(missing_ok=True)
    _log(
        "Local stack stopped." if names is None else "Selected local processes stopped."
    )
    return 0


def cmd_status_local(
    worktree_id: str, ports: dict[str, int], *, quiet: bool = False
) -> int:
    data_dir = local_data_dir(worktree_id)
    services: dict[str, dict[str, object]] = {}
    stack: dict[str, object] = {}
    try:
        stack = json.loads((data_dir / "stack.json").read_text())
    except (OSError, ValueError):
        pass
    urls = stack.get("instances", {"gms-0": f"http://localhost:{ports['gms']}"})
    assert isinstance(urls, dict)
    for name, url in urls.items():
        services[name] = {"url": url, "healthy": _http_ok(f"{url}/health")}
    record = read_instance_record(worktree_id) or {}
    options = cast(dict[str, object], record.get("local_options", {}))
    frontend_pid = _load_pids(data_dir / "pids.json").get("frontend")
    no_frontend = bool(options.get("no_frontend", False))
    # A detached frontend can run on a --no-frontend stack; show it without gating readiness.
    if frontend_pid or not no_frontend:
        frontend_url = f"http://localhost:{ports['frontend']}"
        services["frontend"] = {
            "url": frontend_url,
            "pid": frontend_pid,
            "healthy": bool(frontend_pid and _owned_by(frontend_pid, data_dir))
            and _http_ok(f"{frontend_url}/health"),
            **({"required": False} if no_frontend else {}),
        }
    checks = cast(
        dict[str, dict[str, dict[str, object]]], stack.get("serviceHealth", {})
    )
    for role, instances in checks.items():
        for name, spec in instances.items():
            services[f"{role}/{name}"] = {
                "url": spec["url"],
                "healthy": _http_ok(str(spec["url"])),
                "required": spec["required"],
            }
    ready = bool(services) and all(
        service["healthy"]
        for service in services.values()
        if service.get("required", True)
    )
    if quiet:
        return 0 if ready else 1
    manager = opensearch_manager(recorded_major(_require_owned(worktree_id)))
    status = manager.status()
    if status["healthy"]:
        try:
            usage, shards = shard_inventory(int(status["port"]))
            status["shards"] = {
                "used": usage.used,
                "limit": usage.limit,
                "this_stack": sum(
                    n
                    for name, n in shards.items()
                    if any(name.startswith(p + "_") for p in _prefixes(worktree_id))
                ),
            }
        except (OSError, ValueError, urllib.error.URLError):
            pass
    print(
        json.dumps(
            {
                "mode": "local",
                "ready": ready,
                "data_dir": str(data_dir),
                "stack_file": str(data_dir / "stack.json"),
                "services": services,
                "opensearch": status,
                "profile": profile_name(worktree_id),
                **(
                    {"timings": timings}
                    if (timings := read_timings(data_dir / TIMINGS_FILE)) is not None
                    else {}
                ),
            },
            indent=2,
        )
    )
    return 0 if ready else 1


def log_files(worktree_id: str) -> dict[str, Path]:
    """Logs this stack wrote, by name.

    The launcher writes ``logs/<instance>-<role>.log`` (gms, systemupdate, restoreindices,
    extension services) and ``logs/opensearch.log`` for a private node; datahub-dev writes
    ``launcher.log`` and ``frontend.log``. A shared node logs to its own directory.
    """
    record = _require_owned(worktree_id)
    data_dir = local_data_dir(worktree_id)
    files = {
        name: data_dir / f"{name}.log"
        for name in ("launcher", "frontend")
        if (data_dir / f"{name}.log").is_file()
    }
    try:
        shared = json.loads((data_dir / "stack.json").read_text()).get(
            "opensearchShared", True
        )
    except (OSError, ValueError):
        shared = True
    if shared:
        shared_log = opensearch_manager(recorded_major(record)).log_file
        if shared_log.is_file():
            files["opensearch"] = shared_log
    files.update({path.stem: path for path in (data_dir / "logs").glob("*.log")})
    return dict(sorted(files.items()))


def resolve_log(files: Mapping[str, Path], service: str, default: str) -> Path | None:
    """Map ``gms``, ``gms-1``, ``system-update`` or ``gms-1/integrations`` to a log file."""
    if service in files:
        return files[service]
    if re.fullmatch(r"gms-[0-9]+", service):
        return files.get(f"{service}-gms")
    instance, _, role = service.rpartition("/")
    return files.get(f"{instance or default}-{role.replace('-', '')}")


def cmd_logs_local(
    worktree_id: str, service: str | None, lines: int, follow: bool
) -> int:
    files = log_files(worktree_id)
    if not service:
        print(json.dumps({name: str(path) for name, path in files.items()}, indent=2))
        return 0
    try:
        default = json.loads(
            (local_data_dir(worktree_id) / "stack.json").read_text()
        ).get("default", "gms-0")
    except (OSError, ValueError):
        default = "gms-0"
    path = resolve_log(files, service, default)
    if path is None:
        _log(f"ERROR: no log for {service!r}; available: {', '.join(files) or 'none'}")
        return 1
    command = ["tail", "-n", str(lines), *(["-F"] if follow else []), str(path)]
    try:
        return subprocess.run(command).returncode
    except KeyboardInterrupt:
        return 0


def cmd_nuke_local(worktree_id: str, ports: dict[str, int], keep_data: bool) -> int:
    record = _require_owned(worktree_id)
    if cmd_stop_local(worktree_id) or stack_running(worktree_id, ports):
        _log(
            "ERROR: the local stack is still running after stop; not deleting its data"
        )
        return 1
    data_dir = local_data_dir(worktree_id)
    if keep_data:
        for lock in data_dir.glob("gms-*/gms.lock.db"):
            lock.unlink(missing_ok=True)
        _log("Stack stopped; H2 data, profiles and indices kept")
        return 0
    manager = opensearch_manager(recorded_major(_require_owned(worktree_id)))
    try:
        # An unbooted directory has no indices. Avoid starting a node just to delete it.
        if (data_dir / ".gms-lite").exists():
            status = manager.status()
            if status["running"] and not status["healthy"]:
                raise RuntimeError(
                    "shared OpenSearch is running but unhealthy; retry cleanup later"
                )
            port = int(status["port"]) if status["healthy"] else manager.get_or_start()
            delete_prefixes(port, _prefixes(worktree_id))
        remove_profiles(worktree_id)
        shutil.rmtree(data_dir)
    except (OSError, RuntimeError, ValueError, urllib.error.URLError) as error:
        _log(f"ERROR: could not remove this stack: {error}")
        return 1
    provider = record.get("local_extension")
    if isinstance(provider, dict):
        extension = extensions.load(provider["module"])
        assert extension is not None
        for name in _names_from_record(record):
            extension.after_nuke(
                extensions.context(
                    _REPO_ROOT, data_dir, name, ports, provider["options"]
                )
            )
    _log(f"Removed this stack's indices, profiles and {data_dir}")
    return 0


def reclaim_stale(registry: dict[str, dict[str, object]]) -> list[str]:
    """Reclaim only marked stacks with a recorded, missing worktree and no live GMS."""
    removed = []
    for worktree_id, entry in registry.items():
        if not is_owned_entry(entry):
            continue
        path = entry.get("worktree_path")
        if not isinstance(path, str) or Path(path).exists():
            continue
        if read_instance_record(worktree_id) != entry:
            continue
        ports = cast(dict[str, int], entry["ports"])
        if stack_running(worktree_id, ports):
            continue
        if cmd_nuke_local(worktree_id, ports, keep_data=False) == 0:
            removed.append(worktree_id)
    return removed


def _names_from_record(record: Mapping[str, object]) -> list[str]:
    options = cast(dict[str, object], record.get("local_options", {}))
    return [f"gms-{i}" for i in range(int(str(options.get("processes", 1))))]
