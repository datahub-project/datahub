"""Docker-free datahub-dev lifecycle. Stdlib only; imported lazily by datahub_dev."""

from __future__ import annotations

import errno
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
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, cast

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "metadata-service/gms-lite/bin"))
sys.path.insert(0, str(_REPO_ROOT / "metadata-ingestion/src/datahub/cli"))
from _opensearch_manager import (  # noqa: E402
    SharedOpenSearchManager,
    delete_prefixes,
    index_prefix,
    request_json,
)
from _profile_store import profile_path_in, write_profile_text  # noqa: E402

# Keep stack data separate from other local launchers sharing the instance registry.
LOCAL_DATA_ROOT = Path.home() / ".datahub/local/stacks"
SHARED_OPENSEARCH_DIR = Path.home() / ".datahub/local/opensearch-3"
OWNER = "datahub-dev-oss-local-v1"
INSTANCE_RECORD = "instance.json"
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


def is_owned_entry(entry: Mapping[str, object]) -> bool:
    return (
        entry.get("mode") == "local"
        and entry.get("local_owner") == OWNER
        and entry.get("opensearch_dir") == str(SHARED_OPENSEARCH_DIR)
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
        and ":datahub-web-react:yarnServe" in args
        and f"-PfrontendStackFile={data_dir / 'stack.json'}" in args
        and "-PfrontendMode=lite" in args
    )
    return launcher or frontend


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


def _launcher_alive(data_dir: Path) -> bool:
    pid = _load_pids(data_dir / "pids.json").get("launcher")
    # Unknown ownership blocks deletion, but never authorizes sending a signal.
    return pid is not None and _owned_by(pid, data_dir, unknown=True)


def stack_running(worktree_id: str, ports: Mapping[str, int] | None = None) -> bool:
    data_dir = local_data_dir(worktree_id)
    frontend = _load_pids(data_dir / "pids.json").get("frontend")
    return (
        _launcher_alive(data_dir)
        or bool(frontend and _owned_by(frontend, data_dir, unknown=True))
        or bool(ports and _http_ok(f"http://localhost:{ports['gms']}/health"))
    )


def opensearch_manager() -> SharedOpenSearchManager:
    return SharedOpenSearchManager(SHARED_OPENSEARCH_DIR, _REPO_ROOT)


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
    manager = opensearch_manager()
    try:
        port = manager.get_or_start()
        usage, shards = shard_inventory(port)
        if fresh:
            # H2 is also cleared; old search documents must not survive that wipe.
            delete_prefixes(port, _prefixes(worktree_id, processes))
            usage, shards = shard_inventory(port)
        return shard_budget_error(
            usage, shards, worktree_id, processes, shards_per_instance(registry, shards)
        )
    except (RuntimeError, OSError, ValueError, urllib.error.URLError) as error:
        return f"shared OpenSearch is not usable: {error}"


def profile_name(
    worktree_id: str, instance: str = "gms-0", default: str = "gms-0"
) -> str:
    return f"local-{worktree_id}" + (f"-{instance}" if instance != default else "")


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
) -> int:
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
    artifacts = [
        repo_root / "metadata-service/war/build/libs/war.war",
        repo_root / "datahub-upgrade/build/libs/datahub-upgrade.jar",
        repo_root
        / "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded",
    ]
    if not launcher.exists():
        _log(f"ERROR: launcher not found at {launcher}")
        return 1
    reuse = skip_build or all(path.exists() for path in artifacts)
    if not reuse:
        result = subprocess.run(
            [
                str(repo_root / "gradlew"),
                ":metadata-service:war:bootJar",
                ":datahub-upgrade:bootJar",
                ":metadata-service:opensearch-embedded:installDist",
                "-x",
                "test",
            ],
            cwd=repo_root,
        )
        if result.returncode:
            return result.returncode
    elif not all(path.exists() for path in artifacts):
        _log(
            "ERROR: --skip-build requires GMS, upgrade and OpenSearch artifacts; run without --skip-build"
        )
        return 1
    error = _prepare_shared_opensearch(
        worktree_id, processes, fresh, registry_instances or {}
    )
    if error:
        _log(f"ERROR: {error}")
        return 1
    env_file = data_dir / "env-overrides.env"
    if fresh:
        shutil.rmtree(data_dir)
        write_instance_record(worktree_id, record)
    env_file.touch(mode=0o600, exist_ok=True)
    stack_file.unlink(missing_ok=True)
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
        str(SHARED_OPENSEARCH_DIR),
        "--env-file",
        str(env_file),
        "--skip-build",
    ]
    if no_auth:
        command.append("--no-auth")
    log_file = data_dir / "launcher.log"
    _log(f"Starting GMS-lite stack (data: {data_dir}); tail -f {log_file}")
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
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            _log(f"ERROR: launcher exited with {process.returncode}; see {log_file}")
            return 1
        if stack_file.exists():
            refresh_profiles(worktree_id)
            (data_dir / "env.applied").write_text(env_file.read_text())
            _log(
                f"GMS ready: http://localhost:{ports['gms']}; CLI profile: {profile_name(worktree_id)}"
            )
            return 0
        time.sleep(1)
    _log(f"ERROR: stack did not become ready within {timeout}s; see {log_file}")
    cmd_stop_local(worktree_id)
    return 1


def launch_frontend(
    worktree_id: str, ports: Mapping[str, int], repo_root: Path, timeout: int = 900
) -> int:
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
    if pid and _owned_by(pid, data_dir, unknown=True):
        if _owned_by(pid, data_dir) and _http_ok(f"http://localhost:{port}/health"):
            _log(f"Frontend HMR already running: http://localhost:{port}")
            return 0
        _log("ERROR: frontend is still starting or ownership is unknown; stop it first")
        return 1
    if _occupied_stack_port({"gms": port, "frontend": port}, 1, no_frontend=True):
        _log(f"ERROR: frontend port {port} is in use; stop its owning service first")
        return 1
    command = [
        str(repo_root / "gradlew"),
        "--no-daemon",
        ":datahub-web-react:yarnServe",
        f"-PfrontendPort={port}",
        "-PfrontendMode=lite",
        f"-PfrontendStackFile={stack_file}",
    ]
    log_file = data_dir / "frontend.log"
    _log(f"Starting frontend HMR: http://localhost:{port}; tail -f {log_file}")
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
            _log(f"Frontend HMR ready: http://localhost:{port}")
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

    # The launcher's shutdown handler waits up to 20s for GMS to stop.
    # Poll until all processes exit or deadline is reached.
    deadline = time.time() + 25
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
    if not options.get("no_frontend", False):
        frontend_pid = _load_pids(data_dir / "pids.json").get("frontend")
        frontend_url = f"http://localhost:{ports['frontend']}"
        services["frontend"] = {
            "url": frontend_url,
            "pid": frontend_pid,
            "healthy": bool(frontend_pid and _owned_by(frontend_pid, data_dir))
            and _http_ok(f"{frontend_url}/health"),
        }
    ready = bool(services) and all(service["healthy"] for service in services.values())
    if quiet:
        return 0 if ready else 1
    manager = opensearch_manager()
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
            },
            indent=2,
        )
    )
    return 0 if ready else 1


def cmd_nuke_local(worktree_id: str, ports: dict[str, int], keep_data: bool) -> int:
    _require_owned(worktree_id)
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
    manager = opensearch_manager()
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
