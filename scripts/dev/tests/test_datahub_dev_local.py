"""Local lifecycle regressions: isolation, guardrails and CLI connections."""

import argparse
import copy
import shlex
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock, call

import pytest

import datahub_dev as dev
import datahub_dev_local as local


@pytest.fixture
def entry(tmp_path: Path) -> dict[str, object]:
    return {
        "worktree_path": str(tmp_path),
        "mode": "local",
        "local_owner": local.OWNER,
        "opensearch_dir": str(local.shared_opensearch_dir("3")),
        "slot": 0,
        "ports": local.local_ports(0),
        "local_options": {"processes": 1, "no_auth": False},
    }


def register(entry: dict[str, object], worktree_id: str = "abc123") -> Path:
    dev._save_registry({"version": 1, "instances": {worktree_id: entry}})
    local.write_instance_record(worktree_id, entry)
    return local.local_data_dir(worktree_id)


def ready_stack(data_dir: Path, token: str = "first", processes: int = 2) -> None:
    (data_dir / "stack.json").write_text(
        json.dumps(
            {
                "default": "gms-0",
                "instances": {
                    f"gms-{i}": f"http://localhost:{8080 + i}" for i in range(processes)
                },
                "adminTokens": {f"gms-{i}": token + str(i) for i in range(processes)},
                "adminTokenExpiresAt": {
                    f"gms-{i}": "2030-01-01T00:00:00Z" for i in range(processes)
                },
            }
        )
    )


def test_port_scheme_and_slots_reserve_unmanaged_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    registry = {
        "instances": {
            "unmanaged": {"mode": "local", "slot": 0, "ports": local.local_ports(0)}
        }
    }
    assert dev._pick_slot(registry, local_stack=True) == 1
    assert dev._pick_slot(registry) == 1
    assert (
        local.local_ports(1)["gms"]
        == dev._compute_ports(1)["DATAHUB_MAPPED_GMS_PORT"]
        == 9080
    )
    assert local.local_ports(2)["frontend"] == 11002


def test_cross_slot_ports_and_additional_gms_ports_reserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dev, "_load_dev_config", lambda: {"max_local_instances": 4})
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    registry = {"instances": {"other": {"slot": 3, "ports": {"gms-1": 8081}}}}
    assert dev._pick_slot(registry, local_stack=True, processes=2) == 1


@pytest.mark.parametrize(
    "slots, expected",
    [([0, 1, *range(3, 18)], 18), ([0, 1, *range(3, 21)], 21)],
    ids=["17-foreign-entries", "full-reported-slot-range"],
)
def test_start_ignores_foreign_reservations_for_running_limit(
    slots: list[int],
    expected: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    # Foreign layouts have no verified process contract; never inspect their PIDs.
    monkeypatch.setattr(
        local,
        "_process_command",
        Mock(side_effect=AssertionError("foreign process probe")),
    )
    foreign = {
        f"{i:06x}": {"mode": "local", "slot": slot, "ports": local.local_ports(slot)}
        for i, slot in enumerate(slots)
    }
    before = copy.deepcopy(foreign)
    dev._save_registry({"instances": foreign})
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))
    assert dev.cmd_start(dev.build_parser().parse_args(["start", "--local"])) == 0
    assert start.call_args.kwargs["slot"] == expected
    registry = dev._load_registry()["instances"]
    assert {key: registry[key] for key in before} == before
    assert set(registry["abc123"]["ports"].values()).isdisjoint(
        port for entry in before.values() for port in entry["ports"].values()
    )


@pytest.mark.parametrize(
    "reserved", [False, True], ids=["new-stack", "stopped-reservation"]
)
@pytest.mark.parametrize("role", ["launcher", "frontend"])
def test_running_limit_names_verified_stacks_and_preserves_registry(
    entry: dict[str, object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    reserved: bool,
    role: str,
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_load_dev_config", lambda: {"max_local_instances": 1})
    running_path = tmp_path / "running-worktree"
    other = {
        **entry,
        "slot": 1,
        "ports": local.local_ports(1),
        "worktree_path": str(running_path),
    }
    directory = local.local_data_dir("def456")
    local.write_instance_record("def456", other)
    local._save_pids(directory / "pids.json", {role: 12345})
    command = (
        [
            sys.executable,
            str(dev.REPO_ROOT / "metadata-service/gms-lite/bin/gms-lite-stack.py"),
            "--data-dir",
            str(directory),
        ]
        if role == "launcher"
        else [
            str(dev.REPO_ROOT / "gradlew"),
            ":datahub-web-react:yarnServe",
            "-PfrontendMode=lite",
            f"-PfrontendStackFile={directory / 'stack.json'}",
        ]
    )
    monkeypatch.setattr(local, "_process_command", lambda pid: shlex.join(command))
    registry = {"def456": other}
    if reserved:
        registry["abc123"] = entry
        local.write_instance_record("abc123", entry)
    dev._save_registry({"instances": registry})
    before = dev.INSTANCES_FILE.read_bytes()
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    assert dev.cmd_start(dev.build_parser().parse_args(["start", "--local"])) == 1
    error = capsys.readouterr().err
    assert "def456" in error and str(running_path) in error
    assert (
        "max_docker_free_instances" in error
        and "scripts/dev/datahub-dev.sh stop" in error
    )
    assert dev.INSTANCES_FILE.read_bytes() == before
    start.assert_not_called()


@pytest.mark.parametrize(
    "command", [None, "unrelated process"], ids=["unknown", "reused-pid"]
)
def test_unverified_owned_pids_and_healthy_ports_do_not_count(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    command: str | None,
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_load_dev_config", lambda: {"max_local_instances": 1})
    directory = register(entry, "def456")
    local._save_pids(directory / "pids.json", {"launcher": 12345})
    monkeypatch.setattr(local, "_process_command", lambda pid: command)
    monkeypatch.setattr(
        local,
        "_http_ok",
        Mock(side_effect=AssertionError("health is not process ownership")),
    )
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    assert dev._pick_slot(dev._load_registry(), local_stack=True) == 1


def test_idempotent_start_is_allowed_at_running_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_load_dev_config", lambda: {"max_local_instances": 1})
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    monkeypatch.setattr(local, "cmd_start_local", Mock(return_value=0))
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))
    args = dev.build_parser().parse_args(["start", "--local"])
    assert dev.cmd_start(args) == 0
    directory = local.local_data_dir("abc123")
    local._save_pids(directory / "pids.json", {"launcher": 12345})
    monkeypatch.setattr(
        local,
        "_process_command",
        lambda pid: shlex.join(
            [
                sys.executable,
                str(dev.REPO_ROOT / "metadata-service/gms-lite/bin/gms-lite-stack.py"),
                "--data-dir",
                str(directory),
            ]
        ),
    )
    assert dev.cmd_start(args) == 0


def test_start_marks_and_records_stack(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))
    assert dev.cmd_start(argparse.Namespace(local=True, processes=2)) == 0
    registered = dev._get_instance()
    assert registered is not None and local.is_owned_entry(registered)
    assert local.read_instance_record("abc123") == registered
    assert registered["ports"]["gms-1"] == 8081
    assert start.call_args.kwargs["processes"] == 2


def test_start_can_restore_auth_and_remembers_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))
    parser = dev.build_parser()
    for flags, no_auth in [
        (["--no-auth"], True),
        ([], True),
        (["--auth"], False),
        ([], False),
    ]:
        args = parser.parse_args(["start", "--local", *flags])
        assert dev.cmd_start(args) == 0
        assert start.call_args.kwargs["no_auth"] is no_auth
        record = local.read_instance_record("abc123")
        assert record == dev._get_instance()
        assert record is not None and record["local_options"]["no_auth"] is no_auth


def test_start_refuses_unknown_directory_without_reserving_slot(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    directory = local.local_data_dir("abc123")
    directory.mkdir(parents=True)
    sentinel = directory / "unrecognized"
    sentinel.write_text("keep")
    start = Mock()
    monkeypatch.setattr(local, "cmd_start_local", start)
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))

    assert dev.cmd_start(dev.build_parser().parse_args(["start", "--local"])) == 1
    assert dev._get_instance() is None
    assert local.read_instance_record("abc123") is None
    assert sentinel.read_text() == "keep"
    assert str(directory) in capsys.readouterr().err
    start.assert_not_called()


@pytest.mark.parametrize("remote", [False, True])
def test_registration_holds_registry_lock(
    monkeypatch: pytest.MonkeyPatch, remote: bool
) -> None:
    load = dev._load_registry
    save = dev._save_registry

    def assert_locked() -> None:
        with (dev.DATAHUB_DEV_DIR / "instances.lock").open("a") as lock:
            with pytest.raises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def load_locked() -> dict[str, object]:
        assert_locked()
        return load()

    def save_locked(registry: dict[str, object]) -> None:
        assert_locked()
        save(registry)

    monkeypatch.setattr(dev, "_load_registry", load_locked)
    monkeypatch.setattr(dev, "_save_registry", save_locked)
    if remote:
        instance = dev._register_remote_instance("/path/to/runner.sh", 2)
    else:
        instance = dev._register_instance(0, dev._compute_ports(0))
    assert load()["instances"][dev.WORKTREE_ID] == instance


@pytest.mark.parametrize("contents", ["{invalid", "[]"])
def test_main_reports_bad_registry_without_overwriting(
    contents: str,
) -> None:
    dev.DATAHUB_DEV_DIR.mkdir(parents=True)
    dev.INSTANCES_FILE.write_text(contents)
    result = subprocess.run(
        [sys.executable, dev.__file__, "instances", "clean"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert dev.INSTANCES_FILE.read_text() == contents
    assert str(dev.INSTANCES_FILE) in result.stderr
    assert "Repair" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    "command",
    [
        "cmd_start",
        "cmd_stop",
        "cmd_nuke",
        "cmd_status",
        "cmd_env_set",
        "cmd_env_restart",
    ],
)
def test_unmanaged_local_entry_cannot_be_changed(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    entry.pop("local_owner")
    dev._save_registry({"instances": {"abc123": entry}})
    before = dev.INSTANCES_FILE.read_bytes()
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    args = argparse.Namespace(local=True, keep_data=False, assignment="FOO=bar")
    assert getattr(dev, command)(args) == 1
    assert dev.INSTANCES_FILE.read_bytes() == before
    assert not local.LOCAL_DATA_ROOT.exists()


def test_clean_reclaims_only_marked_missing_stopped_worktrees(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry["worktree_path"] = str(tmp_path / "gone")
    register(entry)
    unmanaged = {
        **entry,
        "local_owner": "unknown",
        "ports": local.local_ports(1),
        "slot": 1,
    }
    registry = {"instances": {"abc123": entry, "def456": unmanaged}}
    dev._save_registry(registry)
    sentinel = tmp_path / ".datahub/local/def456/keep"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("unchanged")
    monkeypatch.setattr(local, "stack_running", lambda *args: False)
    assert (
        dev._cmd_instances_clean(
            argparse.Namespace(
                command="instances", instances_command="clean", yes=True, orphans=False
            )
        )
        == 0
    )
    assert dev._load_registry()["instances"] == {"def456": unmanaged}
    assert sentinel.read_text() == "unchanged"
    assert not local.local_data_dir("abc123").exists()


def test_clean_preserves_running_and_unrecorded_stacks(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry["worktree_path"] = str(tmp_path / "gone")
    data_dir = register(entry)
    monkeypatch.setattr(local, "stack_running", lambda *args: True)
    assert local.reclaim_stale({"abc123": entry}) == []
    (data_dir / local.INSTANCE_RECORD).unlink()
    monkeypatch.setattr(local, "stack_running", lambda *args: False)
    assert local.reclaim_stale({"abc123": entry}) == []


def test_clean_removes_stale_docker_and_remote_entries_only(tmp_path: Path) -> None:
    stale = {"worktree_path": str(tmp_path / "gone")}
    unmanaged = {**stale, "mode": "local"}
    live = {"worktree_path": str(tmp_path)}
    dev._save_registry(
        {
            "instances": {
                "docker": {**stale, "type": "local"},
                "remote": {**stale, "type": "remote"},
                "unmanaged": unmanaged,
                "live": live,
            }
        }
    )
    assert (
        dev._cmd_instances_clean(
            argparse.Namespace(
                command="instances", instances_command="clean", yes=True, orphans=False
            )
        )
        == 0
    )
    assert dev._load_registry()["instances"] == {"unmanaged": unmanaged, "live": live}


def test_clean_releases_lock_and_preserves_concurrent_updates(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry["worktree_path"] = str(tmp_path / "gone")
    register(entry)
    updated = {**entry, "created_at": "updated"}
    live = {"worktree_path": str(tmp_path)}

    def nuke(worktree_id: str, ports: dict[str, int], keep_data: bool) -> int:
        with (dev.DATAHUB_DEV_DIR / "instances.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with dev._registry_transaction() as registry:
            registry["instances"][worktree_id] = updated
            registry["instances"]["live"] = live
        return 0

    monkeypatch.setattr(local, "cmd_nuke_local", nuke)
    assert (
        dev._cmd_instances_clean(
            argparse.Namespace(
                command="instances", instances_command="clean", yes=True, orphans=False
            )
        )
        == 0
    )
    assert dev._load_registry()["instances"] == {"abc123": updated, "live": live}


def test_profiles_refresh_and_remove_preserve_default(
    entry: dict[str, object], tmp_path: Path
) -> None:
    data_dir = register(entry)
    pointer = tmp_path / ".datahub/current-profile"
    pointer.write_text("personal\n")
    default = tmp_path / ".datahubenv"
    default.write_text("personal connection")
    ready_stack(data_dir)
    local.refresh_profiles("abc123")
    profile = local.profile_path_in(tmp_path, "local-abc123")
    second = local.profile_path_in(tmp_path, "local-abc123-gms-1")
    assert json.loads(profile.read_text())["gms"] == {
        "server": "http://localhost:8080",
        "token": "first0",
    }
    assert second.exists()
    assert profile.stat().st_mode & 0o777 == 0o600
    assert profile.parent.stat().st_mode & 0o777 == 0o700
    ready_stack(data_dir, token="refreshed")
    local.refresh_profiles("abc123")
    assert json.loads(profile.read_text())["gms"]["token"] == "refreshed0"
    assert local.cmd_stop_local("abc123") == 0
    assert profile.exists()
    assert local.cmd_nuke_local("abc123", local.local_ports(0), keep_data=False) == 0
    assert not profile.exists() and not second.exists()
    assert pointer.read_text() == "personal\n"
    assert default.read_text() == "personal connection"


def test_profiles_refuse_unmanaged_collision(
    entry: dict[str, object], tmp_path: Path
) -> None:
    data_dir = register(entry)
    ready_stack(data_dir)
    profile = local.profile_path_in(tmp_path, "local-abc123")
    profile.parent.mkdir(parents=True)
    profile.write_text("personal connection")
    with pytest.raises(RuntimeError):
        local.refresh_profiles("abc123")
    assert profile.read_text() == "personal connection"


def test_no_auth_and_reduced_process_count_clear_old_credentials(
    entry: dict[str, object], tmp_path: Path
) -> None:
    data_dir = register(entry)
    ready_stack(data_dir)
    local.refresh_profiles("abc123")
    ready_stack(data_dir, processes=1)
    stack = json.loads((data_dir / "stack.json").read_text())
    stack["adminTokens"] = {}
    stack["adminTokenExpiresAt"] = {}
    (data_dir / "stack.json").write_text(json.dumps(stack))
    local.refresh_profiles("abc123")
    assert (
        json.loads(local.profile_path_in(tmp_path, "local-abc123").read_text())["gms"][
            "token"
        ]
        is None
    )
    assert not local.profile_path_in(tmp_path, "local-abc123-gms-1").exists()


def test_shell_env_selects_only_this_worktree(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    register(entry)
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://other")
    assert dev.cmd_shell_env(argparse.Namespace()) == 0
    assert capsys.readouterr().out.splitlines() == [
        "export DATAHUB_PROFILE=local-abc123",
        "export DATAHUB_GMS_URL=http://localhost:8080",
        "unset DATAHUB_GMS_TOKEN",
    ]


@pytest.fixture
def stack_process(
    entry: dict[str, object],
) -> Iterator[tuple[Path, subprocess.Popen[str]]]:
    data_dir = register(entry)
    script = data_dir / "gms-lite-stack.py"
    script.write_text("import time\ntime.sleep(60)\n")
    process = subprocess.Popen(
        [sys.executable, str(script), "--data-dir", str(data_dir)],
        start_new_session=True,
        text=True,
    )
    local._save_pids(data_dir / "pids.json", {"launcher": process.pid})
    try:
        yield data_dir, process
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)


@pytest.mark.parametrize("fresh", [True, False])
def test_running_stack_guard_before_ready(
    stack_process: tuple[Path, subprocess.Popen[str]], tmp_path: Path, fresh: bool
) -> None:
    data_dir, process = stack_process
    sentinel = data_dir / "keep"
    sentinel.touch()
    assert (
        local.cmd_start_local(0, local.local_ports(0), "abc123", tmp_path, fresh=fresh)
        == 1
    )
    assert process.poll() is None and sentinel.exists()


def test_stop_checks_pid_identity(entry: dict[str, object]) -> None:
    data_dir = register(entry)
    local._save_pids(data_dir / "pids.json", {"launcher": os.getpid()})
    assert local.cmd_stop_local("abc123") == 0
    assert not local._owned_by(os.getpid(), data_dir)


def test_stop_owns_its_launcher(
    stack_process: tuple[Path, subprocess.Popen[str]],
) -> None:
    _, process = stack_process
    assert local.cmd_stop_local("abc123") == 0
    assert process.wait(timeout=5) != 0


def test_occupied_port_preserves_stack_data(
    entry: dict[str, object], tmp_path: Path
) -> None:
    data_dir = register(entry)
    sentinel = data_dir / "keep"
    sentinel.touch()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        ports = {"gms": listener.getsockname()[1], "frontend": 9002}
        assert local.cmd_start_local(0, ports, "abc123", tmp_path, fresh=True) == 1
    assert sentinel.exists()


def test_start_passes_env_file_and_refreshes_profiles(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = register(entry)
    repo = tmp_path / "repo"
    artifacts = [
        "metadata-service/gms-lite/bin/gms-lite-stack.py",
        "metadata-service/war/build/libs/war.war",
        "datahub-upgrade/build/libs/datahub-upgrade.jar",
        "metadata-service/opensearch-embedded/build/install/opensearch-embedded-os3/bin/opensearch-embedded",
    ]
    for artifact in artifacts:
        path = repo / artifact
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    monkeypatch.setattr(local, "_prepare_shared_opensearch", lambda *args: None)
    monkeypatch.setattr(local, "_occupied_stack_port", lambda *args: None)
    process = Mock(pid=12345)
    process.poll.return_value = None

    def launch(command: list[str], **kwargs: object) -> Mock:
        env_file = Path(command[command.index("--env-file") + 1])
        assert env_file == data_dir / "env-overrides.env" and env_file.exists()
        assert command[command.index("--shared-opensearch-dir") + 1] == str(
            local.shared_opensearch_dir("3")
        )
        ready_stack(data_dir, processes=1)
        return process

    monkeypatch.setattr(local.subprocess, "Popen", launch)
    assert local.cmd_start_local(0, local.local_ports(0), "abc123", repo) == 0
    assert local.profile_path_in(tmp_path, "local-abc123").exists()
    assert (data_dir / "env.applied").exists()


def test_env_restart_remembers_options(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry["local_options"] = {"processes": 2, "no_auth": True}
    register(entry)
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(local, "cmd_stop_local", Mock(return_value=0))
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))
    assert dev.cmd_env_set(argparse.Namespace(assignment="FOO=bar")) == 0
    assert "FOO=bar" in dev._local_env_file().read_text()
    assert dev.cmd_env_restart(argparse.Namespace()) == 0
    assert start.call_args.kwargs["processes"] == 2
    assert start.call_args.kwargs["no_auth"] is True


def test_shard_budget_counts_replicas_and_warm_reuse(entry: dict[str, object]) -> None:
    register(entry)
    shards = {
        local.index_prefix(local.local_data_dir("abc123"), "gms-0") + "_index": 100
    }
    assert (
        local.shard_budget_error(local.ShardUsage(1000, 1000), shards, "abc123", 1, 100)
        is None
    )
    assert local.shard_budget_error(
        local.ShardUsage(1000, 1000), shards, "abc123", 2, 100
    )


def test_nuke_deletes_only_own_prefixes_and_keeps_shared_node(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = register(entry)
    (data_dir / ".gms-lite").touch()
    manager = Mock()
    manager.status.return_value = {"running": True, "healthy": True, "port": 1234}
    monkeypatch.setattr(local, "opensearch_manager", lambda *args: manager)
    delete = Mock()
    monkeypatch.setattr(local, "delete_prefixes", delete)
    assert local.cmd_nuke_local("abc123", local.local_ports(0), keep_data=True) == 0
    delete.assert_not_called()
    assert local.cmd_nuke_local("abc123", local.local_ports(0), keep_data=False) == 0
    delete.assert_called_once_with(1234, [local.index_prefix(data_dir, "gms-0")])
    manager.stop.assert_not_called()


def test_repeated_healthy_start_reuses_launcher(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = register(entry)
    ready_stack(data_dir, processes=1)
    before = (data_dir / "stack.json").read_bytes()
    monkeypatch.setattr(local, "_launcher_alive", lambda directory: True)
    monkeypatch.setattr(local, "_http_ok", lambda *args: True)
    launch = Mock()
    monkeypatch.setattr(local.subprocess, "Popen", launch)
    assert local.cmd_start_local(0, local.local_ports(0), "abc123", tmp_path) == 0
    assert (data_dir / "stack.json").read_bytes() == before
    launch.assert_not_called()


@pytest.mark.parametrize("missing_setting", [False, True])
def test_shard_inventory_uses_effective_limit_and_counts_replicas(
    monkeypatch: pytest.MonkeyPatch, missing_setting: bool
) -> None:
    def request(port: int, path: str) -> object:
        if path.startswith("/_cluster/settings"):
            if missing_setting:
                return {}
            return {
                "transient": {"cluster.max_shards_per_node": "200"},
                "persistent": {},
                "defaults": {"cluster.max_shards_per_node": "1000"},
            }
        if path == "/_cluster/health":
            return {"number_of_data_nodes": 2}
        return [
            {"index": "open", "pri": "2", "rep": "1", "status": "open"},
            {"index": "closed", "pri": "100", "rep": "1", "status": "close"},
        ]

    monkeypatch.setattr(local, "request_json", request)
    usage, shards = local.shard_inventory(1234)
    assert (usage.used, usage.limit, shards) == (
        4,
        2000 if missing_setting else 400,
        {"open": 4},
    )


def test_fresh_clears_data_and_retains_instance_record(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = register(entry)
    (data_dir / "env-overrides.env").write_text("FOO=bar\n")
    (data_dir / "obsolete").touch()
    repo = tmp_path / "repo"
    for artifact in [
        "metadata-service/gms-lite/bin/gms-lite-stack.py",
        "metadata-service/war/build/libs/war.war",
        "datahub-upgrade/build/libs/datahub-upgrade.jar",
        "metadata-service/opensearch-embedded/build/install/opensearch-embedded-os3/bin/opensearch-embedded",
    ]:
        path = repo / artifact
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    monkeypatch.setattr(local, "_prepare_shared_opensearch", lambda *args: None)
    monkeypatch.setattr(local, "_occupied_stack_port", lambda *args: None)
    process = Mock(pid=12345)
    process.poll.return_value = 1
    monkeypatch.setattr(local.subprocess, "Popen", lambda *args, **kwargs: process)
    assert (
        local.cmd_start_local(0, local.local_ports(0), "abc123", repo, fresh=True) == 1
    )
    assert not (data_dir / "obsolete").exists()
    assert local.read_instance_record("abc123") == entry
    assert (data_dir / "env-overrides.env").read_text() == ""


@pytest.mark.parametrize("task", ["yarnServe", "yarnPreview"])
@pytest.mark.parametrize("valid", [True, False])
def test_frontend_ownership_requires_task_and_exact_stack_path(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch, valid: bool, task: str
) -> None:
    data_dir = register(entry)
    command = [
        "/usr/bin/java",
        "-jar",
        "/repo/gradle/wrapper/gradle-wrapper.jar",
        "--no-daemon",
        f":datahub-web-react:{task}",
        "-PfrontendMode=lite",
        f"-PfrontendStackFile={data_dir / 'stack.json'}"
        + (".other" if not valid else ""),
    ]
    monkeypatch.setattr(local, "_process_command", lambda pid: " ".join(command))
    assert local._owned_by(123, data_dir) is valid
    if not valid:
        local._save_pids(data_dir / "pids.json", {"frontend": 123})
        signal_process = Mock()
        monkeypatch.setattr(local.os, "killpg", signal_process)
        assert local.cmd_stop_local("abc123") == 0
        signal_process.assert_not_called()


def test_frontend_start_records_both_pids_before_readiness(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = register(entry)
    (data_dir / "stack.json").write_text(json.dumps({"frontendPort": 9002}))
    (data_dir / local.TIMINGS_FILE).write_text(
        json.dumps(
            {
                "warm": True,
                "total_s": 2,
                "phases": {},
                "datahub_dev": {"stack_json_wait_s": 2.0},
            }
        )
    )
    local._save_pids(data_dir / "pids.json", {"launcher": 456})
    monkeypatch.setattr(local, "_occupied_stack_port", lambda *args, **kwargs: None)
    process = Mock(pid=123)
    process.poll.return_value = None
    launch = Mock(return_value=process)
    monkeypatch.setattr(local.subprocess, "Popen", launch)

    def ready(url: str, timeout: float) -> bool:
        assert local._load_pids(data_dir / "pids.json") == {
            "launcher": 456,
            "frontend": 123,
        }
        return True

    monkeypatch.setattr(local, "_http_ok", ready)
    assert local.launch_frontend("abc123", local.local_ports(0), tmp_path) == 0
    command = launch.call_args.args[0]
    assert f"-PfrontendStackFile={data_dir / 'stack.json'}" in command
    assert ":datahub-web-react:yarnServe" in command
    assert launch.call_args.kwargs["start_new_session"] is True

    timings = local.read_timings(data_dir / local.TIMINGS_FILE)
    assert timings is not None
    assert timings["datahub_dev"]["stack_json_wait_s"] == 2.0
    assert timings["datahub_dev"]["frontend_ready_s"] >= 0


@pytest.mark.parametrize("frontend_owned", [True, False])
def test_frontend_timeout_keeps_launcher(
    entry: dict[str, object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    frontend_owned: bool,
) -> None:
    data_dir = register(entry)
    (data_dir / "stack.json").write_text(json.dumps({"frontendPort": 9002}))
    local._save_pids(data_dir / "pids.json", {"launcher": 456})
    monkeypatch.setattr(local, "_occupied_stack_port", lambda *args, **kwargs: None)
    monkeypatch.setattr(local.subprocess, "Popen", Mock(return_value=Mock(pid=123)))
    running = {456, 123} if frontend_owned else {456}
    monkeypatch.setattr(
        local, "_owned_by", lambda pid, directory, unknown=False: pid in running
    )
    monkeypatch.setattr(local.os, "getpgid", lambda pid: pid)
    clock = iter([0, 26])
    monkeypatch.setattr(local.time, "time", lambda: next(clock))

    def signal_group(pgid: int, sig: int) -> None:
        if sig == signal.SIGKILL:
            running.discard(pgid)

    kill = Mock(side_effect=signal_group)
    monkeypatch.setattr(local.os, "killpg", kill)
    assert (
        local.launch_frontend("abc123", local.local_ports(0), tmp_path, timeout=0) == 1
    )
    assert kill.call_args_list == (
        [call(123, signal.SIGTERM), call(123, signal.SIGKILL)] if frontend_owned else []
    )
    assert 456 in running
    assert local._load_pids(data_dir / "pids.json") == {"launcher": 456}
    assert "datahub-dev.sh start --local" in capsys.readouterr().out


@pytest.mark.parametrize(
    "no_frontend,frontend_healthy,gms_healthy,expected",
    [
        (False, False, True, False),
        (False, True, True, True),
        (True, False, True, True),
        (False, True, False, False),
    ],
)
def test_status_requires_enabled_frontend(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    no_frontend: bool,
    frontend_healthy: bool,
    gms_healthy: bool,
    expected: bool,
) -> None:
    entry["local_options"] = {"no_frontend": no_frontend}
    data_dir = register(entry)
    ready_stack(data_dir, processes=1)
    local._save_pids(data_dir / "pids.json", {"frontend": 123})
    monkeypatch.setattr(local, "_owned_by", lambda *args: True)
    monkeypatch.setattr(
        local,
        "_http_ok",
        lambda url: frontend_healthy if ":9002/" in url else gms_healthy,
    )
    manager = Mock()
    manager.status.return_value = {"healthy": False}
    monkeypatch.setattr(local, "opensearch_manager", lambda *args: manager)
    assert local.cmd_status_local("abc123", local.local_ports(0)) == (
        0 if expected else 1
    )
    status = json.loads(capsys.readouterr().out)
    assert status["ready"] is expected
    # The recorded frontend PID is shown either way; it gates readiness only when enabled.
    assert status["services"]["frontend"].get("required", True) is not no_frontend


def test_wait_keeps_polling_until_frontend_is_healthy(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_dir = register(entry)
    ready_stack(data_dir, processes=1)
    local._save_pids(data_dir / "pids.json", {"frontend": 123})
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(local, "_owned_by", lambda *args: True)
    frontend_health = iter([False, True, True])
    monkeypatch.setattr(
        local,
        "_http_ok",
        lambda url: next(frontend_health) if ":9002/" in url else True,
    )
    manager = Mock()
    manager.status.return_value = {"healthy": False}
    monkeypatch.setattr(local, "opensearch_manager", lambda *args: manager)
    sleep = Mock()
    monkeypatch.setattr(dev.time, "sleep", sleep)
    assert dev.cmd_wait(argparse.Namespace(timeout=30)) == 0
    sleep.assert_called_once()
    assert json.loads(capsys.readouterr().out)["ready"] is True


def test_no_frontend_is_remembered_on_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    monkeypatch.setattr(local, "cmd_start_local", Mock(return_value=0))
    frontend = Mock()
    monkeypatch.setattr(local, "launch_frontend", frontend)
    for flags in [["--no-frontend"], []]:
        assert (
            dev.cmd_start(dev.build_parser().parse_args(["start", "--local", *flags]))
            == 0
        )
        assert dev._get_instance()["local_options"]["no_frontend"] is True
    frontend.assert_not_called()
    frontend.return_value = 0
    assert (
        dev.cmd_start(dev.build_parser().parse_args(["start", "--local", "--frontend"]))
        == 0
    )
    frontend.assert_called_once()


def test_occupied_frontend_blocks_start_before_changing_data(
    entry: dict[str, object], tmp_path: Path
) -> None:
    data_dir = register(entry)
    sentinel = data_dir / "keep"
    sentinel.touch()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        ports = {"gms": 0, "frontend": listener.getsockname()[1]}
        assert local.cmd_start_local(0, ports, "abc123", tmp_path, fresh=True) == 1
        assert local._occupied_stack_port(ports, 1, no_frontend=True) is None
    assert sentinel.exists()


@pytest.mark.parametrize("major", ["2", "3"])
def test_start_records_major_and_restart_ignores_changed_default(
    monkeypatch: pytest.MonkeyPatch, major: str
) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    monkeypatch.setattr(local, "cmd_start_local", Mock(return_value=0))
    parser = dev.build_parser()
    assert (
        dev.cmd_start(
            parser.parse_args(
                ["start", "--local", "--opensearch", major, "--no-frontend"]
            )
        )
        == 0
    )
    monkeypatch.setenv("EMBEDDED_OPENSEARCH_VERSION", "3" if major == "2" else "2")
    assert dev.cmd_start(parser.parse_args(["start", "--local"])) == 0
    record = local.read_instance_record("abc123")
    assert record == dev._get_instance()
    assert record["opensearch_version"] == major
    assert record["opensearch_dir"] == str(local.shared_opensearch_dir(major))


def test_legacy_stack_keeps_three_and_switch_requires_fresh(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    register(entry)
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    monkeypatch.setattr(local, "launch_frontend", Mock(return_value=0))
    parser = dev.build_parser()
    assert dev.cmd_start(parser.parse_args(["start", "--local"])) == 0
    assert dev._get_instance()["opensearch_version"] == "3"
    before = dev.INSTANCES_FILE.read_bytes()
    start.reset_mock()
    assert (
        dev.cmd_start(parser.parse_args(["start", "--local", "--opensearch", "2"])) == 1
    )
    assert "--fresh" in capsys.readouterr().err
    assert dev.INSTANCES_FILE.read_bytes() == before
    start.assert_not_called()
    nuke = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_nuke_local", nuke)
    assert (
        dev.cmd_start(
            parser.parse_args(["start", "--local", "--opensearch", "2", "--fresh"])
        )
        == 0
    )
    nuke.assert_called_once_with(
        "abc123", {**entry["ports"], "gms-0": 8080}, keep_data=False
    )
    assert dev._get_instance()["opensearch_version"] == "2"


@pytest.mark.parametrize("major", ["2", "3"])
def test_cleanup_and_status_follow_recorded_node(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    major: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    entry.update(
        opensearch_version=major, opensearch_dir=str(local.shared_opensearch_dir(major))
    )
    data_dir = register(entry)
    (data_dir / ".gms-lite").touch()
    manager = Mock()
    manager.status.return_value = {
        "healthy": True,
        "running": True,
        "port": 1234,
        "major": major,
    }
    factory = Mock(return_value=manager)
    monkeypatch.setattr(local, "opensearch_manager", factory)
    monkeypatch.setattr(
        local, "shard_inventory", lambda port: (local.ShardUsage(0, 2000), {})
    )
    delete = Mock()
    monkeypatch.setattr(local, "delete_prefixes", delete)
    local.cmd_status_local("abc123", local.local_ports(0))
    assert json.loads(capsys.readouterr().out)["opensearch"]["major"] == major
    assert local.cmd_nuke_local("abc123", local.local_ports(0), keep_data=False) == 0
    assert factory.call_args_list == [call(major), call(major)]
    delete.assert_called_once_with(1234, [local.index_prefix(data_dir, "gms-0")])


def test_invalid_default_does_not_reserve_a_stack(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("EMBEDDED_OPENSEARCH_VERSION", "4")
    assert dev.cmd_start(dev.build_parser().parse_args(["start", "--local"])) == 1
    assert "must be 2 or 3" in capsys.readouterr().err
    assert dev._get_instance() is None
    assert not local.LOCAL_DATA_ROOT.exists()


@pytest.fixture
def timed_launcher(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Mock]:
    data_dir = register(entry)
    repo = tmp_path / "repo"
    for artifact in (
        "metadata-service/gms-lite/bin/gms-lite-stack.py",
        "metadata-service/war/build/libs/war.war",
        "datahub-upgrade/build/libs/datahub-upgrade.jar",
        "metadata-service/opensearch-embedded/build/install/opensearch-embedded-os3/bin/opensearch-embedded",
    ):
        path = repo / artifact
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    monkeypatch.setattr(local, "_prepare_shared_opensearch", lambda *args: None)
    monkeypatch.setattr(local, "_occupied_stack_port", lambda *args: None)
    process = Mock(pid=12345)
    process.poll.return_value = None
    monkeypatch.setattr(local.subprocess, "Popen", Mock(return_value=process))
    monkeypatch.setattr(local, "cmd_stop_local", Mock(return_value=0))
    return data_dir, repo, process


def test_start_timeout_verbose_and_failure_tail(
    timed_launcher: tuple[Path, Path, Mock],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    directory, repo, _ = timed_launcher

    def launch(*args: object, **kwargs: object) -> Mock:
        (directory / "launcher.log").write_text(
            "".join(f"phase {i}\n" for i in range(45))
        )
        return timed_launcher[2]

    monkeypatch.setattr(local.subprocess, "Popen", launch)
    assert (
        local.cmd_start_local(
            0, local.local_ports(0), "abc123", repo, timeout=1, verbose=True
        )
        == 1
    )
    output = capsys.readouterr().out
    assert "within 1s" in output and "Last 40 lines" in output
    assert output.count("| phase 0\n") == 1
    assert output.count("| phase 44\n") == 2
    local.cmd_stop_local.assert_called_once_with("abc123")


@pytest.mark.parametrize("exited", [False, True])
def test_launcher_wait_records_timings_without_printing_tokens(
    timed_launcher: tuple[Path, Path, Mock],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    exited: bool,
) -> None:
    directory, repo, process = timed_launcher

    def launch(*args: object, **kwargs: object) -> Mock:
        (directory / local.TIMINGS_FILE).write_text(
            json.dumps(
                {
                    "warm": False,
                    "total_s": 12.0,
                    "phases": {"gms_boot": 10.0},
                }
            )
        )
        (directory / "launcher.log").write_text("gms_boot finished\n")
        if exited:
            process.poll.return_value = 1
            process.returncode = 1
        else:
            ready_stack(directory, token="never-print-this-token", processes=1)
        return process

    monkeypatch.setattr(local.subprocess, "Popen", launch)
    assert local.cmd_start_local(
        0, local.local_ports(0), "abc123", repo, verbose=True
    ) == int(exited)
    local.log_boot_timings(directory / local.TIMINGS_FILE)
    timings = local.read_timings(directory / local.TIMINGS_FILE)
    assert timings is not None
    assert timings["phases"] == {"gms_boot": 10.0}
    assert set(timings["datahub_dev"]) == {"stack_json_wait_s", "opensearch_ensure_s"}
    output = capsys.readouterr().out
    assert "gms_boot=10.0s" in output and "never-print-this-token" not in output
    if exited:
        assert "Last 1 lines" in output


@pytest.mark.parametrize("contents", [None, "{bad json", "[]"])
def test_timings_diagnostics_tolerate_missing_or_malformed_file(
    tmp_path: Path, contents: str | None, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / local.TIMINGS_FILE
    if contents is not None:
        path.write_text(contents)
    local.record_datahub_dev_timings(path, {"stack_json_wait_s": 1.0})
    local.log_boot_timings(path)
    assert local.read_timings(path) is None
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "timings", [None, {"warm": True, "total_s": 20.0, "phases": {"gms_boot": 18.0}}]
)
def test_status_includes_only_available_timings(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    timings: dict[str, object] | None,
) -> None:
    directory = register(entry)
    ready_stack(directory, processes=1)
    if timings is not None:
        (directory / local.TIMINGS_FILE).write_text(json.dumps(timings))
    manager = Mock()
    manager.status.return_value = {"healthy": False}
    monkeypatch.setattr(local, "opensearch_manager", lambda *args: manager)
    local.cmd_status_local("abc123", local.local_ports(0))
    result = json.loads(capsys.readouterr().out)
    assert result.get("timings") == timings
    assert "adminTokens" not in result


def test_timeout_and_verbose_are_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
    start = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_start_local", start)
    assert (
        dev.cmd_start(
            dev.build_parser().parse_args(
                ["start", "--local", "--no-frontend", "--timeout", "42", "--verbose"]
            )
        )
        == 0
    )
    assert start.call_args.kwargs["timeout"] == 42
    assert start.call_args.kwargs["verbose"] is True


def test_plugin_artifacts_are_reused_on_warm_start(
    timed_launcher: tuple[Path, Path, Mock], monkeypatch: pytest.MonkeyPatch
) -> None:
    from extensions.datahub_local_extension import Description

    directory, repo, process = timed_launcher
    plugin = (
        repo
        / "metadata-service/opensearch-embedded/build/install/opensearch-embedded-os3/lib/opensearch-knn-3.jar"
    )
    plugin.parent.mkdir(parents=True)
    plugin.touch()
    record = local.read_instance_record("abc123")
    record["local_extension"] = {"module": "example:extension", "options": {}}
    local.write_instance_record("abc123", record)
    description = Description(
        build_arguments=("-Pknn",),
        artifact_globs=(
            "metadata-service/opensearch-embedded/build/install/opensearch-embedded-os{major}/lib/opensearch-knn-*.jar",
        ),
    )
    monkeypatch.setattr(
        local.extensions, "load", lambda _: Mock(describe=lambda _: description)
    )
    monkeypatch.setattr(local, "refresh_profiles", lambda _: None)
    run = Mock(return_value=subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(local.subprocess, "run", run)

    def launch(*args: object, **kwargs: object) -> Mock:
        (directory / "stack.json").write_text("{}")
        return process

    monkeypatch.setattr(local.subprocess, "Popen", launch)
    assert local.cmd_start_local(0, local.local_ports(0), "abc123", repo) == 0
    run.assert_not_called()


def full_stack(**overrides: object) -> dict[str, object]:
    stack: dict[str, object] = {
        "default": "gms-0",
        "instances": {
            "gms-0": "http://localhost:9080",
            "gms-1": "http://localhost:9081",
        },
        "indexPrefixes": {"gms-0": "lite_abc123_0", "gms-1": "lite_abc123_1"},
        "opensearchPort": 9201,
        "frontendPort": 10002,
        "systemClientId": "__datahub_system",
        "systemClientSecret": "s3cret",
        "adminTokens": {"gms-0": "tok0", "gms-1": "tok1"},
        "services": {
            "integrations": {"gms-0": "http://localhost:10003", "gms-1": "http://x:1"},
            "executor": {"gms-0": "http://localhost:10004"},
        },
    }
    stack.update(overrides)
    return stack


def test_stack_test_env_uses_default_instance_and_logs_in() -> None:
    env = local.stack_test_env(full_stack())
    assert env["DATAHUB_GMS_URL"] == "http://localhost:9080"
    assert env["DATAHUB_GMS_MANAGEMENT_URL"] == "http://localhost:9080"
    assert env["INDEX_PREFIX"] == "lite_abc123_0"
    assert env["ELASTICSEARCH_INDEX"] == "lite_abc123_0_datahub_usage_event"
    assert env["ELASTICSEARCH_URL"] == "http://localhost:9201"
    assert env["DATAHUB_FRONTEND_URL"] == env["BASE_URL"] == "http://localhost:10002"
    assert env["DATAHUB_INTEGRATIONS_SERVICE_URL"] == "http://localhost:10003"
    assert env["DATAHUB_EXECUTOR_URL"] == "http://localhost:10004"
    assert env["DATAHUB_SYSTEM_CLIENT_SECRET"] == "s3cret"
    assert env["SMOKE_BACKEND"] == "local"
    assert (env["ADMIN_USERNAME"], env["ADMIN_PASSWORD"]) == ("datahub", "datahub")
    assert "DATAHUB_GMS_TOKEN" not in env


def test_stack_test_env_omits_services_that_are_off() -> None:
    env = local.stack_test_env(
        full_stack(
            services={"integrations": {"gms-1": "http://x:1"}},
            frontendPort=None,
            systemClientId=None,
        )
    )
    assert (
        not {
            "DATAHUB_INTEGRATIONS_SERVICE_URL",
            "DATAHUB_EXECUTOR_URL",
            "DATAHUB_FRONTEND_URL",
            "BASE_URL",
            "DATAHUB_SYSTEM_CLIENT_ID",
        }
        & env.keys()
    )


def test_shell_env_tests_and_smoke_env_read_stack_json(
    entry: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_dir = register(entry)
    (data_dir / "stack.json").write_text(json.dumps(full_stack()))
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://other-worktree")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "stale")
    assert dev.cmd_shell_env(argparse.Namespace(tests=True)) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "unset DATAHUB_PROFILE DATAHUB_GMS_TOKEN"
    assert "export DATAHUB_GMS_URL=http://localhost:9080" in lines
    env = dev._smoke_test_env()
    assert env["DATAHUB_GMS_URL"] == "http://localhost:9080"
    assert "DATAHUB_GMS_TOKEN" not in env


def test_shell_env_tests_without_stack_json_fails(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    register(entry)
    monkeypatch.setattr(dev, "WORKTREE_ID", "abc123")
    assert dev.cmd_shell_env(argparse.Namespace(tests=True)) == 1


def test_detached_preview_is_recorded_shown_and_stopped(
    entry: dict[str, object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    entry["local_options"] = {"processes": 1, "no_frontend": True}
    data_dir = register(entry)
    (data_dir / "stack.json").write_text(json.dumps({"frontendPort": 9002}))
    local._save_pids(data_dir / "pids.json", {"launcher": 456})
    monkeypatch.setattr(local, "_occupied_stack_port", lambda *args, **kwargs: None)
    launch = Mock(return_value=Mock(pid=123, **{"poll.return_value": None}))
    monkeypatch.setattr(local.subprocess, "Popen", launch)
    monkeypatch.setattr(local, "_http_ok", lambda url, timeout=3.0: True)
    assert (
        local.launch_frontend(
            "abc123", local.local_ports(0), tmp_path, preview=True, build=True
        )
        == 0
    )
    command = launch.call_args.args[0]
    assert command.index(":datahub-web-react:yarnBuild") < command.index(
        ":datahub-web-react:yarnPreview"
    )
    assert local._load_pids(data_dir / "pids.json") == {
        "launcher": 456,
        "frontend": 123,
    }

    running = {123}
    monkeypatch.setattr(
        local, "_owned_by", lambda pid, directory, unknown=False: pid in running
    )
    manager = Mock()
    manager.status.return_value = {"healthy": False}
    monkeypatch.setattr(local, "opensearch_manager", lambda *args: manager)
    local.cmd_status_local("abc123", local.local_ports(0))
    assert capsys.readouterr().out.count('"pid": 123') == 1
    monkeypatch.setattr(local.os, "getpgid", lambda pid: pid)
    kill = Mock(side_effect=lambda pgid, sig: running.discard(pgid))
    monkeypatch.setattr(local.os, "killpg", kill)
    assert local.cmd_stop_local("abc123", names={"frontend"}) == 0
    kill.assert_called_once_with(123, signal.SIGTERM)
    assert local._load_pids(data_dir / "pids.json") == {"launcher": 456}


def test_log_names_resolve_to_files_the_stack_wrote(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = register(entry)
    (data_dir / "stack.json").write_text(
        json.dumps({"default": "gms-0", "opensearchShared": False})
    )
    (data_dir / "logs").mkdir()
    for name in (
        "logs/gms-0-gms",
        "logs/gms-1-gms",
        "logs/gms-0-systemupdate",
        "logs/gms-1-integrations",
        "logs/opensearch",
        "frontend",
    ):
        (data_dir / f"{name}.log").write_text("line\n")
    files = local.log_files("abc123")
    assert set(files) == {
        "frontend",
        "gms-0-gms",
        "gms-1-gms",
        "gms-0-systemupdate",
        "gms-1-integrations",
        "opensearch",
    }
    expected = {
        "gms": "logs/gms-0-gms.log",
        "gms-1": "logs/gms-1-gms.log",
        "system-update": "logs/gms-0-systemupdate.log",
        "gms-1/integrations": "logs/gms-1-integrations.log",
        "gms-1-integrations": "logs/gms-1-integrations.log",
        "frontend": "frontend.log",
        "opensearch": "logs/opensearch.log",
    }
    for service, path in expected.items():
        assert local.resolve_log(files, service, "gms-0") == data_dir / path
    assert local.resolve_log(files, "executor", "gms-0") is None

    run = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr(local.subprocess, "run", run)
    assert local.cmd_logs_local("abc123", "gms", 50, follow=False) == 0
    assert run.call_args.args[0] == [
        "tail",
        "-n",
        "50",
        str(data_dir / "logs/gms-0-gms.log"),
    ]
    assert local.cmd_logs_local("abc123", "executor", 50, follow=False) == 1
