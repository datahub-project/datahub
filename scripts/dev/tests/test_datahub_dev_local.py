"""Local lifecycle regressions: isolation, guardrails and CLI connections."""

import argparse
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
        "opensearch_dir": str(local.SHARED_OPENSEARCH_DIR),
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
    assert dev._cmd_instances_clean(argparse.Namespace()) == 0
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
    assert dev._cmd_instances_clean(argparse.Namespace()) == 0
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
    assert dev._cmd_instances_clean(argparse.Namespace()) == 0
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
        "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded",
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
            local.SHARED_OPENSEARCH_DIR
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
    monkeypatch.setattr(local, "opensearch_manager", lambda: manager)
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
        "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded",
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


@pytest.mark.parametrize("valid", [True, False])
def test_frontend_ownership_requires_task_and_exact_stack_path(
    entry: dict[str, object], monkeypatch: pytest.MonkeyPatch, valid: bool
) -> None:
    data_dir = register(entry)
    command = [
        "/usr/bin/java",
        "-jar",
        "/repo/gradle/wrapper/gradle-wrapper.jar",
        "--no-daemon",
        ":datahub-web-react:yarnServe",
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
    monkeypatch.setattr(local, "opensearch_manager", lambda: manager)
    assert local.cmd_status_local("abc123", local.local_ports(0)) == (
        0 if expected else 1
    )
    status = json.loads(capsys.readouterr().out)
    assert status["ready"] is expected
    assert ("frontend" in status["services"]) is not no_frontend


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
    monkeypatch.setattr(local, "opensearch_manager", lambda: manager)
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
