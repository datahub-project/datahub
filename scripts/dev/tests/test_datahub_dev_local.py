"""Local lifecycle regressions: isolation, guardrails and CLI connections."""

import argparse
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock

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
    assert dev.cmd_start(argparse.Namespace(local=True, processes=2)) == 0
    registered = dev._get_instance()
    assert registered is not None and local.is_owned_entry(registered)
    assert local.read_instance_record("abc123") == registered
    assert registered["ports"]["gms-1"] == 8081
    assert start.call_args.kwargs["processes"] == 2


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
        ports = {"gms": listener.getsockname()[1]}
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


def test_shard_inventory_uses_effective_limit_and_counts_replicas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def request(port: int, path: str) -> object:
        if path.startswith("/_cluster/settings"):
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
    assert (usage.used, usage.limit, shards) == (4, 400, {"open": 4})


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
