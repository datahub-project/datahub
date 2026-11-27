"""Regressions for isolation, cleanup, concurrent allocation and service recovery."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

import datahub_dev as dev
import datahub_dev_local as local
from local import inventory


def owned(tmp_path: Path, worktree: str = "checkout") -> dict[str, object]:
    record = {
        "mode": "local",
        "local_owner": local.OWNER,
        "slot": 0,
        "opensearch_dir": str(local.shared_opensearch_dir("2")),
        "opensearch_version": "2",
        "worktree_path": str(tmp_path / worktree),
        "ports": local.local_ports(0),
    }
    local.write_instance_record("abc123", record)
    dev._save_registry({"instances": {"abc123": record}})
    return record


def test_bare_clean_deletes_nothing_and_yes_covers_all_kinds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = owned(tmp_path)
    entries = {
        "abc123": record,
        "docker": {"worktree_path": str(tmp_path / "gone")},
        "remote": {"type": "remote", "worktree_path": str(tmp_path / "gone")},
    }
    dev._save_registry({"instances": entries})
    monkeypatch.setattr(
        local, "opensearch_manager", lambda _: Mock(status=lambda: {"healthy": False})
    )
    nuke = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_nuke_local", nuke)
    before = dev.INSTANCES_FILE.read_bytes()
    assert (
        dev._cmd_instances_clean(dev.build_parser().parse_args(["instances", "clean"]))
        == 0
    )
    assert dev.INSTANCES_FILE.read_bytes() == before
    nuke.assert_not_called()
    assert (
        dev._cmd_instances_clean(
            dev.build_parser().parse_args(["instances", "clean", "--yes"])
        )
        == 0
    )
    assert dev._load_registry()["instances"] == {}
    nuke.assert_called_once()


@pytest.mark.parametrize("prefix", [["instances", "clean"], ["indices", "prune"]])
def test_orphan_cleanup_requires_explicit_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix: list[str]
) -> None:
    owned(tmp_path)
    dev._save_registry({"instances": {}})
    monkeypatch.setattr(
        local, "opensearch_manager", lambda _: Mock(status=lambda: {"healthy": False})
    )
    nuke = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_nuke_local", nuke)
    flags_list = (
        (["--yes"], ["--orphans"], ["--orphans", "--yes"])
        if prefix[0] == "instances"
        else ([], ["--yes"])
    )
    for flags in flags_list:
        assert (
            dev._cmd_instances_clean(dev.build_parser().parse_args([*prefix, *flags]))
            == 0
        )
    nuke.assert_called_once()


@pytest.mark.parametrize("running", [False, True])
def test_indices_drop_refuses_running_and_keeps_existing_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, running: bool
) -> None:
    (tmp_path / "checkout").mkdir()
    record = owned(tmp_path)
    monkeypatch.setattr(
        local, "opensearch_manager", lambda _: Mock(status=lambda: {"healthy": False})
    )
    monkeypatch.setattr(local, "stack_running", lambda *args: running)
    nuke = Mock(return_value=0)
    monkeypatch.setattr(local, "cmd_nuke_local", nuke)
    args = dev.build_parser().parse_args(["indices", "drop", "abc123", "--yes"])
    if running:
        with pytest.raises(RuntimeError, match="Stop it first"):
            inventory.run(args, dev)
        nuke.assert_not_called()
    else:
        assert inventory.run(args, dev) == 0
        nuke.assert_called_once()
    assert dev._load_registry()["instances"]["abc123"] == record


def test_starting_reservation_counts_during_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = owned(tmp_path)
    monkeypatch.setattr(dev, "WORKTREE_ID", "def456")
    record["starting"] = {
        "pid": os.getpid(),
        "identity": local.process_identity(os.getpid()),
    }
    local.write_instance_record("abc123", record)
    with pytest.raises(RuntimeError, match="limit reached"):
        dev._check_local_running_limit({"abc123": record}, 1)
    record["starting"]["identity"] = "reused pid"
    dev._check_local_running_limit({"abc123": record}, 1)


def test_docker_allocation_survives_many_stopped_local_reservations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dev, "_load_dev_config", lambda: {"max_local_instances": 2})
    monkeypatch.setattr(dev, "_is_port_free", lambda _: True)
    entries = {
        str(i): {"slot": i, "mode": "local", "ports": local.local_ports(i)}
        for i in range(6)
    }
    assert dev._pick_slot({"instances": entries}) >= 6


def test_stop_recovers_owned_service_after_launcher_is_killed(tmp_path: Path) -> None:
    owned(tmp_path)
    directory = local.local_data_dir("abc123")
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; print('ready', flush=True); time.sleep(60)",
        ],
        start_new_session=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "ready"
        (directory / "processes.json").write_text(
            json.dumps({str(child.pid): local.process_identity(child.pid)})
        )
        local._save_pids(directory / "pids.json", {"launcher": 99999999})
        assert local.cmd_stop_local("abc123") == 0
        assert child.wait(timeout=5) != 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        if child.stdout is not None:
            child.stdout.close()
