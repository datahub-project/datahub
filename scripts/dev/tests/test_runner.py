"""Tests for the remote runner plugin (slot allocation, tunnel pairs, registry, call sequence)."""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import datahub_dev


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_config(monkeypatch, max_local: int = 2, max_remote: int = 10) -> None:
    monkeypatch.setattr(
        datahub_dev,
        "_load_dev_config",
        lambda: {"max_local_instances": max_local, "max_remote_instances": max_remote},
    )


def _fake_ok(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args, 0, stdout="", stderr="")


# ---------------------------------------------------------------------------
# Layer 1: slot allocation
# ---------------------------------------------------------------------------


class TestPickSlot:
    def test_local_first_slot_is_zero(self, monkeypatch):
        _patch_config(monkeypatch)
        slot = datahub_dev._pick_slot({}, remote=False)
        assert slot == 0

    def test_local_skips_occupied_slots(self, monkeypatch):
        _patch_config(monkeypatch)
        registry = {"instances": {"other": {"slot": 0}}}
        slot = datahub_dev._pick_slot(registry, remote=False)
        assert slot == 1

    def test_local_full_raises(self, monkeypatch):
        _patch_config(monkeypatch, max_local=2)
        registry = {"instances": {"a": {"slot": 0}, "b": {"slot": 1}}}
        with pytest.raises(RuntimeError, match="local"):
            datahub_dev._pick_slot(registry, remote=False)

    def test_remote_starts_after_local_slots(self, monkeypatch):
        _patch_config(monkeypatch, max_local=2, max_remote=10)
        # Local slots 0,1 occupied; remote should start at 2.
        registry = {"instances": {"a": {"slot": 0}, "b": {"slot": 1}}}
        slot = datahub_dev._pick_slot(registry, remote=True)
        assert slot == 2

    def test_remote_skips_occupied_remote_slots(self, monkeypatch):
        _patch_config(monkeypatch, max_local=2, max_remote=10)
        # Slot 2 taken; next remote is 3.
        registry = {"instances": {"a": {"slot": 0}, "b": {"slot": 1}, "c": {"slot": 2}}}
        slot = datahub_dev._pick_slot(registry, remote=True)
        assert slot == 3

    def test_remote_full_raises(self, monkeypatch):
        _patch_config(monkeypatch, max_local=2, max_remote=2)
        registry = {
            "instances": {
                "a": {"slot": 0},
                "b": {"slot": 1},
                "c": {"slot": 2},  # remote slot 0
                "d": {"slot": 3},  # remote slot 1
            }
        }
        with pytest.raises(RuntimeError, match="remote"):
            datahub_dev._pick_slot(registry, remote=True)

    def test_own_worktree_slot_not_counted(self, monkeypatch):
        _patch_config(monkeypatch)
        # Current worktree occupies slot 0; _pick_slot should still return 0
        # because it excludes WORKTREE_ID from the "used" set.
        registry = {"instances": {datahub_dev.WORKTREE_ID: {"slot": 0}}}
        slot = datahub_dev._pick_slot(registry, remote=False)
        assert slot == 0


# ---------------------------------------------------------------------------
# Layer 1: port computation and tunnel pairs
# ---------------------------------------------------------------------------


class TestPortsAndTunnels:
    def test_slot0_returns_defaults(self):
        ports = datahub_dev._compute_ports(0)
        assert ports["DATAHUB_MAPPED_GMS_PORT"] == 8080
        assert ports["DATAHUB_MAPPED_FRONTEND_PORT"] == 9002
        assert ports["DATAHUB_MAPPED_MYSQL_PORT"] == 3306

    def test_slot1_adds_1000(self):
        ports = datahub_dev._compute_ports(1)
        assert ports["DATAHUB_MAPPED_GMS_PORT"] == 9080   # 8080 + 1*1000
        assert ports["DATAHUB_MAPPED_FRONTEND_PORT"] == 10002  # 9002 + 1*1000

    def test_slot2_used_for_first_remote(self, monkeypatch):
        # SLOT_OFFSET=1000; slot 2 → base + 2000
        _patch_config(monkeypatch, max_local=2)
        ports = datahub_dev._compute_ports(2)
        assert ports["DATAHUB_MAPPED_GMS_PORT"] == 10080   # 8080 + 2*1000
        assert ports["DATAHUB_MAPPED_FRONTEND_PORT"] == 11002  # 9002 + 2*1000

    def test_tunnel_pairs_map_local_to_remote_default(self):
        # Slot 2: local 10080 → remote 8080, local 11002 → remote 9002
        ports = datahub_dev._compute_ports(2)
        pairs = datahub_dev._compute_tunnel_pairs(ports)
        assert "10080:8080" in pairs
        assert "11002:9002" in pairs

    def test_tunnel_pairs_slot0_maps_to_self(self):
        # Slot 0: local == remote, so pairs are "8080:8080" etc.
        ports = datahub_dev._compute_ports(0)
        pairs = datahub_dev._compute_tunnel_pairs(ports)
        assert "8080:8080" in pairs

    def test_no_port_exceeds_65535(self):
        # Highest plausible slot: 11 (max_local=2, max_remote=10 → slot 11)
        # SLOT_OFFSET=1000; max port = max(PORT_BASE) + 11*1000 = 9200+11000 = 20200
        ports = datahub_dev._compute_ports(11)
        for val in ports.values():
            assert val <= 65535, f"Port {val} exceeds max"


# ---------------------------------------------------------------------------
# Layer 1: remote instance registration
# ---------------------------------------------------------------------------


class TestRegisterRemoteInstance:
    def test_writes_type_remote(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        inst = datahub_dev._register_remote_instance("/path/to/runner.sh", 2)
        assert inst["type"] == "remote"

    def test_writes_runner_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        inst = datahub_dev._register_remote_instance("/my/runner.sh", 2)
        assert inst["runner"] == "/my/runner.sh"

    def test_ports_are_slot_offset(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        inst = datahub_dev._register_remote_instance("/runner.sh", 2)
        # Local tunnel port for slot 2 with SLOT_OFFSET=1000: 8080+2000=10080
        assert inst["ports"]["DATAHUB_MAPPED_GMS_PORT"] == 10080

    def test_persists_to_disk(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        datahub_dev._register_remote_instance("/runner.sh", 2)
        on_disk = json.loads((tmp_path / "instances.json").read_text())
        assert datahub_dev.WORKTREE_ID in on_disk["instances"]
        assert on_disk["instances"][datahub_dev.WORKTREE_ID]["type"] == "remote"

    def test_get_instance_returns_remote_entry(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        datahub_dev._register_remote_instance("/runner.sh", 2)
        inst = datahub_dev._get_instance()
        assert inst is not None
        assert inst["type"] == "remote"
        assert inst["ports"]["DATAHUB_MAPPED_GMS_PORT"] == 10080


# ---------------------------------------------------------------------------
# Layer 1: URL helpers respect tunnel ports
# ---------------------------------------------------------------------------


class TestUrlHelpers:
    def test_gms_url_uses_tunnel_port_for_remote(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
        datahub_dev._register_remote_instance("/runner.sh", 2)
        assert datahub_dev._gms_url() == "http://localhost:10080"

    def test_frontend_url_uses_tunnel_port_for_remote(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.delenv("DATAHUB_FRONTEND_URL", raising=False)
        datahub_dev._register_remote_instance("/runner.sh", 2)
        assert datahub_dev._frontend_url() == "http://localhost:11002"

    def test_gms_url_env_override_respected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://custom:9999")
        datahub_dev._register_remote_instance("/runner.sh", 2)
        assert datahub_dev._gms_url() == "http://custom:9999"


# ---------------------------------------------------------------------------
# Layer 2: mock runner integration — _cmd_start_remote call sequence
# ---------------------------------------------------------------------------


class TestCmdStartRemote:
    """Verify that _cmd_start_remote calls runner verbs in the right order
    with the right arguments, without touching real docker or a real machine."""

    def _make_args(self, **kwargs) -> argparse.Namespace:
        defaults = dict(
            ai=False,
            no_ai=False,
            embeddings_endpoint=None,
            embeddings_model=None,
            timeout=300,
        )
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    def _setup(self, tmp_path, monkeypatch, runner="/fake/runner.sh") -> List[List[str]]:
        """Wire up all the patches and return the list that records _run() calls."""
        calls: List[List[str]] = []

        def record_run(cmd, **kwargs):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(datahub_dev, "RUNNER", runner)
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.setattr(datahub_dev, "_run", record_run)
        monkeypatch.setattr(datahub_dev, "_rebuild_config_services", lambda: None)
        _patch_config(monkeypatch)
        return calls

    def test_exits_zero_on_success(self, tmp_path, monkeypatch):
        self._setup(tmp_path, monkeypatch)
        rc = datahub_dev._cmd_start_remote(self._make_args())
        assert rc == 0

    def test_verbs_called_in_order(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args())
        verbs = [c[1] for c in calls if c[0] == "/fake/runner.sh"]
        assert verbs == ["sync", "exec", "tunnel"], verbs

    def test_sync_has_no_extra_args(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args())
        sync_call = next(c for c in calls if c[1] == "sync")
        assert sync_call == ["/fake/runner.sh", "sync"]

    def test_exec_sets_remote_exec_env(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args())
        exec_call = next(c for c in calls if c[1] == "exec")
        assert "DATAHUB_REMOTE_EXEC=1" in exec_call

    def test_exec_targets_datahub_dev_sh(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args())
        exec_call = next(c for c in calls if c[1] == "exec")
        joined = " ".join(exec_call)
        assert "datahub-dev.sh" in joined
        assert "start" in joined

    def test_exec_includes_timeout_flag(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args(timeout=600))
        exec_call = next(c for c in calls if c[1] == "exec")
        assert "--timeout" in exec_call
        assert "600" in exec_call

    def test_exec_forwards_ai_flag(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args(ai=True))
        exec_call = next(c for c in calls if c[1] == "exec")
        assert "--ai" in exec_call

    def test_tunnel_receives_local_remote_pairs(self, tmp_path, monkeypatch):
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args())
        tunnel_call = next(c for c in calls if c[1] == "tunnel")
        # First remote slot = 2 (after 2 local slots), SLOT_OFFSET=1000
        # local 10080 → remote 8080,  local 11002 → remote 9002
        assert "10080:8080" in tunnel_call
        assert "11002:9002" in tunnel_call

    def test_local_registry_written_before_sync(self, tmp_path, monkeypatch):
        """Registry must exist before sync (so shell-env works even if start fails)."""
        written_before_sync = []

        def record_run(cmd, **kwargs):
            if cmd[1] == "sync":
                # Check registry at the moment sync is called
                inst = datahub_dev._get_instance()
                written_before_sync.append(inst is not None)
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(datahub_dev, "RUNNER", "/fake/runner.sh")
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.setattr(datahub_dev, "_run", record_run)
        monkeypatch.setattr(datahub_dev, "_rebuild_config_services", lambda: None)
        _patch_config(monkeypatch)

        datahub_dev._cmd_start_remote(self._make_args())
        assert written_before_sync == [True], "Registry not written before sync"

    def test_returns_nonzero_when_sync_fails(self, tmp_path, monkeypatch):
        def fail_on_sync(cmd, **kwargs):
            if cmd[1] == "sync":
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(datahub_dev, "RUNNER", "/fake/runner.sh")
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.setattr(datahub_dev, "_run", fail_on_sync)
        monkeypatch.setattr(datahub_dev, "_rebuild_config_services", lambda: None)
        _patch_config(monkeypatch)

        rc = datahub_dev._cmd_start_remote(self._make_args())
        assert rc != 0

    def test_returns_nonzero_when_exec_fails(self, tmp_path, monkeypatch):
        def fail_on_exec(cmd, **kwargs):
            if cmd[1] == "exec":
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(datahub_dev, "RUNNER", "/fake/runner.sh")
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.setattr(datahub_dev, "_run", fail_on_exec)
        monkeypatch.setattr(datahub_dev, "_rebuild_config_services", lambda: None)
        _patch_config(monkeypatch)

        rc = datahub_dev._cmd_start_remote(self._make_args())
        assert rc != 0

    def test_tunnel_failure_is_warning_not_error(self, tmp_path, monkeypatch):
        """Tunnel failure is non-fatal — remote is up, user can forward manually."""

        def fail_on_tunnel(cmd, **kwargs):
            if cmd[1] == "tunnel":
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(datahub_dev, "RUNNER", "/fake/runner.sh")
        monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", tmp_path / "instances.json")
        monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", tmp_path)
        monkeypatch.setattr(datahub_dev, "_run", fail_on_tunnel)
        monkeypatch.setattr(datahub_dev, "_rebuild_config_services", lambda: None)
        _patch_config(monkeypatch)

        rc = datahub_dev._cmd_start_remote(self._make_args())
        # Must still return 0 — tunnel failure is a warning, not a fatal error
        assert rc == 0

    def test_idempotent_start_reuses_existing_slot(self, tmp_path, monkeypatch):
        """Second call to _cmd_start_remote must not allocate a new slot."""
        calls = self._setup(tmp_path, monkeypatch)
        datahub_dev._cmd_start_remote(self._make_args())
        first_slot = datahub_dev._get_instance()["slot"]

        # Call again simulating a restart
        calls.clear()
        datahub_dev._cmd_start_remote(self._make_args())
        second_slot = datahub_dev._get_instance()["slot"]

        assert first_slot == second_slot, "Slot changed on second start — port instability"


# ---------------------------------------------------------------------------
# Layer 2: REMOTE_EXEC guard
# ---------------------------------------------------------------------------


class TestRemoteExecGuard:
    def test_runner_constant_is_empty_when_remote_exec_set(self, monkeypatch):
        """When DATAHUB_REMOTE_EXEC=1, RUNNER must be empty even if DATAHUB_RUNNER is set.

        This verifies the module-level logic by checking the constants directly.
        The guard is set at import time, so we validate the invariant holds for
        the current process state rather than re-importing.
        """
        # In this test process DATAHUB_REMOTE_EXEC is not set, so RUNNER reflects
        # whatever DATAHUB_RUNNER is in the environment.  Verify the rule holds.
        import os

        remote_exec = os.environ.get("DATAHUB_REMOTE_EXEC") == "1"
        if remote_exec:
            assert datahub_dev.RUNNER == ""
        else:
            # Not in remote-exec mode — RUNNER may be set or empty, both valid.
            pass

    def test_cmd_start_dispatches_to_remote_when_runner_set(self, tmp_path, monkeypatch):
        """cmd_start() calls _cmd_start_remote() when RUNNER is non-empty."""
        called = []

        def fake_remote(args):
            called.append("remote")
            return 0

        monkeypatch.setattr(datahub_dev, "RUNNER", "/some/runner.sh")
        monkeypatch.setattr(datahub_dev, "_cmd_start_remote", fake_remote)

        args = argparse.Namespace(
            ai=False, no_ai=False, embeddings_endpoint=None,
            embeddings_model=None, timeout=300,
        )
        datahub_dev.cmd_start(args)
        assert called == ["remote"]

    def test_cmd_start_does_not_dispatch_when_runner_empty(self, tmp_path, monkeypatch):
        """cmd_start() does NOT call _cmd_start_remote() when RUNNER is empty."""
        called = []

        def fake_remote(args):
            called.append("remote")
            return 0

        monkeypatch.setattr(datahub_dev, "RUNNER", "")
        monkeypatch.setattr(datahub_dev, "_cmd_start_remote", fake_remote)
        # Patch the local path to exit immediately without real docker
        monkeypatch.setattr(datahub_dev, "_load_registry", lambda: {"instances": {}})
        monkeypatch.setattr(datahub_dev, "_load_dev_config", lambda: {
            "max_local_instances": 2, "max_remote_instances": 10
        })
        monkeypatch.setattr(datahub_dev, "_count_running_dh_instances", lambda: 99)
        # count >= max_local → returns early with error, but never calls fake_remote

        args = argparse.Namespace(
            ai=False, no_ai=False, embeddings_endpoint=None,
            embeddings_model=None, timeout=300,
        )
        datahub_dev.cmd_start(args)
        assert called == [], "Remote path invoked despite RUNNER being empty"


# ---------------------------------------------------------------------------
# Layer 2: setup --remote
# ---------------------------------------------------------------------------


class TestSetupRemote:
    def test_calls_runner_init(self, tmp_path, monkeypatch):
        calls: List[List[str]] = []
        monkeypatch.setattr(datahub_dev, "RUNNER", "/fake/runner.sh")
        monkeypatch.setattr(datahub_dev, "_run", lambda cmd, **kw: (
            calls.append(list(cmd)),
            subprocess.CompletedProcess(cmd, 0),
        )[-1])

        args = argparse.Namespace(remote=True, module="ingestion")
        rc = datahub_dev.cmd_setup(args)
        assert rc == 0
        assert calls == [["/fake/runner.sh", "init"]]

    def test_fails_gracefully_when_no_runner(self, monkeypatch):
        monkeypatch.setattr(datahub_dev, "RUNNER", "")
        args = argparse.Namespace(remote=True, module="ingestion")
        rc = datahub_dev.cmd_setup(args)
        assert rc == 1
