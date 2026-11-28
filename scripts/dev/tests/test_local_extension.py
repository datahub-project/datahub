"""Extension reservations, persistence, service environments and legacy isolation."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

# Console-script pytest does not put the repository root on the import path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import datahub_dev as dev
import datahub_dev_local as local
from local.inventory import run as run_inventory


def test_launcher_protects_connection_settings_and_optional_process_exit(
    tmp_path: Path,
) -> None:
    path = Path(dev.REPO_ROOT) / "metadata-service/gms-lite/bin/gms-lite-stack.py"
    spec = importlib.util.spec_from_file_location("extension_stack_test", path)
    assert spec and spec.loader
    stack = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stack)
    env = stack.instance_env(
        {
            "EBEAN_DATASOURCE_URL": "bad",
            "ELASTICSEARCH_URI": "bad",
            "METADATA_SERVICE_AUTH_ENABLED": "false",
        },
        "gms-0",
        8080,
        9200,
        tmp_path,
        stack.load_credentials(tmp_path),
        True,
        "2",
        9002,
    )
    assert env["EBEAN_DATASOURCE_URL"].startswith("jdbc:h2:file:")
    assert env["ELASTICSEARCH_URI"] == ""
    assert env["METADATA_SERVICE_AUTH_ENABLED"] == "true"
    processes = stack.Processes()
    try:
        child = processes.start(
            [sys.executable, "-c", "raise SystemExit(7)"],
            dict(os.environ),
            tmp_path / "optional.log",
            required=False,
        )
        child.wait(timeout=5)
        processes.check()
        required = processes.start(
            [sys.executable, "-c", "raise SystemExit(8)"],
            dict(os.environ),
            tmp_path / "required.log",
        )
        required.wait(timeout=5)
        with pytest.raises(SystemExit):
            processes.check()
    finally:
        processes.stop()


def test_inventory_repair_requires_owned_record_and_unclaimed_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = {
        "mode": "local",
        "local_owner": local.OWNER,
        "slot": 0,
        "opensearch_dir": str(local.shared_opensearch_dir("2")),
        "opensearch_version": "2",
        "worktree_path": str(tmp_path),
        "ports": local.local_ports(0),
    }
    local.write_instance_record("abc123", record)
    (local.LOCAL_DATA_ROOT / "unknown-directory").mkdir()
    monkeypatch.setattr(
        local,
        "opensearch_manager",
        lambda major: Mock(status=lambda: {"healthy": False}),
    )
    args = argparse.Namespace(
        command="instances", instances_command="repair", yes=False
    )
    assert run_inventory(args, dev) == 0
    assert dev._load_registry()["instances"] == {}
    args.yes = True
    dev._save_registry({"instances": {"def456": {"slot": 0, "ports": {"gms": 18080}}}})
    with pytest.raises(RuntimeError):
        run_inventory(args, dev)
    assert "abc123" not in dev._load_registry()["instances"]
    dev._save_registry({"instances": {}})
    assert run_inventory(args, dev) == 0
    assert dev._load_registry()["instances"]["abc123"] == record
