"""Unit tests for security-relevant guards in datahub_dev.py."""

import argparse
import importlib
import json
import sys
import time
from pathlib import Path

import pytest

# Add the scripts/dev directory to the path so we can import datahub_dev
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import datahub_dev


def _make_set_args(assignment: str) -> argparse.Namespace:
    return argparse.Namespace(assignment=assignment)


def test_env_set_rejects_newline_in_key(tmp_path, monkeypatch):
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", tmp_path / "test.env")
    result = datahub_dev.cmd_env_set(_make_set_args("BAD\nKEY=value"))
    assert result == 1


def test_env_set_rejects_newline_in_value(tmp_path, monkeypatch):
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", tmp_path / "test.env")
    result = datahub_dev.cmd_env_set(_make_set_args("MY_VAR=bad\nvalue"))
    assert result == 1


@pytest.mark.parametrize("bad_key", ["bad key", "#bad", "1INVALID", ""])
def test_env_set_rejects_invalid_key_chars(tmp_path, monkeypatch, bad_key):
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", tmp_path / "test.env")
    result = datahub_dev.cmd_env_set(_make_set_args(f"{bad_key}=value"))
    assert result == 1


def test_env_set_accepts_valid_key(tmp_path, monkeypatch):
    env_file = tmp_path / "test.env"
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", env_file)
    result = datahub_dev.cmd_env_set(_make_set_args("MY_VAR_123=somevalue"))
    assert result == 0
    assert "MY_VAR_123=somevalue" in env_file.read_text()


def test_load_flag_classification_handles_corrupt_json(tmp_path, monkeypatch):
    corrupt_file = tmp_path / "flag-classification.json"
    corrupt_file.write_text("{this is not valid json")
    monkeypatch.setattr(datahub_dev, "GENERATED_MANIFEST", corrupt_file)
    result = datahub_dev._load_flag_classification()
    assert result == {"dynamic": {}, "static": {}}


def test_plugin_loading_extend_config(tmp_path, monkeypatch):
    """A valid ext file's extend_config() is called and augments the config."""
    ext_file = tmp_path / "datahub_dev_ext.py"
    ext_file.write_text(
        "from datahub_dev import DevToolingConfig, ServiceConfig\n"
        "def extend_config(config: DevToolingConfig) -> DevToolingConfig:\n"
        "    config.rebuild_module_aliases['integrations'] = 'datahub-integrations-service'\n"
        "    config.services.append(ServiceConfig('integrations', 'http://localhost:9003/health', required=False))\n"
        "    return config\n"
    )
    # Make _load_config() look for the ext file in tmp_path
    monkeypatch.setattr(datahub_dev, "__file__", str(tmp_path / "datahub_dev.py"))
    config = datahub_dev._load_config()
    assert "integrations" in config.rebuild_module_aliases
    assert config.rebuild_module_aliases["integrations"] == "datahub-integrations-service"
    assert any(svc.name == "integrations" for svc in config.services)


def test_env_list_pending_restart_no_sentinel(tmp_path, monkeypatch, capsys):
    """pending_restart=true when vars exist but sentinel has never been written."""
    env_file = tmp_path / "test.env"
    sentinel = tmp_path / "test.applied.env"
    env_file.write_text("FOO=bar\n")
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", env_file)
    monkeypatch.setattr(datahub_dev, "DEV_ENV_SENTINEL", sentinel)
    monkeypatch.setattr(datahub_dev, "REPO_ROOT", tmp_path)

    result = datahub_dev.cmd_env_list(argparse.Namespace())
    assert result == 0
    out = json.loads(capsys.readouterr().out)
    assert out["pending_restart"] is True
    assert out["vars"] == {"FOO": "bar"}


def test_env_list_pending_restart_false_after_sentinel(tmp_path, monkeypatch, capsys):
    """pending_restart=false when sentinel is newer than env file."""
    env_file = tmp_path / "test.env"
    sentinel = tmp_path / "test.applied.env"
    env_file.write_text("FOO=bar\n")
    # Write sentinel after env file so its mtime is newer
    sentinel.write_text(str(time.time()))
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", env_file)
    monkeypatch.setattr(datahub_dev, "DEV_ENV_SENTINEL", sentinel)
    monkeypatch.setattr(datahub_dev, "REPO_ROOT", tmp_path)

    result = datahub_dev.cmd_env_list(argparse.Namespace())
    assert result == 0
    out = json.loads(capsys.readouterr().out)
    assert out["pending_restart"] is False
    assert out["vars"] == {"FOO": "bar"}


def test_env_list_pending_restart_true_after_newer_env(tmp_path, monkeypatch, capsys):
    """pending_restart=true when env file is newer than sentinel."""
    import os

    sentinel = tmp_path / "test.applied.env"
    env_file = tmp_path / "test.env"
    # Write sentinel first (older mtime), then env file (newer mtime)
    sentinel.write_text("0")
    env_file.write_text("FOO=bar\n")
    # Ensure env file mtime > sentinel mtime
    os.utime(sentinel, (0, 0))

    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", env_file)
    monkeypatch.setattr(datahub_dev, "DEV_ENV_SENTINEL", sentinel)
    monkeypatch.setattr(datahub_dev, "REPO_ROOT", tmp_path)

    result = datahub_dev.cmd_env_list(argparse.Namespace())
    assert result == 0
    out = json.loads(capsys.readouterr().out)
    assert out["pending_restart"] is True


def test_env_list_empty_no_pending(tmp_path, monkeypatch, capsys):
    """pending_restart=false when no vars are set (nothing to apply)."""
    env_file = tmp_path / "test.env"
    sentinel = tmp_path / "test.applied.env"
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", env_file)
    monkeypatch.setattr(datahub_dev, "DEV_ENV_SENTINEL", sentinel)
    monkeypatch.setattr(datahub_dev, "REPO_ROOT", tmp_path)

    result = datahub_dev.cmd_env_list(argparse.Namespace())
    assert result == 0
    out = json.loads(capsys.readouterr().out)
    assert out["pending_restart"] is False
    assert out["vars"] == {}


def test_env_clean_removes_sentinel(tmp_path, monkeypatch):
    """env clean removes sentinel alongside stale env file."""
    # Create a stale env file + sentinel for a branch that doesn't exist
    stale_env = tmp_path / "datahub-dev-old-branch.env"
    stale_sentinel = tmp_path / "datahub-dev-old-branch.applied.env"
    stale_env.write_text("OLD=1\n")
    stale_sentinel.write_text("12345")

    monkeypatch.setattr(datahub_dev, "DOCKER_DIR", tmp_path)
    monkeypatch.setattr(datahub_dev, "DEV_ENV_FILE", tmp_path / "datahub-dev-current.env")
    # No local branches → only 'local' fallback is active
    monkeypatch.setattr(
        datahub_dev,
        "_run",
        lambda *a, **kw: type("R", (), {"returncode": 0, "stdout": ""})(),
    )

    result = datahub_dev.cmd_env_clean(argparse.Namespace(dry_run=False))
    assert result == 0
    assert not stale_env.exists()
    assert not stale_sentinel.exists()


def test_plugin_loading_bad_ext_does_not_crash(tmp_path, monkeypatch):
    """A broken ext file is silently skipped; default config is returned."""
    ext_file = tmp_path / "datahub_dev_ext.py"
    ext_file.write_text("this is not valid python !!!\n")
    monkeypatch.setattr(datahub_dev, "__file__", str(tmp_path / "datahub_dev.py"))
    # Should not raise — bad ext file produces a warning but returns default config
    config = datahub_dev._load_config()
    assert "gms" in config.rebuild_module_aliases
    assert any(svc.name == "gms" for svc in config.services)
