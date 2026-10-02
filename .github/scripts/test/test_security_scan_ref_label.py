"""Ref label group routing for the security-scan Linear sync."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))

MODULE_PATH = SCRIPTS / "security_scan_linear_sync.py"
spec = importlib.util.spec_from_file_location("security_scan_linear_sync", MODULE_PATH)
assert spec and spec.loader
sync = importlib.util.module_from_spec(spec)
sys.modules["security_scan_linear_sync"] = sync
spec.loader.exec_module(sync)


def _stub_groups(monkeypatch, seen: dict):
    def fake_group(_api_key, group_name, team_id=None, *, create_if_missing):
        seen["group"] = (group_name, team_id, create_if_missing)
        return f"group:{group_name}"

    def fake_child(_api_key, group_id, label_name, team_id=None):
        seen["child"] = (group_id, label_name, team_id)
        return sync.ResolvedLabel(f"label:{label_name}", group_id)

    monkeypatch.setattr(sync, "_get_or_create_label_group_id_util", fake_group)
    monkeypatch.setattr(sync, "_get_or_create_group_child_label_id_util", fake_child)


@pytest.mark.parametrize(
    ("ref_name", "group"),
    [
        ("v2.3.0-cloud", "Saas Release"),
        ("v2.3.0.1-cloud", "Saas Release"),
        ("v2.3.0rc1-cloud", "Saas Release"),
        ("v2.3.0-rc1-cloud", "Saas Release"),
        ("v1.7.0", "OSS Release"),
        ("v1.7.0.1", "OSS Release"),
        ("v1.7.0rc1", "OSS Release"),
        ("v1.7.0-rc1", "OSS Release"),
    ],
)
def test_release_group_follows_cloud_suffix(ref_name, group):
    assert sync.release_label_group_for_tag(ref_name) == group
    assert sync.is_semantic_version_tag(ref_name)


@pytest.mark.parametrize(
    "ref_name", ["sha-abc1234", "master", "acryl-main", "v1.0.0-cx42-06"]
)
def test_non_semantic_has_no_release_group(ref_name):
    assert not sync.is_semantic_version_tag(ref_name)
    assert sync.release_label_group_for_tag(ref_name) is None


def test_cloud_release_tag_uses_saas_group(monkeypatch):
    seen: dict = {}
    _stub_groups(monkeypatch, seen)
    out = sync._resolve_ref_label("k", "team-1", "v2.3.0-cloud")
    assert out == sync.RefLabel("label:v2.3.0-cloud", "group:Saas Release")
    assert seen["group"] == ("Saas Release", None, False)
    assert seen["child"] == ("group:Saas Release", "v2.3.0-cloud", None)


@pytest.mark.parametrize(
    ("ref_name", "label_name"),
    [
        ("v1.7.0", "OSS v1.7.0"),
        ("v1.7.0.1", "OSS v1.7.0.1"),
        ("v1.7.0rc1", "OSS v1.7.0rc1"),
        ("v1.7.0-rc1", "OSS v1.7.0-rc1"),
        ("v2.3.0-cloud", "v2.3.0-cloud"),
        ("v2.3.0rc1-cloud", "v2.3.0rc1-cloud"),
        ("master", "master"),
        ("sha-abc1234", "sha-abc1234"),
    ],
)
def test_ref_label_name(ref_name, label_name):
    assert sync.ref_label_name(ref_name) == label_name


def test_oss_release_tag_uses_oss_group(monkeypatch):
    seen: dict = {}
    _stub_groups(monkeypatch, seen)
    out = sync._resolve_ref_label("k", "team-1", "v1.7.0.1")
    assert out == sync.RefLabel("label:OSS v1.7.0.1", "group:OSS Release")
    assert seen["group"] == ("OSS Release", None, False)
    assert seen["child"] == ("group:OSS Release", "OSS v1.7.0.1", None)


def test_rc_uses_release_group_not_security_scan(monkeypatch):
    seen: dict = {}
    _stub_groups(monkeypatch, seen)
    out = sync._resolve_ref_label("k", "team-1", "v2.3.0rc1-cloud")
    assert out == sync.RefLabel("label:v2.3.0rc1-cloud", "group:Saas Release")
    assert seen["group"] == ("Saas Release", None, False)
    assert seen["child"] == ("group:Saas Release", "v2.3.0rc1-cloud", None)


def test_version_named_branch_stays_in_security_scan(monkeypatch):
    seen: dict = {}
    _stub_groups(monkeypatch, seen)
    out = sync._resolve_ref_label("k", "team-1", "v1.7.0.1", "branch")
    assert out == sync.RefLabel("label:v1.7.0.1", "group:Security Scan")
    assert seen["group"] == ("Security Scan", "team-1", True)
    assert seen["child"] == ("group:Security Scan", "v1.7.0.1", "team-1")
    assert sync.ref_label_name("v1.7.0.1", "branch") == "v1.7.0.1"


@pytest.mark.parametrize("ref_name", ["master", "acryl-main", "sha-abc1234"])
def test_non_semantic_uses_security_scan_group(monkeypatch, ref_name):
    seen: dict = {}
    _stub_groups(monkeypatch, seen)
    out = sync._resolve_ref_label("k", "team-1", ref_name)
    assert out == sync.RefLabel(f"label:{ref_name}", "group:Security Scan")
    assert seen["group"] == ("Security Scan", "team-1", True)


def test_missing_release_group_is_not_created(monkeypatch):
    def fail_group(_api_key, group_name, team_id=None, *, create_if_missing):
        assert create_if_missing is False
        raise RuntimeError(
            f"Linear label group {group_name!r} not found in the workspace"
        )

    monkeypatch.setattr(sync, "_get_or_create_label_group_id_util", fail_group)
    with pytest.raises(RuntimeError, match="OSS Release"):
        sync._resolve_ref_label("k", "team-1", "v1.7.0")
