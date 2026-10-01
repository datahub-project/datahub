"""Ref-label reuse and -sec fallback for the security-scan Linear sync."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))

MODULE_PATH = SCRIPTS / "security_scan_linear_sync.py"
spec = importlib.util.spec_from_file_location("security_scan_linear_sync", MODULE_PATH)
assert spec and spec.loader
sync = importlib.util.module_from_spec(spec)
sys.modules["security_scan_linear_sync"] = sync
spec.loader.exec_module(sync)


def test_resolve_ref_label_id_reuses_existing(monkeypatch):
    monkeypatch.setattr(
        sync, "_get_or_create_workspace_label_id_util", lambda *_args: "LBL-EXIST"
    )

    def fail_fallback(*_args):
        raise AssertionError("sec fallback should not run")

    monkeypatch.setattr(sync, "_get_or_create_fallback_ref_label_id", fail_fallback)
    assert sync._resolve_ref_label_id("k", "team-1", "v2.3.0-cloud") == "LBL-EXIST"


def test_resolve_ref_label_id_falls_back_to_sec_when_duplicate_stays_hidden(monkeypatch):
    def hidden(*_args):
        raise RuntimeError(
            "Linear GraphQL errors: [{'message': 'duplicate label name'}]"
        )

    monkeypatch.setattr(sync, "_get_or_create_workspace_label_id_util", hidden)
    monkeypatch.setattr(
        sync,
        "_get_or_create_fallback_ref_label_id",
        lambda _api_key, team_id, ref_name: f"{team_id}:{ref_name}-sec",
    )
    assert (
        sync._resolve_ref_label_id("k", "team-1", "v2.3.0-cloud")
        == "team-1:v2.3.0-cloud-sec"
    )


def test_resolve_ref_label_id_reraises_unrelated_errors(monkeypatch):
    def boom(*_args):
        raise RuntimeError("Linear HTTP 401: unauthorized")

    monkeypatch.setattr(sync, "_get_or_create_workspace_label_id_util", boom)
    try:
        sync._resolve_ref_label_id("k", "team-1", "v2.3.0-cloud")
    except RuntimeError as exc:
        assert "401" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_needs_sec_ref_label_for_group_and_team_mismatch():
    assert sync._needs_sec_ref_label(RuntimeError("not exclusive child labels"))
    assert sync._needs_sec_ref_label(
        RuntimeError("One or more labels are not available in this team.")
    )
    assert not sync._needs_sec_ref_label(RuntimeError("issueCreate failed: timeout"))
