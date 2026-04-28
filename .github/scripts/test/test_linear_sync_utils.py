"""Unit tests for linear_sync_utils module."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent.parent / "utils" / "linear_sync_utils.py"
spec = importlib.util.spec_from_file_location("linear_sync_utils", MODULE_PATH)
assert spec and spec.loader
utils = importlib.util.module_from_spec(spec)
sys.modules["linear_sync_utils"] = utils
spec.loader.exec_module(utils)


def test_dedupe_preserve_order():
    assert utils.dedupe_preserve_order(["a", "b", "a", "", "c", "b"]) == ["a", "b", "c"]


def test_linear_priority_and_due_date_mappings():
    assert utils.linear_priority_for_scan_severity("CRITICAL") == 1
    assert utils.linear_priority_for_scan_severity("HIGH") == 2
    assert utils.linear_priority_for_scan_severity("MEDIUM") == 3
    assert utils.linear_priority_for_scan_severity("LOW") == 4
    assert utils.linear_due_date_for_scan_severity("CRITICAL") is not None
    assert utils.linear_due_date_for_scan_severity("HIGH") is not None
    assert utils.linear_due_date_for_scan_severity("MEDIUM") is not None
    assert utils.linear_due_date_for_scan_severity("LOW") is not None


def test_unique_repo_basenames_from_occurrences():
    occ = [
        ("acryldata/datahub-gms:tag", "x", "x", {}),
        ("acryldata/datahub-gms:tag", "x", "x", {}),
        ("acryldata/datahub-actions:tag-slim", "x", "x", {}),
    ]
    assert utils.unique_repo_basenames_from_occurrences(occ) == [
        "datahub-gms",
        "datahub-actions",
    ]


def test_repo_label_ids_for_occurrences_uses_repo_map():
    repo_map = {"datahub-gms": "label-gms", "datahub-actions": "label-actions"}
    occ = [
        ("acryldata/datahub-gms:tag", "x", "x", {}),
        ("acryldata/datahub-actions:tag-slim", "x", "x", {}),
        ("acryldata/datahub-gms:tag", "x", "x", {}),
    ]
    assert utils.repo_label_ids_for_occurrences(repo_map, occ) == [
        "label-gms",
        "label-actions",
    ]


def test_resolve_issue_create_state_id_uses_explicit_id_without_graphql():
    assert (
        utils.resolve_issue_create_state_id(
            "key", "team-1", explicit_state_id="explicit-state-id"
        )
        == "explicit-state-id"
    )


def test_issue_graphql_label_ids_from_mocked_graphql(monkeypatch):
    def fake_graphql(_api_key, _query, _variables):
        return {"issue": {"labels": {"nodes": [{"id": "L1"}, {"id": "L2"}]}}}

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    assert utils.issue_graphql_label_ids("k", "I1") == ["L1", "L2"]


def test_random_label_color_hex_format():
    color = utils.random_label_color_hex()
    assert re.fullmatch(r"#[0-9a-f]{6}", color)


def test_find_team_label_by_name_from_mocked_graphql(monkeypatch):
    def fake_graphql(_api_key, _query, variables):
        assert variables["teamId"] == "team-1"
        assert variables["name"] == "main"
        return {"issueLabels": {"nodes": [{"id": "LBL-1", "name": "main"}]}}

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    assert utils.find_team_label_by_name("k", "team-1", "main") == "LBL-1"


def test_create_team_label_includes_team_id_and_color(monkeypatch):
    seen: dict[str, object] = {}

    def fake_graphql(_api_key, _query, variables):
        seen["vars"] = variables
        return {"issueLabelCreate": {"success": True, "issueLabel": {"id": "LBL-NEW"}}}

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    label_id = utils.create_team_label("k", "team-7", "release-1", "#12abef")
    assert label_id == "LBL-NEW"
    assert seen["vars"] == {
        "input": {"teamId": "team-7", "name": "release-1", "color": "#12abef"}
    }


def test_get_or_create_team_label_id_reuses_existing(monkeypatch):
    monkeypatch.setattr(utils, "find_team_label_by_name", lambda *_args, **_kwargs: "LBL-EXIST")
    called = {"create": False}

    def fake_create(*_args, **_kwargs):
        called["create"] = True
        return "LBL-NEW"

    monkeypatch.setattr(utils, "create_team_label", fake_create)
    assert utils.get_or_create_team_label_id("k", "team-1", "main") == "LBL-EXIST"
    assert called["create"] is False


def test_get_or_create_team_label_id_creates_when_missing(monkeypatch):
    monkeypatch.setattr(utils, "find_team_label_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(utils, "random_label_color_hex", lambda: "#00ff00")

    def fake_create(_api_key, _team_id, _label_name, color_hex):
        assert color_hex == "#00ff00"
        return "LBL-NEW"

    monkeypatch.setattr(utils, "create_team_label", fake_create)
    assert utils.get_or_create_team_label_id("k", "team-1", "main") == "LBL-NEW"


def test_attach_file_to_issue_uploads_then_attaches(monkeypatch, tmp_path: Path):
    p = tmp_path / "trivy-x.json"
    p.write_text('{"ok":true}', encoding="utf-8")
    monkeypatch.setattr(
        utils,
        "request_file_upload",
        lambda **_kwargs: ("https://upload.example", "https://asset.example/file", {"x-a": "1"}),
    )
    calls: dict[str, object] = {}

    def fake_upload(
        upload_url: str,
        upload_headers: dict[str, str] | None,
        payload: bytes,
        *,
        content_type: str,
    ) -> None:
        calls["upload"] = (upload_url, upload_headers, payload, content_type)

    monkeypatch.setattr(utils, "upload_file_to_signed_url", fake_upload)
    monkeypatch.setattr(
        utils,
        "create_issue_attachment",
        lambda _api_key, _issue_id, title, url: f"{title}|{url}",
    )

    out = utils.attach_file_to_issue("k", "ISSUE-1", p, "Raw scan report: trivy-x.json")
    assert out == "Raw scan report: trivy-x.json|https://asset.example/file"
    assert calls["upload"][0] == "https://upload.example"
    assert calls["upload"][1] == {"x-a": "1"}
    assert calls["upload"][2] == b'{"ok":true}'
    assert calls["upload"][3] == "application/json"


def test_merge_gcs_put_headers_adds_content_type_when_absent():
    m = utils._merge_gcs_put_headers({}, "application/json")
    assert m == {"Content-Type": "application/json"}


def test_merge_gcs_put_headers_preserves_linear_headers():
    m = utils._merge_gcs_put_headers(
        {"Content-Type": "application/json", "X-Test": "1"},
        "text/plain",
    )
    assert m["Content-Type"] == "application/json"
    assert m["X-Test"] == "1"


def test_upload_file_merges_content_type_for_gcs_put(monkeypatch):
    got: dict[str, object] = {}

    def fake_put(
        url: str,
        headers: dict[str, str] | None = None,
        data: object = None,
        timeout: int = 0,
    ) -> object:
        got["headers"] = dict(headers or {})
        got["data"] = data
        return type("R", (), {"ok": True, "status_code": 200, "text": ""})()

    monkeypatch.setattr(utils.requests, "put", fake_put)
    utils.upload_file_to_signed_url(
        "https://example.com/put", {}, b"ab", content_type="application/json"
    )
    assert got["headers"]["Content-Type"] == "application/json"
    assert got["data"] == b"ab"
