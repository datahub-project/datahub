"""Unit tests for linear_sync_utils module."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

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


def test_issue_labels_with_parents_from_mocked_graphql(monkeypatch):
    def fake_graphql(_api_key, _query, _variables):
        return {
            "issue": {
                "labels": {
                    "nodes": [
                        {"id": "L1", "parent": {"id": "G1"}},
                        {"id": "L2", "parent": None},
                    ]
                }
            }
        }

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    assert utils.issue_labels_with_parents("k", "I1") == [
        utils.IssueLabelRef("L1", "G1"),
        utils.IssueLabelRef("L2", None),
    ]


def test_label_ids_replacing_group_sibling_drops_only_same_group_child():
    current = [
        utils.IssueLabelRef("static", None),
        utils.IssueLabelRef("old-ref", "scan-group"),
        utils.IssueLabelRef("release", "release-group"),
    ]
    assert utils.label_ids_replacing_group_sibling(current, "new-ref", "scan-group") == [
        "static",
        "release",
        "new-ref",
    ]
    # Re-applying the same child is a no-op in content.
    assert utils.label_ids_replacing_group_sibling(current, "old-ref", "scan-group") == [
        "static",
        "old-ref",
        "release",
    ]
    # An ungrouped reused label is added without dropping either group's child.
    assert utils.label_ids_replacing_group_sibling(current, "legacy", None) == [
        "static",
        "old-ref",
        "release",
        "legacy",
    ]


def test_random_label_color_hex_format():
    color = utils.random_label_color_hex()
    assert re.fullmatch(r"#[0-9a-f]{6}", color)


def test_find_label_group_id_scopes_to_workspace_or_team(monkeypatch):
    seen: list[tuple[str, dict]] = []

    def fake_graphql(_api_key, query, variables):
        seen.append((query, variables))
        return {"issueLabels": {"nodes": [{"id": "GRP"}]}}

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    assert utils.find_label_group_id("k", "OSS Release") == "GRP"
    assert utils.find_label_group_id("k", "Security Scan", "team-1") == "GRP"
    ws_query, ws_vars = seen[0]
    team_query, team_vars = seen[1]
    assert "team: { null: true }" in ws_query and ws_vars == {"name": "OSS Release"}
    assert "team: { id: { eq: $teamId } }" in team_query
    assert team_vars == {"name": "Security Scan", "teamId": "team-1"}


def test_create_group_child_label_sets_parent_and_optional_team(monkeypatch):
    seen: list[dict] = []

    def fake_graphql(_api_key, _query, variables):
        seen.append(variables)
        return {"issueLabelCreate": {"success": True, "issueLabel": {"id": "LBL"}}}

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    utils.create_group_child_label("k", "GRP", "v1.0.0", "#abcdef")
    utils.create_group_child_label("k", "GRP", "sha-abc", "#abcdef", "team-1")
    assert seen[0] == {"input": {"name": "v1.0.0", "parentId": "GRP", "color": "#abcdef"}}
    assert seen[1]["input"]["teamId"] == "team-1"
    assert seen[1]["input"]["parentId"] == "GRP"


def test_get_or_create_label_group_id_respects_create_if_missing(monkeypatch):
    monkeypatch.setattr(utils, "find_label_group_id", lambda *_a, **_k: None)
    monkeypatch.setattr(utils, "create_label_group", lambda *_a, **_k: "NEW-GRP")
    assert (
        utils.get_or_create_label_group_id("k", "Security Scan", "team-1", create_if_missing=True)
        == "NEW-GRP"
    )
    with pytest.raises(RuntimeError, match="OSS Release"):
        utils.get_or_create_label_group_id("k", "OSS Release", create_if_missing=False)


def test_get_or_create_label_group_id_recovers_after_duplicate(monkeypatch):
    calls = {"n": 0}

    def find_twice(*_args, **_kwargs):
        calls["n"] += 1
        return "GRP-RACE" if calls["n"] >= 2 else None

    monkeypatch.setattr(utils, "find_label_group_id", find_twice)

    def fake_create(*_args, **_kwargs):
        raise RuntimeError("Linear GraphQL errors: [{'message': 'duplicate label name'}]")

    monkeypatch.setattr(utils, "create_label_group", fake_create)
    assert (
        utils.get_or_create_label_group_id("k", "Security Scan", "team-1", create_if_missing=True)
        == "GRP-RACE"
    )
    assert calls["n"] == 2


def test_get_or_create_group_child_label_id_recovers_after_duplicate(monkeypatch):
    calls = {"n": 0}

    def find_twice(_api_key, _group_id, _name):
        calls["n"] += 1
        return "LBL-RACE" if calls["n"] >= 2 else None

    monkeypatch.setattr(utils, "find_group_child_label_id", find_twice)
    monkeypatch.setattr(utils, "find_label_id_by_name", lambda *_a, **_k: None)

    def fake_create(*_args, **_kwargs):
        raise RuntimeError("Linear GraphQL errors: [{'message': 'duplicate label name'}]")

    monkeypatch.setattr(utils, "create_group_child_label", fake_create)
    assert utils.get_or_create_group_child_label_id("k", "GRP", "main") == utils.ResolvedLabel(
        "LBL-RACE", "GRP"
    )
    assert calls["n"] == 2


def test_get_or_create_group_child_label_id_reuses_name_taken_elsewhere(monkeypatch):
    monkeypatch.setattr(utils, "find_group_child_label_id", lambda *_a, **_k: None)
    monkeypatch.setattr(
        utils,
        "find_label_id_by_name",
        lambda *_a, **_k: utils.ResolvedLabel("LBL-EXISTING", "OTHER"),
    )

    def fake_create(*_args, **_kwargs):
        raise AssertionError("create should not run when the label name already exists")

    monkeypatch.setattr(utils, "create_group_child_label", fake_create)
    assert utils.get_or_create_group_child_label_id("k", "GRP", "v1.6.0.3") == utils.ResolvedLabel(
        "LBL-EXISTING", "OTHER"
    )


def test_get_or_create_group_child_label_id_reraises_when_name_still_missing(monkeypatch):
    monkeypatch.setattr(utils, "find_group_child_label_id", lambda *_a, **_k: None)
    monkeypatch.setattr(utils, "find_label_id_by_name", lambda *_a, **_k: None)

    def fake_create(*_args, **_kwargs):
        raise RuntimeError("Linear GraphQL errors: [{'message': 'duplicate label name'}]")

    monkeypatch.setattr(utils, "create_group_child_label", fake_create)
    with pytest.raises(RuntimeError, match="duplicate label name"):
        utils.get_or_create_group_child_label_id("k", "GRP", "v2.3.0-cloud")


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


def test_get_issue_display_and_identifier_url(monkeypatch):
    def fake_graphql(
        api_key: str, query: str, variables: dict | None = None
    ) -> dict[str, object]:
        assert variables == {"id": "issue-uuid"}
        return {
            "issue": {
                "identifier": "ENG-9",
                "url": "https://linear.example/issue/9",
                "title": "CVE fix",
            }
        }

    monkeypatch.setattr(utils, "graphql", fake_graphql)
    d = utils.get_issue_display("key", "issue-uuid")
    assert d == utils.IssueDisplay(
        "ENG-9", "https://linear.example/issue/9", "CVE fix"
    )
    assert utils.get_issue_identifier_url("key", "issue-uuid") == (
        "ENG-9",
        "https://linear.example/issue/9",
    )
