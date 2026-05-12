"""Unit tests for security_scan_utils module."""

from __future__ import annotations

import json
import importlib.util
import os
import sys
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parent.parent / "utils" / "security_scan_utils.py"
)
spec = importlib.util.spec_from_file_location("security_scan_utils", MODULE_PATH)
assert spec and spec.loader
utils = importlib.util.module_from_spec(spec)
sys.modules["security_scan_utils"] = utils
spec.loader.exec_module(utils)


def test_split_image_ref_and_scope_label_with_variant(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATAHUB_VARIANT_TAG_SUFFIXES", "slim,locked")
    utils._variant_suffix_labels_from_env.cache_clear()
    repo, tag = utils.split_image_ref("acryldata/datahub-actions:v1.2.3-locked")
    assert repo == "datahub-actions"
    assert tag == "v1.2.3-locked"
    assert utils.repo_scope_ticket_label("acryldata/datahub-actions:v1.2.3-locked") == (
        "datahub-actions-locked"
    )


def test_split_image_ref_digest_and_primary_scope(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATAHUB_VARIANT_TAG_SUFFIXES", "slim,locked")
    utils._variant_suffix_labels_from_env.cache_clear()
    repo, tag = utils.split_image_ref(
        "acryldata/datahub-gms@sha256:deadbeef"
    )
    assert repo == "datahub-gms"
    assert tag == ""
    assert utils.repo_scope_ticket_label("acryldata/datahub-gms:abc123") == "datahub-gms"


def test_variant_label_uses_configured_suffixes(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATAHUB_VARIANT_TAG_SUFFIXES", "foo,bar")
    utils._variant_suffix_labels_from_env.cache_clear()
    assert utils.variant_label_from_tag("v1.0.0-bar") == "bar"
    assert utils.variant_label_from_tag("v1.0.0-slim") == ""


def test_parse_trivy_reports_groups_by_cve_and_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATAHUB_VARIANT_TAG_SUFFIXES", "slim,locked")
    utils._variant_suffix_labels_from_env.cache_clear()
    report = {
        "ArtifactName": "acryldata/datahub-actions:tag-slim",
        "Results": [
            {
                "Target": "acryldata/datahub-actions:tag-slim",
                "Class": "os-pkgs",
                "Type": "alpine",
                "Vulnerabilities": [
                    {"VulnerabilityID": "CVE-123", "PkgName": "openssl", "Severity": "HIGH"}
                ],
            }
        ],
    }
    p = tmp_path / "trivy.json"
    p.write_text(json.dumps(report), encoding="utf-8")
    groups = utils.parse_trivy_reports([p])
    assert len(groups) == 1
    key = next(iter(groups.keys()))
    assert key.endswith("\x1fdatahub-actions-slim")


def test_parse_trivy_grype_merged_prefers_trivy_and_records_scanners(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATAHUB_VARIANT_TAG_SUFFIXES", "slim,locked")
    utils._variant_suffix_labels_from_env.cache_clear()
    trivy_doc = {
        "ArtifactName": "acryldata/datahub-actions:tag-locked",
        "Results": [
            {
                "Target": "acryldata/datahub-actions:tag-locked",
                "Class": "os-pkgs",
                "Type": "alpine",
                "Vulnerabilities": [
                    {"VulnerabilityID": "CVE-999", "PkgName": "libz", "Severity": "CRITICAL"}
                ],
            }
        ],
    }
    grype_doc = {
        "source": {"target": {"userInput": "acryldata/datahub-actions:tag-locked"}},
        "matches": [
            {
                "vulnerability": {
                    "id": "CVE-999",
                    "severity": "High",
                    "description": "desc",
                    "urls": ["https://example.com/cve-999"],
                },
                "artifact": {"name": "libz", "version": "1.2.3", "type": "apk"},
            }
        ],
    }
    tp = tmp_path / "trivy.json"
    gp = tmp_path / "grype.json"
    tp.write_text(json.dumps(trivy_doc), encoding="utf-8")
    gp.write_text(json.dumps(grype_doc), encoding="utf-8")

    groups = utils.parse_trivy_grype_merged([tp, gp])
    assert len(groups) == 1
    occ = next(iter(groups.values()))
    assert len(occ) == 1
    vuln = occ[0][3]
    assert vuln["_datahub_scanners"] == ["trivy", "grype"]


def test_parse_trivy_grype_merged_filters_out_non_high_critical_grype(tmp_path: Path):
    os_env_key = "DATAHUB_SCAN_SEVERITIES"
    old = os.environ.get(os_env_key)
    os.environ[os_env_key] = "CRITICAL,HIGH"
    utils._scan_severity_allowlist_from_env.cache_clear()
    grype_doc = {
        "source": {"target": {"userInput": "acryldata/datahub-actions:tag"}},
        "matches": [
            {
                "vulnerability": {"id": "CVE-LOW", "severity": "Low"},
                "artifact": {"name": "pkg", "version": "1.0.0", "type": "apk"},
            }
        ],
    }
    gp = tmp_path / "grype.json"
    gp.write_text(json.dumps(grype_doc), encoding="utf-8")
    try:
        assert utils.parse_trivy_grype_merged([gp]) == {}
    finally:
        if old is None:
            os.environ.pop(os_env_key, None)
        else:
            os.environ[os_env_key] = old
        utils._scan_severity_allowlist_from_env.cache_clear()


def test_parse_trivy_grype_merged_allows_low_when_no_severity_filter(tmp_path: Path):
    os.environ.pop("DATAHUB_SCAN_SEVERITIES", None)
    utils._scan_severity_allowlist_from_env.cache_clear()
    grype_doc = {
        "source": {"target": {"userInput": "acryldata/datahub-actions:tag"}},
        "matches": [
            {
                "vulnerability": {"id": "CVE-LOW", "severity": "Low"},
                "artifact": {"name": "pkg", "version": "1.0.0", "type": "apk"},
            }
        ],
    }
    gp = tmp_path / "grype.json"
    gp.write_text(json.dumps(grype_doc), encoding="utf-8")
    groups = utils.parse_trivy_grype_merged([gp])
    assert len(groups) == 1


def test_load_json_files_raises_when_file_missing(tmp_path: Path):
    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        utils.load_json_files([missing])


def test_linear_issue_title_includes_repo_scope():
    vuln = {"PkgName": "openssl"}
    title = utils.linear_issue_title(
        "CVE-2026-1111", vuln, "datahub-gms-slim", scanner="trivy"
    )
    assert title == "[datahub-gms-slim] CVE-2026-1111 in openssl"


def test_merge_refs_comment_is_deduplicated():
    first = utils.merge_refs_comment(
        None,
        "branch",
        "main",
        "abc1234",
        "https://github.com/acryldata/datahub/actions/runs/1",
    )
    second = utils.merge_refs_comment(
        first,
        "branch",
        "main",
        "abc1234",
        "https://github.com/acryldata/datahub/actions/runs/1",
    )
    assert second.count("**Branch**: `main`") == 1
    assert utils.comment_has_refs_anchor(second)
    assert "[//]: # (datahub-security-scan-refs)" in second
    assert "### Git branch/tag scan history" in second
    assert "<!--" not in second


def test_comment_has_refs_anchor_still_finds_legacy_html_block():
    body = "<!-- datahub-security-scan-refs\n### Git branch/tag scan history\n-->\n"
    assert utils.comment_has_refs_anchor(body)


def test_issue_pairs_cve_or_pkg_connects_transitively():
    created = [
        ("I-1", "CVE-1", "pkg-a"),
        ("I-2", "CVE-1", "pkg-b"),
        ("I-3", "CVE-2", "pkg-b"),
    ]
    pairs = utils.issue_pairs_cve_or_pkg(created)
    assert ("I-1", "I-2") in pairs
    assert ("I-2", "I-3") in pairs
    assert ("I-1", "I-3") in pairs
