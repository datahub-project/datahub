"""Tests for validate_inputs.py branch-ref parsing."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import validate_inputs as vi
from release_variables import DEFAULT_BRANCH

_SHA = "0123456789abcdef0123456789ABCDEF01234567"
_REPO = "datahub-project/datahub"


def _make_proc(returncode: int, stdout: str = "", stderr: str = "") -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


def _compare_payload(ahead_by: int) -> str:
    return json.dumps({"ahead_by": ahead_by})


def test_parse_release_ref_release_branch():
    assert vi.parse_release_ref("releases/v1.2.3") == ("release", "1.2.3")


def test_parse_release_ref_hotfix_branch():
    assert vi.parse_release_ref("hotfixes/v1.0.1") == ("hotfix", "1.0.1")


@pytest.mark.parametrize(
    "ref",
    [
        DEFAULT_BRANCH,
        "releases/1.2.3",  # missing 'v' prefix
        "releases/v1.2",  # not X.Y.Z
        "hotfixes/v1.2.3.4",  # too many segments
        "releases/v1.2.3-rc1",  # trailing suffix
        "feature/pfp-4668",
        "",
    ],
)
def test_parse_release_ref_invalid_exits(ref):
    with pytest.raises(SystemExit) as exc:
        vi.parse_release_ref(ref)
    assert exc.value.code == 1


def test_emit_parsed_ref_writes_github_output(tmp_path, monkeypatch, capsys):
    output_file = tmp_path / "gh_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

    vi.emit_parsed_ref("hotfixes/v2.0.1")

    written = output_file.read_text()
    assert "branch_type=hotfix" in written
    assert "version=2.0.1" in written
    # Human-readable confirmation is echoed for the workflow log.
    assert "branch_type=hotfix" in capsys.readouterr().out


def test_emit_parsed_ref_invalid_exits_without_output(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "gh_output"))

    with pytest.raises(SystemExit):
        vi.emit_parsed_ref(DEFAULT_BRANCH)

    assert not (tmp_path / "gh_output").exists()


def test_validate_cut_branch_accepts_empty_sha(monkeypatch):
    called = False

    def fake_run(*_a, **_kw):
        nonlocal called
        called = True
        return _make_proc(0, stdout=_compare_payload(0))

    monkeypatch.setattr(subprocess, "run", fake_run)
    vi.validate_cut_branch("1.0.0", "release", sha="")
    assert called is False


def test_validate_cut_branch_accepts_sha_on_source(monkeypatch):
    captured = {}

    def fake_run(args, **_kw):
        captured["args"] = args
        return _make_proc(0, stdout=_compare_payload(0))

    monkeypatch.setenv("GITHUB_REPOSITORY", _REPO)
    monkeypatch.setattr(subprocess, "run", fake_run)
    vi.validate_cut_branch("1.0.0", "hotfix", sha=_SHA, source="releases/v1.0.0")

    url = captured["args"][2]
    assert f"repos/{_REPO}/compare/" in url
    assert "releases%2Fv1.0.0" in url
    assert _SHA in url


def test_validate_cut_branch_rejects_sha_not_on_source(monkeypatch):
    monkeypatch.setenv("GITHUB_REPOSITORY", _REPO)
    monkeypatch.setattr(
        subprocess, "run", lambda *_a, **_kw: _make_proc(0, stdout=_compare_payload(3))
    )
    with pytest.raises(SystemExit) as exc:
        vi.validate_cut_branch("1.0.0", "release", sha=_SHA, source="master")
    assert exc.value.code == 1


def test_validate_cut_branch_rejects_unknown_sha(monkeypatch):
    def fake_run(*_a, **_kw):
        raise subprocess.CalledProcessError(1, ["gh"], stderr="Not Found (HTTP 404)")

    monkeypatch.setenv("GITHUB_REPOSITORY", _REPO)
    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SystemExit) as exc:
        vi.validate_cut_branch("1.0.0", "release", sha=_SHA, source="master")
    assert exc.value.code == 1


def test_validate_cut_branch_requires_source_when_sha_set():
    with pytest.raises(SystemExit) as exc:
        vi.validate_cut_branch("1.0.0", "release", sha=_SHA)
    assert exc.value.code == 1


@pytest.mark.parametrize("sha", ["abc", "not-a-sha", "a" * 39, "g" * 40])
def test_validate_cut_branch_rejects_invalid_sha(sha):
    with pytest.raises(SystemExit) as exc:
        vi.validate_cut_branch("1.0.0", "release", sha=sha)
    assert exc.value.code == 1
