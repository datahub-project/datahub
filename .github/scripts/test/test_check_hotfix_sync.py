"""Tests for check_hotfix_sync.py"""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import check_hotfix_sync as chs

_REPO = "acryldata/datahub"
_VERSION = "1.0.0"
_RELEASE = "releases/v1.0.0"
_HOTFIX = "hotfixes/v1.0.0"


def _make_proc(returncode: int, stdout: str = "", stderr: str = "") -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


def _make_compare(ahead_by: int, commits: list | None = None) -> str:
    return json.dumps(
        {
            "ahead_by": ahead_by,
            "behind_by": 0,
            "status": "ahead" if ahead_by > 0 else "identical",
            "commits": commits or [],
        }
    )


def _make_commit(sha: str, message: str, date: str = "2025-05-01") -> dict:
    return {
        "sha": sha,
        "commit": {
            "message": message,
            "committer": {"date": f"{date}T00:00:00Z"},
        },
    }


# ---------------------------------------------------------------------------
# compare_branches — URL construction
# ---------------------------------------------------------------------------


def test_compare_branches_encodes_slashes(monkeypatch):
    """Branch names with '/' must be percent-encoded in the API URL."""
    captured = {}

    def fake_run(args, **kwargs):
        captured["args"] = args
        return _make_proc(0, stdout=_make_compare(0))

    monkeypatch.setattr(subprocess, "run", fake_run)
    chs.compare_branches(_REPO, _HOTFIX, _RELEASE)

    url = captured["args"][2]
    assert "hotfixes%2Fv1.0.0" in url
    assert "releases%2Fv1.0.0" in url
    assert f"repos/{_REPO}/compare/" in url


# ---------------------------------------------------------------------------
# check_sync — success path
# ---------------------------------------------------------------------------


def test_check_sync_returns_0_when_in_sync(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: _make_proc(0, stdout=_make_compare(0))
    )
    assert chs.check_sync(_VERSION, _REPO) == 0


def test_check_sync_prints_success_banner(monkeypatch, capsys):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: _make_proc(0, stdout=_make_compare(0))
    )
    chs.check_sync(_VERSION, _REPO)
    out = capsys.readouterr().out
    assert "passed" in out
    assert _RELEASE in out
    assert _HOTFIX in out


# ---------------------------------------------------------------------------
# check_sync — failure path
# ---------------------------------------------------------------------------


def test_check_sync_returns_1_when_commits_missing(monkeypatch):
    commit = _make_commit("abc1234deadbeef", "Fix: resolve null pointer")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: _make_proc(0, stdout=_make_compare(1, [commit])),
    )
    assert chs.check_sync(_VERSION, _REPO) == 1


def test_check_sync_lists_missing_commits(monkeypatch, capsys):
    commits = [
        _make_commit("abc1234deadbeef", "Fix: resolve null pointer", "2025-05-05"),
        _make_commit("def5678cafebabe", "Fix: handle empty schema", "2025-05-04"),
    ]
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: _make_proc(0, stdout=_make_compare(2, commits)),
    )
    chs.check_sync(_VERSION, _REPO)
    out = capsys.readouterr().out
    assert "abc1234" in out
    assert "Fix: resolve null pointer" in out
    assert "2025-05-05" in out
    assert "def5678" in out


def test_check_sync_shows_truncation_when_api_cap_exceeded(monkeypatch, capsys):
    """ahead_by > len(commits): API returned fewer commits than actually missing."""
    commits = [_make_commit(f"sha{i}" * 8, f"Fix {i}") for i in range(5)]
    # Simulate ahead_by=300 but only 5 commits returned (API cap scenario)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: _make_proc(0, stdout=_make_compare(300, commits)),
    )
    chs.check_sync(_VERSION, _REPO)
    out = capsys.readouterr().out
    assert "295 more" in out


def test_check_sync_writes_to_summary_file(monkeypatch, tmp_path):
    commit = _make_commit("abc1234deadbeef", "Fix: something important")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: _make_proc(0, stdout=_make_compare(1, [commit])),
    )
    summary = tmp_path / "summary.md"
    chs.check_sync(_VERSION, _REPO, summary_file=str(summary))
    content = summary.read_text()
    assert "Hotfix Sync Check Failed" in content
    assert "abc1234" in content


def test_check_sync_does_not_write_summary_on_success(monkeypatch, tmp_path):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: _make_proc(0, stdout=_make_compare(0))
    )
    summary = tmp_path / "summary.md"
    chs.check_sync(_VERSION, _REPO, summary_file=str(summary))
    assert not summary.exists()


# ---------------------------------------------------------------------------
# check_sync — API error paths
# ---------------------------------------------------------------------------


def test_check_sync_returns_1_on_404(monkeypatch, capsys):
    def raise_404(*a, **kw):
        raise subprocess.CalledProcessError(1, ["gh"], stderr="HTTP 404: Not Found")

    monkeypatch.setattr(subprocess, "run", raise_404)
    assert chs.check_sync(_VERSION, _REPO) == 1
    out = capsys.readouterr().out
    assert "does not exist" in out


def test_check_sync_returns_1_on_api_error(monkeypatch, capsys):
    def raise_error(*a, **kw):
        raise subprocess.CalledProcessError(1, ["gh"], stderr="Internal Server Error")

    monkeypatch.setattr(subprocess, "run", raise_error)
    assert chs.check_sync(_VERSION, _REPO) == 1
    out = capsys.readouterr().out
    assert "Error" in out


def test_compare_branches_raises_on_malformed_json(monkeypatch):
    """Non-JSON response (e.g. HTML error page) raises RuntimeError with context."""
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: _make_proc(0, stdout="<html>Bad Gateway</html>")
    )
    with pytest.raises(RuntimeError, match="unexpected response"):
        chs.compare_branches(_REPO, _HOTFIX, _RELEASE)


# ---------------------------------------------------------------------------
# main() — arg wiring and env var guards
# ---------------------------------------------------------------------------


def test_main_exits_0_when_in_sync(monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "token")
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: _make_proc(0, stdout=_make_compare(0))
    )
    monkeypatch.setattr(
        sys, "argv", ["check_hotfix_sync.py", "--version", _VERSION, "--repo", _REPO]
    )
    with pytest.raises(SystemExit) as exc:
        chs.main()
    assert exc.value.code == 0


def test_main_exits_1_when_gh_token_missing(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setattr(
        sys, "argv", ["check_hotfix_sync.py", "--version", _VERSION, "--repo", _REPO]
    )
    with pytest.raises(SystemExit) as exc:
        chs.main()
    assert exc.value.code == 1
