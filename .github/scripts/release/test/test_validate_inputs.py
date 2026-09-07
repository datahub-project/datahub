"""Tests for validate_inputs.py branch-ref parsing."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import validate_inputs as vi
from release_variables import DEFAULT_BRANCH


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
