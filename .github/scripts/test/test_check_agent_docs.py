import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import check_agent_docs as linter


def _write(root: Path, rel: str, content: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_existing_paths_resolve_from_file_parents_and_root(tmp_path: Path) -> None:
    _write(tmp_path, "mod/src/pkg/source/sql/config.py", "")
    _write(tmp_path, "docs/how/guide.md", "")
    path = _write(
        tmp_path,
        "mod/src/pkg/source/AGENTS.md",
        "- See `source/sql/config.py:12` and `sql/config.py`.\n"
        "- Repo doc: [guide](../../../../docs/how/guide.md#section) and `docs/how/guide.md`.\n"
        "- Not paths: `report.failures`, `exc=`, `/tmp/*.env`, `:mod:lint`, `@AGENTS.md`.\n"
        "- URL: `https://example.com/x` and [site](https://example.com/y).\n",
    )

    assert linter.lint_file(path, tmp_path) == []


def test_dead_path_and_budget_are_reported(tmp_path: Path) -> None:
    body = "\n".join(f"- line {i}" for i in range(201))
    path = _write(
        tmp_path,
        "mod/AGENTS.md",
        "- Dead: `src/does/not/exist.py`.\n" + body + "\n",
    )

    findings = linter.lint_file(path, tmp_path)

    kinds = {(f.kind, f.key) for f in findings}
    assert ("ref", "src/does/not/exist.py") in kinds
    assert any(f.kind == "budget" for f in findings)


def test_dead_path_with_line_suffix_is_reported_once(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        "- `src/does/not/exist.py:123` and again `src/does/not/exist.py:9`.\n",
    )

    findings = linter.lint_file(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "src/does/not/exist.py")]


def test_fenced_code_is_ignored(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        "```bash\ncat src/missing/in/fence.py\n`also/missing.py`\n```\n",
    )

    assert linter.lint_file(path, tmp_path) == []


def test_bugbot_budget_is_tighter(tmp_path: Path) -> None:
    path = _write(tmp_path, ".cursor/BUGBOT.md", "\n".join(["x"] * 121) + "\n")

    findings = linter.lint_file(path, tmp_path)

    assert [f.kind for f in findings] == ["budget"]


def test_generic_tokens_are_not_findings(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        "- Phrases: `try/finally`, `origin/master`, `tests/`, `docs/generated/...`.\n",
    )

    assert linter.lint_file(path, tmp_path) == []


def test_tracked_tail_match_is_anchored_at_segment_boundaries() -> None:
    tracked = [
        "mod/src/pkg/ingestion/source/sql/sql_config.py",
        "docs/some/page.mdx",
        "src/a/b/mod.pyi",
        "x/ingestion/api/base.py",
    ]

    assert linter.tracked_tail_match("source/sql/sql_config.py", tracked)
    assert linter.tracked_tail_match("ingestion/api", tracked)  # directory tail
    assert not linter.tracked_tail_match("some/page.md", tracked)  # .md vs .mdx
    assert not linter.tracked_tail_match("a/b/mod.py", tracked)  # .py vs .pyi


def test_agent_file_discovery_skips_build_dirs() -> None:
    tracked = [
        "AGENTS.md",
        "mod/CLAUDE.md",
        ".cursor/rules/x.mdc",
        ".claude/skills/s/SKILL.md",
        ".claude/skills/s/references/long.md",
        "web/node_modules/pkg/AGENTS.md",
        "README.md",
    ]

    found = {p.as_posix() for p in linter.tracked_agent_files(Path("/r"), tracked)}

    assert found == {
        "/r/AGENTS.md",
        "/r/mod/CLAUDE.md",
        "/r/.cursor/rules/x.mdc",
        "/r/.claude/skills/s/SKILL.md",
    }


def test_baseline_tolerates_known_findings_and_flags_stale_entries(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        "other/AGENTS.md",
        "- `gone/file.py`\n" + "\n".join(["x"] * 250) + "\n",
    )
    findings = linter.lint_file(path, tmp_path)
    baseline = {
        "other/AGENTS.md\tref\tgone/file.py",
        "other/AGENTS.md\tbudget\t260",
        "other/AGENTS.md\tref\tfixed/already.py",
    }

    unexpected, stale = linter.apply_baseline(findings, baseline)

    assert unexpected == []
    assert stale == ["other/AGENTS.md\tref\tfixed/already.py"]


def test_baseline_budget_is_a_ceiling(tmp_path: Path) -> None:
    path = _write(tmp_path, "other/AGENTS.md", "\n".join(["x"] * 250) + "\n")
    findings = linter.lint_file(path, tmp_path)

    unexpected, stale = linter.apply_baseline(
        findings, {"other/AGENTS.md\tbudget\t240"}
    )

    assert [f.kind for f in unexpected] == ["budget"]
    assert stale == []


def test_update_baseline_refuses_explicit_paths(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as exc:
        linter.main(["metadata-ingestion/AGENTS.md", "--update-baseline"])

    assert exc.value.code == 2
    assert "do not pass paths" in capsys.readouterr().err


def test_subset_run_does_not_report_stale_entries() -> None:
    # Runs against the real repo: a clean file plus a baseline full of entries
    # for other files must pass, because a subset run cannot judge them.
    assert linter.main(["metadata-ingestion/tests/AGENTS.md"]) == 0


def test_markdown_links_resolve_only_against_the_file_directory(tmp_path: Path) -> None:
    _write(tmp_path, "smoke-test/README.md", "")
    path = _write(
        tmp_path,
        "scripts/dev/AGENTS.md",
        "- Broken after a move: [run](smoke-test/README.md).\n"
        "- Correct: [run](../../smoke-test/README.md); backticks may use the root form:"
        " `smoke-test/README.md`.\n",
    )

    findings = linter.lint_file(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "smoke-test/README.md")]
