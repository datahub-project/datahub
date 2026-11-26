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


def test_dead_path_is_reported_and_agents_md_has_no_per_file_budget(
    tmp_path: Path,
) -> None:
    body = "\n".join(f"- line {i}" for i in range(400))
    path = _write(
        tmp_path,
        "mod/AGENTS.md",
        "- Dead: `src/does/not/exist.py`.\n" + body + "\n",
    )

    findings = linter.lint_file(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "src/does/not/exist.py")]


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


def test_bugbot_keeps_a_per_file_line_budget(tmp_path: Path) -> None:
    path = _write(tmp_path, ".cursor/BUGBOT.md", "\n".join(["x"] * 121) + "\n")

    findings = linter.lint_file(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("budget", "121")]


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


def test_guide_chain_sums_root_and_nested_guides_and_skips_shims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(linter, "STACK_CHAR_BUDGET", 1_000)
    _write(tmp_path, "AGENTS.md", "r" * 600)
    _write(tmp_path, "CLAUDE.md", "# CLAUDE.md\n\n@AGENTS.md\n")  # shim, not counted
    _write(tmp_path, "mod/AGENTS.md", "m" * 300)
    _write(tmp_path, "mod/CLAUDE.md", "@AGENTS.md\n")
    _write(tmp_path, "mod/deep/AGENTS.md", "d" * 200)  # 600 + 300 + 200 > 1,000
    _write(tmp_path, "java/CLAUDE.md", "j" * 500)  # real guide: 600 + 500 > 1,000
    tracked = [
        "AGENTS.md",
        "CLAUDE.md",
        "mod/AGENTS.md",
        "mod/CLAUDE.md",
        "mod/deep/AGENTS.md",
        "java/CLAUDE.md",
    ]

    chains = linter.guide_chains(tmp_path, tracked)
    findings = linter.lint_stacks(tmp_path, tracked)

    assert [p.as_posix() for p in chains[Path("mod/deep")]] == [
        "AGENTS.md",
        "mod/AGENTS.md",
        "mod/deep/AGENTS.md",
    ]
    assert {(f.path.as_posix(), f.kind, f.key) for f in findings} == {
        ("mod/deep/AGENTS.md", "stack", "1100"),
        ("java/CLAUDE.md", "stack", "1100"),
    }


def test_baseline_tolerates_known_findings_and_flags_stale_entries(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        ".cursor/BUGBOT.md",
        "- `gone/file.py`\n" + "\n".join(["x"] * 150) + "\n",
    )
    findings = linter.lint_file(path, tmp_path)
    baseline = {
        ".cursor/BUGBOT.md\tref\tgone/file.py",
        ".cursor/BUGBOT.md\tbudget\t160",
        ".cursor/BUGBOT.md\tref\tfixed/already.py",
    }

    unexpected, stale = linter.apply_baseline(findings, baseline)

    assert unexpected == []
    assert stale == [".cursor/BUGBOT.md\tref\tfixed/already.py"]


def test_baseline_ceiling_applies_to_stack_and_budget_kinds() -> None:
    stack = linter.Finding(Path("mod/AGENTS.md"), 1, "stack", "50000", "")
    budget = linter.Finding(Path(".cursor/BUGBOT.md"), 150, "budget", "150", "")

    unexpected, stale = linter.apply_baseline(
        [stack, budget],
        {"mod/AGENTS.md\tstack\t49000", ".cursor/BUGBOT.md\tbudget\t160"},
    )

    assert unexpected == [stack]
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
