import subprocess
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


def _tracked(root: Path) -> list[str]:
    """Every file under ``root``, as git ls-files would list it."""
    return sorted(
        p.relative_to(root).as_posix()
        for p in root.rglob("*")
        if p.is_file() and ".git" not in p.relative_to(root).parts
    )


def _lint(path: Path, root: Path) -> list[linter.Finding]:
    return linter.lint_file(path, root, _tracked(root))


def test_existing_paths_resolve_from_file_parents_and_root(tmp_path: Path) -> None:
    _write(tmp_path, "mod/src/pkg/source/sql/config.py", "")
    _write(tmp_path, "docs/how/guide.md", "")
    _write(tmp_path, "docs/how/my file.md", "")
    path = _write(
        tmp_path,
        "mod/src/pkg/source/AGENTS.md",
        "- See `source/sql/config.py:12` and `sql/config.py`.\n"
        "- Repo doc: [guide](../../../../docs/how/guide.md#section) and `docs/how/guide.md`.\n"
        '- Root-anchored [g](/docs/how/guide.md?plain=1), [t](sql/config.py "title"),\n'
        "  encoded [s](../../../../docs/how/my%20file.md).\n"
        "- Not paths: `report.failures`, `exc=`, `/tmp/*.env`, `:mod:lint`, `@AGENTS.md`.\n"
        "- URL: `https://example.com/x` and [site](https://example.com/y).\n"
        "\n[ref]: ../../../../docs/how/guide.md\n",
    )

    assert _lint(path, tmp_path) == []


def test_dead_path_is_reported_and_agents_md_has_no_per_file_budget(
    tmp_path: Path,
) -> None:
    body = "\n".join(f"- line {i}" for i in range(400))
    path = _write(
        tmp_path,
        "mod/AGENTS.md",
        "- Dead: `src/does/not/exist.py`.\n" + body + "\n",
    )

    findings = _lint(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "src/does/not/exist.py")]


def test_dead_link_forms_are_all_checked(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        '- [t](missing/a.md "title") and [r][ref] and [root](/missing/c.md)\n'
        "\n[ref]: missing/b.md\n",
    )

    findings = _lint(path, tmp_path)

    assert sorted(f.key for f in findings) == [
        "/missing/c.md",
        "missing/a.md",
        "missing/b.md",
    ]


def test_dead_path_with_line_suffix_is_reported_once(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        "- `src/does/not/exist.py:123` and again `src/does/not/exist.py:9`.\n",
    )

    findings = _lint(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "src/does/not/exist.py")]


def test_path_case_is_checked_against_tracked_files(tmp_path: Path) -> None:
    _write(tmp_path, "sub/real.md", "")
    path = _write(tmp_path, "AGENTS.md", "- `Sub/real.md` and `sub/real.md`\n")

    findings = _lint(path, tmp_path)

    assert [f.key for f in findings] == ["Sub/real.md"]


def test_fenced_code_is_ignored(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        "```bash\ncat src/missing/in/fence.py\n`also/missing.py`\n```\n",
    )

    assert _lint(path, tmp_path) == []


def test_bugbot_keeps_a_per_file_line_budget(tmp_path: Path) -> None:
    path = _write(tmp_path, ".cursor/BUGBOT.md", "\n".join(["x"] * 121) + "\n")

    findings = _lint(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("budget", "121")]


def test_generic_tokens_are_not_findings(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        "- Phrases: `try/finally`, `origin/master`, `tests/`, `docs/generated/...`.\n",
    )

    assert _lint(path, tmp_path) == []


def test_backtick_tail_match_but_not_link_tail_match(tmp_path: Path) -> None:
    _write(tmp_path, "mod/src/pkg/ingestion/source/sql/sql_config.py", "")
    path = _write(
        tmp_path,
        "docs/AGENTS.md",
        "- `source/sql/sql_config.py` is fine; [l](source/sql/sql_config.py) is not.\n",
    )

    findings = _lint(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "source/sql/sql_config.py")]


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


def test_gitignored_generated_paths_pass_unless_the_rule_covers_the_file(
    tmp_path: Path,
) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    _write(tmp_path, ".gitignore", "docs/generated/\n.claude/*\n!.claude/agents/\n")
    generated = _write(tmp_path, "AGENTS.md", "- Built output in `docs/generated/`.\n")
    blanket = _write(
        tmp_path, ".claude/agents/x.md", "- Dead: `.github/missing/tags.yaml`.\n"
    )

    assert _lint(generated, tmp_path) == []
    assert [f.key for f in _lint(blanket, tmp_path)] == [".github/missing/tags.yaml"]


def test_agent_file_discovery_skips_build_dirs_and_covers_rules() -> None:
    tracked = [
        "AGENTS.md",
        "mod/CLAUDE.md",
        ".cursor/rules/x.mdc",
        ".claude/rules/r.md",
        ".claude/rules/sub/r.md",
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
        "/r/.claude/rules/r.md",
        "/r/.claude/rules/sub/r.md",
        "/r/.claude/skills/s/SKILL.md",
    }


def test_guide_chain_sums_root_and_nested_agents_md_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(linter, "STACK_CHAR_BUDGET", 1_000)
    _write(tmp_path, "AGENTS.md", "r" * 600)
    _write(tmp_path, "CLAUDE.md", "@AGENTS.md\n")  # shim, never counted
    _write(tmp_path, "mod/AGENTS.md", "m" * 300)
    _write(tmp_path, "mod/CLAUDE.md", "@AGENTS.md\n")
    _write(tmp_path, "mod/deep/AGENTS.md", "d" * 200)  # 600 + 300 + 200 > 1,000
    _write(tmp_path, "mod/deep/CLAUDE.md", "@AGENTS.md\n")
    _write(tmp_path, "other/AGENTS.md", "o" * 100)  # 600 + 100, fine
    _write(tmp_path, "other/CLAUDE.md", "@AGENTS.md\n")
    tracked = _tracked(tmp_path)

    chains = linter.guide_chains(tracked)
    findings = linter.lint_stacks(tmp_path, tracked)

    assert [p.as_posix() for p in chains[Path("mod/deep")]] == [
        "AGENTS.md",
        "mod/AGENTS.md",
        "mod/deep/AGENTS.md",
    ]
    assert [(f.path.as_posix(), f.kind, f.key) for f in findings] == [
        ("mod/deep/AGENTS.md", "stack", "1100")
    ]
    assert linter.lint_shims(tmp_path, tracked) == []


def test_shim_pairing_is_enforced_both_ways(tmp_path: Path) -> None:
    _write(tmp_path, "AGENTS.md", "root")
    _write(tmp_path, "CLAUDE.md", "# CLAUDE.md\n\n@AGENTS.md\n")  # heading: not exact
    _write(tmp_path, "a/AGENTS.md", "no companion")
    _write(tmp_path, "b/CLAUDE.md", "real content, no AGENTS.md")
    _write(tmp_path, "c/AGENTS.md", "ok")
    _write(tmp_path, "c/CLAUDE.md", "@AGENTS.md\n")

    findings = linter.lint_shims(tmp_path, _tracked(tmp_path))

    assert [(f.path.as_posix(), f.kind) for f in findings] == [
        ("AGENTS.md", "shim"),
        ("a/AGENTS.md", "shim"),
        ("b/CLAUDE.md", "shim"),
    ]


def test_missing_worktree_file_is_a_finding_not_a_crash(tmp_path: Path) -> None:
    tracked = ["AGENTS.md", "CLAUDE.md"]  # listed by git, deleted without git rm

    assert [f.kind for f in linter.lint_shims(tmp_path, tracked)] == ["shim"]
    assert linter.lint_stacks(tmp_path, tracked) == []


def test_baseline_round_trips_through_the_file_including_shim_entries(
    tmp_path: Path,
) -> None:
    baseline_path = tmp_path / "baseline.txt"
    shim = linter.Finding(Path("x/CLAUDE.md"), 1, "shim", "", "")
    ref = linter.Finding(Path("y/AGENTS.md"), 3, "ref", "gone/file.py", "")
    baseline_path.write_text(
        "# header\n" + shim.baseline_entry() + "\n" + ref.baseline_entry() + "\n"
    )

    unexpected, stale = linter.apply_baseline(
        [shim, ref], linter.load_baseline(baseline_path)
    )

    assert unexpected == [] and stale == []


def test_baseline_tolerates_known_findings_and_flags_stale_entries(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        ".cursor/BUGBOT.md",
        "- `gone/file.py`\n" + "\n".join(["x"] * 150) + "\n",
    )
    findings = _lint(path, tmp_path)
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


def test_shrink_baseline_never_adds_or_raises() -> None:
    old = {
        "mod/AGENTS.md\tstack\t60000",
        "x/AGENTS.md\tref\tgone.py",
        "x/AGENTS.md\tref\tfixed.py",
    }
    findings = [
        linter.Finding(Path("mod/AGENTS.md"), 1, "stack", "55000", ""),  # lowered
        linter.Finding(Path("x/AGENTS.md"), 1, "ref", "gone.py", ""),  # kept
        linter.Finding(Path("x/AGENTS.md"), 1, "ref", "new.py", ""),  # new: refused
        linter.Finding(Path("y/AGENTS.md"), 1, "stack", "70000", ""),  # new: refused
    ]

    admitted, refused = linter.shrink_baseline(findings, old)

    assert admitted == {"mod/AGENTS.md\tstack\t55000", "x/AGENTS.md\tref\tgone.py"}
    assert refused == ["x/AGENTS.md\tref\tnew.py", "y/AGENTS.md\tstack\t70000"]


def test_update_baseline_refuses_explicit_paths(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as exc:
        linter.main(["metadata-ingestion/AGENTS.md", "--update-baseline"])

    assert exc.value.code == 2
    assert "do not pass paths" in capsys.readouterr().err


def test_full_run_against_the_repo_is_clean() -> None:
    # The real baseline must match the real tree: a stale entry or a new finding
    # anywhere fails here as it does in CI.
    assert linter.main([]) == 0


def test_markdown_links_resolve_only_against_the_file_directory(tmp_path: Path) -> None:
    _write(tmp_path, "smoke-test/README.md", "")
    path = _write(
        tmp_path,
        "scripts/dev/AGENTS.md",
        "- Broken after a move: [run](smoke-test/README.md).\n"
        "- Correct: [run](../../smoke-test/README.md); backticks may use the root form:"
        " `smoke-test/README.md`.\n",
    )

    findings = _lint(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "smoke-test/README.md")]
