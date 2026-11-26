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


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    """A real git repo with ``files`` committed, so main() can run against it."""
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "t")
    for rel, content in files.items():
        _write(tmp_path, rel, content)
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-q", "-m", "init")
    return tmp_path


# ------------------------------------------------------------------ references


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
        "AGENTS.md",
        "- Dead: `src/does/not/exist.py`.\n" + body + "\n",
    )

    findings = _lint(path, tmp_path)

    assert [(f.kind, f.key) for f in findings] == [("ref", "src/does/not/exist.py")]
    assert "its parents" in findings[0].message  # backtick wording, even at the root


def test_dead_link_forms_are_all_checked(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "AGENTS.md",
        '- [t](missing/a.md "title") and [r][ref] and [root](/missing/c.md)\n'
        "- climbs above the repo: [up](../../outside.md)\n"
        "\n[ref]: missing/b.md\n",
    )

    findings = _lint(path, tmp_path)

    assert sorted(f.key for f in findings) == [
        "../../outside.md",
        "/missing/c.md",
        "missing/a.md",
        "missing/b.md",
    ]
    assert all("file's directory" in f.message for f in findings)


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


def test_fenced_code_is_ignored_and_an_open_fence_is_a_finding(tmp_path: Path) -> None:
    closed = _write(
        tmp_path,
        "AGENTS.md",
        "```bash\ncat src/missing/in/fence.py\n`also/missing.py`\n```\n",
    )
    left_open = _write(
        tmp_path,
        "mod/AGENTS.md",
        "- `before/open.py`\n```bash\n- `hidden/by/fence.py`\n",
    )

    assert _lint(closed, tmp_path) == []
    assert [(f.kind, f.line, f.key) for f in _lint(left_open, tmp_path)] == [
        ("fence", 2, ""),
        ("ref", 1, "before/open.py"),
    ]


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


def test_gitignore_tolerates_build_output_but_not_blanket_rules(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _write(
        tmp_path,
        ".gitignore",
        "docs/generated/\n**/build\n.claude/*\n!.claude/agents/\n!.claude/skills/\n"
        ".claude/skills/*\n!.claude/skills/kept\n",
    )
    _write(tmp_path, ".claude/skills/kept/SKILL.md", "")
    generated = _write(
        tmp_path,
        "AGENTS.md",
        "- Built: `docs/generated/` and `mod/build/libs/x.jar`.\n",
    )
    # A renamed skill: the old path is "ignored" by the blanket rule, yet dead.
    renamed = _write(tmp_path, "mod/AGENTS.md", "- `.claude/skills/gone/SKILL.md`\n")
    # A file under the blanket rule referencing a dead path elsewhere.
    under_blanket = _write(
        tmp_path, ".claude/agents/x.md", "- Dead: `.github/missing/tags.yaml`.\n"
    )

    assert _lint(generated, tmp_path) == []
    assert [f.key for f in _lint(renamed, tmp_path)] == [".claude/skills/gone/SKILL.md"]
    assert [f.key for f in _lint(under_blanket, tmp_path)] == [
        ".github/missing/tags.yaml"
    ]


def test_which_ignore_matches_count_as_generated() -> None:
    root_doc = Path("AGENTS.md")
    assert not linter.ignore_match_is_generated(root_doc, ".gitignore", "!keep/")
    assert not linter.ignore_match_is_generated(root_doc, ".gitignore", ".claude/*")
    assert not linter.ignore_match_is_generated(root_doc, ".gitignore", "foo/**")
    assert linter.ignore_match_is_generated(root_doc, ".gitignore", "**/build")
    assert linter.ignore_match_is_generated(root_doc, ".gitignore", "docs/generated/")
    assert linter.ignore_match_is_generated(
        Path("x/AGENTS.md"), "x/.gitignore", "/genDocs"
    )
    # a rule over the linted file's own folder says nothing about the probe
    assert not linter.ignore_match_is_generated(
        Path("x/y/AGENTS.md"), ".gitignore", "x/"
    )


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


# ------------------------------------------------------------- guides and shims


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
    assert findings[0].related == (
        Path("AGENTS.md"),
        Path("mod/AGENTS.md"),
        Path("mod/deep/AGENTS.md"),
    )
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


# ----------------------------------------------------------------------- baseline


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
        ".cursor/BUGBOT.md\tbudget\t151",
        ".cursor/BUGBOT.md\tref\tfixed/already.py",
    }

    unexpected, stale = linter.apply_baseline(findings, baseline)

    assert unexpected == []
    assert stale == [".cursor/BUGBOT.md\tref\tfixed/already.py"]


def test_a_ceiling_only_moves_down() -> None:
    stack = linter.Finding(Path("mod/AGENTS.md"), 1, "stack", "50000", "")
    budget = linter.Finding(Path(".cursor/BUGBOT.md"), 150, "budget", "150", "")

    unexpected, stale = linter.apply_baseline(
        [stack, budget],
        {"mod/AGENTS.md\tstack\t49000", ".cursor/BUGBOT.md\tbudget\t160"},
    )

    assert unexpected == [stack]  # grew past its ceiling
    assert stale == [".cursor/BUGBOT.md\tbudget\t160"]  # shrank: ceiling must drop


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


# --------------------------------------------------------------------------- main


def test_update_baseline_refuses_explicit_paths(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as exc:
        linter.main(["metadata-ingestion/AGENTS.md", "--update-baseline"])

    assert exc.value.code == 2
    assert "do not pass paths" in capsys.readouterr().err


def test_main_end_to_end_on_a_small_repo(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    repo = _repo(
        tmp_path,
        {
            "AGENTS.md": "- `docs/gone.md`\n",
            "CLAUDE.md": "@AGENTS.md\n",
            "mod/AGENTS.md": "- fine\n",
            "mod/CLAUDE.md": "@AGENTS.md\n",
            ".github/scripts/check_agent_docs_baseline.txt": "",
        },
    )

    # 1. A new finding fails, and the shrink-only update refuses to admit it.
    assert linter.main([], root=repo) == 1
    assert "docs/gone.md" in capsys.readouterr().err
    assert linter.main(["--update-baseline"], root=repo) == 1
    assert "Not admitted" in capsys.readouterr().err

    # 2. Admitted explicitly, the run is clean; a subset run on another file too.
    assert linter.main(["--update-baseline", "--allow-growth"], root=repo) == 0
    assert linter.main([], root=repo) == 0
    assert linter.main(["mod/AGENTS.md"], root=repo) == 0

    # 3. Fixing the reference makes the entry stale: exit 1 until the baseline shrinks.
    _write(repo, "docs/gone.md", "")
    _git(repo, "add", "-A")
    assert linter.main([], root=repo) == 1
    assert "no longer matches" in capsys.readouterr().err
    assert linter.main(["--update-baseline"], root=repo) == 0
    assert linter.main([], root=repo) == 0

    # 4. An explicit path that does not exist is a usage error, not a silent pass.
    with pytest.raises(SystemExit) as exc:
        linter.main(["nope/AGENTS.md"], root=repo)
    assert exc.value.code == 2


def test_subset_run_reports_a_chain_overflow_caused_by_the_root_guide(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setattr(linter, "STACK_CHAR_BUDGET", 100)
    repo = _repo(
        tmp_path,
        {
            "AGENTS.md": "r" * 90,
            "CLAUDE.md": "@AGENTS.md\n",
            "mod/AGENTS.md": "m" * 20,
            "mod/CLAUDE.md": "@AGENTS.md\n",
            ".github/scripts/check_agent_docs_baseline.txt": "",
        },
    )

    assert linter.main(["AGENTS.md"], root=repo) == 1
    assert "guide chain for mod" in capsys.readouterr().err


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
