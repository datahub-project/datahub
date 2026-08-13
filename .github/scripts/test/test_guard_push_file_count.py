import os
import re
import subprocess
from pathlib import Path

import pytest

GUARD = Path(__file__).resolve().parents[1] / "pre-commit" / "guard_push_file_count.sh"

# Upstream churn used by the rebase/merge tests. Deliberately above the default
# 500 limit: a guard that counted upstream drift would refuse those pushes on
# the default alone, so these tests stay meaningful even if the configurable
# limit is ignored.
UPSTREAM_CHURN = 501


def _git(repo: Path, *args: str) -> str:
    env = dict(os.environ)
    # Ignore the developer's real git config: hooks, signing and commit.template
    # would otherwise leak into these fixtures.
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    env["GIT_CONFIG_SYSTEM"] = os.devnull
    result = subprocess.run(
        ("git", "-C", str(repo)) + args,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _init(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")


def _commit_files(repo: Path, names: list[str], message: str) -> str:
    for name in names:
        (repo / name).write_text("x\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", message)
    return _git(repo, "rev-parse", "HEAD")


def _run_guard(
    repo: Path,
    to_ref: str | None = None,
    from_ref: str | None = None,
    max_files: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    env["GIT_CONFIG_SYSTEM"] = os.devnull
    for stale in ("PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF", "GUARD_PUSH_MAX_FILES"):
        env.pop(stale, None)
    if to_ref is not None:
        env["PRE_COMMIT_TO_REF"] = to_ref
    if from_ref is not None:
        env["PRE_COMMIT_FROM_REF"] = from_ref
    if max_files is not None:
        env["GUARD_PUSH_MAX_FILES"] = max_files
    return subprocess.run(
        ("bash", str(GUARD)),
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )


def _counted_files(repo: Path, to_ref: str, from_ref: str) -> int:
    """The number of files the guard attributes to this push.

    Asserting on the count rather than the exit code pins the counting
    semantics independently of whatever the limit happens to be.
    """
    result = _run_guard(repo, to_ref=to_ref, from_ref=from_ref, max_files="0")
    if result.returncode == 0:
        return 0
    match = re.search(r"changes (\d+) files", result.stderr)
    assert match is not None, result.stderr
    return int(match.group(1))


@pytest.fixture
def upstream(tmp_path: Path) -> Path:
    """A remote-ish repo with one commit on main."""
    repo = tmp_path / "upstream"
    _init(repo)
    _commit_files(repo, ["base.txt"], "base")
    return repo


@pytest.fixture
def clone(tmp_path: Path, upstream: Path) -> Path:
    repo = tmp_path / "clone"
    subprocess.run(
        ("git", "clone", "-q", str(upstream), str(repo)),
        check=True,
        capture_output=True,
    )
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    return repo


def _churn_upstream(upstream: Path, clone: Path, count: int) -> None:
    """Advance upstream main by `count` files, as master does between pushes."""
    _commit_files(upstream, [f"up{i}.txt" for i in range(count)], "upstream churn")
    _git(clone, "fetch", "-q", "origin")


def test_small_push_on_new_branch_is_allowed(clone: Path) -> None:
    base = _git(clone, "rev-parse", "origin/main")
    _git(clone, "checkout", "-qb", "feat")
    head = _commit_files(clone, ["mine.txt"], "my change")

    result = _run_guard(clone, to_ref=head, from_ref=base, max_files="5")

    assert result.returncode == 0, result.stderr


def test_rebase_onto_churned_upstream_does_not_count_upstream_files(
    upstream: Path, clone: Path
) -> None:
    """The regression this guard originally had.

    A one-file branch rebased onto a busier master must not be judged by the
    upstream drift it now sits on top of.
    """
    _git(clone, "checkout", "-qb", "feat")
    _commit_files(clone, ["mine.txt"], "my change")
    _git(clone, "push", "-q", "origin", "feat")
    old_tip = _git(clone, "rev-parse", "origin/feat")

    _churn_upstream(upstream, clone, UPSTREAM_CHURN)
    _git(clone, "rebase", "-q", "origin/main")
    new_tip = _git(clone, "rev-parse", "HEAD")

    assert _counted_files(clone, new_tip, old_tip) == 1
    assert _run_guard(clone, to_ref=new_tip, from_ref=old_tip).returncode == 0


def test_merging_upstream_into_branch_does_not_count_upstream_files(
    upstream: Path, clone: Path
) -> None:
    _git(clone, "checkout", "-qb", "feat")
    _commit_files(clone, ["mine.txt"], "my change")
    _git(clone, "push", "-q", "origin", "feat")
    old_tip = _git(clone, "rev-parse", "origin/feat")

    _churn_upstream(upstream, clone, UPSTREAM_CHURN)
    _git(clone, "merge", "-q", "--no-edit", "origin/main")
    new_tip = _git(clone, "rev-parse", "HEAD")

    # The merge commit itself authors nothing, and the commits it brings in are
    # already on the remote.
    assert _counted_files(clone, new_tip, old_tip) == 0
    assert _run_guard(clone, to_ref=new_tip, from_ref=old_tip).returncode == 0


def test_large_push_is_refused(clone: Path) -> None:
    base = _git(clone, "rev-parse", "origin/main")
    _git(clone, "checkout", "-qb", "feat")
    head = _commit_files(clone, [f"f{i}.txt" for i in range(10)], "lots of files")

    result = _run_guard(clone, to_ref=head, from_ref=base, max_files="5")

    assert result.returncode == 1
    assert "10 files (limit 5)" in result.stderr


def test_files_are_counted_across_all_unpushed_commits(clone: Path) -> None:
    """A push is the sum of its commits, not just the tip."""
    base = _git(clone, "rev-parse", "origin/main")
    _git(clone, "checkout", "-qb", "feat")
    _commit_files(clone, [f"a{i}.txt" for i in range(4)], "first")
    head = _commit_files(clone, [f"b{i}.txt" for i in range(4)], "second")

    result = _run_guard(clone, to_ref=head, from_ref=base, max_files="5")

    assert result.returncode == 1
    assert "8 files (limit 5)" in result.stderr


def test_count_equal_to_limit_is_allowed(clone: Path) -> None:
    base = _git(clone, "rev-parse", "origin/main")
    _git(clone, "checkout", "-qb", "feat")
    head = _commit_files(clone, [f"f{i}.txt" for i in range(5)], "exactly at limit")

    result = _run_guard(clone, to_ref=head, from_ref=base, max_files="5")

    assert result.returncode == 0, result.stderr


def test_default_limit_is_500(clone: Path) -> None:
    base = _git(clone, "rev-parse", "origin/main")
    _git(clone, "checkout", "-qb", "feat")
    head = _commit_files(clone, [f"f{i}.txt" for i in range(501)], "over the default")

    result = _run_guard(clone, to_ref=head, from_ref=base)

    assert result.returncode == 1
    assert "501 files (limit 500)" in result.stderr


def test_missing_refs_fail_open(clone: Path) -> None:
    """pre-commit omits both refs when it runs against all files."""
    result = _run_guard(clone)

    assert result.returncode == 0, result.stderr


def test_unresolvable_ref_fails_open(clone: Path) -> None:
    result = _run_guard(clone, to_ref="0" * 40, max_files="5")

    assert result.returncode == 0, result.stderr


def test_repo_without_remote_refs_fails_open(tmp_path: Path) -> None:
    """`--not --remotes` has nothing to subtract, so every file would count."""
    repo = tmp_path / "solo"
    _init(repo)
    head = _commit_files(repo, [f"f{i}.txt" for i in range(10)], "lots of files")

    result = _run_guard(repo, to_ref=head, from_ref=head, max_files="5")

    assert result.returncode == 0, result.stderr
