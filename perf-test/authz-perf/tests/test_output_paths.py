from pathlib import Path

from lib.metadata import GitMetadata
from lib.output_paths import build_slug, result_paths, sanitize_slug
from lib.variants import RunVariant


def test_sanitize_slug() -> None:
    assert sanitize_slug("v0.14.0/rc1") == "v0.14.0-rc1"


def test_build_slug_prefers_run_label() -> None:
    git = GitMetadata("abc", "abc", "main", None, False)
    slug = build_slug(RunVariant(run_label="good"), git, "v0.14.0")
    assert slug == "good"


def test_build_slug_git_commit() -> None:
    git = GitMetadata("abc1234", "full", "main", None, False)
    slug = build_slug(RunVariant(), git, None)
    assert slug == "commit-abc1234"


def test_result_paths_single_local_bisect() -> None:
    jsonl, summary = result_paths(
        Path("/tmp/out"),
        "local",
        "commit-abc1234",
        "orch-id",
        multi_cell=False,
    )
    assert jsonl.name == "commit-abc1234.jsonl"
    assert summary.name == "commit-abc1234.summary.json"


def test_result_paths_multi_cell() -> None:
    jsonl, _ = result_paths(
        Path("/tmp/out"),
        "staging",
        "v0.14.0",
        "20250623T120000Z-deadbeef",
        multi_cell=True,
    )
    assert jsonl.name == "staging-v0.14.0-20250623T120000Z-deadbeef.jsonl"
