"""Tests for replay_prs.py and golden_prs.yaml (run by test-github-scripts.yml)."""

import json
from pathlib import Path
from typing import List

import pytest
import yaml

import classify_pr
import render_comment
import replay_prs
from replay_prs import Expectation, GoldenPR, Replay

GOLDEN = replay_prs.load_golden()
PATH_ONLY: List[GoldenPR] = [pr for pr in GOLDEN if replay_prs.is_path_only(pr.files)]


def test_golden_entries_are_filled_in() -> None:
    assert len({pr.number for pr in GOLDEN}) == len(GOLDEN)
    for pr in GOLDEN:
        assert pr.why and "TODO" not in pr.why, pr.number
        assert pr.expect.selections, pr.number
        assert set(pr.expect.selections) <= set(replay_prs.BREADTH), pr.number


def test_golden_set_has_path_only_and_model_prs() -> None:
    assert PATH_ONLY
    assert len(PATH_ONLY) < len(GOLDEN)


@pytest.mark.parametrize("pr", PATH_ONLY, ids=lambda pr: f"pr{pr.number}")
def test_path_only_golden_prs_plan_as_expected(pr: GoldenPR) -> None:
    # These PRs never reach the model, so they replay offline and pin the
    # change-type paths and the selection rules on every change.
    got = replay_prs.replay("unused/repo", pr, verbose=False)
    assert replay_prs.judge(pr.expect, got) == "match", replay_prs.describe(got)


def write_golden(path: Path, number: int) -> None:
    pr = {
        "number": number,
        "base": "base",
        "head": "head",
        "files": ["docs/README.md"],
        "expect": {"selection": "canary"},
        "why": "a docs change",
    }
    path.write_text(yaml.safe_dump({"repository": "org/repo", "prs": [pr]}))


def test_the_saas_file_replaces_golden_prs_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "golden_prs.yaml"
    saas = tmp_path / "golden_prs.saas.yaml"
    monkeypatch.setattr(replay_prs, "GOLDEN_YAML", base)
    monkeypatch.setattr(replay_prs, "GOLDEN_SAAS_YAML", saas)
    write_golden(base, 1)
    assert [pr.number for pr in replay_prs.load_golden()] == [1]
    # The fork replays only its own PRs, never the OSS ones synced in.
    write_golden(saas, 2)
    assert [pr.number for pr in replay_prs.load_golden()] == [2]


def test_offline_run_passes_and_skips_model_prs(capsys: pytest.CaptureFixture) -> None:
    assert replay_prs.main(["run", "--offline"]) == 0
    out = capsys.readouterr().out
    assert out.count("needs-model") >= len(GOLDEN) - len(PATH_ONLY)
    assert f"match: {len(PATH_ONLY)}" in out


@pytest.mark.parametrize(
    ("expect", "got", "outcome"),
    [
        (Expectation(["canary"]), Replay("canary", {}), "match"),
        (Expectation(["canary"]), Replay("p0", {}, "low_confidence"), "costlier"),
        (Expectation(["smart", "p0"]), Replay("p0", {}, "low_confidence"), "match"),
        (
            Expectation(["smart"], features_any={"lineage"}),
            Replay("p0", {}, "low_confidence"),
            "costlier",
        ),
        (
            Expectation(["smart", "p0"], features_any={"lineage"}),
            Replay("p0", {}, "low_confidence"),
            "match",
        ),
        (Expectation(["plumbing"]), Replay("canary", {}), "coverage-risk"),
        (
            Expectation(["plumbing"]),
            Replay("smart", {"feature-user": ["search"]}),
            "costlier",
        ),
        (
            Expectation(["smart"], features_any={"lineage", "search"}),
            Replay("smart", {"feature-user": ["search"]}),
            "match",
        ),
        (
            Expectation(["smart"], features_any={"lineage"}),
            Replay("smart", {"feature-user": ["glossary"]}),
            "coverage-risk",
        ),
        (
            Expectation(["smart"], features_any={"lineage"}),
            Replay("canary", {"change-type": ["ci"]}),
            "coverage-risk",
        ),
        (
            Expectation(["canary"], change_type={"build", "ci"}),
            Replay("canary", {"change-type": ["ci"]}),
            "tags-differ",
        ),
    ],
)
def test_judge(expect: Expectation, got: Replay, outcome: str) -> None:
    assert replay_prs.judge(expect, got) == outcome


def test_comment_fields_reads_the_rendered_pytest_section() -> None:
    section = render_comment.render_pytest_section(
        {
            "SMART_MODE": "active",
            "SMART_SELECTION": "smart",
            "SELECTION_APPLIED": "smart",
            "SMART_CONFIDENCE": "MEDIUM",
            "SMART_TAGS": '{"feature-user": ["search"], "confidence": "MEDIUM"}',
            "CHANGED_FILES": "6",
        }
    )
    body = f"{replay_prs.COMMENT_MARKER}\n## Smart test selection\n\n{section}"
    assert replay_prs.comment_fields(body) == {
        "selection": "smart",
        "applied": "smart",
        "tags": "feature-user: search",
        "confidence": "MEDIUM",
    }


PRODUCT_PR = GoldenPR(
    number=1,
    base="base1",
    head="head1",
    files=["metadata-io/src/main/java/Lineage.java"],
    expect=Expectation(["smart"], features_any={"lineage"}),
    why="a lineage fix",
)


@pytest.fixture
def fetched(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        replay_prs,
        "fetch_changes",
        lambda repo, pr: [
            classify_pr.ChangedFile(path=path, status="modified", patch="+x")
            for path in pr.files
        ],
    )


@pytest.mark.usefixtures("fetched")
def test_model_prs_replay_through_the_classifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: '{"feature-user": ["lineage"], "confidence": "HIGH"}',
    )
    got = replay_prs.replay("org/repo", PRODUCT_PR, verbose=False)
    assert got == Replay("smart", {"feature-user": ["lineage"]})
    assert replay_prs.judge(PRODUCT_PR.expect, got) == "match"


@pytest.mark.usefixtures("fetched")
def test_low_confidence_replays_as_p0(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: '{"feature-user": ["lineage"], "confidence": "LOW"}',
    )
    got = replay_prs.replay("org/repo", PRODUCT_PR, verbose=False)
    assert got == Replay("p0", {}, "low_confidence")
    # The fail-safe runs more than smart, whatever feature tags were expected.
    assert replay_prs.judge(PRODUCT_PR.expect, got) == "costlier"


@pytest.mark.usefixtures("fetched")
def test_classifier_errors_fail_the_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    def no_credentials(changed: object, known: object) -> str:
        raise RuntimeError("no credentials")

    monkeypatch.setattr(classify_pr, "call_llm", no_credentials)
    assert (
        replay_prs.cmd_run(
            "org/repo", [PRODUCT_PR], offline=False, runs=1, verbose=False
        )
        == 1
    )
    assert "error" in capsys.readouterr().out


def answer_in_turn(monkeypatch: pytest.MonkeyPatch, *features: str) -> None:
    """Make the classifier tag one feature per run, in this order."""
    answers = iter(
        json.dumps({"feature-user": [feature], "confidence": "HIGH"})
        for feature in features
    )
    monkeypatch.setattr(classify_pr, "call_llm", lambda changed, known: next(answers))


@pytest.mark.parametrize(
    ("failed", "runs", "fails"),
    [(1, 1, True), (0, 3, False), (1, 3, False), (2, 3, True), (1, 2, False)],
)
def test_a_pr_fails_when_most_of_its_runs_fail(
    failed: int, runs: int, fails: bool
) -> None:
    assert replay_prs.fails_most_runs(failed, runs) is fails


@pytest.mark.usefixtures("fetched")
def test_failed_pr_names_the_pr_and_the_reason(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    answer_in_turn(monkeypatch, "lineage", "search", "search")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert (
        replay_prs.cmd_run(
            "org/repo", [PRODUCT_PR], offline=False, runs=3, verbose=False
        )
        == 1
    )
    failed = capsys.readouterr().out.split("FAILED:", 1)[1]
    assert "#1 failed 2 of 3 runs" in failed
    assert "a lineage fix" in failed
    assert "run 2: coverage-risk: none of the expected feature tags (lineage)" in failed
    assert "::error title=Replay #1 failed 2 of 3 runs" in failed


@pytest.mark.usefixtures("fetched")
def test_one_failed_run_of_three_is_a_warning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    answer_in_turn(monkeypatch, "lineage", "search", "lineage")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert (
        replay_prs.cmd_run(
            "org/repo", [PRODUCT_PR], offline=False, runs=3, verbose=False
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "FAILED:" not in out
    warned = out.split("WARNING:", 1)[1]
    assert "#1 failed 1 of 3 runs" in warned
    assert "::warning title=Replay #1 failed 1 of 3 runs" in warned
