"""Tests for the Playwright PR P0 tier planner (decision logic; no git or Node)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plan_playwright_tier as plan  # noqa: E402

PW = "e2e-test/ui/playwright"
TESTS = f"{PW}/tests"


def _test(file: str, title: str, *tags: str) -> plan.ListedTest:
    return plan.ListedTest(
        file=file, titles=("Suite", title), tags=tags, project="chromium"
    )


SUITE = [
    _test("ingest/sources.spec.ts", "create", "p0", "domain:ingestion"),
    _test("ingest/sources.spec.ts", "search", "domain:ingestion"),
    _test("ingest/history.spec.ts", "view", "domain:ingestion"),
    _test("search/search.spec.ts", "finds", "domain:catalog"),
    _test("lineage/graph.spec.ts", "draws", "domain:catalog"),
]


def _decide(changed: list[str], affected=()) -> plan.Selection:
    return plan.decide(SUITE, list(affected), changed)


def test_product_only_change_selects_only_p0_tests() -> None:
    decision = _decide(["datahub-web-react/src/App.tsx"])
    assert decision.mode == "p0"
    assert decision.reasons == {"ingest/sources.spec.ts": ["p0"]}
    assert decision.p0_only == {
        "ingest/sources.spec.ts": [
            "[chromium] › ingest/sources.spec.ts › Suite › create"
        ]
    }
    assert decision.test_counts == {"ingest/sources.spec.ts": (1, 2)}


def test_empty_selection_runs_the_full_suite() -> None:
    untagged = [test for test in SUITE if "p0" not in test.tags]
    decision = plan.decide(untagged, [], ["datahub-web-react/src/App.tsx"])
    assert decision.mode == "full"


def test_tests_playwright_reports_as_affected_run_in_full() -> None:
    decision = _decide(
        [f"{TESTS}/search/search.spec.ts", f"{PW}/pages/ingest.page.ts"],
        affected=[SUITE[3], SUITE[0], SUITE[1]],
    )
    assert decision.reasons["search/search.spec.ts"] == ["changed"]
    assert decision.reasons["ingest/sources.spec.ts"] == [
        "p0",
        "affected by the change",
    ]
    assert "ingest/sources.spec.ts" not in decision.p0_only  # runs in full
    assert decision.test_counts["ingest/sources.spec.ts"] == (2, 2)


def test_changes_playwright_maps_to_no_test_select_only_p0_tests() -> None:
    # Docs, scripts, types-only modules and deleted specs: Playwright lists nothing for them.
    decision = _decide(
        [
            f"{PW}/README.md",
            f"{PW}/scripts/mce_to_mcp.py",
            f"{PW}/data/types.ts",
            f"{TESTS}/gone/gone.spec.ts",
        ]
    )
    assert decision.mode == "p0"
    assert decision.reasons == {"ingest/sources.spec.ts": ["p0"]}


@pytest.mark.parametrize(
    "path",
    [
        f"{PW}/playwright.config.ts",
        f"{PW}/playwright.no-webserver.config.ts",
        f"{PW}/yarn.lock",
        f"{PW}/test-data/data.json",
        f"{TESTS}/search/fixtures/other.json",
        ".github/scripts/split_playwright_tests.py",
        ".github/workflows/resusable-playwright-tests.yml",
    ],
)
def test_unmappable_changes_run_the_full_suite(path: str) -> None:
    decision = _decide([path])
    assert decision.mode == "full"
    assert path in decision.reason


def test_seed_data_selects_its_directorys_specs_and_declared_borrowers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plan, "SEED_BORROWERS", {"ingest": ("lineage/graph.spec.ts",)})
    decision = _decide([f"{TESTS}/ingest/fixtures/data.json"])
    assert decision.mode == "p0"
    assert decision.reasons["ingest/history.spec.ts"] == ["seeds from ingest"]
    assert decision.reasons["ingest/sources.spec.ts"] == ["seeds from ingest", "p0"]
    assert decision.reasons["lineage/graph.spec.ts"] == ["seeds from ingest"]
    assert "search/search.spec.ts" not in decision.reasons
    # Seeded specs run in full.
    assert decision.test_counts["ingest/sources.spec.ts"] == (2, 2)


def test_seed_data_that_seeds_no_listed_spec_runs_the_full_suite() -> None:
    decision = _decide([f"{TESTS}/nobody/fixtures/data.json"])
    assert decision.mode == "full"
    assert "seeds no spec" in decision.reason


def _completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="boom")


@pytest.mark.parametrize(
    "result",
    [
        # A spec that no longer loads, e.g. it imports a deleted helper.
        _completed(json.dumps({"suites": [], "errors": [{"message": "Cannot find module"}]}), 1),
        _completed(json.dumps({"suites": [], "errors": []}), 1),
        _completed("not json", 1),
    ],
)
def test_playwright_list_raises_when_listing_fails(
    monkeypatch: pytest.MonkeyPatch, result: subprocess.CompletedProcess[str]
) -> None:
    monkeypatch.setattr(plan.subprocess, "run", lambda *a, **k: result)
    with pytest.raises(RuntimeError):
        plan.playwright_list(Path("."))


def test_playwright_list_returns_an_empty_listing(monkeypatch: pytest.MonkeyPatch) -> None:
    # --only-changed matching nothing exits 0 with no suites: a valid "no tests affected".
    listing = {"suites": [], "errors": []}
    monkeypatch.setattr(
        plan.subprocess, "run", lambda *a, **k: _completed(json.dumps(listing))
    )
    assert plan.playwright_list(Path("."), "--only-changed=HEAD^1") == listing


def test_plan_json_separates_this_run_from_the_tiers_decision() -> None:
    decision = _decide([f"{TESTS}/search/search.spec.ts"], affected=[SUITE[3]])
    shadow = plan.plan_json(decision, tier_requested=False, shards=1, predicted=12.0)
    assert (shadow["runMode"], shadow["selectionMode"]) == ("full", "p0")
    enforced = plan.plan_json(decision, tier_requested=True, shards=1, predicted=12.0)
    assert enforced["runMode"] == "p0"
    by_path = {s["path"]: s for s in enforced["specs"]}
    assert by_path["search/search.spec.ts"]["wholeFile"] is True
    assert by_path["ingest/sources.spec.ts"]["testLines"] == [
        "[chromium] › ingest/sources.spec.ts › Suite › create"
    ]
    assert "| `search/search.spec.ts` | 1/1 | changed |" in plan.summary_markdown(
        enforced
    )


def test_shard_count_matches_the_suite_capped_at_spec_files() -> None:
    decision = _decide(["datahub-web-react/src/App.tsx"])
    # sources is 1 of 2 tests at 600 s -> 300 s x 1.4 = 420 s predicted.
    weights = {"ingest/sources.spec.ts": 600.0}
    assert plan.shard_count(decision, weights, 10) == (1, 420.0)
    many = plan.Selection("p0", "x", reasons={f"s{i}.spec.ts": ["p0"] for i in range(12)})
    assert plan.shard_count(many, {}, 10)[0] == 10
    assert plan.shard_count(plan.Selection("full", "x"), {}, 10) == (0, 0.0)


def test_listed_tests_reads_playwrights_json_listing() -> None:
    listing = {
        "suites": [
            {
                "title": "ingest/sources.spec.ts",
                "specs": [],
                "suites": [
                    {
                        "title": "Sources",
                        "specs": [
                            {
                                "title": "create",
                                "file": "ingest/sources.spec.ts",
                                "tags": ["p0"],
                                "tests": [{"projectName": "chromium"}],
                            },
                        ],
                        "suites": [],
                    }
                ],
            }
        ]
    }
    [test] = plan.listed_tests(listing)
    assert (
        test.test_list_line == "[chromium] › ingest/sources.spec.ts › Sources › create"
    )
    assert test.tags == ("p0",)


# ---- contract tests against the real Playwright tree ----

REPO_ROOT = Path(__file__).resolve().parents[3]


def _full_suite(path: str) -> bool:
    return any(p.match(path) for p in plan.FULL_SUITE_PATTERNS)


def test_seed_data_paths_match_the_seeding_fixture() -> None:
    """SEED_DATA and FULL_SUITE_PATTERNS cover the paths fixtures/seeding.fixture.ts reads."""
    seeding = (REPO_ROOT / PW / "fixtures" / "seeding.fixture.ts").read_text(
        encoding="utf-8"
    )
    assert "path.join(TESTS_DIR, featureName, 'fixtures', 'data.json')" in seeding
    assert "const TESTS_DIR = path.join(__dirname, '../tests')" in seeding
    assert "path.join(__dirname, '../test-data/data.json')" in seeding
    assert plan.SEED_DATA.match(f"{TESTS}/search/fixtures/data.json")
    assert _full_suite(f"{PW}/test-data/data.json")


def test_every_json_file_playwright_reads_runs_the_full_suite() -> None:
    """JSON under the test tree is read with fs, which --only-changed can't see:
    each file is either a feature's seed data or a full-suite change."""
    found = [
        path.relative_to(REPO_ROOT).as_posix()
        for root in ("tests", "fixtures", "pages", "helpers", "utils")
        for path in (REPO_ROOT / PW / root).rglob("*.json")
    ]
    assert found, "no JSON found under the test tree; the roots moved"
    assert [
        rel for rel in found if not (plan.SEED_DATA.match(rel) or _full_suite(rel))
    ] == []
