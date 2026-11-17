"""Tests for the Playwright PR P0 tier planner (decision logic; no git or Node)."""

from __future__ import annotations

import importlib.util
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


# ---- smart testing: classifier feature tags select specs ----

SPEC_FEATURES = {
    "ingest/sources.spec.ts": frozenset({"ingestion"}),
    "ingest/history.spec.ts": frozenset({"ingestion"}),
    "search/search.spec.ts": frozenset({"search"}),
    "lineage/graph.spec.ts": frozenset({"lineage", "search"}),
}


def _smart(changed: list[str], features: set[str], affected=()) -> plan.Selection:
    return plan.decide(
        SUITE,
        list(affected),
        changed,
        pr_features=frozenset(features),
        spec_features=SPEC_FEATURES,
    )


def test_smart_selection_replaces_p0_tests_with_tagged_specs() -> None:
    decision = _smart(
        ["datahub-web-react/src/search/Search.tsx"],
        {"search"},
        affected=[SUITE[2]],
    )
    assert decision.mode == "smart"
    assert decision.reasons == {
        "search/search.spec.ts": ["tagged search"],
        "lineage/graph.spec.ts": ["tagged search"],
        "ingest/history.spec.ts": ["affected by the change"],
    }
    assert decision.p0_only == {}  # every selected spec runs in full


def test_feature_tags_no_spec_carries_fall_back_to_the_p0_tier() -> None:
    decision = _smart(["datahub-web-react/src/App.tsx"], {"billing"})
    assert decision.mode == "p0"
    assert decision.reasons == {"ingest/sources.spec.ts": ["p0"]}
    assert "no spec is tagged billing" in decision.reason


def test_unmappable_change_runs_the_full_suite_even_with_feature_tags() -> None:
    decision = _smart([f"{PW}/playwright.config.ts"], {"search"})
    assert decision.mode == "full"


def test_feature_tags_reads_only_the_feature_dimensions() -> None:
    tags = json.dumps(
        {
            "feature-user": ["search"],
            "compound": ["lineage"],
            "technical-layer": ["react-frontend"],
            "change-type": ["test-infra"],
        }
    )
    assert plan.feature_tags(tags) == {"search", "lineage"}
    assert plan.feature_tags("") == frozenset()
    with pytest.raises(ValueError):
        plan.feature_tags('["search"]')


def test_spec_feature_tags_prefers_an_exact_path_then_the_longest_glob() -> None:
    rules = [
        {"path": f"{TESTS}/*/*.spec.ts", "tags": {"feature-user": ["search"]}},
        {"path": f"{TESTS}/ingest/*.spec.ts", "tags": {"feature-user": ["ingestion"]}},
        {
            "path": f"{TESTS}/ingest/history.spec.ts",
            "tags": {"feature-dev": ["change-history"]},
        },
        {
            "path": f"{TESTS}/lineage/*.spec.ts",
            "tags": {"technical-layer": ["react-frontend"]},
        },
    ]
    specs = {
        "ingest/sources.spec.ts",
        "ingest/history.spec.ts",
        "search/search.spec.ts",
        "lineage/graph.spec.ts",
    }
    assert plan.spec_feature_tags(specs, rules) == {
        "ingest/sources.spec.ts": {"ingestion"},
        "ingest/history.spec.ts": {"change-history"},
        "search/search.spec.ts": {"search"},
    }


def test_plan_json_runs_the_smart_selection_and_records_a_shadow_one() -> None:
    smart = _smart(["datahub-web-react/src/App.tsx"], {"search"})
    active = plan.plan_json(
        smart,
        tier_requested=True,
        shards=2,
        predicted=60.0,
        smart_mode="active",
        pr_features=frozenset({"search"}),
    )
    assert (active["runMode"], active["selectionMode"]) == ("smart", "smart")
    assert active["prFeatureTags"] == ["search"]
    assert all(s["wholeFile"] for s in active["specs"])
    assert "smartShadow" not in active

    p0 = _decide(["datahub-web-react/src/App.tsx"])
    shadow = plan.plan_json(
        p0,
        tier_requested=True,
        shards=1,
        predicted=60.0,
        smart_mode="shadow",
        pr_features=frozenset({"search"}),
        shadow=smart,
    )
    assert shadow["runMode"] == "p0"
    assert shadow["smartShadow"]["specs"] == ["lineage/graph.spec.ts", "search/search.spec.ts"]


def _listing(tests: list[plan.ListedTest]) -> dict:
    return {
        "suites": [
            {
                "title": t.file,
                "specs": [
                    {
                        "title": t.titles[-1],
                        "file": t.file,
                        "tags": list(t.tags),
                        "tests": [{"projectName": t.project}],
                    }
                ],
                "suites": [],
            }
            for t in tests
        ]
    }


SEARCH_TAG_FILE = (
    "tests: [{path: '" + TESTS + "/search/*.spec.ts', tags: {feature-user: [search]}}]"
)


@pytest.mark.parametrize(
    ("smart_mode", "tag_file", "run_mode", "has_shadow"),
    [
        ("active", SEARCH_TAG_FILE, "smart", False),
        ("shadow", SEARCH_TAG_FILE, "p0", True),
        ("off", SEARCH_TAG_FILE, "p0", False),
        # An unreadable tag file never fails the plan: the P0 tier runs.
        ("active", "tests: [unclosed", "p0", False),
    ],
)
def test_main_applies_the_smart_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smart_mode: str,
    tag_file: str,
    run_mode: str,
    has_shadow: bool,
) -> None:
    tag_path = tmp_path / plan.TAG_FILES[0]
    tag_path.parent.mkdir(parents=True)
    tag_path.write_text(tag_file + "\n", encoding="utf-8")
    monkeypatch.setattr(
        plan,
        "playwright_list",
        lambda repo_root, *args: _listing([] if args else SUITE),
    )
    monkeypatch.setattr(plan, "changed_files", lambda repo_root, base: [])
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    out = tmp_path / "plan.json"

    assert (
        plan.main(
            [
                "--base=HEAD^1",
                "--tier-requested=true",
                "--suite-shards=4",
                f"--repo-root={tmp_path}",
                f"--plan-out={out}",
                f"--smart-mode={smart_mode}",
                '--smart-tags={"feature-user": ["search"]}',
            ]
        )
        == 0
    )
    written = json.loads(out.read_text(encoding="utf-8"))
    assert written["runMode"] == run_mode
    assert ("smartShadow" in written) is has_shadow


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


def test_feature_dimensions_match_the_classifiers() -> None:
    spec = importlib.util.spec_from_file_location(
        "smart_testing_constants",
        REPO_ROOT / ".github/smart-testing/smart_testing_constants.py",
    )
    assert spec is not None and spec.loader is not None
    constants = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(constants)
    assert plan.FEATURE_DIMENSIONS == constants.FEATURE_DIMENSIONS


def test_tag_files_tag_specs_by_repo_relative_path() -> None:
    rules = plan.load_tag_rules(REPO_ROOT)
    assert rules, "the Playwright tag file has no rules"
    assert [r["path"] for r in rules if not r["path"].startswith(f"{TESTS}/")] == []
