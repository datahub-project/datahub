#!/usr/bin/env python3
"""Unit tests for compare_test_weights (run by test-github-scripts.yml: pytest .github/scripts/test)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import compare_test_weights as ctw  # noqa: E402


def _weights(entries: dict[str, float], key: str = "testId") -> list[dict[str, str]]:
    return [{key: name, "duration": f"{seconds:.3f}s"} for name, seconds in entries.items()]


def _write_weights(path: Path, entries: dict[str, float], key: str = "testId") -> None:
    path.write_text(json.dumps(_weights(entries, key)))


def _compare(
    tmp: Path,
    *,
    old: dict[str, float],
    new: dict[str, float],
    json_path: str,
    framework: str = "Smoke pytest",
    workflow: str = "docker-unified.yml",
    entry_noun: str = "tests",
    batch_hint: str = "python_batch_count in docker-unified.yml (full=7)",
    key: str = "testId",
    threshold: float = 5.0,
    pr_gate: str = "threshold",
) -> dict:
    tmp.mkdir(parents=True, exist_ok=True)
    old_path = tmp / "old.json"
    new_path = tmp / "new.json"
    stats_path = tmp / "stats.json"
    _write_weights(old_path, old, key)
    _write_weights(new_path, new, key)
    argv = [
        "--old",
        str(old_path),
        "--new",
        str(new_path),
        "--stats-out",
        str(stats_path),
        "--file",
        json_path,
        "--framework",
        framework,
        "--workflow",
        workflow,
        "--entry-noun",
        entry_noun,
        "--batch-hint",
        batch_hint,
        "--test-id-key",
        key,
        "--threshold",
        str(threshold),
        "--pr-gate",
        pr_gate,
    ]
    assert ctw.main(argv) == 0
    return json.loads(stats_path.read_text())


class CalculateChangesTests(unittest.TestCase):
    def test_missing_old_is_100_percent_when_new_nonempty(self) -> None:
        changes = ctw.calculate_changes({}, {"a::t": 10.0})
        self.assertEqual(changes["total_change_pct"], 100.0)
        self.assertEqual(changes["new_tests"], {"a::t": 10.0})

    def test_both_empty_is_zero(self) -> None:
        changes = ctw.calculate_changes({}, {})
        self.assertEqual(changes["total_change_pct"], 0.0)

    def test_added_removed_sort_is_by_id_not_first_twenty(self) -> None:
        old = {f"z_test_{i:02d}": 1.0 for i in range(25)}
        new = {f"a_test_{i:02d}": 1.0 for i in range(25)}
        changes = ctw.calculate_changes(old, new)
        first_shown = sorted(changes["new_tests"].items())[:20]
        self.assertEqual(first_shown[0][0], "a_test_00")
        self.assertNotIn("z_test_00", dict(first_shown))


class LoadWeightsTests(unittest.TestCase):
    def test_missing_file_is_empty(self) -> None:
        self.assertEqual(ctw.load_weights("/no/such/weights.json", "testId"), {})

    def test_malformed_existing_file_raises(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "bad.json"
            path.write_text("{not json")
            with self.assertRaises(json.JSONDecodeError):
                ctw.load_weights(str(path), "testId")


class CompareAndAssembleTests(unittest.TestCase):
    def test_two_files_summary_uses_paths_not_pytest_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _compare(
                tmp / "smoke",
                old={"s::old": 100.0},
                new={"s::old": 100.0, "s::added": 50.0},
                json_path="smoke-test/pytest_test_weights.json",
            )
            _compare(
                tmp / "ingestion",
                old={"i::keep": 200.0},
                new={"i::keep": 200.0, "i::added": 80.0},
                json_path="metadata-ingestion/tests/integration_test_weights.json",
                framework="Ingestion pytest",
                workflow="metadata-ingestion.yml",
                batch_hint="_INTEGRATION_BATCH_COUNT = 6",
            )
            smoke = json.loads((tmp / "smoke" / "stats.json").read_text())
            ingestion = json.loads((tmp / "ingestion" / "stats.json").read_text())
            body = ctw.assemble_pr_body([smoke, ingestion])
            self.assertIn("`smoke-test/pytest_test_weights.json`", body)
            self.assertIn(
                "`metadata-ingestion/tests/integration_test_weights.json`", body
            )
            self.assertEqual(body.count("| `smoke-test/pytest_test_weights.json` |"), 1)
            self.assertNotIn("| Pytest |", body)
            self.assertIn("s::added", body)
            self.assertIn("i::added", body)
            smoke_heading = body.index("## `smoke-test/pytest_test_weights.json`")
            ingestion_heading = body.index(
                "## `metadata-ingestion/tests/integration_test_weights.json`"
            )
            self.assertLess(smoke_heading, ingestion_heading)
            smoke_section = body[smoke_heading:ingestion_heading]
            self.assertIn("s::added", smoke_section)
            self.assertNotIn("i::added", smoke_section)

    def test_gradle_and_playwright_in_one_body(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            gradle = _compare(
                tmp / "gradle",
                old={"com.x.FooTest": 12.0},
                new={"com.x.FooTest": 12.0, "com.x.BarTest": 8.0},
                json_path=".github/backend_test_weights.json",
                framework="Gradle (FQCN)",
                workflow="build-and-test.yml",
                entry_noun="classes",
                batch_hint="backend-test-shard matrix batch 0..5",
                pr_gate="changed",
            )
            playwright = _compare(
                tmp / "pw",
                old={"e2e/login.spec.ts": 20.0},
                new={"e2e/login.spec.ts": 20.0, "e2e/search.spec.ts": 15.0},
                json_path="e2e-test/ui/playwright/playwright_test_weights.json",
                framework="Playwright specs",
                workflow="docker-unified.yml",
                entry_noun="spec files",
                batch_hint="playwright_shard_count default 5",
                key="filePath",
                pr_gate="changed",
            )
            body = ctw.assemble_pr_body([gradle, playwright])
            self.assertIn("## `.github/backend_test_weights.json`", body)
            self.assertIn(
                "## `e2e-test/ui/playwright/playwright_test_weights.json`", body
            )
            self.assertIn("**Added: 1 classes**", body)
            self.assertIn("**Added: 1 spec files**", body)
            self.assertIn("com.x.BarTest", body)
            self.assertIn("e2e/search.spec.ts", body)

    def test_identical_omitted_from_body(self) -> None:
        stats = ctw.build_stats(
            old_weights={"a": 1.0},
            new_weights={"a": 1.0},
            json_path="smoke-test/pytest_test_weights.json",
            framework="Smoke pytest",
            workflow="docker-unified.yml",
            entry_noun="tests",
            batch_hint="hint",
            threshold=5.0,
        )
        self.assertFalse(stats["file_changed"])
        self.assertEqual(ctw.assemble_pr_body([stats]), "")

    def test_missing_old_exceeds_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            stats = _compare(
                tmp,
                old={},
                new={"a::t": 10.0},
                json_path="smoke-test/pytest_test_weights.json",
            )
            self.assertTrue(stats["file_changed"])
            self.assertTrue(stats["exceeds_threshold"])
            self.assertEqual(stats["total_change_pct"], 100.0)

    def test_below_five_percent_still_writes_stats(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            stats = _compare(
                tmp,
                old={"a::t": 100.0},
                new={"a::t": 101.0},
                json_path="smoke-test/pytest_test_weights.json",
            )
            self.assertTrue(stats["file_changed"])
            self.assertFalse(stats["exceeds_threshold"])
            body = ctw.assemble_pr_body([stats])
            self.assertIn("`smoke-test/pytest_test_weights.json`", body)
            self.assertIn(
                "No tests added/removed; no changes above the significant threshold.",
                body,
            )

    def test_significant_cap_shows_and_n_more(self) -> None:
        old = {f"mod::test_{i:02d}": 10.0 for i in range(25)}
        new = {f"mod::test_{i:02d}": 20.0 for i in range(25)}
        stats = ctw.build_stats(
            old_weights=old,
            new_weights=new,
            json_path="smoke-test/pytest_test_weights.json",
            framework="Smoke pytest",
            workflow="docker-unified.yml",
            entry_noun="tests",
            batch_hint="hint",
            threshold=5.0,
        )
        body = ctw.assemble_pr_body([stats])
        self.assertIn("*... and 10 more*", body)
        shown = body.count("**`mod::test_")
        self.assertEqual(shown, 15)

    def test_assemble_skips_missing_ingestion_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _compare(
                tmp / "smoke",
                old={"s::t": 10.0},
                new={"s::t": 20.0},
                json_path="smoke-test/pytest_test_weights.json",
            )
            _compare(
                tmp / "gradle",
                old={"com.x.FooTest": 5.0},
                new={"com.x.FooTest": 5.0, "com.x.NewTest": 3.0},
                json_path=".github/backend_test_weights.json",
                framework="Gradle (FQCN)",
                workflow="build-and-test.yml",
                entry_noun="classes",
                pr_gate="changed",
            )
            out = tmp / "pr-body.md"
            assert (
                ctw.main(
                    [
                        "--assemble",
                        str(tmp / "smoke" / "stats.json"),
                        str(tmp / "gradle" / "stats.json"),
                        "--output",
                        str(out),
                    ]
                )
                == 0
            )
            body = out.read_text()
            self.assertIn("smoke-test/pytest_test_weights.json", body)
            self.assertIn(".github/backend_test_weights.json", body)
            self.assertNotIn("integration_test_weights.json", body)

    def test_pr_gate_changed_not_threshold_zero(self) -> None:
        stats = ctw.build_stats(
            old_weights={"a": 10.0},
            new_weights={"a": 10.0},
            json_path=".github/backend_test_weights.json",
            framework="Gradle (FQCN)",
            workflow="build-and-test.yml",
            entry_noun="classes",
            batch_hint="hint",
            threshold=0.0,
        )
        # Identical files must not open a PR even if threshold is 0.
        self.assertFalse(stats["file_changed"])
        self.assertTrue(stats["exceeds_threshold"])
