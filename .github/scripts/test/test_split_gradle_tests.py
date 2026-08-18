#!/usr/bin/env python3
"""Unit tests for split_gradle_tests (run by test-github-scripts.yml: pytest .github/scripts/test)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import split_gradle_tests as sgt  # noqa: E402


def _touch(root: Path, rel: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("")


class DiscoveryTests(unittest.TestCase):
    def test_modules_discovered_by_any_test_source(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _touch(root, "modA/src/test/java/com/x/FooTest.java")
            _touch(root, "modB/src/test/kotlin/com/y/BarTest.kt")
            _touch(root, "metadata-service/services/src/test/java/com/z/BazTest.java")
            mods = sgt.discover_modules(
                ["**/src/test/**/*.java", "**/src/test/**/*.kt"], [], str(root)
            )
            self.assertEqual(
                set(mods), {":modA", ":modB", ":metadata-service:services"}
            )

    def test_duplicate_fqcn_across_modules_both_modules_present(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _touch(root, "modA/src/test/java/com/x/ConfigTest.java")
            _touch(root, "modB/src/test/java/com/x/ConfigTest.java")
            mods = sgt.discover_modules(["**/src/test/**/*.java"], [], str(root))
            self.assertEqual(set(mods), {":modA", ":modB"})

    def test_root_project_maps_to_colon(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _touch(root, "src/test/java/com/x/RootTest.java")
            mods = sgt.discover_modules(["**/src/test/**/*.java"], [], str(root))
            self.assertIn(":", mods)
            plan = sgt.plan_for_shard([":"], {":": 1.0})
            self.assertEqual(sgt.gradle_args(plan), [":test"])

    def test_exclude_and_outside_repo(self) -> None:
        with tempfile.TemporaryDirectory() as outer:
            root = Path(outer) / "repo"
            root.mkdir()
            _touch(root, "modA/src/test/java/com/x/KeepTest.java")
            _touch(root, "metadata-io/src/test/java/com/x/DropTest.java")
            _touch(Path(outer), "other/src/test/java/com/x/OutTest.java")
            mods = sgt.discover_modules(
                ["**/src/test/**/*.java"], ["metadata-io/src/test/**"], str(root)
            )
            self.assertEqual(set(mods), {":modA"})


class WeightsTests(unittest.TestCase):
    def test_load_weights_parses_and_strips_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "w.json"
            p.write_text(json.dumps([
                {"testId": "com.x.FooTest", "duration": "12.500s"},
                {"testId": "com.x.BarTest", "duration": "bad"},
                {"testId": "com.x.BazTest", "duration": "3s"},
            ]))
            self.assertEqual(
                sgt.load_weights(str(p)), {"com.x.FooTest": 12.5, "com.x.BazTest": 3.0}
            )

    def test_missing_weights_file_is_empty(self) -> None:
        self.assertEqual(sgt.load_weights("/no/such/file.json"), {})

    def test_module_weight_sums_classes_with_fallback(self) -> None:
        modules = {":m": ["com.x.A", "com.x.New"], ":n": ["com.y.B"]}
        # A=4, B=6 known; New unknown -> median of known (5).
        weights = {"com.x.A": 4.0, "com.y.B": 6.0, "com.x.Deleted": 999.0}
        mw = sgt.module_weights(modules, weights)
        self.assertEqual(mw[":m"], 4.0 + 5.0)  # A + fallback(median[4,6]=5)
        self.assertEqual(mw[":n"], 6.0)


class ShardingTests(unittest.TestCase):
    def test_only_bare_module_tasks_no_tests_flag(self) -> None:
        plan = sgt.plan_for_shard([":a", ":b:c"], {":a": 1.0, ":b:c": 1.0})
        args = sgt.gradle_args(plan)
        self.assertEqual(args, [":a:test", ":b:c:test"])
        self.assertNotIn("--tests", args)

    def test_empty_shard(self) -> None:
        buckets = sgt.bin_pack({":only": 1.0}, total=4)
        empty = [b for b in buckets if not b][0]
        plan = sgt.plan_for_shard(empty, {":only": 1.0})
        self.assertFalse(plan["hasTests"])
        self.assertEqual(sgt.gradle_args(plan), [])

    def test_every_module_assigned_exactly_once(self) -> None:
        weighted = {f":m{i}": float(i + 1) for i in range(9)}
        buckets = sgt.bin_pack(weighted, total=4)
        flat = [p for b in buckets for p in b]
        self.assertEqual(sorted(flat), sorted(weighted))  # no loss, no duplication
        self.assertEqual(len(flat), len(set(flat)))

    def test_lpt_balances_by_weight(self) -> None:
        weighted = {":heavy": 30.0, ":l0": 10.0, ":l1": 10.0, ":l2": 10.0}
        buckets = sgt.bin_pack(weighted, total=2)
        loads = sorted(sum(weighted[p] for p in b) for b in buckets)
        self.assertEqual(loads, [30.0, 30.0])

    def test_heavy_whale_isolated(self) -> None:
        weighted = {":whale": 100.0, ":a": 5.0, ":b": 5.0}
        buckets = sgt.bin_pack(weighted, total=3)
        whale_bucket = [b for b in buckets if ":whale" in b][0]
        self.assertEqual(whale_bucket, [":whale"])  # whale alone


if __name__ == "__main__":
    unittest.main(verbosity=2)
