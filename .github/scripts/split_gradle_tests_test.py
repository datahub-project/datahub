#!/usr/bin/env python3
"""Unit tests for split_gradle_tests. Run: python3 .github/scripts/split_gradle_tests_test.py"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "split_gradle_tests", os.path.join(os.path.dirname(__file__), "split_gradle_tests.py")
)
sgt = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = sgt  # so @dataclass can resolve cls.__module__ (py3.9)
_SPEC.loader.exec_module(sgt)
TestId = sgt.TestId


def _touch(root: Path, rel: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("")


class DiscoveryTests(unittest.TestCase):
    def test_duplicate_fqcn_across_modules_both_kept(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _touch(root, "modA/src/test/java/com/x/ConfigTest.java")
            _touch(root, "modB/src/test/java/com/x/ConfigTest.java")
            tests = sgt.discover_tests(["**/src/test/**/*Test.java"], [], str(root))
            self.assertEqual(
                tests,
                {TestId(":modA", "com.x.ConfigTest"), TestId(":modB", "com.x.ConfigTest")},
            )

    def test_nested_module_and_root_project_paths(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _touch(root, "metadata-service/services/src/test/java/com/x/AaTest.java")
            _touch(root, "src/test/java/com/x/RootTest.java")
            tests = sgt.discover_tests(["**/src/test/**/*Test.java"], [], str(root))
            self.assertIn(TestId(":metadata-service:services", "com.x.AaTest"), tests)
            self.assertIn(TestId(":", "com.x.RootTest"), tests)
            self.assertEqual(TestId(":", "com.x.RootTest").task_path, ":test")

    def test_file_outside_repo_root_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as outer:
            root = Path(outer) / "repo"
            root.mkdir()
            _touch(Path(outer), "other/src/test/java/com/x/OutTest.java")
            tests = sgt.discover_tests(
                [str(Path(outer) / "**/src/test/**/*Test.java")], [], str(root)
            )
            self.assertEqual(tests, set())

    def test_exclude_glob(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _touch(root, "modA/src/test/java/com/x/KeepTest.java")
            _touch(root, "modA/src/test/java/com/x/DropTest.java")
            tests = sgt.discover_tests(
                ["**/src/test/**/*Test.java"], ["**/DropTest.java"], str(root)
            )
            self.assertEqual(tests, {TestId(":modA", "com.x.KeepTest")})


class WeightsTests(unittest.TestCase):
    def test_load_weights_parses_and_strips_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "w.json"
            p.write_text(json.dumps([
                {"testId": "com.x.FooTest", "duration": "12.500s"},
                {"testId": "com.x.BarTest", "duration": "bad"},   # ignored
                {"testId": "com.x.BazTest", "duration": "3s"},
            ]))
            w = sgt.load_weights(str(p))
            self.assertEqual(w, {"com.x.FooTest": 12.5, "com.x.BazTest": 3.0})

    def test_missing_weights_file_is_empty(self) -> None:
        self.assertEqual(sgt.load_weights("/no/such/file.json"), {})

    def test_fallback_is_median_of_known(self) -> None:
        tests = {TestId(":m", "com.x.A"), TestId(":m", "com.x.New")}
        weight_of = sgt.build_weight_of(tests, {"com.x.A": 4.0, "com.x.Deleted": 999.0})
        self.assertEqual(weight_of(TestId(":m", "com.x.A")), 4.0)
        # Unknown -> median of matched (only A=4.0); stale Deleted must not skew it.
        self.assertEqual(weight_of(TestId(":m", "com.x.New")), 4.0)


class HybridShardingTests(unittest.TestCase):
    def test_small_modules_stay_whole_bare_task(self) -> None:
        tests = {TestId(":a", "com.x.A"), TestId(":b", "com.x.B")}
        units = sgt.build_units(tests, lambda t: 1.0, ideal_load=10.0)
        self.assertTrue(all(u.whole for u in units))
        plan = sgt.plan_for_shard(units)  # all in one bucket
        # Whole modules emit a bare `:proj:test` (no --tests).
        self.assertEqual(
            sorted(sgt.gradle_args(plan)), sorted([":a:test", ":b:test"])
        )

    def test_whale_module_is_class_split(self) -> None:
        # One heavy module (:w, 4 classes @ 10s = 40) vs a light one; ideal load small.
        whale = {TestId(":w", f"com.x.W{i}") for i in range(4)}
        light = {TestId(":s", "com.x.S")}
        tests = whale | light
        weight_of = lambda t: 10.0 if t.project == ":w" else 1.0
        ideal = sum(weight_of(t) for t in tests) / 4  # ~10.25
        units = sgt.build_units(tests, weight_of, ideal_load=ideal)
        # :w exceeds ideal -> split into 4 class-units; :s stays whole.
        whale_units = [u for u in units if u.project == ":w"]
        self.assertEqual(len(whale_units), 4)
        self.assertTrue(all(not u.whole for u in whale_units))
        self.assertTrue(all(u.whole for u in units if u.project == ":s"))

    def test_whale_shard_emits_module_scoped_tests(self) -> None:
        whale = {TestId(":w", f"com.x.W{i}") for i in range(4)}
        weight_of = lambda t: 10.0
        units = sgt.build_units(whale, weight_of, ideal_load=10.0)
        buckets = sgt.bin_pack(units, total=2)
        # Each bucket has 2 of the whale's classes -> `:w:test --tests X --tests Y`.
        for bucket in buckets:
            plan = sgt.plan_for_shard(bucket)
            args = sgt.gradle_args(plan)
            self.assertEqual(args[0], ":w:test")
            self.assertEqual(args.count("--tests"), 2)

    def test_empty_shard_has_no_tests(self) -> None:
        units = sgt.build_units({TestId(":m", "com.x.Only")}, lambda t: 1.0, ideal_load=100.0)
        buckets = sgt.bin_pack(units, total=4)
        empties = [b for b in buckets if not b]
        self.assertTrue(empties)
        plan = sgt.plan_for_shard(empties[0])
        self.assertFalse(plan["hasTests"])
        self.assertEqual(sgt.gradle_args(plan), [])

    def test_lpt_balances_by_weight(self) -> None:
        # heavy whole module (30) vs three light modules (10 each) -> 30 vs 30 across 2 shards.
        tests = {TestId(":heavy", "com.x.H")} | {TestId(f":l{i}", "com.x.L") for i in range(3)}
        weight_of = lambda t: 30.0 if t.project == ":heavy" else 10.0
        units = sgt.build_units(tests, weight_of, ideal_load=100.0)  # nothing splits
        buckets = sgt.bin_pack(units, total=2)
        loads = sorted(sum(u.weight for u in b) for b in buckets)
        self.assertEqual(loads, [30.0, 30.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
