#!/usr/bin/env python3
"""Unit tests for split_playwright_tests (run by test-github-scripts.yml: pytest .github/scripts/test)."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import split_playwright_tests as spt  # noqa: E402

SCRIPT = Path(__file__).resolve().parent.parent / "split_playwright_tests.py"


def _touch(root: Path, rel: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("")


def _write_weights(path: Path, weights: dict[str, float]) -> None:
    path.write_text(
        json.dumps([{"filePath": k, "duration": f"{v}s"} for k, v in weights.items()])
    )


class BuildPlanTests(unittest.TestCase):
    def test_plan_covers_every_spec_once_and_reports_gaps(self) -> None:
        specs = ["a.spec.ts", "b.spec.ts", "c.spec.ts", "new.spec.ts"]
        weights = {"a.spec.ts": 30.0, "b.spec.ts": 20.0, "c.spec.ts": 10.0, "gone.spec.ts": 5.0}
        plan = spt.build_plan(specs, weights, 2)

        assigned = [s for shard in plan["shards"] for s in shard["specs"]]
        self.assertEqual(sorted(assigned), sorted(specs))
        self.assertEqual(plan["unweightedSpecs"], ["new.spec.ts"])
        self.assertEqual(plan["staleWeightEntries"], ["gone.spec.ts"])
        self.assertEqual(plan["fallbackSeconds"], 20.0)
        # 30 + 20 + 10 + 20 (median fallback for new.spec.ts)
        self.assertEqual(plan["totalPredictedSeconds"], 80.0)
        self.assertEqual(plan["maxShardPredictedSeconds"], 40.0)
        self.assertEqual([s["index"] for s in plan["shards"]], [0, 1])
        self.assertEqual(sum(s["diagnostics"]["unweightedSpecFiles"] for s in plan["shards"]), 1)

    def test_more_shards_than_specs_leaves_empty_shards(self) -> None:
        plan = spt.build_plan(["a.spec.ts"], {"a.spec.ts": 5.0}, 3)
        self.assertEqual(sum(s["hasTests"] for s in plan["shards"]), 1)
        self.assertEqual(plan["minShardPredictedSeconds"], 0.0)


class CliTests(unittest.TestCase):
    def _repo(self, d: str) -> tuple[Path, Path]:
        root = Path(d)
        for name in ("a", "b", "c", "d"):
            _touch(root, f"tests/{name}.spec.ts")
        weights = root / "weights.json"
        _write_weights(weights, {"a.spec.ts": 40.0, "b.spec.ts": 30.0, "c.spec.ts": 20.0})
        return root, weights

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False
        )

    def test_dump_plan_matches_each_shards_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root, weights = self._repo(d)
            common = ["--split-total", "2", "--repo-root", str(root), "--weights", str(weights)]

            dump = self._run(*common, "--dump-plan", "-")
            self.assertEqual(dump.returncode, 0, dump.stderr)
            plan = json.loads(dump.stdout)

            for index in range(2):
                shard = self._run(*common, "--split-index", str(index))
                self.assertEqual(shard.returncode, 0, shard.stderr)
                self.assertEqual(json.loads(shard.stdout)["specs"], plan["shards"][index]["specs"])

    def test_logs_unweighted_specs(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root, weights = self._repo(d)
            result = self._run(
                "--split-total", "2", "--split-index", "0",
                "--repo-root", str(root), "--weights", str(weights),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("d.spec.ts", result.stderr)

    def test_tier_plan_packs_selected_specs_and_writes_test_lists(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root, weights = self._repo(d)
            tier_plan = root / "plan.json"
            tier_plan.write_text(json.dumps({"specs": [
                {"path": "a.spec.ts", "wholeFile": True, "testLines": [], "selectedTests": 2, "totalTests": 2},
                {"path": "c.spec.ts", "wholeFile": False, "testLines": ["[chromium] › c.spec.ts › C › p0 test"],
                 "selectedTests": 1, "totalTests": 4},
            ]}))
            common = ["--split-total", "1", "--repo-root", str(root), "--weights", str(weights),
                      "--tier-plan", str(tier_plan)]
            dump = self._run(*common, "--dump-plan", "-")
            self.assertEqual(dump.returncode, 0, dump.stderr)
            plan = json.loads(dump.stdout)
            # c weighs a quarter of its 20 s: 1 of its 4 tests is selected.
            self.assertEqual(plan["totalPredictedSeconds"], 45.0)
            self.assertEqual(plan["staleWeightEntries"], [])
            test_list = root / "tests.txt"
            shard = self._run(*common, "--split-index", "0", "--output-test-list", str(test_list))
            self.assertEqual(shard.returncode, 0, shard.stderr)
            self.assertEqual(test_list.read_text().splitlines(), ["a.spec.ts", "[chromium] › c.spec.ts › C › p0 test"])

    def test_scaled_spec_weights(self) -> None:
        scaled = spt.scaled_spec_weights({"a.spec.ts": (1, 4), "new.spec.ts": (2, 2)}, {"a.spec.ts": 40.0})
        self.assertEqual(scaled, {"a.spec.ts": 10.0, "new.spec.ts": 40.0})

    def test_split_index_required_without_dump_plan(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root, weights = self._repo(d)
            result = self._run("--split-total", "2", "--repo-root", str(root))
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
