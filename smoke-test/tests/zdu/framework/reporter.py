from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import TestContext
    from .failure_bundle import FailureBundleWriter
    from .runner import ZDUReport

log = logging.getLogger(__name__)


class Reporter:
    def __init__(
        self,
        report_path: str,
        bundle_writer: "FailureBundleWriter | None" = None,
    ) -> None:
        self._path = report_path
        self._bundle_writer = bundle_writer

    def write(self, report: "ZDUReport", ctx: "TestContext | None" = None) -> None:
        self._write_json(report)
        self._print_terminal(report)
        if self._bundle_writer is not None and ctx is not None:
            try:
                bundle_path = self._bundle_writer.write(report, ctx)
                if bundle_path is not None:
                    log.info("[failure-bundle] wrote %s", bundle_path)
            except Exception as exc:
                log.warning("[failure-bundle] writer raised: %s", exc)

    def _write_json(self, report: "ZDUReport") -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        data = {
            "summary": {
                "total_phases": len(report.phase_results),
                "passed_phases": sum(
                    1 for p in report.phase_results if p.status == "passed"
                ),
                "failed_phases": sum(
                    1 for p in report.phase_results if p.status == "failed"
                ),
                "total_scenarios": len(report.scenario_results),
                "passed": sum(1 for r in report.scenario_results if r.status == "PASS"),
                "failed": sum(1 for r in report.scenario_results if r.status == "FAIL"),
                "xfail": sum(1 for r in report.scenario_results if r.status == "XFAIL"),
                "skipped": sum(
                    1 for r in report.scenario_results if r.status in ("SKIP", "XPASS")
                ),
                "duration_s": round(
                    (report.ended_at - report.started_at).total_seconds(), 1
                )
                if report.ended_at
                else 0,
            },
            "phases": [
                {
                    "name": p.phase_name,
                    "status": p.status,
                    "started_at": p.started_at.isoformat() + "Z",
                    "duration_s": round(p.duration_s, 1),
                    "details": p.details,
                    "error": p.error,
                }
                for p in report.phase_results
            ],
            "scenarios": [
                {
                    "tc_number": r.tc_number,
                    "name": r.name,
                    "status": r.status,
                    "actual_result": r.actual_result,
                    "failure_reason": r.failure_reason,
                }
                for r in report.scenario_results
            ],
        }
        with open(self._path, "w") as f:
            json.dump(data, f, indent=2)
        log.info("JSON report written to %s", self._path)

    @staticmethod
    def _print_terminal(report: "ZDUReport") -> None:
        print("\n" + "=" * 70)
        print("ZDU UPGRADE TEST REPORT")
        print("=" * 70)
        for phase in report.phase_results:
            icon = "✓" if phase.status == "passed" else "✗"
            print(
                f"  {icon} {phase.phase_name:<20} {phase.status}  ({phase.duration_s:.1f}s)"
            )
        print("-" * 70)
        for r in report.scenario_results:
            icon = {"PASS": "✓", "FAIL": "✗", "XFAIL": "x", "XPASS": "~", "SKIP": "-"}[
                r.status
            ]
            print(f"  {icon} TC-{r.tc_number:03d} {r.name[:40]:<40} {r.status}")
            if r.failure_reason:
                print(f"        → {r.failure_reason}")
        print("=" * 70)
        sr = report.scenario_results
        passed = sum(1 for r in sr if r.status == "PASS")
        failed = sum(1 for r in sr if r.status == "FAIL")
        xfail = sum(1 for r in sr if r.status == "XFAIL")
        skipped = sum(1 for r in sr if r.status in ("SKIP", "XPASS"))
        print(f"SCENARIOS: {passed} PASS  {failed} FAIL  {xfail} XFAIL  {skipped} SKIP")
        print("=" * 70 + "\n")
