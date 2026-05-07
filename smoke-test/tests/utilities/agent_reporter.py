"""
Pytest plugin for structured JSON test reporting in agent-driven workflows.

Generates a machine-readable JSON report at a configurable path, making it
easy for agents to parse test results and act on failures.

Activation:
    1. Set AGENT_MODE=1 environment variable, OR
    2. Pass --agent-report=<path> to pytest

The report includes:
    - Overall exit status and pass/fail counts
    - Structured failure details with file, line, and error categorization
    - Hints for common error patterns (GraphQL, HTTP, assertion)
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption(
        "--agent-report",
        action="store",
        default=None,
        help="Path for agent-friendly JSON test report",
    )


class AgentReporter:
    """Collects test results and generates a structured JSON report."""

    def __init__(self, report_path: str):
        self.report_path = Path(report_path)
        self.results: List[Dict[str, Any]] = []
        self.total = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors = 0

    def pytest_runtest_logreport(self, report):
        if report.when != "call" and not (report.when == "setup" and report.failed):
            return

        self.total += 1
        entry: Dict[str, Any] = {
            "test_id": report.nodeid,
            "outcome": report.outcome,
            "duration": round(report.duration, 3),
        }

        if report.passed:
            self.passed += 1
        elif report.failed:
            self.failed += 1
            entry["error_message"] = _extract_error_message(report)
            entry["file"], entry["line"] = _extract_location(report)
            entry["traceback"] = str(report.longrepr) if report.longrepr else ""
            entry["hint"] = _categorize_error(
                entry["error_message"], entry["traceback"]
            )
        elif report.skipped:
            self.skipped += 1
            entry["skip_reason"] = str(report.longrepr) if report.longrepr else ""

        self.results.append(entry)

    def pytest_sessionfinish(self, session, exitstatus):
        report = {
            "exit_status": exitstatus,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "errors": self.errors,
            "failures": [r for r in self.results if r["outcome"] == "failed"],
            "all_results": self.results,
        }

        try:
            self.report_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.report_path, "w") as f:
                json.dump(report, f, indent=2)
            logger.info("Agent report written to %s", self.report_path)
        except Exception as e:
            logger.warning(
                "Failed to write agent report to %s: %s", self.report_path, e
            )


def _extract_error_message(report) -> str:
    """Extract a clean error message from a test report."""
    if report.longrepr:
        reprstr = str(report.longrepr)
        # Look for AssertionError or the last line of the traceback
        lines = reprstr.strip().splitlines()
        for line in reversed(lines):
            line = line.strip()
            if line.startswith("AssertionError") or line.startswith("E "):
                return line.lstrip("E ")
        if lines:
            return lines[-1]
    return "Unknown error"


def _extract_location(report) -> tuple:
    """Extract file and line number from a test report."""
    if hasattr(report, "location") and report.location:
        file_path, line_no, _ = report.location
        return file_path, line_no
    return "", 0


def _categorize_error(error_message: str, tb: str) -> str:
    """Provide a categorization hint based on common error patterns."""
    msg = (error_message + " " + tb).lower()

    if "graphql" in msg and ("error" in msg or "none" in msg):
        return "GraphQL error: check resolvers and schema definitions"
    if "404" in msg or "not found" in msg:
        return "404 Not Found: check endpoint paths and controllers"
    if "403" in msg or "forbidden" in msg or "unauthorized" in msg:
        return "Auth error: check METADATA_SERVICE_AUTH_ENABLED and tokens"
    if "500" in msg or "internal server error" in msg:
        return "Server error: check GMS logs (docker logs datahub-gms)"
    if "connection" in msg and ("refused" in msg or "error" in msg):
        return "Connection error: is the DataHub stack running? Try: python3 scripts/datahub_dev.py status"
    if "timeout" in msg:
        return "Timeout: service may be slow or unresponsive"
    if "assertionerror" in msg or "assert" in msg:
        return "Assertion failed: check expected vs actual values"
    if "keyerror" in msg:
        return "KeyError: response structure differs from expected"

    return "Check the traceback for details"


def pytest_configure(config):
    """Register the agent reporter plugin if enabled."""
    report_path = config.getoption("--agent-report", default=None)

    if report_path is None and os.environ.get("AGENT_MODE") == "1":
        report_path = str(Path(config.rootdir) / "build" / "test-report.json")

    if report_path:
        reporter = AgentReporter(report_path)
        config.pluginmanager.register(reporter, "agent_reporter")
