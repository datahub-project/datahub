"""Failure-artifact bundle writer.

Design doc §7 / §14.1 Phase 10: on any phase or scenario failure, write a
structured directory with everything an operator needs to triage without
re-running the test.

The bundle is one directory per run, stamped with the start timestamp.
Per-artifact failures (e.g., ES unreachable) are caught and logged — the
bundle is best-effort; missing artifacts are preferable to no bundle at all.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from collections.abc import Callable
from dataclasses import is_dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .context import TestContext
    from .docker_compose import DockerComposeClient
    from .es_client import ElasticsearchClient
    from .mysql_client import MySQLClient
    from .runner import ZDUReport

log = logging.getLogger(__name__)

# Services we attempt to dump logs for. Missing ones are silently skipped.
_DEFAULT_SERVICES = (
    "datahub-gms-debug",
    "datahub-mae-consumer-debug",
    "datahub-mce-consumer-debug",
    "mysql",
    "opensearch",
)


def _has_failures(report: "ZDUReport") -> bool:
    if any(p.status == "failed" for p in report.phase_results):
        return True
    if any(s.status == "FAIL" for s in report.scenario_results):
        return True
    return False


def _serialize(value: Any) -> Any:
    """Recursively coerce dataclasses, datetimes, and other types to JSON-safe."""
    if is_dataclass(value) and not isinstance(value, type):
        return {k: _serialize(v) for k, v in dataclasses.asdict(value).items()}
    if isinstance(value, datetime):
        return value.isoformat() + "Z"
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(v) for v in value]
    return value


class FailureBundleWriter:
    def __init__(
        self,
        docker: "DockerComposeClient",
        es: "ElasticsearchClient",
        mysql: "MySQLClient",
        build_dir: Path,
        services: tuple[str, ...] = _DEFAULT_SERVICES,
    ) -> None:
        self._docker = docker
        self._es = es
        self._mysql = mysql
        self._build_dir = build_dir
        self._services = services

    def write(self, report: "ZDUReport", ctx: "TestContext") -> Path | None:
        """Write the failure bundle if anything failed. Returns the bundle path
        or None when no failures."""
        if not _has_failures(report):
            return None
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        path = self._build_dir / f"zdu-failure-{ts}"
        path.mkdir(parents=True, exist_ok=True)
        log.info("[failure-bundle] writing to %s", path)

        self._safe(self._write_summary, "summary.json", path, report)
        self._safe(self._write_compose_logs, "compose-logs/", path)
        self._safe(self._write_es_cats, "es-cat-*", path)
        self._safe(self._write_upgrade_result, "upgrade-result.json", path, ctx)
        self._safe(self._write_mysql_aspects, "mysql-aspects.csv", path)
        self._safe(
            self._write_reader_writer_events, "reader-writer-events.jsonl", path, ctx
        )

        return path

    @staticmethod
    def _safe(fn: Callable[..., Any], label: str, *args: Any) -> None:
        try:
            fn(*args)
        except Exception as exc:
            log.warning("[failure-bundle] %s failed: %s", label, exc)

    def _write_summary(self, path: Path, report: "ZDUReport") -> None:
        summary = {
            "failed_phases": [
                {"name": p.phase_name, "status": p.status, "error": p.error}
                for p in report.phase_results
                if p.status == "failed"
            ],
            "failed_scenarios": [
                {"tc": s.tc_number, "name": s.name, "reason": s.failure_reason}
                for s in report.scenario_results
                if s.status == "FAIL"
            ],
        }
        (path / "summary.json").write_text(json.dumps(summary, indent=2))

    def _write_compose_logs(self, path: Path) -> None:
        log_dir = path / "compose-logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        for service in self._services:
            try:
                content = self._docker.get_service_logs(service, tail=2000)
            except Exception as exc:
                log.debug("compose-logs[%s] failed: %s", service, exc)
                content = ""
            (log_dir / f"{service}.log").write_text(content)

    def _write_es_cats(self, path: Path) -> None:
        try:
            (path / "es-cat-indices.txt").write_text(self._es.cat_indices() or "")
        except Exception as exc:
            log.debug("es-cat-indices failed: %s", exc)
            (path / "es-cat-indices.txt").write_text("")
        try:
            (path / "es-cat-aliases.txt").write_text(self._es.cat_aliases() or "")
        except Exception as exc:
            log.debug("es-cat-aliases failed: %s", exc)
            (path / "es-cat-aliases.txt").write_text("")

    def _write_upgrade_result(self, path: Path, ctx: "TestContext") -> None:
        body: dict[str, Any] = {
            "blocking": ctx.upgrade_blocking.raw if ctx.upgrade_blocking else None,
            "nonblocking": ctx.upgrade_nonblocking.raw
            if ctx.upgrade_nonblocking
            else None,
        }
        (path / "upgrade-result.json").write_text(json.dumps(body, indent=2))

    def _write_mysql_aspects(self, path: Path) -> None:
        csv_text = self._mysql.dump_zdu_aspects_csv()
        (path / "mysql-aspects.csv").write_text(csv_text)

    def _write_reader_writer_events(self, path: Path, ctx: "TestContext") -> None:
        out = path / "reader-writer-events.jsonl"
        with out.open("w") as f:
            for obs in ctx.io_observations:
                f.write(json.dumps(_serialize(obs)) + "\n")
            for w in ctx.io_write_results:
                f.write(json.dumps(_serialize(w)) + "\n")
