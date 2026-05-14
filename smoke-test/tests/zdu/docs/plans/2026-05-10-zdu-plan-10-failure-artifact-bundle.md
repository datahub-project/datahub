# ZDU E2E — Plan 10: Failure Artifact Bundle

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement design doc §7 / §14.1 Phase 10 — on any failure (phase or scenario), the framework writes a structured artifact bundle to `smoke-test/build/zdu-failure-{timestamp}/` containing Compose logs, ES `_cat` outputs, the captured upgrade-result aspects, the MySQL aspect rows for the test URN range, and the reader/writer thread events. CI uploads this directory as a build artifact, giving operators everything they need to triage a failure without re-running the test.

**Architecture:** One new module (`framework/failure_bundle.py`) with a `FailureBundleWriter` class. Three small helper methods are added to existing clients: `DockerComposeClient.get_service_logs(service, tail)`, `ElasticsearchClient.cat_indices()` / `cat_aliases()`, `MySQLClient.dump_zdu_aspects_csv(...)`. The `Reporter.write()` flow is extended with a single line: after the JSON report is written, if `report` shows any failed phase or scenario, invoke `FailureBundleWriter.write(report, ctx)`. The bundle is one directory per run (not per scenario) — failed scenarios are flat-listed in a `summary.json` inside the bundle.

**Tech Stack:** Python 3 (existing). No new dependencies.

**Out of scope (deferred):**

- Per-scenario sub-directories under `scenarios/TC-{NNN}/`. Design doc shows `{scenario}/` but in practice the underlying state (Compose logs, ES indices, upgrade-result) is shared across all scenarios in a single run; per-scenario duplication adds bytes without insight. Bundle is flat.
- ES index mappings dump (`GET /_mapping/...`) — `_cat/indices` + `_cat/aliases` cover index-existence. Mappings can be a follow-up if needed.
- `system-update-debug` container logs — that container is one-off (`docker compose run --rm`) and has exited by the time Reporter runs. We DO capture its real-time stdout via `LogMonitor` already; the Phase 7/8 phase result `details` includes the parsed events.
- Failure-bundle GZIP / tar.gz packaging — leave directory uncompressed for easy inspection. CI uploads can compress at the artifact layer.
- Bundle eviction / retention — caller's responsibility (e.g., CI cleans build/\* every run).
- Per-suite separation — single bundle covers the whole run.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/framework/
├── docker_compose.py            MODIFY — add get_service_logs(service, tail)
├── es_client.py                 MODIFY — add cat_indices / cat_aliases
├── mysql_client.py              MODIFY — add dump_zdu_aspects_csv
├── reporter.py                  MODIFY — invoke FailureBundleWriter after JSON write
├── failure_bundle.py            CREATE — FailureBundleWriter class
└── test_failure_bundle.py       CREATE — unit tests with tmp_path + mocked clients
```

`Reporter.__init__` is updated to accept the runner's `DockerComposeClient`, `ElasticsearchClient`, `MySQLClient`, and the test `ctx` so it can call clients lazily. The `Runner` already has all four — wiring is a small constructor change.

---

## Task 1: Helper methods on existing clients

**Files:**

- Modify: `smoke-test/tests/zdu/framework/docker_compose.py`
- Modify: `smoke-test/tests/zdu/framework/es_client.py`
- Modify: `smoke-test/tests/zdu/framework/mysql_client.py`
- Modify: `smoke-test/tests/zdu/framework/test_docker_compose.py`
- Modify: `smoke-test/tests/zdu/framework/test_es_client.py`
- Modify: `smoke-test/tests/zdu/framework/test_mysql_client.py`

**Pattern:** Three thin helpers, one per client, each backed by 1-2 unit tests with mocked HTTP / subprocess.

### 1.1 — `DockerComposeClient.get_service_logs`

- [ ] **Step 1: Append failing test in `test_docker_compose.py`**

```python
class TestGetServiceLogs:
    def test_returns_stdout_from_docker_compose_logs(
        self, client: DockerComposeClient
    ) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(stdout="line 1\nline 2\n", returncode=0)
            out = client.get_service_logs("datahub-gms-debug", tail=500)
        assert "line 1" in out
        # Verify the args contain --tail and service name.
        cmd = mock_run.call_args.args[0]
        assert "logs" in cmd
        assert "--tail" in cmd
        assert "500" in cmd
        assert "datahub-gms-debug" in cmd

    def test_returns_empty_string_on_failure(
        self, client: DockerComposeClient
    ) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=1, stderr="err")
            out = client.get_service_logs("nonexistent-service")
        assert out == ""
```

(Verify the existing test file already has a `client` fixture.)

- [ ] **Step 2: Run tests — expect failure** (`AttributeError`).

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_docker_compose.py -v 2>&1 | tail -10
```

- [ ] **Step 3: Implement in `docker_compose.py`** — insert after `get_all_service_images`:

```python
    def get_service_logs(self, service: str, tail: int = 2000) -> str:
        """Return stdout from ``docker compose logs --tail=N <service>``.

        Returns an empty string if the compose call fails (e.g., service not
        running). Used by ``FailureBundleWriter`` to dump per-service logs.
        """
        result = self._run(["logs", "--tail", str(tail), "--no-color", service])
        if result.returncode != 0:
            log.debug(
                "get_service_logs failed for %s (rc=%s): %s",
                service,
                result.returncode,
                (result.stderr or "").strip()[:200],
            )
            return ""
        return result.stdout or ""
```

- [ ] **Step 4: Tests pass.**

### 1.2 — `ElasticsearchClient.cat_indices` / `cat_aliases`

- [ ] **Step 1: Append failing tests in `test_es_client.py`**

```python
class TestCatEndpoints:
    def test_cat_indices_returns_text_body(
        self, client: ElasticsearchClient
    ) -> None:
        with patch.object(
            client._es_session, "get",
            return_value=_resp(text="health status index\nyellow open dashboardindex_v2\n"),
        ) as mock_get:
            out = client.cat_indices()
        assert "dashboardindex_v2" in out
        called = mock_get.call_args.args[0]
        assert called.endswith("/_cat/indices?v")

    def test_cat_aliases_returns_text_body(
        self, client: ElasticsearchClient
    ) -> None:
        with patch.object(
            client._es_session, "get",
            return_value=_resp(text="alias index\ndashboardindex_v2 dashboardindex_v2_old\n"),
        ) as mock_get:
            out = client.cat_aliases()
        assert "dashboardindex_v2_old" in out
        called = mock_get.call_args.args[0]
        assert called.endswith("/_cat/aliases?v")

    def test_cat_returns_empty_string_on_error(
        self, client: ElasticsearchClient
    ) -> None:
        with patch.object(
            client._es_session, "get", side_effect=RuntimeError("ES down")
        ):
            assert client.cat_indices() == ""
            assert client.cat_aliases() == ""
```

The existing `_resp(...)` helper in `test_es_client.py` returns a mock response with `.json()` and probably `.text`; if it doesn't accept `text=` kwarg, extend it (one-line change). Verify by reading the test file.

- [ ] **Step 2: Implement in `es_client.py`**:

```python
    def cat_indices(self) -> str:
        """Return ``GET /_cat/indices?v`` body. Empty string on error."""
        try:
            resp = self._es_session.get(f"{self._es_url}/_cat/indices?v", timeout=30)
            resp.raise_for_status()
            return resp.text or ""
        except Exception as exc:
            log.debug("cat_indices failed: %s", exc)
            return ""

    def cat_aliases(self) -> str:
        """Return ``GET /_cat/aliases?v`` body. Empty string on error."""
        try:
            resp = self._es_session.get(f"{self._es_url}/_cat/aliases?v", timeout=30)
            resp.raise_for_status()
            return resp.text or ""
        except Exception as exc:
            log.debug("cat_aliases failed: %s", exc)
            return ""
```

- [ ] **Step 3: Tests pass.**

### 1.3 — `MySQLClient.dump_zdu_aspects_csv`

- [ ] **Step 1: Append failing test in `test_mysql_client.py`**

```python
class TestDumpZduAspectsCsv:
    def test_returns_csv_with_header_and_rows(
        self, client: MySQLClient
    ) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "urn": "urn:li:dashboard:(test,zdu-tc-1)",
                "aspect": "embed",
                "version": 0,
                "metadata": '{"x":1}',
                "systemmetadata": '{"schemaVersion":4}',
                "createdon": "2026-01-01 00:00:00",
                "createdby": "urn:li:corpuser:datahub",
            },
        ]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            csv_text = client.dump_zdu_aspects_csv()
        lines = csv_text.splitlines()
        assert lines[0] == "urn,aspect,version,metadata,systemmetadata,createdon,createdby"
        assert "zdu-tc-1" in lines[1]
        assert "embed" in lines[1]
        # Verify the SQL filters by urn LIKE 'zdu-...' patterns.
        sql = cursor.execute.call_args.args[0]
        assert "urn LIKE" in sql
        assert "zdu-" in sql

    def test_returns_only_header_when_no_rows(
        self, client: MySQLClient
    ) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            csv_text = client.dump_zdu_aspects_csv()
        assert csv_text.strip() == "urn,aspect,version,metadata,systemmetadata,createdon,createdby"
```

- [ ] **Step 2: Implement in `mysql_client.py`** — insert after `find_upgrade_result_with_field`:

```python
    def dump_zdu_aspects_csv(self) -> str:
        """Return CSV of all ZDU test aspect rows (zdu-tc-*, zdu-io-pool-*,
        zdu-gap-*, zdu-dual-*, zdu-rt-*) for failure-bundle inclusion.

        Header row first, then one row per ``metadata_aspect_v2`` row whose
        ``urn`` matches the ZDU test prefixes. Used by ``FailureBundleWriter``.
        """
        sql = (
            "SELECT urn, aspect, version, metadata, systemmetadata, "
            "createdon, createdby FROM metadata_aspect_v2 "
            "WHERE urn LIKE %s OR urn LIKE %s OR urn LIKE %s OR urn LIKE %s OR urn LIKE %s "
            "ORDER BY urn, aspect, version"
        )
        patterns = [
            "%zdu-tc-%",
            "%zdu-io-pool-%",
            "%zdu-gap-%",
            "%zdu-dual-%",
            "%zdu-rt-%",
        ]
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, tuple(patterns))
            rows = cur.fetchall()
        import csv
        import io

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            ["urn", "aspect", "version", "metadata", "systemmetadata", "createdon", "createdby"]
        )
        for r in rows:
            writer.writerow(
                [
                    r["urn"],
                    r["aspect"],
                    r["version"],
                    r["metadata"],
                    r["systemmetadata"],
                    str(r["createdon"]),
                    r["createdby"],
                ]
            )
        return buf.getvalue()
```

- [ ] **Step 3: Tests pass — full framework suite reaches 197 + 7 new = ~204 pass.**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

- [ ] **Step 4: Commit**

```bash
git add smoke-test/tests/zdu/framework/docker_compose.py \
        smoke-test/tests/zdu/framework/es_client.py \
        smoke-test/tests/zdu/framework/mysql_client.py \
        smoke-test/tests/zdu/framework/test_docker_compose.py \
        smoke-test/tests/zdu/framework/test_es_client.py \
        smoke-test/tests/zdu/framework/test_mysql_client.py
git commit -m "feat(zdu): client helpers for FailureBundleWriter (logs / cat / aspects-csv)"
```

Re-stage if pre-commit reformats.

---

## Task 2: Implement `FailureBundleWriter` + unit tests

**Files:**

- Create: `smoke-test/tests/zdu/framework/failure_bundle.py`
- Create: `smoke-test/tests/zdu/framework/test_failure_bundle.py`

**Pattern:** Single-class module. Constructor takes `(docker, es, mysql, build_dir)`. `write(report, ctx)` is the entry point — checks if anything failed, computes the bundle path, and writes each artifact. All client calls are best-effort; if any one raises, log and continue.

### 2.1 — Write failing tests

- [ ] **Step 1: Create the test file**

Create `smoke-test/tests/zdu/framework/test_failure_bundle.py`:

```python
"""Unit tests for FailureBundleWriter — uses tmp_path + mocked clients."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.zdu.framework.context import (
    IOObservation,
    IOWriteResult,
    TestContext,
    UpgradeBlockingResult,
    UpgradeNonBlockingResult,
    ValidationResult,
)
from tests.zdu.framework.failure_bundle import FailureBundleWriter
from tests.zdu.framework.phases.base import PhaseResult
from tests.zdu.framework.runner import ZDUReport


def _failed_report() -> ZDUReport:
    cfg = MagicMock()
    cfg.gms_service = "datahub-gms-debug"
    rpt = ZDUReport(config=cfg)
    rpt.phase_results.append(
        PhaseResult(
            phase_name="upgrade_nonblocking",
            status="failed",
            started_at=datetime.utcnow(),
            duration_s=10.0,
            error="something broke",
            details={},
        )
    )
    return rpt


def _passing_report() -> ZDUReport:
    cfg = MagicMock()
    cfg.gms_service = "datahub-gms-debug"
    rpt = ZDUReport(config=cfg)
    rpt.phase_results.append(
        PhaseResult(
            phase_name="discovery",
            status="passed",
            started_at=datetime.utcnow(),
            duration_s=1.0,
            details={},
        )
    )
    return rpt


@pytest.fixture
def writer(tmp_path: Path) -> tuple[FailureBundleWriter, MagicMock, MagicMock, MagicMock]:
    docker = MagicMock()
    es = MagicMock()
    mysql = MagicMock()
    docker.get_service_logs.return_value = "GMS log line\n"
    es.cat_indices.return_value = "indices\n"
    es.cat_aliases.return_value = "aliases\n"
    mysql.dump_zdu_aspects_csv.return_value = "urn,aspect\n"
    return FailureBundleWriter(
        docker=docker, es=es, mysql=mysql, build_dir=tmp_path
    ), docker, es, mysql


class TestFailureBundleWriter:
    def test_skips_when_no_failures(self, writer) -> None:
        bundle, docker, es, mysql = writer
        path = bundle.write(_passing_report(), TestContext())
        assert path is None
        docker.get_service_logs.assert_not_called()
        es.cat_indices.assert_not_called()
        mysql.dump_zdu_aspects_csv.assert_not_called()

    def test_writes_bundle_on_phase_failure(self, writer, tmp_path) -> None:
        bundle, docker, es, mysql = writer
        path = bundle.write(_failed_report(), TestContext())
        assert path is not None
        assert path.parent == tmp_path
        assert path.name.startswith("zdu-failure-")
        # Required files present
        assert (path / "summary.json").exists()
        assert (path / "compose-logs").is_dir()
        assert (path / "es-cat-indices.txt").exists()
        assert (path / "es-cat-aliases.txt").exists()
        assert (path / "upgrade-result.json").exists()
        assert (path / "mysql-aspects.csv").exists()
        assert (path / "reader-writer-events.jsonl").exists()
        # Body contents survive
        assert "GMS log line" in (path / "compose-logs" / "datahub-gms-debug.log").read_text()
        assert "indices" in (path / "es-cat-indices.txt").read_text()
        assert "urn,aspect" in (path / "mysql-aspects.csv").read_text()

    def test_writes_bundle_on_scenario_failure(self, writer) -> None:
        bundle, *_ = writer
        rpt = _passing_report()
        rpt.scenario_results = [
            ValidationResult(
                tc_number=20,
                name="Read path in-memory only",
                status="FAIL",
                expected_to_fail=False,
                actual_result="expected v4 got v2",
                failure_reason="schema mismatch",
            )
        ]
        path = bundle.write(rpt, TestContext())
        assert path is not None
        # Failed scenario appears in summary
        import json as _json

        summary = _json.loads((path / "summary.json").read_text())
        failed_scenarios = [s for s in summary.get("failed_scenarios", []) if s.get("tc") == 20]
        assert len(failed_scenarios) == 1

    def test_upgrade_result_includes_blocking_and_nonblocking(self, writer) -> None:
        bundle, *_ = writer
        ctx = TestContext()
        ctx.upgrade_blocking = UpgradeBlockingResult(raw={"blocking": True})
        ctx.upgrade_nonblocking = UpgradeNonBlockingResult(raw={"nonblocking": True})
        path = bundle.write(_failed_report(), ctx)
        assert path is not None
        import json as _json

        body = _json.loads((path / "upgrade-result.json").read_text())
        assert body["blocking"] == {"blocking": True}
        assert body["nonblocking"] == {"nonblocking": True}

    def test_reader_writer_events_dump_observations_and_writes(self, writer) -> None:
        bundle, *_ = writer
        ctx = TestContext()
        ts = datetime.utcnow()
        ctx.io_observations = [
            IOObservation(
                worker="reader-0", urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                aspect_name="embed", observed_version=4, expected_version=4,
                timestamp=ts,
            )
        ]
        ctx.io_write_results = [
            IOWriteResult(
                worker="writer-0", urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                observed_version=4, expected_version=4, passed=True, timestamp=ts,
            )
        ]
        path = bundle.write(_failed_report(), ctx)
        assert path is not None
        lines = (path / "reader-writer-events.jsonl").read_text().splitlines()
        assert len(lines) == 2
        assert any('"reader-0"' in line for line in lines)
        assert any('"writer-0"' in line for line in lines)

    def test_continues_on_per_artifact_failure(self, writer) -> None:
        bundle, docker, es, mysql = writer
        # Make ES dump raise — the bundle should still be created.
        es.cat_indices.side_effect = RuntimeError("ES unreachable")
        path = bundle.write(_failed_report(), TestContext())
        assert path is not None
        # ES file is empty / present-but-empty, but MySQL CSV still works.
        assert (path / "mysql-aspects.csv").exists()
        # The cat-indices.txt either doesn't exist or is empty — both acceptable.
        if (path / "es-cat-indices.txt").exists():
            assert (path / "es-cat-indices.txt").read_text() == ""
```

- [ ] **Step 2: Run tests — expect ImportError (`FailureBundleWriter`).**

### 2.2 — Implement the writer

- [ ] **Step 3: Create `failure_bundle.py`**

```python
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

    def write(
        self, report: "ZDUReport", ctx: "TestContext"
    ) -> Path | None:
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
        self._safe(self._write_reader_writer_events, "reader-writer-events.jsonl", path, ctx)

        return path

    @staticmethod
    def _safe(fn, label: str, *args: Any) -> None:
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
            "nonblocking": ctx.upgrade_nonblocking.raw if ctx.upgrade_nonblocking else None,
        }
        (path / "upgrade-result.json").write_text(json.dumps(body, indent=2))

    def _write_mysql_aspects(self, path: Path) -> None:
        csv_text = self._mysql.dump_zdu_aspects_csv()
        (path / "mysql-aspects.csv").write_text(csv_text)

    def _write_reader_writer_events(
        self, path: Path, ctx: "TestContext"
    ) -> None:
        out = path / "reader-writer-events.jsonl"
        with out.open("w") as f:
            for obs in ctx.io_observations:
                f.write(json.dumps(_serialize(obs)) + "\n")
            for w in ctx.io_write_results:
                f.write(json.dumps(_serialize(w)) + "\n")
```

- [ ] **Step 4: Tests pass.**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_failure_bundle.py -v 2>&1 | tail -10
```

Expected: 6 tests pass.

- [ ] **Step 5: Run full framework suite.**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: ~210 pass (197 baseline + 7 helper tests + 6 bundle tests).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/failure_bundle.py \
        smoke-test/tests/zdu/framework/test_failure_bundle.py
git commit -m "feat(zdu): FailureBundleWriter — captures compose logs / ES cats / upgrade result / MySQL CSV / IO events on FAIL"
```

Re-stage if pre-commit reformats.

---

## Task 3: Wire `FailureBundleWriter` into `Reporter`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/reporter.py`
- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** `Reporter.__init__` gains a `bundle_writer: FailureBundleWriter | None = None` parameter. After `_write_json`, if the writer is non-None, call its `write(report, ctx)` method. The runner passes the writer in.

- [ ] **Step 1: Modify `Reporter`**

In `reporter.py`, change the constructor and `write` method:

```python
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
```

(Add the imports at the top: `from .failure_bundle import FailureBundleWriter` under `if TYPE_CHECKING:`, and `from .context import TestContext` under the same guard.)

- [ ] **Step 2: Modify `Runner`**

In `runner.py`, change the Reporter construction and the write call:

```python
        self._bundle_writer = FailureBundleWriter(
            docker=self._docker,
            es=self._es,
            mysql=self._mysql,
            build_dir=Path(config.build_dir or "smoke-test/build"),
        )
        self._reporter = Reporter(config.report_path, bundle_writer=self._bundle_writer)
```

And at the end of `run()`:

```python
        self._reporter.write(report, ctx=ctx)
```

(Add the imports: `from pathlib import Path`, `from .failure_bundle import FailureBundleWriter`. Also add `build_dir: str | None = None` to `ZDUTestConfig` if not present — it likely isn't.)

- [ ] **Step 3: Verify the runner construction smoke-imports**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
ZDU_SKIP_BOOTJAR=1
import os; os.environ['ZDU_SKIP_BOOTJAR'] = '1'
r = ZDUTestRunner(ZDUTestConfig(), scenarios=[])
print('runner OK; bundle writer:', r._bundle_writer is not None)
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`. Expected: `runner OK; bundle writer: True`.

- [ ] **Step 4: Run full framework suite.**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: ~210 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/reporter.py \
        smoke-test/tests/zdu/framework/runner.py \
        smoke-test/tests/zdu/framework/config.py
git commit -m "feat(zdu): wire FailureBundleWriter into Reporter — bundle written on any FAIL"
```

Re-stage if pre-commit reformats.

---

## Task 4: README — Failure Artifact Bundle subsection

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Insert "Failure Artifact Bundle" subsection**

Find the "Understanding Results" section (around line ~290 / `## Understanding Results`). Append a new subsection AFTER `### Report File`:

```markdown
### Failure Artifact Bundle

When any phase or scenario fails, the framework writes a structured artifact bundle to `smoke-test/build/zdu-failure-{timestamp}/` containing everything an operator needs to triage without re-running the test:
```

zdu-failure-20260510T142503Z/
├── summary.json list of failed phases + scenarios with reasons
├── compose-logs/
│ ├── datahub-gms-debug.log tail 2000 lines from each running service
│ ├── datahub-mae-consumer-debug.log
│ ├── mysql.log
│ └── opensearch.log
├── es-cat-indices.txt GET /\_cat/indices?v
├── es-cat-aliases.txt GET /\_cat/aliases?v
├── upgrade-result.json { "blocking": ..., "nonblocking": ... } from MySQL DataHubUpgradeResult
├── mysql-aspects.csv every metadata*aspect_v2 row matching zdu-tc-* / zdu-io-pool-_ / zdu-gap-_ / zdu-dual-\_ / zdu-rt-\*
└── reader-writer-events.jsonl ctx.io_observations + ctx.io_write_results, one event per line

```

The bundle is best-effort — if a per-artifact dump fails (e.g., ES unreachable, container gone), the failure is logged and the bundle is still written with the remaining artifacts. Missing artifacts are preferable to no bundle at all.

CI uploads `smoke-test/build/zdu-failure-*/` as a build artifact on test failure, so operators can download and inspect locally.
```

- [ ] **Step 2: Verify**

```bash
grep -n "Failure Artifact Bundle\|## Understanding Results" smoke-test/tests/zdu/README.md
```

- [ ] **Step 3: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document Failure Artifact Bundle"
```

---

## Task 5: Live integration check

**Pre-requisite:** Compose stack up.

- [ ] **Step 1: Force a failure and verify the bundle exists**

Run Suite A with a deliberately-broken skip list to force a phase fail (or just rely on the pre-existing TC-020 scenario fail):

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,rolling_restart \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -30
```

Expected: same baseline 14 PASS / 7 XFAIL / 1 pre-existing TC-020 FAIL / 1 SKIP. The validation phase reports `failed` (because TC-020 failed) AND a `[failure-bundle] wrote ...` log line appears.

- [ ] **Step 2: Inspect the bundle directory**

```bash
ls -la <REPO_ROOT>/smoke-test/smoke-test/build/zdu-failure-* 2>&1 | tail -20
ls -la <REPO_ROOT>/smoke-test/smoke-test/build/zdu-failure-*/compose-logs/ 2>&1 | head -20
```

Expected: one `zdu-failure-{timestamp}/` directory with all 7 files / sub-dirs (`summary.json`, `compose-logs/`, `es-cat-indices.txt`, `es-cat-aliases.txt`, `upgrade-result.json`, `mysql-aspects.csv`, `reader-writer-events.jsonl`).

- [ ] **Step 3: Verify each artifact is non-empty**

```bash
BUNDLE=$(ls -1d <REPO_ROOT>/smoke-test/smoke-test/build/zdu-failure-* | head -1)
echo "Bundle: $BUNDLE"
for f in summary.json es-cat-indices.txt es-cat-aliases.txt upgrade-result.json mysql-aspects.csv reader-writer-events.jsonl; do
  size=$(wc -c < "$BUNDLE/$f")
  echo "$f: $size bytes"
done
echo "compose-logs:"
ls -la "$BUNDLE/compose-logs/" 2>&1 | head -10
```

Expected: every file non-empty (compose-logs/\* may have some empty files for services that aren't running, e.g. `mce-consumer-debug` if that profile isn't active — that's acceptable).

- [ ] **Step 4: Verify summary.json has the TC-020 entry**

```bash
python3 -c "
import json
import sys
import glob
bundles = sorted(glob.glob('<REPO_ROOT>/smoke-test/smoke-test/build/zdu-failure-*'))
latest = bundles[-1]
print('Latest bundle:', latest)
data = json.load(open(f'{latest}/summary.json'))
print('failed_phases:', len(data['failed_phases']))
print('failed_scenarios:', len(data['failed_scenarios']))
for s in data['failed_scenarios']:
    print(f\"  TC-{s['tc']:03d}: {s['name']} → {s['reason']}\")
"
```

Expected: at least one `failed_scenarios` entry referencing `TC-020`.

- [ ] **Step 5: Cleanup older bundles** (housekeeping; optional)

```bash
find <REPO_ROOT>/smoke-test/smoke-test/build/ -maxdepth 1 -type d -name 'zdu-failure-*' -mtime +1 -exec rm -rf {} \;
```

- [ ] **Step 6: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in Plan 10"
```

---

## Task 6: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff b9dc6148e4..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-10.diff
wc -l /tmp/zdu-plan-10.diff
```

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send this prompt:

> Review the diff at `/tmp/zdu-plan-10.diff`. This PR implements design doc §7 / §14.1 Phase 10 — failure artifact bundle written on any phase or scenario FAIL.
>
> Concretely:
>
> - Three small client helpers: `DockerComposeClient.get_service_logs(service, tail)`, `ElasticsearchClient.cat_indices()` / `cat_aliases()`, `MySQLClient.dump_zdu_aspects_csv()`.
> - `FailureBundleWriter(docker, es, mysql, build_dir)` with `write(report, ctx) -> Path | None`. Skips silently when no failures. On failure, creates `build_dir/zdu-failure-{timestamp}/` with: `summary.json`, `compose-logs/`, `es-cat-indices.txt`, `es-cat-aliases.txt`, `upgrade-result.json`, `mysql-aspects.csv`, `reader-writer-events.jsonl`.
> - Per-artifact failures are caught (`_safe(...)` wrapper) — partial bundle preferable to no bundle.
> - `Reporter.__init__` gains `bundle_writer: FailureBundleWriter | None`. `Reporter.write(report, ctx)` invokes `bundle_writer.write` after JSON write.
> - `Runner` instantiates the writer and passes it to `Reporter`.
> - 6 unit tests for `FailureBundleWriter` + 7 for the client helpers.
> - README documents the bundle layout.
>
> Check specifically:
>
> 1. **Best-effort semantics:** `FailureBundleWriter._safe` catches per-artifact failures and logs them. The bundle directory is created even if some artifacts fail. Confirm.
> 2. **Pass-through when no failures:** `write(passing_report, ctx)` returns `None` and writes nothing.
> 3. **Timestamp format:** UTC, ISO-ish (`%Y%m%dT%H%M%SZ`). Stable across processes.
> 4. **Per-artifact serialization:** `upgrade-result.json` combines `blocking` + `nonblocking` raw aspects. `reader-writer-events.jsonl` is one JSON object per line, dataclasses serialized via `dataclasses.asdict`. `mysql-aspects.csv` has a header row first.
> 5. **Path collision:** `build_dir/zdu-failure-{timestamp}` — if two runs share the same UTC second, the second overwrites the first. Acceptable for now (low collision rate); add a counter if it ever bites.
> 6. **Type hints complete:** `_serialize`, `write`, helpers all annotated.
> 7. **Dataclass `_serialize` recursion:** handles nested datetime, dict, list, dataclass. Confirm by reading the recursive call structure.
> 8. **`_DEFAULT_SERVICES` list:** dashboard / dataset / dummy services we can extend later. Missing services produce empty log files (acceptable).
> 9. **`Reporter` backward compat:** `bundle_writer` defaults to None; existing callers that don't pass it (the framework unit tests) keep working.
> 10. **YAGNI:** No tar.gz, no per-scenario sub-dirs, no eviction, no ES mappings dump.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**
- [ ] **Step 4: Final commit (if any)**

---

## Self-Review

**Spec coverage** (against design doc §7 Reporter and §14.1 Phase 10 failure artifact bundle item):

| Requirement                                | Task                                                                         |
| ------------------------------------------ | ---------------------------------------------------------------------------- |
| `build/zdu-failure-{timestamp}/` directory | Task 2 (`FailureBundleWriter`)                                               |
| `compose-logs/` per service                | Task 1 + Task 2                                                              |
| `es-cat-indices.txt`                       | Task 1 + Task 2                                                              |
| `es-cat-aliases.txt`                       | Task 1 + Task 2                                                              |
| `upgrade-result.json`                      | Task 2 (combines `ctx.upgrade_blocking.raw` + `ctx.upgrade_nonblocking.raw`) |
| `mysql-aspects.csv`                        | Task 1 + Task 2                                                              |
| `reader-writer-events.jsonl`               | Task 2                                                                       |
| Per-scenario `{scenario}/` sub-directories | OUT OF SCOPE — flat bundle; failed scenarios listed in `summary.json`        |
| Failure-bundle GZIP / tar.gz               | OUT OF SCOPE — directory only                                                |

**Placeholder scan:** None.

**Type / signature consistency:**

- `DockerComposeClient.get_service_logs(service: str, tail: int = 2000) -> str` (Task 1).
- `ElasticsearchClient.cat_indices() -> str` / `cat_aliases() -> str` (Task 1).
- `MySQLClient.dump_zdu_aspects_csv() -> str` (Task 1).
- `FailureBundleWriter.write(report: ZDUReport, ctx: TestContext) -> Path | None` (Task 2).
- `Reporter.__init__(report_path: str, bundle_writer: FailureBundleWriter | None = None)` (Task 3).

**Risks called out:**

1. **`compose-logs/` for services not in the active profile.** `_DEFAULT_SERVICES` includes `mce-consumer-debug` and `mae-consumer-debug` which may not be present on every profile. We dump empty files for those. Acceptable; alternative is to enumerate via `docker compose ps --format '{{.Service}}'` first, but that adds a subprocess call. Defer.
2. **Bundle size on a flaky stack.** Each `compose-logs/{svc}.log` is `--tail 2000` — bounded. ES cat outputs are sub-KB. MySQL CSV is bounded by the number of `zdu-*` test rows (low single-digit thousands worst case). reader-writer-events.jsonl scales with sweep duration but is text-line-bounded by reader/writer thread workers × duration. Total bundle should stay under ~5 MB on a typical Suite A run.
3. **Path collision under rapid CI re-runs.** `%Y%m%dT%H%M%SZ` has 1-second resolution. Two runs starting in the same second is very unlikely in practice but technically possible. The second one would overwrite. Add a `.{run_id}` suffix only if the issue ever surfaces.
4. **`bundle_writer.write` exceptions.** The `Reporter` wrapper catches them (`try/except`) so the JSON report still completes. Confirmed in Task 3 wiring.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-zdu-plan-10-failure-artifact-bundle.md`.

Per session policy: defaulting to subagent-driven execution.
