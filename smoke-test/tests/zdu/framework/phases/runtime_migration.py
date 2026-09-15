"""Phase 9 — RuntimeMigrationPhase.

Runs lightweight read/write probes against the live NEW GMS after Phase 8
completes. Captures the schema-versions observed via the read-path and
write-path mutator chains into ``TestContext.runtime_migration``. Phase 10
(Validation) consumes the captured probes to assert the mutator chain is
operating correctly.

The phase always reports ``passed`` — failures are recorded as probe-level
errors so Phase 10 can produce a unified verdict alongside other validator
findings. This matches the design doc's "validation does not fail fast"
principle (§5.10).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from .base import Phase, PhaseResult
from ..context import (
    RuntimeMigrationProbe,
    RuntimeMigrationResult,
    SeededEntity,
    TestContext,
)
from ..datahub_client import DataHubClient

log = logging.getLogger(__name__)

_DEFAULT_N_READ_PROBES = 5
_DEFAULT_N_WRITE_PROBES = 3
_DEFAULT_ASPECT = "embed"
_DEFAULT_EXPECTED_WRITE_VERSION = 4
_WRITE_PROBE_PAYLOAD: dict[str, Any] = {
    "renderUrl": "http://zdu-rt.example.com/embed",
}


class RuntimeMigrationPhase(Phase):
    name = "runtime_migration"

    def __init__(
        self,
        datahub: DataHubClient,
        n_read_probes: int = _DEFAULT_N_READ_PROBES,
        n_write_probes: int = _DEFAULT_N_WRITE_PROBES,
        expected_write_version: int = _DEFAULT_EXPECTED_WRITE_VERSION,
        aspect_name: str = _DEFAULT_ASPECT,
    ) -> None:
        self._datahub = datahub
        self._n_read_probes = n_read_probes
        self._n_write_probes = n_write_probes
        self._expected_write_version = expected_write_version
        self._aspect_name = aspect_name

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()

        read_probes = self._do_read_probes(ctx.seeded_entities)
        write_probes = self._do_write_probes()

        result = RuntimeMigrationResult(
            read_probes=read_probes,
            write_probes=write_probes,
            duration_s=time.monotonic() - t0,
        )
        ctx.runtime_migration = result

        log.info(
            "RuntimeMigration complete — reads %d/%d passed; writes %d/%d passed",
            result.passed_read_count,
            len(read_probes),
            result.passed_write_count,
            len(write_probes),
        )
        return PhaseResult(
            phase_name=self.name,
            status="passed",
            started_at=start,
            duration_s=time.monotonic() - t0,
            details={
                "read_probes": len(read_probes),
                "read_passed": result.passed_read_count,
                "write_probes": len(write_probes),
                "write_passed": result.passed_write_count,
            },
        )

    def _do_read_probes(
        self, seeded: list[SeededEntity]
    ) -> list[RuntimeMigrationProbe]:
        if not seeded:
            return []
        # Take the first N entities. Their expected_schema_version is the
        # post-mutator-chain target; the read-path mutator should produce it.
        sample = seeded[: self._n_read_probes]
        out: list[RuntimeMigrationProbe] = []
        for entity in sample:
            try:
                resp = self._datahub.get_aspect(entity.urn, entity.aspect_name)
                observed = resp.schema_version
                error: str | None = None
            except Exception as exc:
                log.warning(
                    "RuntimeMigration: read probe failed for %s: %s", entity.urn, exc
                )
                observed = 0
                error = str(exc)
            out.append(
                RuntimeMigrationProbe(
                    urn=entity.urn,
                    aspect_name=entity.aspect_name,
                    mode="read",
                    observed_version=observed,
                    expected_version=entity.expected_schema_version,
                    timestamp=datetime.utcnow(),
                    error=error,
                )
            )
        return out

    def _do_write_probes(self) -> list[RuntimeMigrationProbe]:
        out: list[RuntimeMigrationProbe] = []
        for i in range(self._n_write_probes):
            urn = f"urn:li:dashboard:(test,zdu-rt-{i})"
            error: str | None = None
            observed = 0
            try:
                self._datahub.ingest_mcp(
                    urn,
                    self._aspect_name,
                    dict(_WRITE_PROBE_PAYLOAD),
                    system_metadata={},
                )
                resp = self._datahub.get_aspect(urn, self._aspect_name)
                observed = resp.schema_version
            except Exception as exc:
                log.warning("RuntimeMigration: write probe failed for %s: %s", urn, exc)
                error = str(exc)
            out.append(
                RuntimeMigrationProbe(
                    urn=urn,
                    aspect_name=self._aspect_name,
                    mode="write",
                    observed_version=observed,
                    expected_version=self._expected_write_version,
                    timestamp=datetime.utcnow(),
                    error=error,
                )
            )
        return out
