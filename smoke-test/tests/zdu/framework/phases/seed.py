from __future__ import annotations

import json
import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import SeededEntity, TestContext
from ..datahub_client import DataHubClient
from ..docker_compose import DockerComposeClient
from ..es_client import ElasticsearchClient
from ..mysql_client import MySQLClient
from ..scenario_loader import (
    ScenarioExecutor,
    ZDUTestScenario,
    _make_urn,
    make_old_data,
)

log = logging.getLogger(__name__)

_IO_POOL_SIZE = 5
_IO_POOL_ASPECT = "embed"
_IO_POOL_OLD_DATA = {"renderUrl": "http://zdu-io-pool.example.com/embed"}
_IO_POOL_EXPECTED_VERSION = 4

# Platforms referenced by scenario URNs (e.g. urn:li:dataset:(urn:li:dataPlatform:test,...)).
# Current master GMS validates that referenced platforms exist before allowing
# dataset writes — without this registration the seed phase hits 403 on every
# scenario. Earlier debug images didn't enforce this.
_PLATFORMS_TO_REGISTER: tuple[str, ...] = ("test",)

# Entity-type → ES alias mapping. Mirrors ValidationPhase's mapping so the
# wait-for-drain step queries the same index the validators eventually do.
_ENTITY_TYPE_TO_ES_ALIAS: dict[str, str] = {
    "dashboard": "dashboardindex_v2",
    "dataset": "datasetindex_v2",
}

# Default budget for waiting on MCL → ES propagation after seed writes.
# 60s covers ~21 entities × ~2s per MCL drain even on a cold-booted MAE
# consumer; in practice it completes in <5s.
_DEFAULT_DRAIN_TIMEOUT_S = 60
_DEFAULT_DRAIN_POLL_INTERVAL_S = 1.0


def _noop_validator(data: dict) -> bool:
    return True


class SeedPhase(Phase):
    name = "seed"

    def __init__(
        self,
        executor: ScenarioExecutor,
        scenarios: list[ZDUTestScenario],
        datahub: DataHubClient,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        es: ElasticsearchClient | None = None,
        drain_timeout_s: int = _DEFAULT_DRAIN_TIMEOUT_S,
        drain_poll_interval_s: float = _DEFAULT_DRAIN_POLL_INTERVAL_S,
    ) -> None:
        self._executor = executor
        self._scenarios = scenarios
        self._datahub = datahub
        self._docker = docker
        self._mysql = mysql
        # Optional: when present, seed waits for each scenario URN to land in
        # ES before declaring success. Required for the clean_build flow where
        # ES starts empty — without this, Phase 6's reindex runs on a stale
        # source index and post-reindex docs come up missing (TC-015 failure
        # mode discovered during G19c-v2 live-run).
        self._es = es
        self._drain_timeout_s = drain_timeout_s
        self._drain_poll_interval_s = drain_poll_interval_s

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        seeded_count = 0
        try:
            self._register_referenced_platforms()
            for scenario in self._scenarios:
                urns = self._executor.seed(scenario)
                if not urns:
                    continue
                old_data = make_old_data(scenario)
                for urn in urns:
                    expected = scenario.expected_schema_version
                    ctx.seeded_entities.append(
                        SeededEntity(
                            urn=urn,
                            aspect_name=scenario.aspect_name,
                            tc_number=scenario.tc_number,
                            seeded_data=old_data,
                            expected_schema_version=expected
                            if expected is not None
                            else 1,
                            validator=_noop_validator,
                        )
                    )
                    seeded_count += 1
                log.info("TC-%03d: seeded %s", scenario.tc_number, urns[0])

            self._seed_io_pool_via_mysql(ctx)

            # Wait for MCL → ES drain so Phase 6 sees a populated source index.
            # Reports per-URN status in the phase result so failures are
            # diagnosable from the JSON report alone.
            drain_pending = self._wait_for_es_drain(ctx)
            if drain_pending:
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=(
                        f"{len(drain_pending)} scenario URNs did not propagate "
                        f"to ES within {self._drain_timeout_s}s — first 5: "
                        f"{drain_pending[:5]}. MAE consumer may be lagging or "
                        f"the entity-type→alias mapping is incomplete."
                    ),
                    details={
                        "seeded": seeded_count,
                        "io_pool": _IO_POOL_SIZE,
                        "drain_pending": drain_pending,
                    },
                )

            return PhaseResult(
                phase_name=self.name,
                status="passed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={
                    "seeded": seeded_count,
                    "io_pool": _IO_POOL_SIZE,
                    "drain_skipped": self._es is None,
                },
            )
        except Exception as exc:
            log.exception("SeedPhase failed")
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                error=str(exc),
            )

    def _wait_for_es_drain(self, ctx: TestContext | None = None) -> list[str]:
        """Poll ES until each ACTUALLY-seeded scenario URN appears in its alias.

        Returns the list of URNs that did NOT drain within the timeout. An
        empty list means everything propagated successfully.

        Why this exists: scenario seed writes go through GMS REST →
        ingestProposal, which is synchronous to MySQL but asynchronous to ES
        (MCL → MAE consumer → ES doc). When clean_build wipes ES, the freshly
        created indices start empty, and Phase 6's reindex can run before all
        seeded MCLs drain — causing seeded scenario docs to be missing from
        the post-reindex backing index (TC-015 failure mode).

        Uses ``ctx.seeded_entities`` so XFAIL scenarios that the executor
        skipped (TC-7, 11, 12, 13, 14, 19, 21, 23 — "Requires bridgeGap" /
        "Requires fault injection" etc.) aren't waited on. Falls back to
        all scenarios when ctx is None (backwards-compat for legacy tests).

        Skipped when no ES client is configured or when no seeded URN has a
        known entity-type→alias mapping.
        """
        if self._es is None:
            return []
        targets: list[tuple[str, str]] = []
        # Build (urn, alias) pairs for entities that were actually written —
        # not every scenario gets seeded (some are XFAIL with no fixture).
        if ctx is not None and ctx.seeded_entities:
            # Map URN → entity_type via a lookup over the scenario list so we
            # can derive the alias. seeded_entities only carries URN+aspect.
            tc_to_entity_type = {s.tc_number: s.entity_type for s in self._scenarios}
            for entity in ctx.seeded_entities:
                entity_type = tc_to_entity_type.get(entity.tc_number)
                if entity_type is None:
                    continue
                alias = _ENTITY_TYPE_TO_ES_ALIAS.get(entity_type)
                if alias is None:
                    continue
                targets.append((entity.urn, alias))
        else:
            for scenario in self._scenarios:
                alias = _ENTITY_TYPE_TO_ES_ALIAS.get(scenario.entity_type)
                if alias is None:
                    continue
                targets.append((_make_urn(scenario), alias))
        if not targets:
            return []
        log.info(
            "[seed] waiting for %d scenario URN(s) to drain to ES "
            "(timeout %ds, poll every %.1fs)",
            len(targets),
            self._drain_timeout_s,
            self._drain_poll_interval_s,
        )
        deadline = time.monotonic() + self._drain_timeout_s
        pending = list(targets)
        while pending and time.monotonic() < deadline:
            still_pending = []
            for urn, alias in pending:
                doc = self._es.get_doc(alias, urn)
                if doc is None:
                    still_pending.append((urn, alias))
            if not still_pending:
                log.info(
                    "[seed] all %d scenario URN(s) drained to ES in %.1fs",
                    len(targets),
                    self._drain_timeout_s - (deadline - time.monotonic()),
                )
                return []
            pending = still_pending
            time.sleep(self._drain_poll_interval_s)
        # Timeout: return the still-pending URNs so the caller can surface
        # them in the phase result.
        return [urn for urn, _ in pending]

    def _register_referenced_platforms(self) -> None:
        """Write dataPlatformInfo for every platform name our scenarios reference.

        Idempotent (UPSERT). Skips silently if the platform write itself raises —
        the per-scenario seed will surface the real error if registration was
        actually required.
        """
        for name in _PLATFORMS_TO_REGISTER:
            urn = f"urn:li:dataPlatform:{name}"
            try:
                self._datahub.ingest_mcp(
                    urn=urn,
                    aspect_name="dataPlatformInfo",
                    data={
                        "name": name,
                        "type": "OTHERS",
                        "datasetNameDelimiter": ".",
                        "displayName": name.capitalize(),
                    },
                )
                log.info("registered dataPlatform: %s", urn)
            except Exception as exc:
                log.warning(
                    "could not register %s (will fall through to per-scenario "
                    "ingest which surfaces the underlying error): %s",
                    urn,
                    exc,
                )

    def _seed_io_pool_via_mysql(self, ctx: TestContext) -> None:
        """Seed the IO pool with direct-MySQL inserts.

        Bypasses the GMS write-path entirely: rows are written to
        ``metadata_aspect_v2`` with ``systemmetadata='{}'`` so they always
        land at ``schemaVersion=null`` (v1) in the DB regardless of
        ``ASPECT_MIGRATION_MUTATOR_ENABLED`` on the running GMS. This makes
        the deterministic race-window assertion (TC-403) reproducible across
        single-image and two-image-tag runs.

        See design doc §4 / Plan F-5 for the rationale (closes Notion D5).
        """
        for i in range(_IO_POOL_SIZE):
            pool_urn = f"urn:li:dashboard:(test,zdu-io-pool-{i})"
            self._mysql.upsert_aspect_raw(
                urn=pool_urn,
                aspect=_IO_POOL_ASPECT,
                metadata=json.dumps(_IO_POOL_OLD_DATA),
                systemmetadata="{}",
            )
            log.info(
                "IO-pool[%d]: direct-MySQL upsert → %s schemaVersion=null",
                i,
                pool_urn,
            )
            ctx.io_pool_entities.append(
                SeededEntity(
                    urn=pool_urn,
                    aspect_name=_IO_POOL_ASPECT,
                    tc_number=0,
                    seeded_data=_IO_POOL_OLD_DATA,
                    expected_schema_version=_IO_POOL_EXPECTED_VERSION,
                    validator=_noop_validator,
                )
            )
