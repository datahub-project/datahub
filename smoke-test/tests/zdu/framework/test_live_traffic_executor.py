"""Unit tests for LiveTrafficExecutor."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from tests.zdu.framework.context import (
    DataIntegritySnapshot,
    IOObservation,
    IOWriteResult,
    TestContext,
)
from tests.zdu.framework.live_traffic_executor import LiveTrafficExecutor
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _scenario(
    tc: int,
    *,
    expected_to_fail: bool = False,
    skip_reason: str | None = None,
    expected_schema_version: int | None = None,
) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="Live Traffic",
        name=f"TC-{tc}",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=expected_schema_version,
        action="live_traffic",
        aspect_name="",
        entity_type="",
        expected_to_fail=expected_to_fail,
        skip_reason=skip_reason,
        scenario_type="live_traffic",
        suite=Suite.F,
    )


@pytest.fixture
def executor() -> LiveTrafficExecutor:
    return LiveTrafficExecutor()


class TestSeedIsNoop:
    def test_seed_returns_empty(self, executor: LiveTrafficExecutor) -> None:
        # Suite F doesn't seed per-scenario.
        assert executor.seed(_scenario(tc=403)) == []


class TestXfailDispatch:
    def test_expected_to_fail_returns_xfail_with_skip_reason(
        self, executor: LiveTrafficExecutor
    ) -> None:
        scen = _scenario(tc=401, expected_to_fail=True, skip_reason="needs load gen")
        result = executor.validate(scen, TestContext())
        assert result.status == "XFAIL"
        assert result.expected_to_fail is True
        assert result.failure_reason == "needs load gen"


class TestUnknownTc:
    def test_unknown_tc_returns_skip(self, executor: LiveTrafficExecutor) -> None:
        result = executor.validate(_scenario(tc=599), TestContext())
        assert result.status == "SKIP"
        assert "No validator registered" in (result.actual_result or "")


class TestTC501WritesPersistAtTarget:
    def _ctx_with_versions(self, versions: dict[str, int | None]) -> TestContext:
        ctx = TestContext()
        snap = DataIntegritySnapshot(entity_index_alias="dashboardindex_v2")
        snap.embed_schema_versions = dict(versions)
        ctx.data_integrity_snapshot = snap
        return ctx

    def test_passes_when_all_urns_at_target(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = self._ctx_with_versions(
            {"urn:li:dashboard:(t,a)": 4, "urn:li:dashboard:(t,b)": 4}
        )
        result = executor.validate(_scenario(tc=401, expected_schema_version=4), ctx)
        assert result.status == "PASS"
        assert "schemaVersion=4" in (result.actual_result or "")

    def test_fails_when_any_urn_below_target(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = self._ctx_with_versions(
            {"urn:li:dashboard:(t,a)": 4, "urn:li:dashboard:(t,b)": 3}
        )
        result = executor.validate(_scenario(tc=401, expected_schema_version=4), ctx)
        assert result.status == "FAIL"
        assert "urn:li:dashboard:(t,b)" in (result.actual_result or "")

    def test_fails_when_urn_missing_schema_version(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = self._ctx_with_versions({"urn:li:dashboard:(t,a)": None})
        result = executor.validate(_scenario(tc=401, expected_schema_version=4), ctx)
        assert result.status == "FAIL"

    def test_skips_when_snapshot_missing(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        result = executor.validate(_scenario(tc=401, expected_schema_version=4), ctx)
        assert result.status == "SKIP"

    def test_skips_when_expected_schema_version_unset(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = self._ctx_with_versions({"urn:li:dashboard:(t,a)": 4})
        result = executor.validate(_scenario(tc=401), ctx)
        assert result.status == "SKIP"


class TestTC502ReadsMonotonicPerUrn:
    @staticmethod
    def _obs(urn: str, version: int, offset_s: int) -> IOObservation:
        return IOObservation(
            worker="reader-0",
            urn=urn,
            aspect_name="embed",
            observed_version=version,
            expected_version=version,
            timestamp=datetime(2026, 1, 1) + timedelta(seconds=offset_s),
        )

    def test_passes_when_each_urn_monotonic(
        self, executor: LiveTrafficExecutor
    ) -> None:
        ctx = TestContext()
        ctx.io_observations = [
            self._obs("urn:a", 1, 0),
            self._obs("urn:a", 2, 1),
            self._obs("urn:a", 2, 2),
            self._obs("urn:b", 3, 0),
            self._obs("urn:b", 4, 1),
        ]
        result = executor.validate(_scenario(tc=402), ctx)
        assert result.status == "PASS"

    def test_fails_on_version_regression(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        ctx.io_observations = [
            self._obs("urn:a", 4, 0),
            self._obs("urn:a", 2, 1),
        ]
        result = executor.validate(_scenario(tc=402), ctx)
        assert result.status == "FAIL"
        assert "urn:a" in (result.actual_result or "")

    def test_skips_when_no_observations(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        result = executor.validate(_scenario(tc=402), ctx)
        assert result.status == "SKIP"

    def test_skips_when_no_urn_read_twice(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        ctx.io_observations = [
            self._obs("urn:a", 2, 0),
            self._obs("urn:b", 3, 1),
        ]
        result = executor.validate(_scenario(tc=402), ctx)
        assert result.status == "SKIP"

    def test_unsorted_input_sorted_by_timestamp(
        self, executor: LiveTrafficExecutor
    ) -> None:
        # Out-of-order capture must still be sorted before the monotonic check.
        ctx = TestContext()
        ctx.io_observations = [
            self._obs("urn:a", 4, 5),
            self._obs("urn:a", 2, 0),
            self._obs("urn:a", 3, 2),
        ]
        result = executor.validate(_scenario(tc=402), ctx)
        assert result.status == "PASS"


class TestTC503ConcurrentWrites:
    def test_passes_when_all_writes_passed(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        ctx.io_write_results = [
            IOWriteResult(
                worker="writer-0",
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                observed_version=4,
                expected_version=4,
                passed=True,
                timestamp=datetime.utcnow(),
            ),
        ]
        result = executor.validate(_scenario(tc=403), ctx)
        assert result.status == "PASS"
        assert result.expected_to_fail is False

    def test_fails_when_some_writes_failed(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        ctx.io_write_results = [
            IOWriteResult(
                worker="writer-0",
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                observed_version=4,
                expected_version=4,
                passed=True,
                timestamp=datetime.utcnow(),
            ),
            IOWriteResult(
                worker="writer-1",
                urn="urn:li:dashboard:(test,zdu-io-pool-1)",
                observed_version=3,
                expected_version=4,
                passed=False,
                timestamp=datetime.utcnow(),
                error="version mismatch",
            ),
        ]
        result = executor.validate(_scenario(tc=403), ctx)
        assert result.status == "FAIL"
        assert result.expected_to_fail is False
        assert "sweep clobbered" in (result.failure_reason or "")
        assert "zdu-io-pool-1" in (result.actual_result or "")

    def test_skips_when_no_io_writes(self, executor: LiveTrafficExecutor) -> None:
        ctx = TestContext()
        ctx.io_write_results = []
        result = executor.validate(_scenario(tc=403), ctx)
        assert result.status == "SKIP"
        assert result.expected_to_fail is False
