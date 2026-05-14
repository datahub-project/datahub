import warnings

import pytest

from tests.zdu.framework.runner import ZDUReport
from tests.zdu.framework.scenario_loader import ZDUTestScenario

# ── Infrastructure phase tests ───────────────────────────────────────────────


def _get_phase_or_skip(zdu_report: ZDUReport, name: str):
    try:
        return zdu_report.phase(name)
    except KeyError:
        pytest.skip(f"Phase '{name}' was skipped in this run")


def test_discovery_phase(zdu_report: ZDUReport) -> None:
    phase = _get_phase_or_skip(zdu_report, "discovery")
    assert phase.status == "passed", f"Discovery failed: {phase.error}"


def test_seed_phase(zdu_report: ZDUReport) -> None:
    phase = _get_phase_or_skip(zdu_report, "seed")
    assert phase.status == "passed", f"Seed failed: {phase.error}"
    assert phase.details.get("seeded", 0) > 0, "No entities seeded"


def test_upgrade_phase(zdu_report: ZDUReport) -> None:
    phase = _get_phase_or_skip(zdu_report, "upgrade")
    assert phase.status == "passed", f"Upgrade failed: {phase.error}"


def test_upgrade_nonblocking_phase(zdu_report: ZDUReport) -> None:
    phase = _get_phase_or_skip(zdu_report, "upgrade_nonblocking")
    assert phase.status == "passed", f"NonBlocking upgrade failed: {phase.error}"
    assert phase.details.get("total_migrated", 0) > 0, "Sweep migrated 0 entities"
    assert phase.details.get("write_failures", 0) == 0, (
        f"{phase.details['write_failures']} concurrent write(s) got wrong schemaVersion"
    )


# ── Per-scenario tests ───────────────────────────────────────────────────────


def _scenario_id(scenario: ZDUTestScenario) -> str:
    return f"TC-{scenario.tc_number:03d}"


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Inject zdu_scenarios into test_scenario parametrize."""
    if "scenario" in metafunc.fixturenames:
        scenarios = (
            metafunc.config._zdu_scenarios
            if hasattr(metafunc.config, "_zdu_scenarios")
            else []
        )
        params = [
            pytest.param(
                s,
                marks=getattr(pytest.mark, f"suite_{s.suite.value}"),
                id=_scenario_id(s),
            )
            for s in scenarios
        ]
        metafunc.parametrize("scenario", params)


@pytest.fixture(scope="session", autouse=True)
def _cache_scenarios_on_config(
    request: pytest.FixtureRequest,
    zdu_scenarios: list[ZDUTestScenario],
) -> None:
    request.config._zdu_scenarios = zdu_scenarios  # type: ignore[attr-defined]


def test_scenario(
    scenario: ZDUTestScenario,
    zdu_report: ZDUReport,
) -> None:
    if scenario.tc_number == 23:
        pytest.skip("TC-023 Rolling Upgrade — test steps not yet defined in sheet")

    result = zdu_report.scenario_result(scenario.tc_number)

    if result.status == "XFAIL":
        pytest.xfail(result.failure_reason or scenario.skip_reason or "known failure")

    if result.status == "SKIP":
        pytest.skip(result.actual_result)

    if result.status == "XPASS":
        warnings.warn(
            f"TC-{scenario.tc_number:03d} XPASS: {scenario.name} — "
            f"known failure now passes; consider removing from KNOWN_FAILURES",
            stacklevel=2,
        )
        return

    assert result.status == "PASS", (
        f"TC-{scenario.tc_number:03d} [{scenario.name}] FAILED\n"
        f"  Expected: {scenario.expected_result}\n"
        f"  Actual:   {result.actual_result}\n"
        f"  Reason:   {result.failure_reason}"
    )
