from __future__ import annotations

import pytest

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUReport, ZDUTestRunner
from tests.zdu.framework.scenario_loader import ScenarioLoader, ZDUTestScenario


@pytest.fixture(scope="session")
def zdu_config() -> ZDUTestConfig:
    return ZDUTestConfig.from_env()


@pytest.fixture(scope="session")
def zdu_scenarios() -> list[ZDUTestScenario]:
    return ScenarioLoader().load()


@pytest.fixture(scope="session")
def zdu_report(
    zdu_config: ZDUTestConfig,
    zdu_scenarios: list[ZDUTestScenario],
) -> ZDUReport:
    runner = ZDUTestRunner(zdu_config, scenarios=zdu_scenarios)
    return runner.run()
