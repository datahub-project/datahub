from __future__ import annotations

import os

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
    # Defense-in-depth (in addition to the module-level skipif in
    # test_zdu_upgrade.py): requesting this fixture launches the full,
    # stack-destructive ZDU pipeline. Refuse to run it unless explicitly
    # opted in via ZDU_E2E_ENABLED so that any future pytest module
    # depending on this fixture can't accidentally fire it inside the
    # regular smoke-test batches.
    if os.environ.get("ZDU_E2E_ENABLED") not in ("1", "true", "True", "yes"):
        pytest.skip(
            "ZDU end-to-end pipeline is destructive (nukes the Compose "
            "stack). Set ZDU_E2E_ENABLED=1 to run."
        )
    runner = ZDUTestRunner(zdu_config, scenarios=zdu_scenarios)
    return runner.run()
