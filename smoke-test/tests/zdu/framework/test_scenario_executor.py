"""Unit tests for ScenarioTypeRegistry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from tests.zdu.framework.context import TestContext, ValidationResult
from tests.zdu.framework.scenario_executor import (
    ScenarioTypeExecutor,
    ScenarioTypeRegistry,
)
from tests.zdu.framework.scenario_loader import ZDUTestScenario
from tests.zdu.framework.suite import Suite


def _scenario(tc: int, stype: str) -> ZDUTestScenario:
    return ZDUTestScenario(
        tc_number=tc,
        category="x",
        name="x",
        description="",
        prerequisite_steps="",
        test_steps="",
        expected_result="",
        current_status="",
        details="",
        starting_schema_version=None,
        expected_schema_version=None,
        action="",
        aspect_name="embed",
        entity_type="dataset",
        expected_to_fail=False,
        skip_reason=None,
        scenario_type=stype,
        suite=Suite.B,
    )


@dataclass
class _FakeExecutor:
    label: str

    def validate(self, scenario: ZDUTestScenario, ctx: TestContext) -> ValidationResult:
        return ValidationResult(
            tc_number=scenario.tc_number,
            name=self.label,
            status="PASS",
            expected_to_fail=False,
            actual_result=f"validated by {self.label}",
        )


def test_register_and_dispatch():
    reg = ScenarioTypeRegistry()
    reg.register("aspect_migration", cast(ScenarioTypeExecutor, _FakeExecutor("am")))
    result = reg.validate(_scenario(1, "aspect_migration"), TestContext())
    assert result.status == "PASS"
    assert "am" in result.actual_result


def test_unknown_type_returns_skip():
    reg = ScenarioTypeRegistry()
    result = reg.validate(_scenario(1, "no_such_type"), TestContext())
    assert result.status == "SKIP"
    assert "no_such_type" in result.actual_result


def test_double_register_raises():
    reg = ScenarioTypeRegistry()
    reg.register("aspect_migration", cast(ScenarioTypeExecutor, _FakeExecutor("a")))
    with pytest.raises(ValueError, match="already registered"):
        reg.register("aspect_migration", cast(ScenarioTypeExecutor, _FakeExecutor("b")))
