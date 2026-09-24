"""Strategy pattern for per-scenario-type validation.

Each ``scenario_type`` (``aspect_migration``, ``es_phase1``, …) has one
executor implementing :class:`ScenarioTypeExecutor`. ValidationPhase
dispatches by asking the registry to validate a scenario; the registry
returns ``SKIP`` for unregistered types so a missing executor never
silently passes.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .context import TestContext, ValidationResult
from .scenario_loader import ZDUTestScenario


@runtime_checkable
class ScenarioTypeExecutor(Protocol):
    """Implemented by per-scenario-type validators (one per phase plan)."""

    def validate(
        self, scenario: ZDUTestScenario, ctx: TestContext
    ) -> ValidationResult: ...


class ScenarioTypeRegistry:
    """Maps ``scenario_type`` → :class:`ScenarioTypeExecutor`."""

    def __init__(self) -> None:
        self._executors: dict[str, ScenarioTypeExecutor] = {}

    def register(self, scenario_type: str, executor: ScenarioTypeExecutor) -> None:
        if scenario_type in self._executors:
            raise ValueError(
                f"Executor for scenario_type {scenario_type!r} already registered"
            )
        self._executors[scenario_type] = executor

    def validate(self, scenario: ZDUTestScenario, ctx: TestContext) -> ValidationResult:
        executor = self._executors.get(scenario.scenario_type)
        if executor is None:
            return ValidationResult(
                tc_number=scenario.tc_number,
                name=scenario.name,
                status="SKIP",
                expected_to_fail=False,
                actual_result=(
                    f"No executor registered for scenario_type "
                    f"{scenario.scenario_type!r}"
                ),
            )
        return executor.validate(scenario, ctx)
