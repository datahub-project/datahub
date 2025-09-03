"""Propagation test framework for DataHub smoke tests.

Provides a clean, fluent API for building propagation test scenarios.
"""

from tests.propagation.framework.builders import PropagationScenarioBuilder
from tests.propagation.framework.core import (
    BasePropagationTest,
    DocumentationPropagationTest,
    PropagationTestConfig,
    PropagationTestFramework,
    PropagationTestResult,
    PropagationTestScenario,
    TagPropagationTest,
    TermPropagationTest,
    TestPhase,
)
from tests.propagation.framework.plugins import (
    DocumentationPropagationExpectation,
    TagPropagationExpectation,
)
from tests.propagation.framework.utils import (
    TestScenarioLibrary,
    create_standard_fixtures,
    run_simple_propagation_test,
)

__all__ = [
    # Core framework
    "PropagationTestFramework",
    "PropagationTestConfig",
    "PropagationTestResult",
    "TestPhase",
    "BasePropagationTest",
    "TermPropagationTest",
    "TagPropagationTest",
    "DocumentationPropagationTest",
    # Builders
    "PropagationScenarioBuilder",
    # Plugins
    "TagPropagationExpectation",
    "DocumentationPropagationExpectation",
    # Utilities
    "TestScenarioLibrary",
    "create_standard_fixtures",
    "run_simple_propagation_test",
    # Models
    "PropagationTestScenario",
]
