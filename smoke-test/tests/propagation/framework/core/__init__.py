"""Core framework components for propagation testing.

This module contains the essential building blocks of the propagation test framework:
- Base classes and framework infrastructure
- Data models and expectations
- Validation utilities
- Action lifecycle management
"""

from tests.propagation.framework.core.base import (
    BasePropagationTest,
    DocumentationPropagationTest,
    PropagationTestConfig,
    PropagationTestFramework,
    PropagationTestResult,
    TagPropagationTest,
    TermPropagationTest,
    TestPhase,
)

# Base expectation class
from tests.propagation.framework.core.expectations import ExpectationBase
from tests.propagation.framework.core.models import (
    DocumentationPropagationExpectation,
    PropagationExpectation,
    PropagationTestScenario,
    TagPropagationExpectation,
    TermPropagationExpectation,
)
from tests.propagation.framework.core.mutations import BaseMutation, FieldMutation

# Plugin-specific expectations
from tests.propagation.framework.plugins.documentation.expectations import (
    DatasetDocumentationPropagationExpectation,
    DocumentationPropagationExpectation as TypedDocumentationPropagationExpectation,
    NoDocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.tag.expectations import (
    DatasetTagPropagationExpectation,
    TagPropagationExpectation as TypedTagPropagationExpectation,
)
from tests.propagation.framework.plugins.term.expectations import (
    NoTermPropagationExpectation,
    TermPropagationExpectation as TypedTermPropagationExpectation,
)

__all__ = [
    # Base framework classes
    "PropagationTestFramework",
    "PropagationTestConfig",
    "PropagationTestResult",
    "TestPhase",
    "BasePropagationTest",
    # Test types
    "TermPropagationTest",
    "TagPropagationTest",
    "DocumentationPropagationTest",
    # Models and expectations (legacy)
    "PropagationTestScenario",
    "PropagationExpectation",
    "TermPropagationExpectation",
    "TagPropagationExpectation",
    "DocumentationPropagationExpectation",
    # Typed expectations
    "ExpectationBase",
    "TypedTermPropagationExpectation",
    "NoTermPropagationExpectation",
    "TypedTagPropagationExpectation",
    "DatasetTagPropagationExpectation",
    "TypedDocumentationPropagationExpectation",
    "DatasetDocumentationPropagationExpectation",
    "NoDocumentationPropagationExpectation",
    # Base mutation classes
    "BaseMutation",
    "FieldMutation",
]
