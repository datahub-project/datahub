"""Documentation propagation plugin package.

This package contains all components related to documentation propagation:
- Expectation classes for documentation propagation validation
- Mutation classes for documentation-related live testing
"""

from tests.propagation.framework.plugins.documentation.expectations import (
    DocumentationPropagationExpectation,
)

__all__ = [
    "DocumentationPropagationExpectation",
]
