"""Tag propagation plugin package.

This package contains all components related to tag propagation:
- Expectation classes for tag propagation validation
- Mutation classes for tag-related live testing
"""

from tests.propagation.framework.plugins.tag.expectations import (
    TagPropagationExpectation,
)

__all__ = [
    "TagPropagationExpectation",
]
