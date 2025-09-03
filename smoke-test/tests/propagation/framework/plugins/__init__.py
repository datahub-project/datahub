"""Plugin system for propagation tests.

This module provides expectation and mutation classes organized by feature:
- term/: Glossary term propagation components
- tag/: Tag propagation components
- documentation/: Documentation propagation components
"""

from tests.propagation.framework.plugins.documentation import (
    DocumentationPropagationExpectation,
)
from tests.propagation.framework.plugins.tag import TagPropagationExpectation

__all__ = [
    # Tag components
    "TagPropagationExpectation",
    # Documentation components
    "DocumentationPropagationExpectation",
]
