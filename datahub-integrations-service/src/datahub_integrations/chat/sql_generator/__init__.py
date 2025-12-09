"""
SQL Generator module for text-to-SQL generation using DataHub semantic understanding.

This module provides tools for generating SQL queries from natural language
by building semantic models from DataHub metadata.
"""

from datahub_integrations.chat.sql_generator.models import (
    Dimension,
    Fact,
    LogicalTable,
    Relationship,
    SemanticModel,
    TimeDimension,
)
from datahub_integrations.chat.sql_generator.semantic_model_builder import (
    SemanticModelBuilder,
)
from datahub_integrations.chat.sql_generator.tools import generate_sql

__all__ = [
    # Models
    "Dimension",
    "Fact",
    "LogicalTable",
    "Relationship",
    "SemanticModel",
    "TimeDimension",
    # Builder
    "SemanticModelBuilder",
    # Tools
    "generate_sql",
]
