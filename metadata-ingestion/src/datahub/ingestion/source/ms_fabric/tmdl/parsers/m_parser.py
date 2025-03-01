import logging
import re
from typing import Any, Dict, Optional, Set

from datahub.ingestion.source.ms_fabric.tmdl.exceptions import ValidationError
from datahub.ingestion.source.powerbi.m_query.parser import get_lark_parser

logger = logging.getLogger(__name__)


class MockTable:
    """Mock table class to satisfy resolver requirements."""

    def __init__(self, name: str = "mock_table", expression: Optional[str] = None):
        """Initialize mock table."""
        self.name = name
        self.full_name = f"mock_schema.{name}"
        self.expression = expression
        self.database_name = "mock_database"
        self.schema_name = "mock_schema"

    @property
    def id(self) -> str:
        """Return table ID."""
        return f"{self.schema_name}.{self.name}"


class MParser:
    """Parser for M language expressions using PowerBI's existing M parser."""

    def __init__(self) -> None:
        """Initialize M parser."""
        self._lark_parser = get_lark_parser()
        self._last_validation_result: Optional[Dict[str, Any]] = None

    def validate(self, expression: str) -> None:
        """Validate M expression syntax."""
        try:
            parse_tree = self._lark_parser.parse(expression)
            self._last_validation_result = {
                "parse_tree": parse_tree,
                "expression": expression,
            }
        except Exception as e:
            raise ValidationError(f"Invalid M syntax: {str(e)}")

    def analyze_dependencies(self, expression: str) -> Set[str]:
        """Analyze expression dependencies."""
        try:
            # Validate if not already validated
            if (
                not self._last_validation_result
                or self._last_validation_result.get("expression") != expression
            ):
                self.validate(expression)

            dependencies = set()

            # Extract database references
            db_matches = re.finditer(r'Sql\.Database\s*\(\s*"([^"]+)"', expression)
            for match in db_matches:
                dependencies.add(match.group(1))

            # Extract schema/table references
            schema_matches = re.finditer(
                r'\[Schema="([^"]+)"\s*,\s*Item="([^"]+)"]', expression
            )
            for match in schema_matches:
                schema = match.group(1)
                table = match.group(2)
                dependencies.add(f"{schema}.{table}")

            return dependencies

        except Exception as e:
            logger.warning(f"Error analyzing dependencies: {str(e)}")
            return set()
