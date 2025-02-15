import logging
import re
from typing import Optional

from datahub.sql_parsing import sqlglot_utils

logger = logging.getLogger(__name__)


class SQLUtils:
    """Utilities for SQL operations."""

    @staticmethod
    def clean_sql(sql: str) -> str:
        """Clean SQL string."""
        if not sql:
            return ""

        # Remove comments
        sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        # Normalize whitespace
        sql = re.sub(r"\s+", " ", sql.strip())

        return sql

    @staticmethod
    def set_dialect(custom_dialect: Optional[str]) -> None:
        """Set SQL dialect for parsing."""

        def custom_sql_dialect(platform: str) -> str:
            return custom_dialect if custom_dialect else "postgres"

        sqlglot_utils._get_dialect_str = custom_sql_dialect
