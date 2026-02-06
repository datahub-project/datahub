"""JDBC URL parsing and SQL extraction for Google Sheets formulas."""

import logging
from typing import List

from datahub.ingestion.source.google_sheets.constants import (
    BIGQUERY_JDBC_PATTERN,
    JDBC_DATABASE_PARAM_PATTERN,
    JDBC_DATABASE_PATH_PATTERN,
    POSTGRES_MYSQL_JDBC_PATTERN,
    REDSHIFT_JDBC_PATTERN,
    SNOWFLAKE_JDBC_PATTERN,
    SQL_SELECT_PATTERN,
)
from datahub.ingestion.source.google_sheets.models import JdbcParseResult

logger = logging.getLogger(__name__)


class JDBCParser:
    """Parses JDBC URLs and extracts SQL queries from formulas."""

    JDBC_PATTERNS = [
        BIGQUERY_JDBC_PATTERN,
        SNOWFLAKE_JDBC_PATTERN,
        POSTGRES_MYSQL_JDBC_PATTERN,
        REDSHIFT_JDBC_PATTERN,
    ]

    PLATFORM_MATCHERS = {
        "bigquery": ("jdbc:bigquery", JDBC_DATABASE_PATH_PATTERN),
        "snowflake": ("jdbc:snowflake", JDBC_DATABASE_PARAM_PATTERN),
        "postgres": ("jdbc:postgresql", JDBC_DATABASE_PATH_PATTERN),
        "mysql": ("jdbc:mysql", JDBC_DATABASE_PATH_PATTERN),
        "redshift": ("jdbc:redshift", JDBC_DATABASE_PATH_PATTERN),
    }

    @classmethod
    def extract_jdbc_urls(cls, formula: str) -> List[str]:
        """Extract all JDBC connection strings from a formula."""
        urls = []
        for pattern in cls.JDBC_PATTERNS:
            for match in pattern.finditer(formula):
                urls.append(match.group(0))
        return urls

    @classmethod
    def parse_jdbc_url_and_sql(cls, jdbc_url: str, formula: str) -> JdbcParseResult:
        """Parse JDBC URL to get platform, database, and extract SQL query."""
        platform, database = cls._parse_jdbc_url(jdbc_url)

        if not platform or not database:
            return JdbcParseResult(platform=None, database=None, sql_query=None)

        sql_query = cls._extract_sql_from_formula(formula)

        return JdbcParseResult(
            platform=platform, database=database, sql_query=sql_query
        )

    @classmethod
    def _parse_jdbc_url(cls, jdbc_url: str) -> tuple[str | None, str | None]:
        """Parse JDBC URL to extract platform and database name."""
        jdbc_lower = jdbc_url.lower()

        for platform, (prefix, pattern) in cls.PLATFORM_MATCHERS.items():
            if prefix in jdbc_lower:
                match = pattern.search(jdbc_url)
                if match:
                    database = match.group(1)
                    return platform, database

        return None, None

    @classmethod
    def _extract_sql_from_formula(cls, formula: str) -> str | None:
        """Extract SQL query from formula."""
        sql_match = SQL_SELECT_PATTERN.search(formula)
        if sql_match:
            return sql_match.group(0).strip()
        return None
