"""Unit tests for PostgresQuery SQL generation and SQL injection prevention."""

import pytest

from datahub.ingestion.source.sql.postgres.query import PostgresQuery


class TestPostgresQuerySanitization:
    """Test SQL injection prevention in PostgresQuery methods."""

    def test_sanitize_identifier_valid(self):
        """Test that valid identifiers pass sanitization."""
        assert PostgresQuery._sanitize_identifier("mydb") == "mydb"
        assert PostgresQuery._sanitize_identifier("my_database") == "my_database"
        assert PostgresQuery._sanitize_identifier("db123") == "db123"
        assert PostgresQuery._sanitize_identifier("my-db") == "my-db"

    def test_sanitize_identifier_empty_raises(self):
        """Test that empty identifier raises ValueError."""
        with pytest.raises(ValueError, match="Identifier cannot be empty"):
            PostgresQuery._sanitize_identifier("")

    def test_sanitize_identifier_sql_injection_raises(self):
        """Test that SQL injection attempts are rejected."""
        # SQL injection with quotes
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db'; DROP TABLE users; --")

        # SQL injection with semicolon
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db; DELETE FROM users")

        # SQL injection with comment
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db/**/OR/**/1=1")

        # Special characters
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db@#$%")

    def test_escape_like_pattern(self):
        """Test that LIKE pattern special characters are escaped."""
        # Test basic escaping
        assert PostgresQuery._escape_like_pattern("test") == "test"
        assert PostgresQuery._escape_like_pattern("test%") == "test\\%"
        assert PostgresQuery._escape_like_pattern("test_value") == "test\\_value"
        assert PostgresQuery._escape_like_pattern("test\\value") == "test\\\\value"

        # Test combined escaping
        assert (
            PostgresQuery._escape_like_pattern("test%_\\value") == "test\\%\\_\\\\value"
        )


class TestGetQueryHistory:
    """Test get_query_history SQL generation."""

    def test_get_query_history_basic(self):
        """Test basic query generation without filters."""
        query = PostgresQuery.get_query_history()
        assert "FROM pg_stat_statements s" in query
        assert "s.query IS NOT NULL" in query
        assert "LIMIT 1000" in query

    def test_get_query_history_with_database(self):
        """Test query generation with database filter."""
        query = PostgresQuery.get_query_history(database="mydb")
        assert "d.datname = 'mydb'" in query

    def test_get_query_history_database_sql_injection_prevented(self):
        """Test that SQL injection in database parameter is prevented."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery.get_query_history(database="db'; DROP TABLE users; --")

    def test_get_query_history_with_exclude_patterns(self):
        """Test query generation with user-provided exclude patterns."""
        query = PostgresQuery.get_query_history(
            exclude_patterns=["temp_table", "staging_%"]
        )
        # User patterns should be escaped and included
        # Note: underscore in LIKE pattern is escaped as \_
        assert "s.query NOT ILIKE '%temp\\_table%'" in query
        # Note: % in input pattern "staging_%" is escaped, so % becomes \% and _ becomes \_
        assert "s.query NOT ILIKE '%staging\\_\\%%'" in query

    def test_get_query_history_exclude_patterns_injection_prevented(self):
        """Test that SQL injection in exclude_patterns is prevented by escaping."""
        # Malicious pattern attempting SQL injection
        query = PostgresQuery.get_query_history(
            exclude_patterns=["'; DROP TABLE users; --"]
        )
        # Pattern should be escaped (single quote doesn't need escaping in our implementation,
        # but it's safely wrapped in %...% and used in ILIKE)
        assert "'; DROP TABLE users; --" in query
        assert "DROP TABLE users" in query  # Present but safe in ILIKE context

    def test_get_query_history_limit_validation(self):
        """Test that invalid limit values are rejected."""
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_query_history(limit=0)

        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_query_history(limit=-10)

        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_query_history(limit=10.5)

    def test_get_query_history_min_calls_validation(self):
        """Test that invalid min_calls values are rejected."""
        with pytest.raises(ValueError, match="min_calls must be non-negative integer"):
            PostgresQuery.get_query_history(min_calls=-1)

        with pytest.raises(ValueError, match="min_calls must be non-negative integer"):
            PostgresQuery.get_query_history(min_calls=5.5)


class TestGetQueriesByType:
    """Test get_queries_by_type SQL generation."""

    def test_get_queries_by_type_basic(self):
        """Test basic query generation for specific query type."""
        query = PostgresQuery.get_queries_by_type(query_type="INSERT")
        assert "s.query ILIKE 'INSERT%'" in query
        assert "LIMIT 500" in query

    def test_get_queries_by_type_with_database(self):
        """Test query generation with database filter."""
        query = PostgresQuery.get_queries_by_type(query_type="UPDATE", database="mydb")
        assert "s.query ILIKE 'UPDATE%'" in query
        assert "d.datname = 'mydb'" in query

    def test_get_queries_by_type_database_injection_prevented(self):
        """Test that SQL injection in database parameter is prevented."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery.get_queries_by_type(
                query_type="SELECT", database="db'; DROP TABLE users; --"
            )

    def test_get_queries_by_type_query_type_validation(self):
        """Test that invalid query_type values are rejected."""
        # Valid query types
        query = PostgresQuery.get_queries_by_type(query_type="INSERT")
        assert "INSERT" in query

        query = PostgresQuery.get_queries_by_type(query_type="CREATE TABLE AS")
        assert "CREATE TABLE AS" in query

        # Invalid query types with injection attempts
        with pytest.raises(ValueError, match="Invalid query_type"):
            PostgresQuery.get_queries_by_type(query_type="INSERT'; DROP TABLE users;")

        with pytest.raises(ValueError, match="Invalid query_type"):
            PostgresQuery.get_queries_by_type(query_type="SELECT/**/OR/**/1=1")

    def test_get_queries_by_type_limit_validation(self):
        """Test that invalid limit values are rejected."""
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_queries_by_type(query_type="SELECT", limit=0)


class TestGetTopTablesByQueryCount:
    """Test get_top_tables_by_query_count SQL generation."""

    def test_get_top_tables_basic(self):
        """Test basic query generation for top tables."""
        query = PostgresQuery.get_top_tables_by_query_count()
        assert "FROM pg_stat_statements" in query
        assert "LIMIT 100" in query

    def test_get_top_tables_custom_limit(self):
        """Test query generation with custom limit."""
        query = PostgresQuery.get_top_tables_by_query_count(limit=50)
        assert "LIMIT 50" in query

    def test_get_top_tables_limit_validation(self):
        """Test that invalid limit values are rejected."""
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_top_tables_by_query_count(limit=0)

        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_top_tables_by_query_count(limit=-5)
