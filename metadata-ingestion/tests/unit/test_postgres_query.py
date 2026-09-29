import pytest

from datahub.ingestion.source.sql.postgres.query import PostgresQuery


class TestPostgresQuerySanitization:
    def test_sanitize_identifier_valid(self):
        assert PostgresQuery._sanitize_identifier("mydb") == "mydb"
        assert PostgresQuery._sanitize_identifier("my_database") == "my_database"
        assert PostgresQuery._sanitize_identifier("db123") == "db123"
        assert PostgresQuery._sanitize_identifier("_private") == "_private"

    def test_sanitize_identifier_empty_raises(self):
        with pytest.raises(ValueError, match="Identifier cannot be empty"):
            PostgresQuery._sanitize_identifier("")

    def test_sanitize_identifier_sql_injection_raises(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db'; DROP TABLE users; --")

        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db; DELETE FROM users")

        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db/**/OR/**/1=1")

        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("db@#$%")

        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("my-db")

        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery._sanitize_identifier("123_table")


class TestGetQueryHistory:
    def test_get_query_history_basic(self):
        query, params = PostgresQuery.get_query_history()
        query_str = str(query)
        assert "FROM pg_stat_statements s" in query_str
        assert "s.query IS NOT NULL" in query_str
        assert "LIMIT :limit" in query_str
        assert params["limit"] == 1000
        assert params["min_calls"] == 1

    def test_get_query_history_with_database(self):
        query, params = PostgresQuery.get_query_history(database="mydb")
        query_str = str(query)
        assert "d.datname = :database" in query_str
        assert params["database"] == "mydb"

    def test_get_query_history_database_sql_injection_prevented(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            PostgresQuery.get_query_history(database="db'; DROP TABLE users; --")

    def test_get_query_history_with_exclude_patterns(self):
        query, params = PostgresQuery.get_query_history(
            exclude_patterns=["temp_table", "staging_%"]
        )
        query_str = str(query)
        # Default exclusions are parameterized (0-4), user patterns start at 5
        assert "s.query NOT ILIKE :exclude_pattern_5" in query_str
        assert "s.query NOT ILIKE :exclude_pattern_6" in query_str
        assert params["exclude_pattern_5"] == "%temp_table%"
        assert params["exclude_pattern_6"] == "%staging_%%"
        # Verify default exclusions are also parameterized
        assert "exclude_pattern_0" in params
        assert "exclude_pattern_1" in params

    def test_get_query_history_exclude_patterns_injection_prevented(self):
        # Malicious pattern attempting SQL injection
        query, params = PostgresQuery.get_query_history(
            exclude_patterns=["'; DROP TABLE users; --"]
        )
        query_str = str(query)
        # Pattern should be safely parameterized (after default exclusions 0-4)
        assert "s.query NOT ILIKE :exclude_pattern_5" in query_str
        # The malicious content is in the parameters, not in the SQL query itself
        assert params["exclude_pattern_5"] == "%'; DROP TABLE users; --%"
        # The actual query should NOT contain the malicious SQL
        assert "DROP TABLE users" not in query_str

    def test_get_query_history_limit_validation(self):
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_query_history(limit=0)

        with pytest.raises(ValueError, match="limit must be a positive integer"):
            PostgresQuery.get_query_history(limit=-10)

        assert PostgresQuery.get_query_history(limit=10)

    def test_get_query_history_min_calls_validation(self):
        with pytest.raises(ValueError, match="min_calls must be non-negative integer"):
            PostgresQuery.get_query_history(min_calls=-1)

        assert PostgresQuery.get_query_history(min_calls=5)
