from datetime import datetime, timezone

import pytest

from datahub.metadata.schema_classes import DatasetLineageTypeClass
from datahub.sql_parsing.sql_parsing_aggregator import QueryMetadata
from datahub.sql_parsing.sql_parsing_common import QueryType


def create_test_query_metadata(
    raw_query: str, query_id: str = "query123"
) -> QueryMetadata:
    """Helper to create QueryMetadata with minimal required fields."""
    return QueryMetadata(
        query_id=query_id,
        raw_query_string=raw_query,
        session_id="test_session",
        query_type=QueryType.SELECT,
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        latest_timestamp=datetime.now(tz=timezone.utc),
        actor=None,
        upstreams=[],
        column_lineage=[],
        column_usage={},
        confidence_score=1.0,
    )


class TestLazyQueryFormatting:
    """Tests for lazy query formatting behavior."""

    def test_raw_query_stored_not_formatted(self):
        """Test that raw query is stored, not formatted query."""
        raw_query = "SELECT a, b FROM foo WHERE x > 10"

        query_metadata = create_test_query_metadata(raw_query)

        assert query_metadata.raw_query_string == raw_query
        assert query_metadata._formatted_query_cache is None

    @pytest.mark.parametrize(
        "format_queries,should_format,description",
        [
            (True, True, "formatting enabled - formats and caches"),
            (False, False, "formatting disabled - returns raw query"),
        ],
    )
    def test_get_formatted_query_behavior(
        self, format_queries: bool, should_format: bool, description: str
    ) -> None:
        """Test get_formatted_query behavior with format_queries flag."""
        raw_query = "SELECT a, b FROM foo WHERE x > 10"
        query_metadata = create_test_query_metadata(raw_query)

        assert query_metadata._formatted_query_cache is None

        formatted = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=format_queries
        )

        if should_format:
            assert formatted is not None
            assert formatted != raw_query
            assert query_metadata._formatted_query_cache == formatted
        else:
            assert formatted == raw_query
            assert query_metadata._formatted_query_cache is None

    def test_get_formatted_query_returns_cached(self):
        """Test that subsequent calls return cached formatted query."""
        raw_query = "SELECT a, b FROM foo"
        query_metadata = create_test_query_metadata(raw_query)

        query_metadata.get_formatted_query(platform="bigquery", format_queries=True)

        query_metadata._formatted_query_cache = "CACHED_VALUE"

        formatted2 = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=True
        )

        assert formatted2 == "CACHED_VALUE"

    def test_cache_cleared_on_query_update(self):
        """Test that formatted cache is cleared when query is updated."""
        import dataclasses

        raw_query = "SELECT a FROM foo"

        query_metadata = create_test_query_metadata(raw_query)

        formatted1 = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=True
        )
        assert query_metadata._formatted_query_cache is not None

        updated_query = dataclasses.replace(
            query_metadata,
            raw_query_string="SELECT a, b FROM foo_staging; SELECT * FROM foo_staging",
        )

        assert (
            updated_query._formatted_query_cache is None
            or updated_query._formatted_query_cache != formatted1
        )

    def test_composite_query_formatting(self):
        """Test that composite queries (multiple statements) are formatted correctly."""
        composite_query = "CREATE TABLE foo_staging AS SELECT a, b FROM foo_dep; CREATE TABLE foo_downstream AS SELECT a, b FROM foo_staging"

        query_metadata = create_test_query_metadata(composite_query)

        formatted = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=True
        )

        assert "foo_staging" in formatted
        assert "foo_downstream" in formatted
        assert formatted.count("CREATE") == 2 or formatted.count("SELECT") >= 2

    def test_formatted_query_cache_reused(self):
        """Test that pre-cached formatted query is used."""
        raw_query = "SELECT a FROM foo"
        pre_formatted = "CACHED_FORMATTED_QUERY"

        query_metadata = create_test_query_metadata(raw_query)
        query_metadata._formatted_query_cache = pre_formatted

        formatted = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=True
        )

        assert formatted == pre_formatted

    def test_formatting_performance_with_large_query(self):
        """Test that formatting is only done once for large queries."""
        columns = ", ".join([f"col{i}" for i in range(100)])
        raw_query = f"SELECT {columns} FROM large_table WHERE id > 1000"

        query_metadata = create_test_query_metadata(raw_query)

        formatted1 = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=True
        )

        cached_value = query_metadata._formatted_query_cache
        assert cached_value is not None

        formatted2 = query_metadata.get_formatted_query(
            platform="bigquery", format_queries=True
        )

        assert formatted2 == formatted1
        assert formatted2 is cached_value
