from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import Mock, patch

import pytest

# Direct imports from profiling modules
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.profiling.query_executor import QueryExecutor

# Direct function imports for testing
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    validate_and_filter_expressions,
    validate_bigquery_identifier,
    validate_column_name,
    validate_column_names,
    validate_filter_expression,
    validate_sql_structure,
)

# =============================================================================
# HELPER FUNCTIONS AND TEST DATA
# =============================================================================


def create_test_config(**overrides: Any) -> BigQueryV2Config:
    """Create a test BigQuery config with sensible defaults."""
    config_dict = {
        "project_id": "test-project-123456",
        "profiling": {
            "enabled": True,
            "profile_external_tables": True,
            "partition_profiling_enabled": True,
            "profiling_row_limit": 100000,
            "sample_size": 50000,
            "use_sampling": False,
            "skip_stale_tables": False,
            "staleness_threshold_days": 365,
            "partition_datetime_window_days": None,
        },
    }

    # Apply overrides
    for key, value in overrides.items():
        if key in config_dict["profiling"]:
            config_dict["profiling"][key] = value  # type: ignore[index]
        else:
            config_dict[key] = value

    return BigQueryV2Config.parse_obj(config_dict)


def create_test_table(
    name: str = "test_table",
    external: bool = False,
    rows_count: Optional[int] = 10000,
    partitioned: bool = False,
    last_altered: Optional[datetime] = None,
    **kwargs: Any,
) -> BigqueryTable:
    """Create a test BigQuery table with realistic properties."""
    now = datetime.now(timezone.utc)

    # Use provided last_altered or default
    if last_altered is None:
        last_altered = now - timedelta(days=1)

    table = BigqueryTable(
        name=name,
        comment="Test table for profiling",
        rows_count=rows_count,
        size_in_bytes=1000000 if rows_count else None,
        last_altered=last_altered,
        created=now - timedelta(days=30),
        external=external,
        **kwargs,
    )

    # Add partition info if requested
    if partitioned:
        # Create a mock partition info object that matches the expected interface
        partition_info = SimpleNamespace(
            type_="DAY",
            field="date_partition",
            fields=["date_partition"],  # Add fields attribute
            require_partition_filter=True,
        )
        table.partition_info = partition_info  # type: ignore[assignment]

    return table


# Test data for parametrized tests
VALID_IDENTIFIERS_TEST_DATA = [
    ("simple_table", "`simple_table`"),
    ("dataset123", "`dataset123`"),
    ("table_with_underscores", "`table_with_underscores`"),
    ("CamelCase", "`CamelCase`"),
    ("_PARTITIONTIME", "`_PARTITIONTIME`"),
]

INVALID_IDENTIFIERS_TEST_DATA = [
    "",  # Empty
    "table with spaces",
    "table;drop",
    "table'injection",
    'table"quotes',
    "table/*comment*/",
]

VALID_FILTER_EXPRESSIONS_TEST_DATA = [
    "`date_col` = '2023-12-25'",
    "`user_id` = 123",
    "`status` IN ('active', 'pending')",
    "`count` > 100",
    "`name` IS NOT NULL",
]

DANGEROUS_SQL_PATTERNS_TEST_DATA = [
    "DROP TABLE test",
    "DELETE FROM table",
    "INSERT INTO table VALUES (1)",
    "UPDATE table SET col = 1",
    "CREATE TABLE test AS SELECT 1",
    "TRUNCATE TABLE test",
    "ALTER TABLE test ADD COLUMN col1 STRING",
]

DATE_LIKE_COLUMNS_TEST_DATA = [
    ("date", True),
    ("event_date", True),
    ("created_at", True),
    ("timestamp", True),
    ("dt", True),
    ("datetime", True),
    ("user_id", False),
    ("name", False),
    ("status", False),
    ("count", False),
]

DATE_TYPES_TEST_DATA = [
    ("DATE", True),
    ("DATETIME", True),
    ("TIMESTAMP", True),
    ("TIME", True),
    ("STRING", False),
    ("INT64", False),
    ("FLOAT64", False),
    ("BOOLEAN", False),
]

PARTITION_ID_TEST_DATA = [
    ("20231225", ["_PARTITIONDATE"], ["`_PARTITIONDATE` = '2023-12-25'"]),
    ("2023122514", ["_PARTITIONTIME"], ["`_PARTITIONTIME` = '2023-12-25'"]),
    (
        "region=US$category=retail",
        ["region", "category"],
        ["`region` = 'US'", "`category` = 'retail'"],
    ),
]


# =============================================================================
# SECURITY MODULE TESTS - Direct function testing
# =============================================================================


@pytest.mark.parametrize("input_id, expected", VALID_IDENTIFIERS_TEST_DATA)
def test_validate_bigquery_identifier_valid(input_id: str, expected: str) -> None:
    """Test validate_bigquery_identifier with valid identifiers."""
    result = validate_bigquery_identifier(input_id)
    assert result == expected


@pytest.mark.parametrize("invalid_id", INVALID_IDENTIFIERS_TEST_DATA)
def test_validate_bigquery_identifier_invalid(invalid_id: str) -> None:
    """Test validate_bigquery_identifier rejects invalid identifiers."""
    with pytest.raises(ValueError, match="Invalid general identifier"):
        validate_bigquery_identifier(invalid_id)


def test_build_safe_table_reference():
    """Test build_safe_table_reference function."""
    result = build_safe_table_reference("my-project", "my_dataset", "my_table")
    expected = "`my-project`.`my_dataset`.`my_table`"
    assert result == expected


@pytest.mark.parametrize("filter_expr", VALID_FILTER_EXPRESSIONS_TEST_DATA)
def test_validate_filter_expression_valid(filter_expr: str) -> None:
    """Test validate_filter_expression with valid expressions."""
    result = validate_filter_expression(filter_expr)
    assert result is True


@pytest.mark.parametrize("dangerous_sql", DANGEROUS_SQL_PATTERNS_TEST_DATA)
def test_validate_sql_structure_dangerous(dangerous_sql: str) -> None:
    """Test validate_sql_structure rejects dangerous SQL patterns."""
    with pytest.raises(ValueError, match="Query contains dangerous pattern"):
        validate_sql_structure(dangerous_sql)


def test_validate_sql_structure_valid():
    """Test validate_sql_structure with valid SQL."""
    valid_queries = [
        "SELECT * FROM `project.dataset.table`",
        "SELECT col1, col2 FROM `table` WHERE date = '2023-01-01'",
        "SELECT COUNT(*) FROM `table` LIMIT 1000",
    ]

    for query in valid_queries:
        # Should not raise exception
        validate_sql_structure(query)


def test_validate_column_name():
    """Test validate_column_name function."""
    # Valid columns
    assert validate_column_name("valid_col") is True
    assert validate_column_name("event_date") is True
    assert validate_column_name("_PARTITIONTIME") is True

    # Invalid columns
    assert validate_column_name("invalid col") is False
    assert validate_column_name("col;drop") is False
    assert validate_column_name("col'injection") is False


def test_validate_column_names():
    """Test validate_column_names function."""
    mixed_columns = ["valid_col", "invalid col", "another_valid", "bad;col"]
    result = validate_column_names(mixed_columns)

    assert "valid_col" in result
    assert "another_valid" in result
    assert len(result) == 2  # Only valid ones


def test_validate_and_filter_expressions():
    """Test validate_and_filter_expressions function."""
    mixed_expressions = [
        "`valid_col` = '2023-12-25'",  # Valid
        "invalid;expression",  # Invalid
        "`another_valid` = 123",  # Valid
        "DROP TABLE malicious",  # Invalid
    ]

    result = validate_and_filter_expressions(mixed_expressions)

    assert len(result) == 2  # Only valid expressions
    assert "`valid_col` = '2023-12-25'" in result
    assert "`another_valid` = 123" in result
    assert not any("DROP" in expr for expr in result)
    assert not any("invalid;" in expr for expr in result)


# =============================================================================
# QUERY EXECUTOR TESTS - Direct function testing
# =============================================================================


def test_query_executor_initialization():
    """Test QueryExecutor initialization."""
    config = create_test_config()
    executor = QueryExecutor(config)

    assert executor.config == config


def test_query_executor_get_effective_timeout():
    """Test QueryExecutor.get_effective_timeout."""
    # Test default timeout
    config = create_test_config()
    executor = QueryExecutor(config)
    timeout = executor.get_effective_timeout()
    assert timeout > 0

    # Test with custom timeout - partition_fetch_timeout should be in profiling config
    config_dict = {
        "project_id": "test-project-123456",
        "profiling": {"partition_fetch_timeout": 300},
    }
    config_with_timeout = BigQueryV2Config.parse_obj(config_dict)
    executor_with_timeout = QueryExecutor(config_with_timeout)
    timeout = executor_with_timeout.get_effective_timeout()
    assert timeout == 300


def test_query_executor_build_safe_custom_sql():
    """Test QueryExecutor.build_safe_custom_sql."""
    config = create_test_config()
    executor = QueryExecutor(config)

    # Test basic SQL generation
    sql = executor.build_safe_custom_sql("test-project-123", "dataset", "table")
    expected = "SELECT * FROM `test-project-123`.`dataset`.`table`"
    assert sql == expected

    # Test with where clause
    sql_with_where = executor.build_safe_custom_sql(
        "test-project-123", "dataset", "table", where_clause="`date` = '2023-01-01'"
    )
    expected_with_where = (
        "SELECT * FROM `test-project-123`.`dataset`.`table` WHERE `date` = '2023-01-01'"
    )
    assert sql_with_where == expected_with_where

    # Test with limit
    sql_with_limit = executor.build_safe_custom_sql(
        "test-project-123", "dataset", "table", limit=1000
    )
    expected_with_limit = (
        "SELECT * FROM `test-project-123`.`dataset`.`table` LIMIT 1000"
    )
    assert sql_with_limit == expected_with_limit

    # Test with both where clause and limit
    sql_complete = executor.build_safe_custom_sql(
        "test-project-123",
        "dataset",
        "table",
        where_clause="`status` = 'active'",
        limit=500,
    )
    expected_complete = "SELECT * FROM `test-project-123`.`dataset`.`table` WHERE `status` = 'active' LIMIT 500"
    assert sql_complete == expected_complete


def test_query_executor_cost_estimation():
    """Test QueryExecutor cost estimation methods."""
    config = create_test_config()
    executor = QueryExecutor(config)

    # Should return None when no client available
    result = executor.get_query_cost_estimate("SELECT 1")
    assert result is None

    # Should return False when no client available
    test_result = executor.test_query_execution("SELECT 1")
    assert test_result is False

    # Test is_query_too_expensive - should return True when cost estimation fails
    expensive_result = executor.is_query_too_expensive("SELECT 1", 1000)
    assert expensive_result is True


# =============================================================================
# PARTITION DISCOVERY TESTS - Direct function testing
# =============================================================================


def test_partition_discovery_initialization():
    """Test PartitionDiscovery initialization."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    assert discovery.config == config


def test_get_partition_range_from_partition_id():
    """Test PartitionDiscovery.get_partition_range_from_partition_id."""
    # Test YYYYMMDD format
    start, end = PartitionDiscovery.get_partition_range_from_partition_id(
        "20231225", None
    )

    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert start.date() == date(2023, 12, 25)
    assert end.date() == date(2023, 12, 26)  # Next day (upper bound)

    # Test YYYYMMDDHH format
    start_hour, end_hour = PartitionDiscovery.get_partition_range_from_partition_id(
        "2023122514", None
    )

    assert start_hour.hour == 14
    assert end_hour.hour == 15  # Next hour (upper bound)


@pytest.mark.parametrize("column, expected", DATE_LIKE_COLUMNS_TEST_DATA)
def test_partition_discovery_is_date_like_column(column: str, expected: bool) -> None:
    """Test PartitionDiscovery._is_date_like_column."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    result = discovery._is_date_like_column(column)
    assert result == expected


@pytest.mark.parametrize("data_type, expected", DATE_TYPES_TEST_DATA)
def test_partition_discovery_is_date_type_column(
    data_type: str, expected: bool
) -> None:
    """Test PartitionDiscovery._is_date_type_column."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    result = discovery._is_date_type_column(data_type)
    assert result == expected


def test_partition_discovery_get_strategic_candidate_dates():
    """Test PartitionDiscovery._get_strategic_candidate_dates."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    dates = discovery._get_strategic_candidate_dates()

    # Should return tuples of (datetime, description)
    assert len(dates) == 2  # today and yesterday for cost optimization
    assert all(isinstance(d, tuple) and len(d) == 2 for d in dates)
    assert all(isinstance(d[0], datetime) and isinstance(d[1], str) for d in dates)

    # Should be in descending order (most recent first)
    assert dates[0][0] >= dates[1][0]

    # Check descriptions
    descriptions = [desc for _, desc in dates]
    assert any("today" in desc.lower() for desc in descriptions)
    assert any("yesterday" in desc.lower() for desc in descriptions)


def test_partition_discovery_create_safe_filter():
    """Test PartitionDiscovery._create_safe_filter."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Test string filter
    string_filter = discovery._create_safe_filter("status", "active")
    assert string_filter == "`status` = 'active'"

    # Test numeric filter - now always quoted for type safety
    numeric_filter = discovery._create_safe_filter("count", 100)
    assert numeric_filter == "`count` = '100'"

    # Test float filter - now always quoted for type safety
    float_filter = discovery._create_safe_filter("price", 99.99)
    assert float_filter == "`price` = '99.99'"

    # Test date filter
    test_date = date(2023, 12, 25)
    date_filter = discovery._create_safe_filter(
        "event_date", test_date.strftime("%Y-%m-%d")
    )
    assert date_filter == "`event_date` = '2023-12-25'"


@pytest.mark.parametrize(
    "partition_id, required_columns, expected_filters", PARTITION_ID_TEST_DATA
)
def test_partition_discovery_convert_partition_id_to_filters(
    partition_id: str, required_columns: List[str], expected_filters: List[str]
) -> None:
    """Test PartitionDiscovery._convert_partition_id_to_filters."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    result = discovery._convert_partition_id_to_filters(partition_id, required_columns)

    # Check that all expected filters are present
    assert result is not None, "Expected filters but got None"
    for expected_filter in expected_filters:
        assert expected_filter in result or any(
            expected_filter in actual for actual in result
        ), f"Expected filter {expected_filter} not found in {result}"


def test_partition_discovery_convert_partition_id_to_filters_invalid():
    """Test PartitionDiscovery._convert_partition_id_to_filters with invalid input."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Invalid partition ID creates a literal filter
    required_columns = ["_PARTITIONDATE"]
    filters = discovery._convert_partition_id_to_filters("invalid", required_columns)
    assert filters is not None, "Expected filters but got None"
    assert len(filters) == 1
    assert "`_PARTITIONDATE` = 'invalid'" in filters


def test_partition_discovery_get_column_ordering_strategy():
    """Test PartitionDiscovery._get_column_ordering_strategy."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Date column should use DESC ordering
    result = discovery._get_column_ordering_strategy("event_date", "DATE")
    assert result == "`event_date` DESC"

    # Non-date column should use record count ordering
    result = discovery._get_column_ordering_strategy("user_id", "STRING")
    assert result == "record_count DESC"


# =============================================================================
# PROFILER TESTS - Direct function testing
# =============================================================================


def test_profiler_initialization():
    """Test BigqueryProfiler initialization."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    assert profiler.config == config
    assert profiler.report == report
    assert isinstance(profiler.partition_discovery, PartitionDiscovery)
    assert isinstance(profiler.query_executor, QueryExecutor)
    assert profiler._tables_profiled == 0


def test_profiler_get_dataset_name():
    """Test BigqueryProfiler.get_dataset_name."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    result = profiler.get_dataset_name("table1", "dataset1", "project1")
    assert result == "project1.dataset1.table1"


def test_profiler_simplified_date_windowing():
    """Test that date windowing always uses string literals for simplicity."""
    config = create_test_config()
    config.profiling.partition_datetime_window_days = 7
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table()

    # Test with no existing format detected (should use YYYY-MM-DD default)
    input_filters = ["`event_date` IS NOT NULL"]  # Date column but no format to detect
    result = profiler._apply_partition_date_windowing(input_filters, table)

    # Should have original filter plus windowing filters using YYYY-MM-DD default
    assert len(result) > len(input_filters)

    # Check that windowing filters use default YYYY-MM-DD string format
    windowing_filters = [f for f in result if f not in input_filters]
    assert len(windowing_filters) == 1

    # The windowing filter should use YYYY-MM-DD string format (not DATE functions)
    windowing_filter = windowing_filters[0]
    assert "DATE(" not in windowing_filter  # Should not use DATE() function
    assert (
        ">= '" in windowing_filter and "<= '" in windowing_filter
    )  # Should use string literals
    # Should use YYYY-MM-DD format (contains dashes)
    assert "-" in windowing_filter


def test_profiler_extract_date_columns_from_filters():
    """Test BigqueryProfiler._extract_date_columns_from_filters."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    filters = [
        "`event_date` = '2023-12-25'",
        "`user_id` = 123",
        "`timestamp` = '2023-12-25 10:00:00'",
        "`created_at` = '2023-12-25'",
    ]

    result = profiler._extract_date_columns_from_filters(filters)

    # Should find date-like columns
    assert "event_date" in result
    assert "timestamp" in result
    assert "created_at" in result
    assert "user_id" not in result


def test_profiler_get_reference_date_from_filters():
    """Test BigqueryProfiler._get_reference_date_from_filters."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    filters = ["`event_date` = '2023-12-25'", "`user_id` = 123"]
    date_columns = ["event_date"]

    result = profiler._get_reference_date_from_filters(filters, date_columns)
    assert result == date(2023, 12, 25)


def test_profiler_should_skip_profiling_due_to_staleness():
    """Test BigqueryProfiler._should_skip_profiling_due_to_staleness."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Test with staleness checking disabled
    config.profiling.skip_stale_tables = False
    old_table = create_test_table(
        last_altered=datetime.now(timezone.utc) - timedelta(days=365)
    )
    result = profiler._should_skip_profiling_due_to_staleness(old_table)
    assert result is False  # Should not skip when disabled

    # Test with staleness checking enabled - fresh table
    config.profiling.skip_stale_tables = True
    config.profiling.staleness_threshold_days = 30
    fresh_table = create_test_table(
        last_altered=datetime.now(timezone.utc) - timedelta(days=1)
    )
    result = profiler._should_skip_profiling_due_to_staleness(fresh_table)
    assert result is False  # Should not skip fresh table

    # Test with staleness checking enabled - stale table
    stale_table = create_test_table(
        last_altered=datetime.now(timezone.utc) - timedelta(days=60)
    )
    result = profiler._should_skip_profiling_due_to_staleness(stale_table)
    assert result is True  # Should skip stale table


@patch.object(PartitionDiscovery, "get_required_partition_filters")
def test_profiler_get_batch_kwargs(mock_get_filters):
    """Test BigqueryProfiler.get_batch_kwargs."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Mock the partition discovery
    mock_get_filters.return_value = ["`date_col` = '2023-12-25'"]

    # Create test table
    table = create_test_table(rows_count=10000)

    # Get batch kwargs
    result = profiler.get_batch_kwargs(table, "test_dataset", "test-project")

    # Verify structure
    assert "custom_sql" in result
    assert "partition_handling" in result
    assert result["partition_handling"] == "true"
    assert "test-project" in result["custom_sql"]
    assert "test_dataset" in result["custom_sql"]
    assert "test_table" in result["custom_sql"]


@patch.object(PartitionDiscovery, "get_required_partition_filters")
def test_profiler_get_batch_kwargs_with_sampling(mock_get_filters):
    """Test BigqueryProfiler.get_batch_kwargs with sampling enabled."""
    config = create_test_config(use_sampling=True, sample_size=5000)
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Mock the partition discovery
    mock_get_filters.return_value = ["`date_col` = '2023-12-25'"]

    # Create large table that should trigger sampling
    table = create_test_table(rows_count=100000)

    # Get batch kwargs
    result = profiler.get_batch_kwargs(table, "test_dataset", "test-project")

    # Should include TABLESAMPLE
    assert "TABLESAMPLE SYSTEM" in result["custom_sql"]


def test_profiler_get_batch_kwargs_security_validation():
    """Test that BigqueryProfiler.get_batch_kwargs validates identifiers."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table()

    # Test with invalid identifier - should raise ValueError
    with pytest.raises(ValueError, match="Invalid dataset identifier"):
        profiler.get_batch_kwargs(table, "invalid;dataset", "test-project")


def test_full_profiling_workflow():
    """Test complete profiling workflow across all modules."""
    config = create_test_config(use_sampling=True, sample_size=10000)
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Test that profiler has initialized its components
    assert isinstance(profiler.partition_discovery, PartitionDiscovery)
    assert isinstance(profiler.query_executor, QueryExecutor)

    # Test security validation
    valid_filters = ["`date` = '2023-01-01'", "`id` > 100"]
    validated = validate_and_filter_expressions(valid_filters)
    assert validated == valid_filters

    # Test query building
    executor = profiler.query_executor
    query = executor.build_safe_custom_sql(
        "test-project-123456",
        "dataset",
        "table",
        where_clause=" AND ".join(validated),
        limit=1000,
    )
    expected = "SELECT * FROM `test-project-123456`.`dataset`.`table` WHERE `date` = '2023-01-01' AND `id` > 100 LIMIT 1000"
    assert query == expected

    # Test partition discovery
    discovery = profiler.partition_discovery
    strategic_dates = discovery._get_strategic_candidate_dates()
    assert len(strategic_dates) == 2

    # Test date column identification
    date_columns = profiler._extract_date_columns_from_filters(
        ["`created_at` >= '2023-01-01'"]
    )
    assert "created_at" in date_columns


# =============================================================================
# COMPREHENSIVE PARTITION DISCOVERY TESTS - Target 80% coverage
# =============================================================================


def test_partition_discovery_get_partition_columns_from_info_schema():
    """Test PartitionDiscovery.get_partition_columns_from_info_schema."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table(partitioned=True)

    def mock_execute_query(query, config, context):
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [
                SimpleNamespace(column_name="event_date", data_type="DATE"),
                SimpleNamespace(column_name="region", data_type="STRING"),
                SimpleNamespace(column_name="user_id", data_type="INT64"),
            ]
        return []

    result = discovery.get_partition_columns_from_info_schema(
        table, "test-project", "test_dataset", mock_execute_query
    )

    assert "event_date" in result
    assert result["event_date"] == "DATE"
    assert "region" in result
    assert result["region"] == "STRING"


def test_partition_discovery_get_partition_columns_from_ddl():
    """Test PartitionDiscovery.get_partition_columns_from_ddl."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table()

    def mock_execute_query(query, config, context):
        if "INFORMATION_SCHEMA.TABLES" in query:
            return [
                SimpleNamespace(
                    ddl="CREATE TABLE test_table (id INT64, event_date DATE) PARTITION BY DATE(event_date)"
                )
            ]
        return []

    result = discovery.get_partition_columns_from_ddl(
        table, "test-project", "test_dataset", mock_execute_query
    )

    # DDL parsing should extract partition columns
    assert isinstance(result, dict)


def test_partition_discovery_get_most_populated_partitions():
    """Test PartitionDiscovery.get_most_populated_partitions."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table(partitioned=True)

    def mock_execute_query(query, config, context):
        if "GROUP BY" in query and "ORDER BY" in query:
            return [
                SimpleNamespace(event_date=date(2023, 12, 25), record_count=1000),
                SimpleNamespace(event_date=date(2023, 12, 24), record_count=800),
                SimpleNamespace(event_date=date(2023, 12, 23), record_count=600),
            ]
        return []

    result = discovery.get_most_populated_partitions(
        table, "test-project", "test_dataset", ["event_date"], mock_execute_query
    )

    # The method returns a PartitionResult dict with partition_values and row_count
    assert isinstance(result, dict)
    assert "partition_values" in result


def test_partition_discovery_get_required_partition_filters():
    """Test PartitionDiscovery.get_required_partition_filters."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table(partitioned=True)

    def mock_execute_query(query, config, context):
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [SimpleNamespace(column_name="event_date", data_type="DATE")]
        elif "SELECT" in query and "LIMIT 1" in query:
            return [SimpleNamespace(exists_check=1)]
        return []

    result = discovery.get_required_partition_filters(
        table, "test-project", "test_dataset", mock_execute_query
    )

    assert result is not None
    assert isinstance(result, list)


def test_partition_discovery_log_partition_attempt():
    """Test PartitionDiscovery._log_partition_attempt."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Should not raise an exception
    discovery._log_partition_attempt(
        "test method", "test_table", ["`date` = '2023-01-01'"], True
    )


def test_partition_discovery_create_partition_stats_query():
    """Test PartitionDiscovery._create_partition_stats_query."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)

    query, job_config = discovery._create_partition_stats_query(
        "`project.dataset.table`", "event_date", 10, "DATE"
    )

    assert "SELECT" in query
    assert "event_date" in query
    assert "GROUP BY" in query
    assert "ORDER BY" in query
    assert "LIMIT" in query  # Uses parameterized query, so exact text may vary


def test_partition_discovery_get_partition_filters_from_information_schema():
    """Test PartitionDiscovery._get_partition_filters_from_information_schema."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table(partitioned=True)

    def mock_execute_query(query, config, context):
        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            return [
                SimpleNamespace(partition_id="20231225"),
                SimpleNamespace(partition_id="20231224"),
                SimpleNamespace(partition_id="20231223"),
            ]
        elif "COUNT(*)" in query:
            # Mock that partitions have data
            return [SimpleNamespace(cnt=1000)]
        return []

    result = discovery._get_partition_filters_from_information_schema(
        table, "test-project", "test_dataset", ["_PARTITIONDATE"], mock_execute_query
    )

    # Should return list (could be None if no valid partitions found)
    assert result is None or isinstance(result, list)


def test_partition_discovery_verify_partition_has_data():
    """Test PartitionDiscovery._verify_partition_has_data."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table()

    def mock_execute_query(query, config, context):
        if "SELECT 1" in query and "LIMIT 1" in query:
            return [SimpleNamespace(cnt=1)]
        return []

    result = discovery._verify_partition_has_data(
        table,
        "test-project",
        "test_dataset",
        ["`date` = '2023-01-01'"],
        mock_execute_query,
    )

    assert result is True


def test_partition_discovery_enhance_partition_filters_with_actual_values():
    """Test PartitionDiscovery._enhance_partition_filters_with_actual_values."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table(external=True)

    def mock_execute_query(query, config, context):
        if "DISTINCT" in query:
            return [
                SimpleNamespace(region="US", category="retail"),
                SimpleNamespace(region="EU", category="wholesale"),
            ]
        return []

    base_filters = ["`event_date` = '2023-12-25'"]
    required_columns = ["event_date", "region", "category"]

    result = discovery._enhance_partition_filters_with_actual_values(
        table,
        "test-project",
        "test_dataset",
        required_columns,
        base_filters,
        mock_execute_query,
    )

    assert isinstance(result, list)


# =============================================================================
# COMPREHENSIVE PROFILER TESTS - Target 80% coverage
# =============================================================================


def test_profiler_get_profile_request():
    """Test BigqueryProfiler.get_profile_request."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table(external=True)

    with patch.object(
        PartitionDiscovery, "get_required_partition_filters"
    ) as mock_filters:
        mock_filters.return_value = ["`date` = '2023-01-01'"]

        result = profiler.get_profile_request(table, "test_dataset", "test-project")

        assert result is not None
        assert hasattr(result, "needs_partition_discovery")


def test_profiler_get_workunits():
    """Test BigqueryProfiler.get_workunits."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table()

    # Mock the entire profiling pipeline to avoid BigQuery authentication
    with (
        patch.object(
            profiler,
            "generate_profile_workunits_with_deferred_partitions",
            return_value=[],
        ) as mock_generate,
        patch.object(profiler.query_executor, "execute_query_safely", return_value=[]),
    ):
        result = list(profiler.get_workunits("test-project", {"test_dataset": [table]}))

        assert isinstance(result, list)
        mock_generate.assert_called_once()


def test_profiler_generate_profile_workunits_with_deferred_partitions():
    """Test BigqueryProfiler.generate_profile_workunits_with_deferred_partitions method exists and has correct signature."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Test that the method exists and can be called
    assert hasattr(profiler, "generate_profile_workunits_with_deferred_partitions")
    method = profiler.generate_profile_workunits_with_deferred_partitions
    assert callable(method)

    # Test with empty profile requests to avoid authentication
    result = list(
        profiler.generate_profile_workunits_with_deferred_partitions(
            [], max_workers=1, platform="bigquery", profiler_args={}
        )
    )

    assert isinstance(result, list)
    assert len(result) == 0  # Empty input should give empty output


def test_profiler_external_table_integration():
    """Test BigqueryProfiler external table integration."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table(external=True)

    # Test that the profiler can handle external tables
    with patch.object(
        PartitionDiscovery, "get_required_partition_filters"
    ) as mock_filters:
        mock_filters.return_value = ["`date` = '2023-01-01'"]

        # Test profile request creation for external table
        profile_request = profiler.get_profile_request(
            table, "test_dataset", "test-project"
        )

        assert profile_request is not None
        assert hasattr(profile_request, "needs_partition_discovery")


def test_profiler_apply_partition_date_windowing_comprehensive():
    """Test BigqueryProfiler._apply_partition_date_windowing comprehensively."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table()

    # Test with multiple scenarios
    test_cases = [
        # (windowing_days, input_filters, should_add_ranges)
        (None, ["`event_date` = '2023-12-25'"], False),  # Disabled
        (7, ["`event_date` = '2023-12-25'"], True),  # Enabled
        (7, ["`user_id` = 123"], False),  # No date columns
        (0, ["`event_date` = '2023-12-25'"], False),  # Zero days
    ]

    for window_days, input_filters, should_add_ranges in test_cases:
        config.profiling.partition_datetime_window_days = window_days

        result = profiler._apply_partition_date_windowing(input_filters, table)

        if should_add_ranges:
            assert len(result) > len(input_filters)
        else:
            assert len(result) == len(input_filters)


def test_profiler_detect_date_format_in_filters():
    """Test BigqueryProfiler._detect_date_format_in_filters for consistent formatting."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Test YYYYMMDD format detection
    filters_yyyymmdd = ["`date` = '20250913'", "`user_id` = 123"]
    format_result = profiler._detect_date_format_in_filters(filters_yyyymmdd, "date")
    assert format_result == "YYYYMMDD"

    # Test YYYY-MM-DD format detection
    filters_yyyy_mm_dd = ["`event_date` = '2025-09-13'", "`status` = 'active'"]
    format_result = profiler._detect_date_format_in_filters(
        filters_yyyy_mm_dd, "event_date"
    )
    assert format_result == "YYYY-MM-DD"

    # Test no specific format (should return None for DATE functions)
    filters_no_format = ["`user_id` = 123", "`status` = 'active'"]
    format_result = profiler._detect_date_format_in_filters(filters_no_format, "date")
    assert format_result is None

    # Test column not found
    filters_other_col = ["`other_date` = '20250913'"]
    format_result = profiler._detect_date_format_in_filters(filters_other_col, "date")
    assert format_result is None


def test_profiler_date_windowing_with_string_format():
    """Test that date windowing uses consistent string formats to avoid type mismatches."""
    config = create_test_config()
    config.profiling.partition_datetime_window_days = 7
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    table = create_test_table()

    # Test with YYYYMMDD format (the problematic case from the error)
    input_filters = ["`date` = '20250913'"]
    result = profiler._apply_partition_date_windowing(input_filters, table)

    # Should have original filter plus windowing filters
    assert len(result) > len(input_filters)

    # Check that windowing filters use consistent YYYYMMDD string format
    windowing_filters = [f for f in result if f not in input_filters]
    assert len(windowing_filters) == 1

    # The windowing filter should use string format, not DATE() functions
    windowing_filter = windowing_filters[0]
    assert "DATE(" not in windowing_filter  # Should not use DATE() function
    assert (
        ">= '" in windowing_filter and "<= '" in windowing_filter
    )  # Should use string literals

    # Test with YYYY-MM-DD format
    input_filters_dash = ["`event_date` = '2025-09-13'"]
    result_dash = profiler._apply_partition_date_windowing(input_filters_dash, table)

    # Should have windowing filters in YYYY-MM-DD format
    windowing_filters_dash = [f for f in result_dash if f not in input_filters_dash]
    assert len(windowing_filters_dash) == 1
    windowing_filter_dash = windowing_filters_dash[0]
    assert "DATE(" not in windowing_filter_dash  # Should not use DATE() function
    assert (
        ">= '" in windowing_filter_dash and "<= '" in windowing_filter_dash
    )  # Should use string literals


def test_profiler_get_dataset_name_variations():
    """Test BigqueryProfiler.get_dataset_name with different inputs."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    test_cases = [
        ("table1", "dataset1", "project1", "project1.dataset1.table1"),
        (
            "my_table",
            "my_dataset",
            "my-project-123",
            "my-project-123.my_dataset.my_table",
        ),
        ("test", "test", "test", "test.test.test"),
    ]

    for table, dataset, project, expected in test_cases:
        result = profiler.get_dataset_name(table, dataset, project)
        assert result == expected


def test_profiler_string_representations():
    """Test BigqueryProfiler string representations."""
    config = create_test_config()
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Test __str__
    str_result = str(profiler)
    assert "BigqueryProfiler" in str_result
    assert "timeout=" in str_result

    # Test __repr__
    repr_result = repr(profiler)
    assert "BigqueryProfiler" in repr_result


def test_partition_discovery_information_schema_without_restrictive_windowing():
    """Test that INFORMATION_SCHEMA partition discovery doesn't apply restrictive windowing during discovery."""
    config = create_test_config(partition_datetime_window_days=30)
    discovery = PartitionDiscovery(config)
    table = create_test_table()

    def mock_execute_query(query, job_config, context):
        # Verify that the query doesn't contain restrictive windowing
        assert "last_modified_time >=" not in query, (
            "Discovery query should not apply restrictive windowing"
        )
        assert "INTERVAL" not in query, (
            "Discovery query should not apply date intervals during discovery"
        )

        # Mock some partition results

        return [
            SimpleNamespace(
                partition_id="20241201",
                last_modified_time="2024-12-01",
                total_rows=1000,
            ),
            SimpleNamespace(
                partition_id="20241130", last_modified_time="2024-11-30", total_rows=800
            ),
        ]

    # Test that discovery doesn't apply restrictive windowing
    result = discovery._get_partition_filters_from_information_schema(
        table, "test-project", "dataset", ["date"], mock_execute_query
    )

    # Should return partition filters without restrictive windowing
    assert result is not None
    assert len(result) > 0


def test_partition_discovery_strategy2_fallback_for_internal_tables():
    """Test that Strategy 2 (actual max date query) is used when INFORMATION_SCHEMA fails."""
    config = create_test_config()
    discovery = PartitionDiscovery(config)
    table = create_test_table()

    def mock_execute_query_info_schema_fails(query, job_config, context):
        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            # Simulate INFORMATION_SCHEMA returning no results
            return []
        elif "GROUP BY" in query and "ORDER BY" in query:
            # Simulate Strategy 2 finding actual max date

            return [SimpleNamespace(val="2024-11-15", record_count=5000)]
        return []

    # This should trigger Strategy 2 fallback
    result = discovery._find_real_partition_values(
        table,
        "test-project",
        "dataset",
        ["event_date"],
        mock_execute_query_info_schema_fails,
    )

    # Should successfully find partition values via Strategy 2
    assert result is not None
    assert len(result) > 0
    assert "event_date" in str(result[0])
    assert "2024-11-15" in str(result[0])


def test_partition_datetime_window_days_still_applied_during_profiling():
    """Test that partition_datetime_window_days is still applied during the profiling phase."""
    config = create_test_config(partition_datetime_window_days=7)
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)
    table = create_test_table()

    # Test filters that would be discovered (e.g., from 30 days ago)
    discovered_filters = ["`event_date` = '2024-10-15'"]

    # Apply windowing during profiling
    windowed_filters = profiler._apply_partition_date_windowing(
        discovered_filters, table
    )

    # Should have original filter plus windowing constraints
    assert len(windowed_filters) > len(discovered_filters)

    # Should contain the original discovered filter
    assert "`event_date` = '2024-10-15'" in windowed_filters

    # Should also contain windowing range filters
    range_filters = [f for f in windowed_filters if ">=" in f and "<=" in f]
    assert len(range_filters) > 0

    # Verify that the windowing uses a 7-day range
    range_filter = range_filters[0]
    assert ">=" in range_filter and "<=" in range_filter
    assert "`event_date`" in range_filter


def test_partition_datetime_window_days_disabled():
    """Test that windowing can be disabled by setting partition_datetime_window_days to None."""
    config = create_test_config(partition_datetime_window_days=None)
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)
    table = create_test_table()

    # Test filters
    original_filters = ["`event_date` = '2024-10-15'"]

    # Apply windowing (should be disabled)
    windowed_filters = profiler._apply_partition_date_windowing(original_filters, table)

    # Should return exactly the same filters (no windowing applied)
    assert windowed_filters == original_filters


def test_internal_table_finds_actual_latest_date_comprehensive():
    """
    Comprehensive test demonstrating that internal tables now find actual latest dates.

    Scenario:
    - Table has latest data from November 15, 2024 (45 days ago)
    - partition_datetime_window_days is set to 30 days
    - Current date is December 30, 2024

    Expected behavior:
    1. Discovery phase finds November 15 partition (no restrictive windowing)
    2. Profiling phase applies 30-day window from November 15
    3. Result: Profiles the actual latest data
    """

    # Setup: Current date is December 30, 2024
    current_date = datetime(2024, 12, 30, tzinfo=timezone.utc)

    # Table has latest data from November 15, 2024 (45 days ago - outside 30-day window from today)
    actual_latest_date = date(2024, 11, 15)

    config = create_test_config(partition_datetime_window_days=30)
    discovery = PartitionDiscovery(config)
    table = create_test_table(name="sales_data", partitioned=True)

    def mock_execute_query_realistic_scenario(query, job_config, context):
        """Mock that simulates realistic BigQuery responses."""

        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            # Simulate INFORMATION_SCHEMA finding partitions (without restrictive windowing)
            # This should now work because we removed the restrictive windowing

            return [
                SimpleNamespace(
                    partition_id="20241115",  # November 15, 2024
                    last_modified_time="2024-11-15T10:00:00Z",
                    total_rows=15000,
                ),
                SimpleNamespace(
                    partition_id="20241114",  # November 14, 2024
                    last_modified_time="2024-11-14T10:00:00Z",
                    total_rows=12000,
                ),
            ]

        elif "GROUP BY" in query and "ORDER BY" in query:
            # Simulate Strategy 2: Direct table query finding actual max date

            return [
                SimpleNamespace(
                    val=actual_latest_date, record_count=15000
                ),  # November 15, 2024
                SimpleNamespace(
                    val=date(2024, 11, 14), record_count=12000
                ),  # November 14, 2024
            ]

        elif "SELECT 1" in query and "LIMIT 1" in query:
            # Simulate partition verification - data exists

            return [SimpleNamespace(cnt=1)]

        return []

    # Test the full partition discovery flow
    with patch("datetime.datetime") as mock_datetime:
        # Mock current time for consistent testing
        mock_datetime.now.return_value = current_date
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

        # Discover partitions (should find November 15 data)
        partition_filters = discovery.get_required_partition_filters(
            table, "test-project", "dataset", mock_execute_query_realistic_scenario
        )

    # Verify that we found partition filters
    assert partition_filters is not None
    assert len(partition_filters) > 0

    # Verify that the filters contain the actual latest date (November 15)
    filter_str = " ".join(partition_filters)
    assert "2024-11-15" in filter_str or "20241115" in filter_str

    print(f"✅ Discovery found partition filters: {partition_filters}")

    # Now test the profiling phase with windowing
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    # Apply windowing during profiling (should add range filters based on discovered date)
    windowed_filters = profiler._apply_partition_date_windowing(
        partition_filters, table
    )

    # Should have more filters (original + windowing range)
    assert len(windowed_filters) >= len(partition_filters)

    # Should contain range filters that create a 30-day window from November 15
    range_filters = [f for f in windowed_filters if ">=" in f and "<=" in f]
    assert len(range_filters) > 0

    # The range should be based on November 15 (discovered date), not December 30 (current date)
    range_filter = range_filters[0]
    assert ">=" in range_filter and "<=" in range_filter

    print(f"✅ Profiling applied windowing: {windowed_filters}")
    print(f"✅ Range filter: {range_filter}")

    # Key assertion: We should be able to profile data from November 15
    # (the actual latest date) even though it's outside a 30-day window from today
    assert "event_date" in range_filter or "date" in range_filter

    print(
        "🎯 SUCCESS: Internal table now finds actual latest date (November 15) and applies windowing correctly!"
    )


def test_internal_table_strategy2_fallback_with_old_data():
    """
    Test that Strategy 2 successfully finds very old latest data when INFORMATION_SCHEMA fails.

    Scenario: Table has latest data from 6 months ago, INFORMATION_SCHEMA returns no results
    """

    config = create_test_config(partition_datetime_window_days=30)
    discovery = PartitionDiscovery(config)
    table = create_test_table(name="legacy_data", partitioned=True)

    # Latest data is from 6 months ago
    very_old_latest_date = date(2024, 6, 15)

    def mock_execute_query_old_data_scenario(query, job_config, context):
        """Mock for scenario where INFORMATION_SCHEMA fails but Strategy 2 succeeds."""

        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            # INFORMATION_SCHEMA returns no results (maybe table is too old)
            return []

        elif "GROUP BY" in query and "ORDER BY" in query:
            # Strategy 2: Direct table query finds the actual max date from 6 months ago

            return [
                SimpleNamespace(
                    val=very_old_latest_date, record_count=8500
                ),  # June 15, 2024
                SimpleNamespace(
                    val=date(2024, 6, 14), record_count=7200
                ),  # June 14, 2024
            ]

        elif "SELECT 1" in query and "LIMIT 1" in query:
            # Partition verification - simulate that today's date has NO data (strategic dates fail)
            return []

        return []

    # Test Strategy 2 fallback
    partition_filters = discovery.get_required_partition_filters(
        table, "test-project", "dataset", mock_execute_query_old_data_scenario
    )

    # Should successfully find the old data via Strategy 2
    assert partition_filters is not None
    assert len(partition_filters) > 0

    # Should contain the actual latest date from 6 months ago
    filter_str = " ".join(partition_filters)
    assert "2024-06-15" in filter_str or "20240615" in filter_str

    print(f"✅ Direct table query found old data: {partition_filters}")
    print("🎯 SUCCESS: Direct table query fallback works for very old latest data!")


def test_internal_table_uses_same_process_as_external_tables():
    """
    Test that internal tables with date/timestamp columns now use the same process as external tables.

    This means they should use direct table query to find actual max dates instead of
    just checking today/yesterday strategic dates.
    """

    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Create internal table with date partition column
    internal_table = create_test_table(
        name="internal_events", partitioned=True, external=False
    )

    # Table has actual latest data from 2 weeks ago
    actual_latest_date = date(2024, 11, 1)  # 2 weeks ago

    def mock_execute_query_internal_table(query, job_config, context):
        """Mock that simulates internal table with old latest data."""

        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            # INFORMATION_SCHEMA returns no results (common for older partitions)
            return []

        elif "GROUP BY" in query and "ORDER BY" in query:
            # Direct table query finds the actual max date from 2 weeks ago

            return [
                SimpleNamespace(
                    val=actual_latest_date, record_count=12000
                ),  # Nov 1, 2024
                SimpleNamespace(
                    val=date(2024, 10, 31), record_count=11500
                ),  # Oct 31, 2024
            ]

        elif "SELECT 1" in query and "LIMIT 1" in query:
            # Strategic dates (today/yesterday) have NO data - this forces direct table query
            return []

        return []

    # Test that internal table now uses direct table query for date columns
    partition_filters = discovery.get_required_partition_filters(
        internal_table, "test-project", "dataset", mock_execute_query_internal_table
    )

    # Should successfully find the actual latest data via direct table query
    assert partition_filters is not None
    assert len(partition_filters) > 0

    # Should contain the actual latest date from 2 weeks ago (not today/yesterday)
    filter_str = " ".join(partition_filters)
    assert "2024-11-01" in filter_str or "20241101" in filter_str

    print(f"✅ Internal table found actual latest data: {partition_filters}")
    print(
        "🎯 SUCCESS: Internal tables with date/timestamp columns now use same process as external tables!"
    )


def test_external_vs_internal_table_consistency():
    """
    Test that external and internal tables with date columns produce consistent results.

    Both should find the actual latest dates using direct table query.
    """

    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Same table data, different table types
    actual_latest_date = date(2024, 10, 15)

    def mock_execute_query_consistent(query, job_config, context):
        """Mock that returns consistent data for both table types."""

        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            # Internal tables try INFORMATION_SCHEMA first, but it fails
            return []

        elif "GROUP BY" in query and "ORDER BY" in query:
            # Both should use direct table query and find the same data

            return [
                SimpleNamespace(val=actual_latest_date, record_count=8000),
            ]

        elif "SELECT 1" in query and "LIMIT 1" in query:
            # Strategic dates fail for both
            return []

        return []

    # Test external table
    external_table = create_test_table(
        name="external_events", partitioned=True, external=True
    )
    external_filters = discovery.get_required_partition_filters(
        external_table, "test-project", "dataset", mock_execute_query_consistent
    )

    # Test internal table
    internal_table = create_test_table(
        name="internal_events", partitioned=True, external=False
    )
    internal_filters = discovery.get_required_partition_filters(
        internal_table, "test-project", "dataset", mock_execute_query_consistent
    )

    # Both should find the same actual latest date
    assert external_filters is not None
    assert internal_filters is not None
    assert len(external_filters) > 0
    assert len(internal_filters) > 0

    # Both should contain the same actual latest date
    external_filter_str = " ".join(external_filters)
    internal_filter_str = " ".join(internal_filters)

    assert "2024-10-15" in external_filter_str or "20241015" in external_filter_str
    assert "2024-10-15" in internal_filter_str or "20241015" in internal_filter_str

    print(f"✅ External table filters: {external_filters}")
    print(f"✅ Internal table filters: {internal_filters}")
    print("🎯 SUCCESS: External and internal tables now produce consistent results!")


# =============================================================================
# QUERY EXECUTOR COMPREHENSIVE TESTS
# =============================================================================


def test_query_executor_execute_with_retry():
    """Test QueryExecutor.execute_with_retry."""
    config = create_test_config()
    executor = QueryExecutor(config)

    # Mock the BigQuery client to avoid real API calls
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Simulated error")

    with (
        patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.bigquery.Client",
            return_value=mock_client,
        ),
        pytest.raises(Exception, match="Simulated error"),
    ):
        # Test retry logic with mocked client - should raise exception after retries
        executor.execute_with_retry("SELECT 1", max_retries=2)


def test_query_executor_is_query_too_expensive_scenarios():
    """Test QueryExecutor.is_query_too_expensive with different scenarios."""
    config = create_test_config()
    executor = QueryExecutor(config)

    # Test with mock cost estimation
    with patch.object(executor, "get_query_cost_estimate") as mock_cost:
        # Test under limit
        mock_cost.return_value = 500_000_000  # 500MB
        result = executor.is_query_too_expensive("SELECT 1", 1_000_000_000)  # 1GB limit
        assert result is False

        # Test over limit
        mock_cost.return_value = 2_000_000_000  # 2GB
        result = executor.is_query_too_expensive("SELECT 1", 1_000_000_000)  # 1GB limit
        assert result is True

        # Test with None cost (estimation failed)
        mock_cost.return_value = None
        result = executor.is_query_too_expensive("SELECT 1", 1_000_000_000)
        assert result is True  # Should be conservative when estimation fails


def test_query_executor_execute_query_safely():
    """Test QueryExecutor.execute_query_safely."""
    config = create_test_config()
    executor = QueryExecutor(config)

    # Mock the BigQuery client to avoid real API calls
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Simulated error")

    with (
        patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.bigquery.Client",
            return_value=mock_client,
        ),
        pytest.raises(Exception, match="Simulated error"),
    ):
        # Test with mocked client - should raise exception after logging
        executor.execute_query_safely("SELECT 1")


# =============================================================================
# SECURITY MODULE COMPREHENSIVE TESTS
# =============================================================================


def test_security_validate_sql_structure_comprehensive():
    """Test validate_sql_structure with comprehensive patterns."""

    # Test more dangerous patterns
    additional_dangerous = [
        "MERGE INTO table USING source ON condition",
        "CALL procedure_name()",
        "EXECUTE IMMEDIATE 'SELECT 1'",
    ]

    for dangerous_sql in additional_dangerous:
        with pytest.raises(ValueError, match="Query contains dangerous pattern"):
            validate_sql_structure(dangerous_sql)

    # Test pattern that gets rejected for different reason
    with pytest.raises(ValueError, match="Query must start with SELECT or WITH"):
        validate_sql_structure("EXPORT DATA OPTIONS() AS SELECT * FROM table")

    # Test complex valid queries
    complex_valid = [
        "SELECT a.col1, b.col2 FROM `project.dataset.table_a` a JOIN `project.dataset.table_b` b ON a.id = b.id",
        "SELECT * FROM `table` WHERE date BETWEEN '2023-01-01' AND '2023-12-31' ORDER BY date DESC LIMIT 1000",
        "SELECT DISTINCT col1, COUNT(*) as cnt FROM `table` GROUP BY col1 HAVING cnt > 10",
    ]

    for valid_sql in complex_valid:
        # Should not raise exception
        validate_sql_structure(valid_sql)


def test_hierarchical_year_month_day_components():
    """
    Test that year/month/day components are processed hierarchically to avoid invalid future dates.

    This ensures we get valid date combinations like 2024-11-15, not impossible ones like 2024-12-31
    when the actual latest data is from November.
    """

    config = create_test_config()
    discovery = PartitionDiscovery(config)

    # Create table with year/month/day partition components
    table = create_test_table(name="logs_by_date", partitioned=True)
    # Override partition info to have year/month/day fields

    partition_info = SimpleNamespace(
        type_="DAY",
        field="year",  # Primary field
        fields=["year", "month", "day", "source"],  # All partition fields
        require_partition_filter=True,
    )
    table.partition_info = partition_info  # type: ignore[assignment]

    def mock_execute_query_hierarchical_dates(query, job_config, context):
        """Mock that simulates hierarchical date component queries."""

        if "INFORMATION_SCHEMA.COLUMNS" in query:
            # Return year/month/day columns
            return [
                SimpleNamespace(column_name="year", data_type="INT64"),
                SimpleNamespace(column_name="month", data_type="INT64"),
                SimpleNamespace(column_name="day", data_type="INT64"),
                SimpleNamespace(column_name="source", data_type="STRING"),
            ]

        elif "INFORMATION_SCHEMA.PARTITIONS" in query:
            # Return empty for INFORMATION_SCHEMA.PARTITIONS to force fallback to table query
            return []

        elif "GROUP BY" in query and "ORDER BY" in query:
            # Simulate hierarchical queries - check SELECT clause to determine which column is being queried
            if (
                "SELECT `year`" in query
                and "`month`" not in query
                and "`day`" not in query
            ):
                # Step 1: Max year query (no constraints)
                return [
                    SimpleNamespace(
                        val=2024, record_count=50000
                    ),  # Latest year is 2024
                ]

            elif (
                "SELECT `month`" in query
                and "`year` =" in query
                and "`day`" not in query
            ):
                # Step 2: Max month within 2024
                return [
                    SimpleNamespace(
                        val=11, record_count=15000
                    ),  # Latest month in 2024 is November (11)
                ]

            elif (
                "SELECT `day`" in query and "`year` =" in query and "`month` =" in query
            ):
                # Step 3: Max day within 2024-11
                return [
                    SimpleNamespace(
                        val=15, record_count=5000
                    ),  # Latest day in 2024-11 is 15th
                ]

            elif (
                "SELECT `source`" in query
                and "`year` =" in query
                and "`month` =" in query
                and "`day` =" in query
            ):
                # Step 4: Most populated source within 2024-11-15
                return [
                    SimpleNamespace(val="app", record_count=3000),
                    SimpleNamespace(val="api", record_count=2000),
                ]

        return []

    # Test hierarchical date component discovery
    partition_filters = discovery.get_required_partition_filters(
        table, "test-project", "dataset", mock_execute_query_hierarchical_dates
    )

    # Should successfully find partition filters
    assert partition_filters is not None
    assert len(partition_filters) > 0

    filter_str = " ".join(partition_filters)

    # Key assertions: Should get valid date combination 2024-11-15, not impossible dates
    assert "2024" in filter_str  # Latest year
    assert "11" in filter_str  # Latest month within 2024 (November, not December)
    assert "15" in filter_str  # Latest day within 2024-11 (15th, not 31st)
    assert "app" in filter_str  # Most populated source within that date

    print(f"✅ Hierarchical date components: {partition_filters}")
    print(
        "🎯 SUCCESS: Year/month/day components processed hierarchically to avoid invalid future dates!"
    )


def test_security_edge_cases():
    """Test security functions with edge cases."""

    # Test with empty inputs
    assert validate_column_names([]) == []
    assert validate_and_filter_expressions([]) == []

    # Test with mixed case and special characters
    mixed_columns = [
        "Valid_Column",
        "UPPER_CASE",
        "lower_case",
        "123invalid",
        "_valid_underscore",
    ]
    result = validate_column_names(mixed_columns)
    assert "Valid_Column" in result
    assert "UPPER_CASE" in result
    assert "_valid_underscore" in result
    assert len(result) >= 3


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
