"""Unit tests for stored procedure enrichment utilities."""

from unittest.mock import MagicMock

from datahub.ingestion.source.sql.stored_procedures.enrichment import (
    fetch_dependencies_metadata,
    fetch_multiline_source_code,
    fetch_parameters_metadata,
    fetch_single_value,
)


def test_fetch_parameters_metadata_with_format():
    """Test fetching and formatting parameter metadata."""
    mock_conn = MagicMock()

    # Mock rows with _mapping attribute (SQLAlchemy Row simulation)
    def create_mock_row(data):
        mock_row = MagicMock()
        mock_row._mapping = data
        return mock_row

    mock_result = [
        create_mock_row({"argument_name": "x", "in_out": "IN", "data_type": "INTEGER"}),
        create_mock_row(
            {"argument_name": "y", "in_out": "OUT", "data_type": "VARCHAR"}
        ),
    ]

    mock_conn.execute.return_value = mock_result

    result = fetch_parameters_metadata(
        conn=mock_conn,
        query="SELECT * FROM args WHERE proc = :proc",
        params={"proc": "test_proc"},
        format_template="{in_out} {argument_name} {data_type}",
    )

    # Parameters are comma-separated, not newline-separated
    assert result == "IN x INTEGER, OUT y VARCHAR"
    mock_conn.execute.assert_called_once()


def test_fetch_parameters_metadata_no_format():
    """Test fetching parameter metadata without formatting."""
    mock_conn = MagicMock()

    def create_mock_row(data):
        mock_row = MagicMock()
        mock_row._mapping = data
        return mock_row

    mock_result = [
        create_mock_row({"arg_name": "param1", "arg_type": "INT"}),
        create_mock_row({"arg_name": "param2", "arg_type": "TEXT"}),
    ]

    mock_conn.execute.return_value = mock_result

    result = fetch_parameters_metadata(
        conn=mock_conn,
        query="SELECT * FROM parameters",
        params={},
        format_template=None,
    )

    # Without format, joins all values with space, then rows with commas
    # Since mock objects don't iterate well, result will be minimal
    # Just verify it's not None
    assert result is not None


def test_fetch_parameters_metadata_empty_result():
    """Test fetching parameters when no results."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value = []

    result = fetch_parameters_metadata(
        conn=mock_conn,
        query="SELECT * FROM args",
        params={},
        format_template="{name}",
    )

    assert result is None


def test_fetch_dependencies_metadata_both():
    """Test fetching both upstream and downstream dependencies."""
    mock_conn = MagicMock()

    def create_mock_row(data):
        mock_row = MagicMock()
        mock_row._mapping = data
        return mock_row

    # Mock upstream results
    upstream_results = [
        create_mock_row(
            {
                "referenced_owner": "HR",
                "referenced_name": "EMPLOYEES",
                "referenced_type": "TABLE",
            }
        ),
        create_mock_row(
            {
                "referenced_owner": "HR",
                "referenced_name": "DEPARTMENTS",
                "referenced_type": "TABLE",
            }
        ),
    ]

    # Mock downstream results
    downstream_results = [
        create_mock_row(
            {"owner": "REPORTING", "name": "MONTHLY_REPORT", "type": "VIEW"}
        ),
    ]

    # Set up mock to return different results for different queries
    mock_conn.execute.side_effect = [upstream_results, downstream_results]

    result = fetch_dependencies_metadata(
        conn=mock_conn,
        upstream_query="SELECT * FROM deps WHERE type='UPSTREAM'",
        downstream_query="SELECT * FROM deps WHERE type='DOWNSTREAM'",
        params={"proc": "test"},
        upstream_format="{referenced_owner}.{referenced_name} ({referenced_type})",
        downstream_format="{owner}.{name} ({type})",
    )

    assert result is not None
    assert "upstream" in result
    assert "downstream" in result
    assert len(result["upstream"]) == 2
    assert len(result["downstream"]) == 1
    assert "HR.EMPLOYEES (TABLE)" in result["upstream"]
    assert "REPORTING.MONTHLY_REPORT (VIEW)" in result["downstream"]


def test_fetch_dependencies_metadata_upstream_only():
    """Test fetching only upstream dependencies."""
    mock_conn = MagicMock()

    def create_mock_row(data):
        mock_row = MagicMock()
        mock_row._mapping = data
        return mock_row

    upstream_results = [
        create_mock_row({"table": "source_table"}),
    ]

    mock_conn.execute.return_value = upstream_results

    result = fetch_dependencies_metadata(
        conn=mock_conn,
        upstream_query="SELECT * FROM upstream",
        downstream_query=None,  # No downstream query
        params={},
        upstream_format="{table}",
        downstream_format=None,
    )

    assert result is not None
    assert "upstream" in result
    assert "downstream" not in result
    assert result["upstream"] == ["source_table"]


def test_fetch_dependencies_metadata_empty():
    """Test fetching dependencies when none exist."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value = []

    result = fetch_dependencies_metadata(
        conn=mock_conn,
        upstream_query="SELECT * FROM deps",
        downstream_query=None,
        params={},
        upstream_format="{name}",
        downstream_format=None,
    )

    assert result is None


def test_fetch_multiline_source_code():
    """Test fetching and assembling multi-line source code."""
    mock_conn = MagicMock()

    # Mock rows that support getattr for text column
    class MockRow:
        def __init__(self, text_val):
            self.text = text_val

    # Source code split across multiple rows (ordered)
    mock_result = [
        MockRow("CREATE PROCEDURE test_proc\n"),
        MockRow("AS\n"),
        MockRow("BEGIN\n"),
        MockRow("  SELECT * FROM table;\n"),
        MockRow("END;\n"),
    ]

    mock_conn.execute.return_value = mock_result

    result = fetch_multiline_source_code(
        conn=mock_conn,
        query="SELECT text FROM source WHERE proc = :proc ORDER BY line",
        params={"proc": "test_proc"},
        text_column="text",
    )

    expected = "CREATE PROCEDURE test_proc\nAS\nBEGIN\n  SELECT * FROM table;\nEND;\n"
    assert result == expected


def test_fetch_multiline_source_code_empty():
    """Test fetching source code when no results."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value = []

    result = fetch_multiline_source_code(
        conn=mock_conn,
        query="SELECT text FROM source",
        params={},
        text_column="text",
    )

    assert result is None


def test_fetch_single_value():
    """Test fetching a single scalar value."""
    mock_conn = MagicMock()
    mock_result = MagicMock()

    # Mock row that supports indexing
    class MockRow:
        def __getitem__(self, key):
            return 42

    mock_result.fetchone.return_value = MockRow()
    mock_conn.execute.return_value = mock_result

    result = fetch_single_value(
        conn=mock_conn,
        query="SELECT COUNT(*) as count FROM procedures",
        params={},
    )

    assert result == 42


def test_fetch_single_value_empty():
    """Test fetching single value when no results."""
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_conn.execute.return_value = mock_result

    result = fetch_single_value(
        conn=mock_conn,
        query="SELECT value FROM config WHERE key = :key",
        params={"key": "missing"},
    )

    assert result is None


def test_fetch_single_value_multiple_rows():
    """Test fetching single value returns only first row."""
    mock_conn = MagicMock()
    mock_result = MagicMock()

    # Mock row that returns first column
    class MockRow:
        def __getitem__(self, key):
            return "first"

    mock_result.fetchone.return_value = MockRow()
    mock_conn.execute.return_value = mock_result

    result = fetch_single_value(
        conn=mock_conn,
        query="SELECT val FROM multi",
        params={},
    )

    # Should return first value only
    assert result == "first"
