"""
Unit tests for MSSQL source get_prefer_lowercase method.

Tests the fix for issue #13792: MSSQL View Ingestion Causes Lowercased Lineage Table URNs
"""

from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource


def test_mssql_get_prefer_lowercase_true():
    """Test that get_prefer_lowercase returns True when convert_urns_to_lowercase=True."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password="test_password",
        convert_urns_to_lowercase=True,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)
        assert source.get_prefer_lowercase() is True


def test_mssql_get_prefer_lowercase_false():
    """Test that get_prefer_lowercase returns False when convert_urns_to_lowercase=False."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password="test_password",
        convert_urns_to_lowercase=False,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)
        assert source.get_prefer_lowercase() is False


def test_mssql_aggregator_receives_prefer_lowercase():
    """Test that SqlParsingAggregator receives the prefer_lowercase parameter."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password="test_password",
        convert_urns_to_lowercase=False,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)

        # Check that aggregator's schema resolver has the correct preference
        assert source.aggregator._schema_resolver._prefers_urn_lower() is False


def test_mssql_aggregator_with_convert_urns_to_lowercase_true():
    """Test backward compatibility: convert_urns_to_lowercase=True should still work."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password="test_password",
        convert_urns_to_lowercase=True,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)

        # Check that aggregator's schema resolver prefers lowercase
        assert source.aggregator._schema_resolver._prefers_urn_lower() is True


def test_mssql_default_config():
    """Test that default config (convert_urns_to_lowercase=False) works correctly."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password="test_password",
        # convert_urns_to_lowercase defaults to False
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)

        # Default is False, so should NOT prefer lowercase
        assert source.get_prefer_lowercase() is False
        assert source.aggregator._schema_resolver._prefers_urn_lower() is False


def test_mssql_no_warning_after_fix():
    """Test that the warning about lineage issues is removed after the fix."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password="test_password",
        convert_urns_to_lowercase=False,
        include_lineage=True,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)

        # Check that no warning about lineage was issued
        warnings = [
            w
            for w in source.report.warnings
            if "lineage" in w.get("message", "").lower()
        ]
        assert len(warnings) == 0, (
            "Warning about lineage should not be present after fix"
        )
