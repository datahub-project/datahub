"""
Unit tests for MSSQL source get_prefer_lowercase method.

Tests the fix for issue #13792: MSSQL View Ingestion Causes Lowercased Lineage Table URNs
"""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource


def test_mssql_get_prefer_lowercase_true():
    """Test that get_prefer_lowercase returns True when convert_urns_to_lowercase=True."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password=SecretStr("test_password"),
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
        password=SecretStr("test_password"),
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
        password=SecretStr("test_password"),
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
        password=SecretStr("test_password"),
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
        password=SecretStr("test_password"),
        # convert_urns_to_lowercase defaults to False
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)

        # Default is False, so should NOT prefer lowercase
        assert source.get_prefer_lowercase() is False
        assert source.aggregator._schema_resolver._prefers_urn_lower() is False


def test_mssql_source_init_does_not_warn_about_lineage():
    """Test that source initialization does not generate lineage-related warnings.

    This ensures that the fix for issue #13792 doesn't introduce unnecessary
    warnings when using the default convert_urns_to_lowercase=False setting.
    """
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password=SecretStr("test_password"),
        convert_urns_to_lowercase=False,
        include_lineage=True,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)

        # Verify no lineage-related warnings are generated during initialization
        lineage_warnings = [
            w for w in source.report.warnings if "lineage" in w.message.lower()
        ]
        assert not lineage_warnings, (
            f"Unexpected lineage warnings during source init: {lineage_warnings}"
        )


def test_base_sqlalchemy_source_get_prefer_lowercase_returns_none():
    """Test that the base SQLAlchemySource.get_prefer_lowercase returns None (platform default)."""
    # Create a mock source to test the base class method
    mock_source = MagicMock(spec=SQLAlchemySource)

    # Call the actual base class method
    result = SQLAlchemySource.get_prefer_lowercase(mock_source)

    assert result is None, (
        "Base SQLAlchemySource should return None for platform defaults"
    )


@pytest.mark.parametrize(
    "convert_urns_to_lowercase,expected",
    [
        (True, True),
        (False, False),
    ],
)
def test_mssql_get_prefer_lowercase_parametrized(
    convert_urns_to_lowercase: bool, expected: bool
) -> None:
    """Parametrized test for get_prefer_lowercase with different config values."""
    config = SQLServerConfig(
        host_port="localhost:1433",
        database="test_db",
        username="test_user",
        password=SecretStr("test_password"),
        convert_urns_to_lowercase=convert_urns_to_lowercase,
    )

    ctx = PipelineContext(run_id="test")

    with patch.object(SQLServerSource, "get_inspectors", return_value=[]):
        source = SQLServerSource(config, ctx)
        assert source.get_prefer_lowercase() is expected
        assert source.aggregator._schema_resolver._prefers_urn_lower() is expected
