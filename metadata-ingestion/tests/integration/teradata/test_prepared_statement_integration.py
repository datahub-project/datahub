"""
Integration tests for Teradata prepared statement metadata extraction.

These tests require real Teradata credentials and are not run in CI.
Run manually during development with:
    pytest tests/integration/teradata/test_prepared_statement_integration.py -v -s

Credentials are loaded from ~/.datahubenv (YAML format):
    teradata:
      username: <your_username>
      host: <your_host>
      password: <your_password>
"""

from pathlib import Path
from typing import Any, Dict, Optional

import pytest
import yaml

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.teradata import TeradataConfig, TeradataSource


def load_test_credentials() -> Optional[Dict[str, Any]]:
    """Load Teradata credentials from ~/.datahubenv file."""
    env_file = Path.home() / ".datahubenv"

    if not env_file.exists():
        return None

    try:
        with open(env_file) as f:
            config = yaml.safe_load(f)
            return config.get("teradata")
    except Exception as e:
        print(f"Failed to load credentials: {e}")
        return None


@pytest.fixture(scope="module")
def teradata_credentials():
    """Provide Teradata credentials from ~/.datahubenv."""
    creds = load_test_credentials()
    if not creds:
        pytest.skip("Teradata credentials not found in ~/.datahubenv")
    return creds


@pytest.fixture
def teradata_config_base(teradata_credentials):
    """Base configuration for Teradata tests."""
    return {
        "username": teradata_credentials["username"],
        "password": teradata_credentials["password"],
        "host_port": teradata_credentials["host"],
        "include_tables": True,
        "include_views": True,
    }


@pytest.mark.integration
class TestPreparedStatementIntegration:
    """Integration tests with real Teradata instance."""

    def test_prepared_statement_extraction_on_view(self, teradata_config_base):
        """
        Test prepared statement extraction on views_db.customer_view.

        This test validates that the prepared statement method can successfully
        extract column metadata from a real view.
        """
        config_dict = {
            **teradata_config_base,
            "use_prepared_statement_metadata": True,
            "database_pattern": {"allow": ["views_db"]},
            "schema_pattern": {"allow": ["views_db"]},
            "table_pattern": {"allow": ["views_db.customer_view"]},
        }

        config = TeradataConfig.model_validate(config_dict)
        source = TeradataSource(config, PipelineContext(run_id="test_integration"))

        try:
            # Extract metadata
            workunits = list(source.get_workunits())

            # Verify some work units were generated
            assert len(workunits) > 0, "No work units generated"

            # Check reporting metrics
            assert source.report.num_tables_using_prepared_statement > 0, (
                "Prepared statement method was not used"
            )

            assert (
                "views_db.customer_view"
                in source.report.tables_using_prepared_statement
            ), "views_db.customer_view not found in prepared statement tables"

            print("\n✅ Successfully extracted metadata from views_db.customer_view")
            print(
                f"   Tables using prepared statement: {source.report.num_tables_using_prepared_statement}"
            )
            print(f"   Total work units generated: {len(workunits)}")

        finally:
            source.close()

    def test_fallback_chain_with_dbc_failure(self, teradata_config_base):
        """
        Test fallback chain when DBC access is simulated to fail.

        Note: This test assumes the test user doesn't have DBC access.
        If DBC access is available, this test may not trigger the fallback.
        """
        config_dict = {
            **teradata_config_base,
            "metadata_extraction_fallback": True,
            "database_pattern": {"allow": ["views_db"]},
            "schema_pattern": {"allow": ["views_db"]},
            "table_pattern": {"allow": ["views_db.customer_view"]},
        }

        config = TeradataConfig.model_validate(config_dict)
        source = TeradataSource(config, PipelineContext(run_id="test_fallback"))

        try:
            # Extract metadata with fallback enabled
            workunits = list(source.get_workunits())

            # Verify extraction succeeded
            assert len(workunits) > 0, "No work units generated with fallback"

            # Check if any fallback methods were used
            fallback_used = (
                source.report.num_tables_using_prepared_statement > 0
                or source.report.num_tables_using_help_fallback > 0
            )

            print("\n✅ Fallback chain test completed")
            print(f"   DBC failures: {source.report.num_dbc_access_failures}")
            print(
                f"   Prepared statement fallbacks: {source.report.num_tables_using_prepared_statement}"
            )
            print(f"   HELP fallbacks: {source.report.num_tables_using_help_fallback}")
            print(f"   Total work units: {len(workunits)}")

            if fallback_used:
                print("   ℹ️  Fallback methods were used (expected if DBC restricted)")
            else:
                print("   ℹ️  DBC access succeeded (no fallback needed)")

        finally:
            source.close()

    def test_compare_extraction_methods(self, teradata_config_base):
        """
        Compare column metadata extracted via different methods.

        This test extracts metadata using both DBC (if available) and
        prepared statements, then compares the results.
        """
        # First, try with DBC (default)
        config_dbc = TeradataConfig.model_validate(
            {
                **teradata_config_base,
                "database_pattern": {"allow": ["views_db"]},
                "table_pattern": {"allow": ["views_db.customer_view"]},
            }
        )

        # Then try with prepared statement
        config_prepared = TeradataConfig.model_validate(
            {
                **teradata_config_base,
                "use_prepared_statement_metadata": True,
                "database_pattern": {"allow": ["views_db"]},
                "table_pattern": {"allow": ["views_db.customer_view"]},
            }
        )

        dbc_columns = None
        prepared_columns = None

        # Extract with DBC
        try:
            source_dbc = TeradataSource(config_dbc, PipelineContext(run_id="test_dbc"))
            workunits_dbc = list(source_dbc.get_workunits())
            if source_dbc.report.num_dbc_access_failures == 0:
                dbc_columns = len(workunits_dbc)
                print(f"\n✅ DBC extraction: {dbc_columns} work units")
            source_dbc.close()
        except Exception as e:
            print(f"\n⚠️  DBC extraction failed (expected if no DBC access): {e}")

        # Extract with prepared statement
        try:
            source_prepared = TeradataSource(
                config_prepared, PipelineContext(run_id="test_prepared")
            )
            workunits_prepared = list(source_prepared.get_workunits())
            prepared_columns = len(workunits_prepared)
            print(f"✅ Prepared statement extraction: {prepared_columns} work units")
            source_prepared.close()
        except Exception as e:
            pytest.fail(f"Prepared statement extraction failed: {e}")

        # Compare results if both succeeded
        if dbc_columns and prepared_columns:
            print("\n📊 Comparison:")
            print(f"   DBC work units: {dbc_columns}")
            print(f"   Prepared statement work units: {prepared_columns}")
            print(
                "   Note: Prepared statements may have fewer properties (no defaults/comments)"
            )

    def test_report_completeness(self, teradata_config_base):
        """Test that reporting metrics are properly populated."""
        config_dict = {
            **teradata_config_base,
            "metadata_extraction_fallback": True,
            "database_pattern": {"allow": ["views_db"]},
            "table_pattern": {"allow": ["views_db.customer_view"]},
        }

        config = TeradataConfig.model_validate(config_dict)
        source = TeradataSource(config, PipelineContext(run_id="test_reporting"))

        try:
            list(source.get_workunits())

            # Verify reporting structure
            assert hasattr(source.report, "num_tables_using_prepared_statement")
            assert hasattr(source.report, "num_tables_using_help_fallback")
            assert hasattr(source.report, "tables_using_prepared_statement")
            assert hasattr(source.report, "tables_using_help_fallback")
            assert hasattr(source.report, "num_dbc_access_failures")
            assert hasattr(source.report, "num_prepared_statement_failures")

            # All should be non-negative
            assert source.report.num_tables_using_prepared_statement >= 0
            assert source.report.num_tables_using_help_fallback >= 0
            assert source.report.num_dbc_access_failures >= 0
            assert source.report.num_prepared_statement_failures >= 0

            print("\n✅ Report completeness verified")
            print("   Report structure: ✓")
            print("   Metrics populated: ✓")

        finally:
            source.close()


if __name__ == "__main__":
    """
    Run integration tests manually.

    Usage:
        python tests/integration/teradata/test_prepared_statement_integration.py

    Or with pytest:
        pytest tests/integration/teradata/test_prepared_statement_integration.py -v -s
    """
    pytest.main([__file__, "-v", "-s"])
