from unittest.mock import Mock

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.singlestore import (
    SingleStoreConfig,
    SingleStoreSource,
)


class TestSingleStoreConfig:
    def test_default_config(self):
        config_dict: dict[str, object] = {}
        config = SingleStoreConfig.model_validate(config_dict)

        assert config.host_port == "localhost:3306"
        assert config.scheme == "singlestoredb"
        assert config.include_stored_procedures
        assert config.procedure_pattern == AllowDenyPattern.allow_all()


class TestSingleStoreSource:
    def test_add_profile_metadata_handles_errors(self):
        config_dict: dict[str, object] = {
            "host_port": "localhost:3306",
            "username": "testuser",
            "database": "testdb",
            "password": "testpass",
            "profiling": {"enabled": True},
        }
        config = SingleStoreConfig.model_validate(config_dict)

        ctx = PipelineContext(run_id="test-run")

        source = SingleStoreSource(config, ctx)
        source.report = Mock()

        # Mock inspector and connection
        mock_inspector = Mock()
        mock_connection = Mock()

        # Set up context manager support
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_inspector.engine.connect.return_value = mock_context_manager

        # Simulate database error
        mock_connection.execute.side_effect = Exception("DB error")

        source.add_profile_metadata(inspector=mock_inspector)

        # Should log a warning
        source.report.warning.assert_called()

    def test_error_handling_in_get_procedures(self):
        config_dict: dict[str, object] = {
            "host_port": "localhost:3306",
            "username": "testuser",
            "database": "testdb",
            "password": "testpass",
        }
        config = SingleStoreConfig.model_validate(config_dict)

        ctx = PipelineContext(run_id="test-run")

        source = SingleStoreSource(config, ctx)
        source.report = Mock()

        # Mock inspector and connection
        mock_inspector = Mock()
        mock_connection = Mock()

        # Set up context manager support
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_inspector.engine.connect.return_value = mock_context_manager

        # Simulate database error
        mock_connection.execute.side_effect = Exception("DB error")

        result = source.get_procedures_for_schema(
            inspector=mock_inspector, schema="db", db_name=""
        )

        # Should return an empty list on error
        assert result == []

        # Should log a warning
        source.report.warning.assert_called()
