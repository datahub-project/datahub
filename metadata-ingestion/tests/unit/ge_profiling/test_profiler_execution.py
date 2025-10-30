"""
Tests for GE profiler execution functionality, specifically testing the tags_to_ignore_profiling feature.

These tests focus on the actual execution logic of the profiler, including:
- Column filtering based on tags
- Integration with DataHub graph client
- Profiler workflow with tagged columns
"""

from unittest.mock import Mock, patch

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import EditableSchemaMetadata
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass


class TestGetColumnsToIgnoreProfiling:
    """Test the _get_columns_to_ignore_profiling function."""

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_no_tags_configured(self, mock_get_graph):
        """Test _get_columns_to_ignore_profiling with no tags configured."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", None, "snowflake", "PROD"
        )

        assert ignore_table is False
        assert columns_to_ignore == []
        mock_get_graph.assert_not_called()

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_table_level_tag_ignore(self, mock_get_graph):
        """Test _get_columns_to_ignore_profiling with table-level tag that should ignore entire table."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        # Mock table-level tags
        mock_dataset_tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:PII")]
        )
        mock_graph.get_tags.return_value = mock_dataset_tags

        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", ["PII"], "snowflake", "PROD"
        )

        assert ignore_table is True
        assert columns_to_ignore == []
        mock_graph.get_tags.assert_called_once()

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_column_level_tags_ignore(self, mock_get_graph):
        """Test _get_columns_to_ignore_profiling with column-level tags."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        # Mock no table-level tags
        mock_graph.get_tags.return_value = None

        # Mock column-level tags
        mock_schema_field1 = Mock()
        mock_schema_field1.fieldPath = "email"
        mock_schema_field1.globalTags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:PII")]
        )

        mock_schema_field2 = Mock()
        mock_schema_field2.fieldPath = "phone"
        mock_schema_field2.globalTags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:Sensitive")]
        )

        mock_schema_field3 = Mock()
        mock_schema_field3.fieldPath = "name"
        mock_schema_field3.globalTags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:Public")]
        )

        mock_metadata = EditableSchemaMetadata(
            editableSchemaFieldInfo=[
                mock_schema_field1,
                mock_schema_field2,
                mock_schema_field3,
            ]
        )
        mock_graph.get_aspect.return_value = mock_metadata

        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", ["PII", "Sensitive"], "snowflake", "PROD"
        )

        assert ignore_table is False
        assert set(columns_to_ignore) == {"email", "phone"}
        mock_graph.get_tags.assert_called_once()
        mock_graph.get_aspect.assert_called_once()

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_no_matching_tags(self, mock_get_graph):
        """Test _get_columns_to_ignore_profiling with no matching tags."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        # Mock no table-level tags
        mock_graph.get_tags.return_value = None

        # Mock column-level tags that don't match
        mock_schema_field = Mock()
        mock_schema_field.fieldPath = "email"
        mock_schema_field.globalTags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:Public")]
        )

        mock_metadata = EditableSchemaMetadata(
            editableSchemaFieldInfo=[mock_schema_field]
        )
        mock_graph.get_aspect.return_value = mock_metadata

        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", ["PII", "Sensitive"], "snowflake", "PROD"
        )

        assert ignore_table is False
        assert columns_to_ignore == []

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_mixed_tagged_and_untagged_columns(self, mock_get_graph):
        """Test with a mix of tagged and untagged columns."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        # Mock no table-level tags
        mock_graph.get_tags.return_value = None

        # Mock mix of tagged and untagged columns
        mock_schema_field1 = Mock()
        mock_schema_field1.fieldPath = "id"
        mock_schema_field1.globalTags = None  # No tags

        mock_schema_field2 = Mock()
        mock_schema_field2.fieldPath = "email"
        mock_schema_field2.globalTags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:PII")]
        )

        mock_schema_field3 = Mock()
        mock_schema_field3.fieldPath = "name"
        mock_schema_field3.globalTags = None  # No tags

        mock_metadata = EditableSchemaMetadata(
            editableSchemaFieldInfo=[
                mock_schema_field1,
                mock_schema_field2,
                mock_schema_field3,
            ]
        )
        mock_graph.get_aspect.return_value = mock_metadata

        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", ["PII"], "snowflake", "PROD"
        )

        assert ignore_table is False
        assert columns_to_ignore == ["email"]  # Only the tagged column


class TestSingleDatasetProfilerIntegration:
    """Test the integration of tags_to_ignore_profiling with _SingleDatasetProfiler."""

    @patch("datahub.ingestion.source.ge_data_profiler._get_columns_to_ignore_profiling")
    def test_get_columns_to_profile_excludes_tagged_columns(
        self, mock_get_columns_to_ignore
    ):
        """Test that _get_columns_to_profile excludes tagged columns."""
        from datahub.ingestion.source.ge_data_profiler import _SingleDatasetProfiler

        # Mock the function to return tagged columns
        mock_get_columns_to_ignore.return_value = (False, ["email", "ssn"])

        # Create a mock dataset with columns
        mock_dataset = Mock()
        mock_dataset.columns = [
            {"name": "id", "type": "INTEGER"},
            {"name": "email", "type": "VARCHAR"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "ssn", "type": "VARCHAR"},
            {"name": "age", "type": "INTEGER"},
        ]

        # Create a mock config
        mock_config = Mock()
        mock_config.tags_to_ignore_profiling = ["PII"]
        mock_config.any_field_level_metrics_enabled.return_value = True
        mock_config._allow_deny_patterns.allowed.return_value = True
        mock_config.profile_nested_fields = True
        mock_config.max_number_of_fields_to_profile = None

        # Create a mock report
        mock_report = Mock()

        # Create profiler instance
        profiler = _SingleDatasetProfiler(
            dataset=mock_dataset,
            dataset_name="test_dataset",
            partition=None,
            config=mock_config,
            report=mock_report,
            custom_sql=None,
            query_combiner=Mock(),
            platform="snowflake",
            env="PROD",
        )

        # Call the method
        columns_to_profile = profiler._get_columns_to_profile()

        # Verify tagged columns are excluded
        assert set(columns_to_profile) == {"id", "name", "age"}
        assert "email" not in columns_to_profile
        assert "ssn" not in columns_to_profile

        # Verify the ignore function was called
        mock_get_columns_to_ignore.assert_called_once_with(
            "test_dataset", ["PII"], "snowflake", "PROD"
        )

        # Verify reporting
        mock_report.report_dropped.assert_called_once()
        call_args = mock_report.report_dropped.call_args[0][0]
        assert "columns by tags" in call_args
        assert "email" in call_args
        assert "ssn" in call_args

    @patch("datahub.ingestion.source.ge_data_profiler._get_columns_to_ignore_profiling")
    def test_get_columns_to_profile_ignores_entire_table(
        self, mock_get_columns_to_ignore
    ):
        """Test that _get_columns_to_profile ignores entire table when tagged."""
        from datahub.ingestion.source.ge_data_profiler import _SingleDatasetProfiler

        # Mock the function to return table-level ignore
        mock_get_columns_to_ignore.return_value = (True, [])

        # Create a mock dataset with columns
        mock_dataset = Mock()
        mock_dataset.columns = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
        ]

        # Create a mock config
        mock_config = Mock()
        mock_config.tags_to_ignore_profiling = ["PII"]
        mock_config.any_field_level_metrics_enabled.return_value = True

        # Create a mock report
        mock_report = Mock()

        # Create profiler instance
        profiler = _SingleDatasetProfiler(
            dataset=mock_dataset,
            dataset_name="test_dataset",
            partition=None,
            config=mock_config,
            report=mock_report,
            custom_sql=None,
            query_combiner=Mock(),
            platform="snowflake",
            env="PROD",
        )

        # Call the method
        columns_to_profile = profiler._get_columns_to_profile()

        # Verify entire table is ignored
        assert columns_to_profile == []

        # Verify the ignore function was called
        mock_get_columns_to_ignore.assert_called_once_with(
            "test_dataset", ["PII"], "snowflake", "PROD"
        )

        # Verify table-level reporting
        mock_report.report_dropped.assert_called_once()
        call_args = mock_report.report_dropped.call_args[0][0]
        assert "table test_dataset" in call_args
        assert "tagged with tags_to_ignore_profiling" in call_args

    @patch("datahub.ingestion.source.ge_data_profiler._get_columns_to_ignore_profiling")
    def test_get_columns_to_profile_no_tags_configured(
        self, mock_get_columns_to_ignore
    ):
        """Test that profiler works normally when no tags_to_ignore_profiling is configured."""
        from datahub.ingestion.source.ge_data_profiler import _SingleDatasetProfiler

        # Mock the function to return no ignored columns
        mock_get_columns_to_ignore.return_value = (False, [])

        # Create a mock dataset with columns
        mock_dataset = Mock()
        mock_dataset.columns = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
        ]

        # Create a mock config with no tags_to_ignore_profiling
        mock_config = Mock()
        mock_config.tags_to_ignore_profiling = None
        mock_config.any_field_level_metrics_enabled.return_value = True
        mock_config._allow_deny_patterns.allowed.return_value = True
        mock_config.profile_nested_fields = True
        mock_config.max_number_of_fields_to_profile = None

        # Create a mock report
        mock_report = Mock()

        # Create profiler instance
        profiler = _SingleDatasetProfiler(
            dataset=mock_dataset,
            dataset_name="test_dataset",
            partition=None,
            config=mock_config,
            report=mock_report,
            custom_sql=None,
            query_combiner=Mock(),
            platform="snowflake",
            env="PROD",
        )

        # Call the method
        columns_to_profile = profiler._get_columns_to_profile()

        # Verify all columns are included
        assert set(columns_to_profile) == {"id", "name"}

        # Verify the ignore function was called with None
        mock_get_columns_to_ignore.assert_called_once_with(
            "test_dataset", None, "snowflake", "PROD"
        )

    def test_profiler_config_integration(self):
        """Test that the profiler can access tags_to_ignore_profiling from config."""
        # Create a real config with tags_to_ignore_profiling
        config = GEProfilingConfig.model_validate(
            {
                "enabled": True,
                "tags_to_ignore_profiling": ["PII", "Sensitive"],
                "include_field_null_count": True,
            }
        )

        # Verify the config field is accessible
        assert hasattr(config, "tags_to_ignore_profiling")
        assert config.tags_to_ignore_profiling == ["PII", "Sensitive"]
        assert config.include_field_null_count is True


class TestEdgeCases:
    """Test edge cases and error scenarios."""

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_datahub_graph_error_handling(self, mock_get_graph):
        """Test error handling when DataHub graph client fails."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        # Mock DataHub graph to raise an exception
        mock_graph.get_tags.side_effect = Exception("DataHub connection failed")

        # Function should handle the error gracefully and return no ignored columns
        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", ["PII"], "snowflake", "PROD"
        )

        # Should default to not ignoring anything when there's an error
        assert ignore_table is False
        assert columns_to_ignore == []

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_empty_tags_list(self, mock_get_graph):
        """Test with empty tags list."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", [], "snowflake", "PROD"
        )

        # Empty tags list should be treated the same as None
        assert ignore_table is False
        assert columns_to_ignore == []
        mock_get_graph.assert_not_called()

    @patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
    def test_malformed_tag_urns(self, mock_get_graph):
        """Test handling of malformed tag URNs."""
        from datahub.ingestion.source.ge_data_profiler import (
            _get_columns_to_ignore_profiling,
        )

        mock_graph = Mock()
        mock_get_graph.return_value = mock_graph

        # Mock table-level tags with malformed URN
        mock_dataset_tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag="malformed-tag-urn")]
        )
        mock_graph.get_tags.return_value = mock_dataset_tags

        # Function should handle malformed URNs gracefully
        ignore_table, columns_to_ignore = _get_columns_to_ignore_profiling(
            "test_dataset", ["PII"], "snowflake", "PROD"
        )

        # Should not crash and should not match malformed URNs
        assert ignore_table is False
        assert columns_to_ignore == []
