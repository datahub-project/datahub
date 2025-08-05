import logging
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.looker.looker_common import (
    ExploreUpstreamViewField,
    LookerExplore,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
    LookerViewNamingPattern,
)
from datahub.ingestion.source.looker.looker_dataclasses import ProjectInclude
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.sql_parsing.sqlglot_lineage import ColumnRef


class TestExploreUpstreamViewFieldFormFieldName:
    """Test the _form_field_name method with empty field validation."""

    @pytest.fixture
    def mock_explore(self):
        """Create a mock explore object."""
        explore = MagicMock()
        explore.name = "test_explore"
        return explore

    @pytest.fixture
    def mock_config(self):
        """Create a mock config object."""
        config = MagicMock(spec=LookerCommonConfig)
        config.platform_name = "looker"
        config.platform_instance = None
        config.env = "PROD"
        config.view_naming_pattern = LookerViewNamingPattern(
            pattern="{project}.view.{name}"
        )
        return config

    @pytest.fixture
    def default_params(self, mock_config):
        """Default parameters for _form_field_name method."""
        return {
            "view_project_map": {"test_view": "test_project"},
            "explore_project_name": "test_project",
            "model_name": "test_model",
            "upstream_views_file_path": {"test_view": "test_file.view.lkml"},
            "config": mock_config,
        }

    def test_form_field_name_with_valid_field(self, mock_explore, default_params):
        """Test _form_field_name with a valid field that has a proper name."""
        # Setup - create a field with valid name
        valid_field = MagicMock()
        valid_field.name = "test_view.test_field"
        valid_field.original_view = None
        valid_field.field_group_variant = None

        upstream_field = ExploreUpstreamViewField(
            field=valid_field, explore=mock_explore
        )

        # Execute
        result = upstream_field._form_field_name(**default_params)

        # Assert
        assert result is not None
        assert result.column == "test_field"

    def test_form_field_name_with_empty_field_name(
        self, mock_explore, default_params, caplog
    ):
        """Test _form_field_name with empty field name returns None and logs warning."""
        # Setup - create a field that will result in empty field name after processing
        empty_field = MagicMock()
        empty_field.name = (
            "test_view."  # Field ending with dot - will result in empty field name
        )
        empty_field.original_view = None
        empty_field.field_group_variant = None

        upstream_field = ExploreUpstreamViewField(
            field=empty_field, explore=mock_explore
        )

        with caplog.at_level(logging.WARNING):
            # Execute
            result = upstream_field._form_field_name(**default_params)

            # Assert
            assert result is None
            assert "Empty field name detected" in caplog.text
            assert "test_view." in caplog.text
            assert "test_explore" in caplog.text

    def test_form_field_name_with_whitespace_only_field_name(
        self, mock_explore, default_params, caplog
    ):
        """Test _form_field_name with whitespace-only field name returns None and logs warning."""
        # Setup - create a field that results in whitespace-only field name
        whitespace_field = MagicMock()
        whitespace_field.name = "test_view.   "  # Field with whitespace after dot
        whitespace_field.original_view = None
        whitespace_field.field_group_variant = None

        upstream_field = ExploreUpstreamViewField(
            field=whitespace_field, explore=mock_explore
        )

        with caplog.at_level(logging.WARNING):
            # Execute
            result = upstream_field._form_field_name(**default_params)

            # Assert
            assert result is None
            assert "Empty field name detected" in caplog.text
            assert "test_view.   " in caplog.text
            assert "test_explore" in caplog.text

    def test_form_field_name_with_field_group_variant_removal_causing_empty_name(
        self, mock_explore, default_params, caplog
    ):
        """Test _form_field_name when field group variant removal results in empty field name."""
        # Setup - create a field where removing the field group variant results in empty name
        variant_field = MagicMock()
        variant_field.name = (
            "test_view.month"  # Field that might become empty after variant removal
        )
        variant_field.original_view = None
        variant_field.field_group_variant = (
            "month"  # Variant that when removed would make name empty
        )

        upstream_field = ExploreUpstreamViewField(
            field=variant_field, explore=mock_explore
        )

        with caplog.at_level(logging.WARNING):
            # Execute with remove_variant=True to trigger the problematic scenario
            result = upstream_field._form_field_name(
                remove_variant=True, **default_params
            )

            # Assert
            assert result is None
            assert "Empty field name detected" in caplog.text
            assert "test_view.month" in caplog.text
            assert "test_explore" in caplog.text

    def test_form_field_name_prevents_invalid_schema_field_urn(
        self, mock_explore, default_params
    ):
        """Test that empty field names don't create invalid schema field URNs."""
        # Setup - create a field that will result in empty field name
        empty_field = MagicMock()
        empty_field.name = "test_view."
        empty_field.original_view = None
        empty_field.field_group_variant = None

        upstream_field = ExploreUpstreamViewField(
            field=empty_field, explore=mock_explore
        )

        # Execute
        result = upstream_field._form_field_name(**default_params)

        # Assert - this should return None to prevent creation of invalid URN
        assert result is None

    def test_form_field_name_with_inconsistent_field_name_format(
        self, mock_explore, default_params
    ):
        """Test _form_field_name with field name that doesn't have proper view.field format."""
        # Setup - create a field with inconsistent name format (no dot)
        inconsistent_field = MagicMock()
        inconsistent_field.name = "just_field_name"  # No view prefix
        inconsistent_field.original_view = None
        inconsistent_field.field_group_variant = None

        upstream_field = ExploreUpstreamViewField(
            field=inconsistent_field, explore=mock_explore
        )

        # Execute
        result = upstream_field._form_field_name(**default_params)

        # Assert - should return None due to inconsistent format (not 2 parts when split by dot)
        assert result is None


class TestLookerExploreFineGrainedLineage:
    """Test fine-grained lineage creation with empty field name validation."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config object with extract_column_level_lineage enabled."""
        config = MagicMock(spec=LookerCommonConfig)
        config.platform_name = "looker"
        config.platform_instance = None
        config.env = "PROD"
        config.extract_column_level_lineage = True
        config.explore_naming_pattern = MagicMock()
        config.explore_naming_pattern.replace_variables.return_value = "test_explore"
        config.explore_browse_pattern = MagicMock()
        config.explore_browse_pattern.replace_variables.return_value = (
            "/test_browse_path"
        )
        config.view_naming_pattern = MagicMock()
        config.view_naming_pattern.replace_variables.return_value = "test_view"
        return config

    @pytest.fixture
    def mock_reporter(self):
        """Create a mock reporter."""
        return MagicMock()

    def test_to_metadata_events_skips_empty_field_names(
        self, mock_config, mock_reporter, caplog
    ):
        """Test that _to_metadata_events skips fields with empty names in fine-grained lineage."""
        # Setup - create an explore with fields including one with empty name
        valid_field = ViewField(
            name="valid_field",
            label="Valid Field",
            type="string",
            description="A valid field",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column")
            ],
        )

        empty_name_field = ViewField(
            name="",  # Empty field name
            label="Empty Name Field",
            type="string",
            description="A field with empty name",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column2")
            ],
        )

        whitespace_name_field = ViewField(
            name="   ",  # Whitespace-only field name
            label="Whitespace Name Field",
            type="string",
            description="A field with whitespace name",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column3")
            ],
        )

        explore = LookerExplore(
            name="test_explore",
            model_name="test_model",
            project_name="test_project",
            fields=[valid_field, empty_name_field, whitespace_name_field],
            upstream_views=[
                ProjectInclude(project="test_project", include="test_view")
            ],
            upstream_views_file_path={"test_view": "test_view.view.lkml"},
        )

        with caplog.at_level(logging.WARNING):
            # Execute
            result = explore._to_metadata_events(
                config=mock_config,
                reporter=mock_reporter,
                base_url="https://test.looker.com",
                extract_embed_urls=False,
            )

            # Assert
            assert result is not None

            # Check that warnings were logged for empty field names
            warning_messages = [
                record.message
                for record in caplog.records
                if record.levelname == "WARNING"
            ]
            empty_field_warnings = [
                msg
                for msg in warning_messages
                if "Skipping fine-grained lineage for field with empty name" in msg
            ]
            assert (
                len(empty_field_warnings) == 2
            )  # One for empty string, one for whitespace

            # Verify the warning messages contain the explore name
            assert any("test_explore" in msg for msg in empty_field_warnings)

    def test_to_metadata_events_processes_valid_field_names(
        self, mock_config, mock_reporter
    ):
        """Test that _to_metadata_events processes fields with valid names in fine-grained lineage."""
        # Setup - create an explore with only valid fields
        valid_field1 = ViewField(
            name="valid_field1",
            label="Valid Field 1",
            type="string",
            description="A valid field",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column1")
            ],
        )

        valid_field2 = ViewField(
            name="valid_field2",
            label="Valid Field 2",
            type="number",
            description="Another valid field",
            field_type=ViewFieldType.MEASURE,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column2")
            ],
        )

        explore = LookerExplore(
            name="test_explore",
            model_name="test_model",
            project_name="test_project",
            fields=[valid_field1, valid_field2],
            upstream_views=[
                ProjectInclude(project="test_project", include="test_view")
            ],
            upstream_views_file_path={"test_view": "test_view.view.lkml"},
        )

        # Execute
        result = explore._to_metadata_events(
            config=mock_config,
            reporter=mock_reporter,
            base_url="https://test.looker.com",
            extract_embed_urls=False,
        )

        # Assert
        assert result is not None
        assert len(result) > 0

        # Find the MCE with upstream lineage
        upstream_lineage_aspect = None
        for event in result:
            if hasattr(event, "proposedSnapshot") and event.proposedSnapshot.aspects:
                for aspect in event.proposedSnapshot.aspects:
                    if isinstance(aspect, UpstreamLineage):
                        upstream_lineage_aspect = aspect
                        break

        # Should have fine-grained lineages for the valid fields
        assert upstream_lineage_aspect is not None
        if upstream_lineage_aspect.fineGrainedLineages:
            assert (
                len(upstream_lineage_aspect.fineGrainedLineages) == 2
            )  # Two valid fields

    def test_to_metadata_events_mixed_valid_and_invalid_fields(
        self, mock_config, mock_reporter, caplog
    ):
        """Test that _to_metadata_events handles mix of valid and invalid field names."""
        # Setup - create an explore with mix of valid and invalid fields
        valid_field = ViewField(
            name="valid_field",
            label="Valid Field",
            type="string",
            description="A valid field",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column")
            ],
        )

        invalid_field = ViewField(
            name="",  # Empty field name
            label="Invalid Field",
            type="string",
            description="An invalid field",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column2")
            ],
        )

        explore = LookerExplore(
            name="test_explore",
            model_name="test_model",
            project_name="test_project",
            fields=[valid_field, invalid_field],
            upstream_views=[
                ProjectInclude(project="test_project", include="test_view")
            ],
            upstream_views_file_path={"test_view": "test_view.view.lkml"},
        )

        with caplog.at_level(logging.WARNING):
            # Execute
            result = explore._to_metadata_events(
                config=mock_config,
                reporter=mock_reporter,
                base_url="https://test.looker.com",
                extract_embed_urls=False,
            )

            # Assert
            assert result is not None

            # Should have warning for the invalid field
            warning_messages = [
                record.message
                for record in caplog.records
                if record.levelname == "WARNING"
            ]
            empty_field_warnings = [
                msg
                for msg in warning_messages
                if "Skipping fine-grained lineage for field with empty name" in msg
            ]
            assert len(empty_field_warnings) == 1

    def test_to_metadata_events_no_extract_column_level_lineage(self, mock_reporter):
        """Test that no fine-grained lineage is created when extract_column_level_lineage is False."""
        # Setup config with extract_column_level_lineage disabled
        config = MagicMock(spec=LookerCommonConfig)
        config.platform_name = "looker"
        config.platform_instance = None
        config.env = "PROD"
        config.extract_column_level_lineage = False  # Disabled
        config.explore_naming_pattern = MagicMock()
        config.explore_naming_pattern.replace_variables.return_value = "test_explore"
        config.explore_browse_pattern = MagicMock()
        config.explore_browse_pattern.replace_variables.return_value = (
            "/test_browse_path"
        )

        # Create explore with fields
        field = ViewField(
            name="test_field",
            label="Test Field",
            type="string",
            description="A test field",
            field_type=ViewFieldType.DIMENSION,
            upstream_fields=[
                ColumnRef(table="urn:li:dataset:test", column="source_column")
            ],
        )

        explore = LookerExplore(
            name="test_explore",
            model_name="test_model",
            project_name="test_project",
            fields=[field],
        )

        # Execute
        result = explore._to_metadata_events(
            config=config,
            reporter=mock_reporter,
            base_url="https://test.looker.com",
            extract_embed_urls=False,
        )

        # Assert - should not process fine-grained lineage at all
        assert result is not None

        # Find the MCE with upstream lineage
        upstream_lineage_aspect = None
        for event in result:
            if hasattr(event, "proposedSnapshot") and event.proposedSnapshot.aspects:
                for aspect in event.proposedSnapshot.aspects:
                    if isinstance(aspect, UpstreamLineage):
                        upstream_lineage_aspect = aspect
                        break

        # Should have no fine-grained lineages when disabled
        if upstream_lineage_aspect:
            assert upstream_lineage_aspect.fineGrainedLineages is None
