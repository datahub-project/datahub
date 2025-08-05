import logging
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.looker.looker_common import ExploreUpstreamViewField
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
    LookerViewNamingPattern,
)


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
