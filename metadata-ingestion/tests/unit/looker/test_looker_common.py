import logging
from unittest.mock import MagicMock

import pytest
from looker_sdk.sdk.api40.models import LookmlModelExplore, LookmlModelExploreField

from datahub.ingestion.source.looker.looker_common import ExploreUpstreamViewField
from datahub.ingestion.source.looker.looker_config import LookerCommonConfig


class TestExploreUpstreamViewFieldFormFieldName:
    """Test empty field name validation in _form_field_name method."""

    @pytest.mark.parametrize(
        "field_name",
        [
            "test_view.",  # Empty after dot
            "test_view.   ",  # Whitespace after dot
        ],
    )
    def test_returns_none_for_empty_field_name(self, field_name, caplog):
        """Test that empty field names return None and log warnings."""
        explore = LookmlModelExplore(name="test_explore")
        field = LookmlModelExploreField(
            name=field_name, type="string", original_view=None, field_group_variant=None
        )
        upstream_field = ExploreUpstreamViewField(field=field, explore=explore)
        config = MagicMock(spec=LookerCommonConfig)

        with caplog.at_level(logging.WARNING):
            result = upstream_field._form_field_name(
                view_project_map={},
                explore_project_name="test_project",
                model_name="test_model",
                upstream_views_file_path={},
                config=config,
            )

            assert result is None
            assert "Empty field name detected" in caplog.text
            assert field_name in caplog.text
            assert "test_explore" in caplog.text

    def test_returns_none_for_invalid_field_format(self):
        """Test that fields without proper view.field format return None."""
        explore = LookmlModelExplore(name="test_explore")
        field = LookmlModelExploreField(
            name="just_field_name",  # No dot separator
            type="string",
            original_view=None,
            field_group_variant=None,
        )
        upstream_field = ExploreUpstreamViewField(field=field, explore=explore)

        result = upstream_field._form_field_name(
            view_project_map={},
            explore_project_name="test_project",
            model_name="test_model",
            upstream_views_file_path={},
            config=MagicMock(spec=LookerCommonConfig),
        )

        assert result is None

    def test_variant_removal_causing_empty_name(self, caplog):
        """Test that variant removal resulting in empty name returns None."""
        explore = LookmlModelExplore(name="test_explore")
        # In Looker, dimension groups create fields with variants like "created_date_month", "created_date_year"
        # This test simulates a pathological case where:
        # - field name is "test_view.month" (after splitting by ".", field_name becomes "month")
        # - field_group_variant is "month"
        # - When remove_variant=True, it removes "_month" from "month", resulting in empty string
        # - This should be handled gracefully by returning None and logging a warning
        field = LookmlModelExploreField(
            name="test_view.month",
            type="string",
            original_view=None,
            field_group_variant="month",  # When removed with underscore ("_month"), leaves empty field name
        )
        upstream_field = ExploreUpstreamViewField(field=field, explore=explore)

        with caplog.at_level(logging.WARNING):
            result = upstream_field._form_field_name(
                view_project_map={},
                explore_project_name="test_project",
                model_name="test_model",
                upstream_views_file_path={},
                config=MagicMock(spec=LookerCommonConfig),
                remove_variant=True,
            )

            assert result is None
            assert "Empty field name detected" in caplog.text
