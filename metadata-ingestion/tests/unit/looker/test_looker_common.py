import logging
from unittest.mock import MagicMock

import pytest
from looker_sdk.sdk.api40.models import LookmlModelExplore, LookmlModelExploreField

from datahub.ingestion.source.looker.looker_common import (
    ExploreUpstreamViewField,
    ViewField,
    ViewFieldType,
    create_view_project_map,
    extract_project_from_imported_file_path,
)
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


class TestExtractProjectFromImportedFilePath:
    @pytest.mark.parametrize(
        "file_path,expected",
        [
            (
                "imported_projects/project-a/views/foo.view.lkml",
                "project-a",
            ),
            (
                "imported_projects/my-project/path/to/file.view.lkml",
                "my-project",
            ),
            (
                "views/foo.view.lkml",  # same-project path, no imported_projects/ prefix
                None,
            ),
            (
                "imported_projects",  # malformed: no slash after the prefix
                None,
            ),
        ],
    )
    def test_extract(self, file_path: str, expected: "str | None") -> None:
        assert extract_project_from_imported_file_path(file_path) == expected


class TestCreateViewProjectMap:
    def _make_view_field(self, view_name: str, project_name: "str | None") -> ViewField:
        return ViewField(
            name=f"{view_name}.some_field",
            label=None,
            type="string",
            description="",
            field_type=ViewFieldType.DIMENSION,
            project_name=project_name,
            view_name=view_name,
        )

    def test_cross_project_primary_view_not_overridden(self) -> None:
        # Regression test: the primary view's project must NOT be replaced with the explore's
        # project when the view is a cross-project import (project_name already set correctly).
        view_field = self._make_view_field("my_view", project_name="project-a")
        result = create_view_project_map(
            view_fields=[view_field],
            explore_primary_view="my_view",
            explore_project_name="project-b",
        )
        assert result["my_view"] == "project-a"

    def test_cross_project_non_primary_view(self) -> None:
        view_field = self._make_view_field("other_view", project_name="project-a")
        result = create_view_project_map(
            view_fields=[view_field],
            explore_primary_view="my_view",
            explore_project_name="project-b",
        )
        assert result["other_view"] == "project-a"

    def test_same_project_view_not_in_map(self) -> None:
        # Same-project views have project_name=None; they should not appear in the map
        # and fall back to explore_project_name via the BASE_PROJECT_NAME sentinel.
        view_field = self._make_view_field("my_view", project_name=None)
        result = create_view_project_map(
            view_fields=[view_field],
            explore_primary_view="my_view",
            explore_project_name="project-b",
        )
        assert "my_view" not in result
