import logging
from typing import List
from unittest.mock import MagicMock

import pytest
from looker_sdk.sdk.api40.models import (
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelExploreJoins,
)

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.looker.looker_common import (
    ExploreUpstreamViewField,
    LookerExplore,
    LookerExploreJoin,
    ViewField,
    ViewFieldType,
    create_view_project_map,
    extract_project_from_imported_file_path,
    get_view_file_path,
)
from datahub.ingestion.source.looker.looker_config import LookerCommonConfig
from datahub.ingestion.source.looker.lookml_config import BASE_PROJECT_NAME


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


class TestLookerExploreJoinReconstruction:
    """Explore joins carry the relationship semantics needed to reconstruct the
    semantic model. These tests cover capturing them and serializing them back
    to LookML view logic."""

    def test_from_api_join_captures_relationship_semantics(self):
        join = LookerExploreJoin.from_api_join(
            LookmlModelExploreJoins(
                name="orders",
                from_="orders_base",
                sql_on="${orders.customer_id} = ${customers.id}",
                relationship="many_to_one",
                type="left_outer",
                foreign_key="customer_id",
            )
        )
        assert join is not None
        assert join.name == "orders"
        assert join.from_view == "orders_base"
        assert join.sql_on == "${orders.customer_id} = ${customers.id}"
        assert join.relationship == "many_to_one"
        assert join.join_type == "left_outer"
        assert join.foreign_key == "customer_id"

    def test_from_lkml_join_reads_from_key(self):
        join = LookerExploreJoin.from_lkml_join(
            {"name": "orders", "from": "orders_base", "sql_on": "1=1"}
        )
        assert join is not None
        assert join.from_view == "orders_base"
        assert join.sql_on == "1=1"

    def test_build_explore_view_logic_renders_joins(self):
        explore = LookerExplore(
            name="customer_orders",
            model_name="ecommerce",
            label="Customer Orders",
            join_definitions=[
                LookerExploreJoin(
                    name="orders",
                    sql_on="${orders.customer_id} = ${customers.id}",
                    relationship="many_to_one",
                    join_type="left_outer",
                ),
            ],
        )
        view_logic = explore._build_explore_view_logic()
        assert view_logic is not None
        assert "explore: customer_orders {" in view_logic
        assert 'label: "Customer Orders"' in view_logic
        assert "join: orders {" in view_logic
        assert "type: left_outer" in view_logic
        assert "relationship: many_to_one" in view_logic
        assert "sql_on: ${orders.customer_id} = ${customers.id} ;;" in view_logic

    def test_build_explore_view_logic_none_without_joins(self):
        explore = LookerExplore(name="passthrough", model_name="ecommerce")
        assert explore._build_explore_view_logic() is None

    def test_build_explore_view_logic_escapes_label_quotes(self):
        explore = LookerExplore(
            name="customer_orders",
            model_name="ecommerce",
            label='My "Q3" Explore',
            join_definitions=[LookerExploreJoin(name="orders")],
        )
        view_logic = explore._build_explore_view_logic()
        assert view_logic is not None
        assert r'label: "My \"Q3\" Explore"' in view_logic


class TestExtractProjectFromImportedFilePath:
    @pytest.mark.parametrize(
        "file_path,expected",
        [
            (
                "imported_projects/project-a/views/foo.view.lkml",
                "project-a",
            ),
            (
                "views/foo.view.lkml",
                None,
            ),
            (
                "imported_projects/",
                None,
            ),
            (
                "imported_projects//views/x",
                None,
            ),
            (
                "imported_projects_v2/views/foo.view.lkml",
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

    def test_cross_project_views_keep_their_own_project(self) -> None:
        reporter = SourceReport()
        result = create_view_project_map(
            view_fields=[
                self._make_view_field("my_view", project_name="project-a"),
                self._make_view_field("other_view", project_name="project-b"),
            ],
            reporter=reporter,
        )
        assert result == {"my_view": "project-a", "other_view": "project-b"}

    def test_same_project_view_not_in_map(self) -> None:
        reporter = SourceReport()
        view_field = self._make_view_field("my_view", project_name=None)
        result = create_view_project_map(view_fields=[view_field], reporter=reporter)
        assert "my_view" not in result

    def test_mixed_view_not_in_map(self) -> None:
        reporter = SourceReport()
        result = create_view_project_map(
            view_fields=[
                self._make_view_field("my_view", project_name=None),
                self._make_view_field("my_view", project_name="project-a"),
            ],
            reporter=reporter,
        )
        assert "my_view" not in result

    def test_all_imported_conflicting_projects_keeps_first(self) -> None:
        reporter = SourceReport()
        result = create_view_project_map(
            view_fields=[
                self._make_view_field("my_view", project_name="project-a"),
                self._make_view_field("my_view", project_name="project-b"),
            ],
            reporter=reporter,
        )
        assert result == {"my_view": "project-a"}
        assert len(reporter.warnings) == 1


class TestGetViewFilePath:
    def test_prefers_local_source_file_over_imported(self) -> None:
        reporter = SourceReport()
        lkml_fields = [
            LookmlModelExploreField(
                name="dim1",
                type="string",
                view="my_view",
                source_file="imported_projects/project-a/views/foo.view.lkml",
            ),
            LookmlModelExploreField(
                name="dim2",
                type="string",
                view="my_view",
                source_file="views/foo.view.lkml",
            ),
        ]
        assert (
            get_view_file_path(lkml_fields, "my_view", reporter)
            == "views/foo.view.lkml"
        )

    def test_all_imported_same_path(self) -> None:
        reporter = SourceReport()
        imported_path = "imported_projects/project-a/views/foo.view.lkml"
        lkml_fields = [
            LookmlModelExploreField(
                name="dim1",
                type="string",
                view="my_view",
                source_file=imported_path,
            ),
            LookmlModelExploreField(
                name="dim2",
                type="string",
                view="my_view",
                source_file=imported_path,
            ),
        ]
        assert get_view_file_path(lkml_fields, "my_view", reporter) == imported_path

    def test_conflicting_local_paths_keeps_first_and_warns(self) -> None:
        reporter = SourceReport()
        lkml_fields = [
            LookmlModelExploreField(
                name="dim1",
                type="string",
                view="my_view",
                source_file="views/a.view.lkml",
            ),
            LookmlModelExploreField(
                name="dim2",
                type="string",
                view="my_view",
                source_file="views/b.view.lkml",
            ),
        ]
        assert (
            get_view_file_path(lkml_fields, "my_view", reporter) == "views/a.view.lkml"
        )
        assert len(reporter.warnings) == 1


class TestFromApiImportedViewProject:
    def _from_api(
        self,
        *,
        dimensions: List[LookmlModelExploreField],
        parameters: List[LookmlModelExploreField],
    ) -> LookerExplore:
        client = MagicMock()
        client.lookml_model_explore.return_value = LookmlModelExplore(
            name="spoke_explore",
            project_name="spoke",
            view_name="hub_view",
            fields=LookmlModelExploreFieldset(
                dimensions=dimensions,
                measures=[],
                parameters=parameters,
            ),
        )
        source_config = MagicMock()
        real_config = LookerCommonConfig()
        source_config.platform_name = real_config.platform_name
        source_config.env = real_config.env
        source_config.platform_instance = real_config.platform_instance
        source_config.view_naming_pattern = real_config.view_naming_pattern
        source_config.extract_column_level_lineage = (
            real_config.extract_column_level_lineage
        )
        explore = LookerExplore.from_api(
            model="m",
            explore_name="spoke_explore",
            client=client,
            reporter=SourceReport(),
            source_config=source_config,
        )
        assert explore is not None
        return explore

    def test_parameter_only_imported_view_uses_imported_project(self) -> None:
        explore = self._from_api(
            dimensions=[],
            parameters=[
                LookmlModelExploreField(
                    name="hub_view.date_filter",
                    type="date",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                )
            ],
        )
        assert explore.fields == []
        assert explore.upstream_views is not None
        assert len(explore.upstream_views) == 1
        assert explore.upstream_views[0].project == "hub"
        assert explore.upstream_views[0].include == "hub_view"
        assert explore.upstream_views_file_path == {
            "hub_view": "imported_projects/hub/views/hub_view.view.lkml"
        }
        assert explore.upstream_views[0].project != BASE_PROJECT_NAME

    def test_imported_dims_ignore_parameter_without_source_file(self) -> None:
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                )
            ],
            parameters=[
                LookmlModelExploreField(
                    name="hub_view.date_filter",
                    type="date",
                    view="hub_view",
                    source_file=None,
                )
            ],
        )
        assert explore.upstream_views is not None
        assert explore.upstream_views[0].project == "hub"
        assert explore.upstream_views[0].project != BASE_PROJECT_NAME

    def test_imported_dims_ignore_local_parameter(self) -> None:
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                )
            ],
            parameters=[
                LookmlModelExploreField(
                    name="hub_view.date_filter",
                    type="date",
                    view="hub_view",
                    source_file="explores/spoke.explore.lkml",
                )
            ],
        )
        assert explore.upstream_views is not None
        assert explore.upstream_views[0].project == "hub"
        assert explore.upstream_views[0].project != BASE_PROJECT_NAME
