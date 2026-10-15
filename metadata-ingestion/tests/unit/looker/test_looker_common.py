import logging
from typing import List, Optional
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
    LookerViewId,
    extract_project_from_imported_file_path,
    resolve_view_locations,
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

    def test_aliased_explore_does_not_remap_join_view(self) -> None:
        explore = LookmlModelExplore(name="spoke_explore", view_name="hub_view")
        field = LookmlModelExploreField(
            name="other_view.col_a",
            type="string",
            view="other_view",
            original_view=None,
        )
        result = ExploreUpstreamViewField(
            field=field, explore=explore
        )._form_field_name(
            view_project_map={},
            explore_project_name="spoke",
            model_name="m",
            upstream_views_file_path={"other_view": "views/other.view.lkml"},
            config=LookerCommonConfig(),
            view_aliases={"spoke_explore": "hub_view"},
        )
        assert result is not None
        assert "other_view" in result.table
        assert "hub.view.hub_view" not in result.table
        assert "spoke.view.spoke_explore" not in result.table


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
    def test_extract(self, file_path: str, expected: Optional[str]) -> None:
        assert extract_project_from_imported_file_path(file_path) == expected


class TestLookerViewIdPreprocessFilePath:
    def test_strips_imported_prefix_when_view_project_differs(self) -> None:
        view_id = LookerViewId(
            project_name="spoke",
            model_name="m",
            view_name="hub_view",
            file_path="imported_projects/hub/views/hub_view.view.lkml",
        )
        assert (
            view_id.preprocess_file_path("imported_projects/hub/views/hub_view")
            == "views.hub_view"
        )

    def test_local_path_is_unchanged_except_slash_replacement(self) -> None:
        view_id = LookerViewId(
            project_name="spoke",
            model_name="m",
            view_name="local_view",
            file_path="views/local_view.view.lkml",
        )
        assert view_id.preprocess_file_path("views/local_view") == "views.local_view"


class TestResolveViewLocations:
    def _field(
        self,
        view_name: str,
        source_file: Optional[str],
        name: str = "col",
    ) -> LookmlModelExploreField:
        return LookmlModelExploreField(
            name=f"{view_name}.{name}",
            type="string",
            view=view_name,
            source_file=source_file,
        )

    def _imported(
        self, view_name: str, project: str, name: str = "col"
    ) -> LookmlModelExploreField:
        return self._field(
            view_name,
            f"imported_projects/{project}/views/{view_name}.view.lkml",
            name=name,
        )

    def test_cross_project_views_keep_their_own_project(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view", "other_view"],
            schema_fields=[
                self._imported("my_view", "project-a"),
                self._imported("other_view", "project-b"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].imported_project == "project-a"
        assert locations["other_view"].imported_project == "project-b"

    def test_same_project_view_not_imported(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[self._field("my_view", "views/foo.view.lkml")],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].imported_project is None
        assert locations["my_view"].file_path == "views/foo.view.lkml"

    def test_mixed_view_stays_local(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[
                self._field("my_view", "views/foo.view.lkml", name="local"),
                self._imported("my_view", "project-a", name="imported"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].imported_project is None
        assert locations["my_view"].file_path == "views/foo.view.lkml"

    def test_imported_plus_missing_source_file_stays_imported(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[
                self._imported("my_view", "project-a", name="a"),
                self._field("my_view", None, name="b"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].imported_project == "project-a"

    def test_all_imported_conflicting_projects_keeps_first(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[
                self._imported("my_view", "project-a", name="a"),
                self._imported("my_view", "project-b", name="b"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].imported_project == "project-a"
        assert len(reporter.warnings) == 1
        warning = reporter.warnings[0]
        assert warning.message == "View has fields from different imported projects."
        assert "my_view" in str(warning.context)
        assert "project-a" in str(warning.context)

    def test_prefers_local_source_file_over_imported(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[
                self._imported("my_view", "project-a", name="imported"),
                self._field("my_view", "views/foo.view.lkml", name="local"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].file_path == "views/foo.view.lkml"

    def test_all_imported_same_path(self) -> None:
        reporter = SourceReport()
        imported_path = "imported_projects/project-a/views/my_view.view.lkml"
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[
                self._field("my_view", imported_path, name="a"),
                self._field("my_view", imported_path, name="b"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].file_path == imported_path
        assert locations["my_view"].imported_project == "project-a"

    def test_conflicting_local_paths_keeps_first_and_warns(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["my_view"],
            schema_fields=[
                self._field("my_view", "views/a.view.lkml", name="a"),
                self._field("my_view", "views/b.view.lkml", name="b"),
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["my_view"].file_path == "views/a.view.lkml"
        assert len(reporter.warnings) == 1
        warning = reporter.warnings[0]
        assert warning.message == "View has fields with different source_file paths."
        assert "my_view" in str(warning.context)
        assert "views/a.view.lkml" in str(warning.context)

    def test_explore_alias_is_canonicalized_to_actual_view(self) -> None:
        reporter = SourceReport()
        locations = resolve_view_locations(
            view_names=["hub_view"],
            schema_fields=[
                LookmlModelExploreField(
                    name="spoke_explore.col_a",
                    type="string",
                    view="spoke_explore",
                    original_view=None,
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                )
            ],
            parameter_fields=[],
            reporter=reporter,
            view_aliases={"spoke_explore": "hub_view"},
        )
        assert locations["hub_view"].imported_project == "hub"
        assert (
            locations["hub_view"].file_path
            == "imported_projects/hub/views/hub_view.view.lkml"
        )

    def test_extends_parent_original_view_is_classified(self) -> None:
        reporter = SourceReport()
        parent_path = "imported_projects/hub/views/parent_view.view.lkml"
        locations = resolve_view_locations(
            view_names=["hub_view"],
            schema_fields=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    original_view="parent_view",
                    source_file=parent_path,
                )
            ],
            parameter_fields=[],
            reporter=reporter,
        )
        assert locations["hub_view"].imported_project is None
        assert locations["parent_view"].imported_project == "hub"
        assert locations["parent_view"].file_path == parent_path

    def test_explore_alias_local_field_does_not_veto_imported_view(self) -> None:
        reporter = SourceReport()
        imported_path = "imported_projects/hub/views/hub_view.view.lkml"
        locations = resolve_view_locations(
            view_names=["hub_view"],
            schema_fields=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file=imported_path,
                ),
                LookmlModelExploreField(
                    name="spoke_explore.local_dim",
                    type="string",
                    view="spoke_explore",
                    original_view=None,
                    source_file="explores/spoke.explore.lkml",
                ),
            ],
            parameter_fields=[],
            reporter=reporter,
            view_aliases={"spoke_explore": "hub_view"},
        )
        assert locations["hub_view"].imported_project == "hub"
        assert locations["hub_view"].file_path == imported_path


class TestFromApiImportedViewProject:
    def _from_api(
        self,
        *,
        dimensions: List[LookmlModelExploreField],
        parameters: List[LookmlModelExploreField],
        reporter: Optional[SourceReport] = None,
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
            reporter=reporter or SourceReport(),
            source_config=source_config,
        )
        assert explore is not None
        return explore

    def _assert_imported_hub(self, explore: LookerExplore) -> None:
        imported_path = "imported_projects/hub/views/hub_view.view.lkml"
        assert explore.upstream_views is not None
        assert explore.upstream_views[0].project == "hub"
        assert explore.upstream_views[0].include == "hub_view"
        assert explore.upstream_views[0].project != BASE_PROJECT_NAME
        assert explore.upstream_views_file_path is not None
        assert explore.upstream_views_file_path["hub_view"] == imported_path
        assert explore.upstream_views_file_path["spoke_explore"] == imported_path

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
        self._assert_imported_hub(explore)

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
        self._assert_imported_hub(explore)

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
        self._assert_imported_hub(explore)

    def test_imported_dims_ignore_dimension_without_source_file(self) -> None:
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                ),
                LookmlModelExploreField(
                    name="hub_view.col_b",
                    type="string",
                    view="hub_view",
                    source_file=None,
                ),
            ],
            parameters=[],
        )
        self._assert_imported_hub(explore)

    def test_mixed_local_and_imported_fields_stay_on_explore_project(self) -> None:
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file="views/hub_view.view.lkml",
                ),
                LookmlModelExploreField(
                    name="hub_view.col_b",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                ),
            ],
            parameters=[],
        )
        assert explore.upstream_views is not None
        assert explore.upstream_views[0].project == BASE_PROJECT_NAME
        assert explore.upstream_views_file_path is not None
        assert (
            explore.upstream_views_file_path["hub_view"] == "views/hub_view.view.lkml"
        )
        assert (
            explore.upstream_views_file_path["spoke_explore"]
            == "views/hub_view.view.lkml"
        )

    def test_aliased_explore_field_without_original_view_stays_imported(self) -> None:
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="spoke_explore.col_a",
                    type="string",
                    view="spoke_explore",
                    original_view=None,
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                )
            ],
            parameters=[],
        )
        self._assert_imported_hub(explore)
        assert explore.fields is not None
        assert explore.fields[0].upstream_fields
        column_urn = explore.fields[0].upstream_fields[0].table
        assert "hub.view.hub_view" in column_urn
        assert "spoke_explore" not in column_urn

    def test_explore_scoped_local_field_does_not_veto_imported_view(self) -> None:
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                ),
                LookmlModelExploreField(
                    name="spoke_explore.local_dim",
                    type="string",
                    view="spoke_explore",
                    original_view=None,
                    source_file="explores/spoke.explore.lkml",
                ),
            ],
            parameters=[],
        )
        self._assert_imported_hub(explore)

    def test_conflicting_imported_projects_keeps_first_and_warns(self) -> None:
        reporter = SourceReport()
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/hub/views/hub_view.view.lkml",
                ),
                LookmlModelExploreField(
                    name="hub_view.col_b",
                    type="string",
                    view="hub_view",
                    source_file="imported_projects/other/views/hub_view.view.lkml",
                ),
            ],
            parameters=[],
            reporter=reporter,
        )
        self._assert_imported_hub(explore)
        assert len(reporter.warnings) == 1
        warning = reporter.warnings[0]
        assert warning.message == "View has fields from different imported projects."
        assert "hub_view" in str(warning.context)
        assert "hub" in str(warning.context)

    def test_extends_parent_keeps_imported_project_for_column_lineage(self) -> None:
        parent_path = "imported_projects/hub/views/parent_view.view.lkml"
        explore = self._from_api(
            dimensions=[
                LookmlModelExploreField(
                    name="hub_view.col_a",
                    type="string",
                    view="hub_view",
                    original_view="parent_view",
                    source_file=parent_path,
                )
            ],
            parameters=[],
        )
        assert explore.upstream_views is not None
        assert explore.upstream_views[0].include == "hub_view"
        assert explore.upstream_views[0].project == BASE_PROJECT_NAME
        assert explore.upstream_views_file_path is not None
        assert explore.upstream_views_file_path["parent_view"] == parent_path
        assert explore.fields is not None
        assert len(explore.fields) == 1
        assert explore.fields[0].upstream_fields
        column_urn = explore.fields[0].upstream_fields[0].table
        assert "hub.view.parent_view" in column_urn
        assert "spoke.view.parent_view" not in column_urn
        assert explore.fields[0].upstream_fields[0].column == "col_a"
