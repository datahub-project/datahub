import logging
from unittest.mock import MagicMock

import pytest
from looker_sdk.sdk.api40.models import (
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreJoins,
)

from datahub.ingestion.source.looker.looker_common import (
    ExploreUpstreamViewField,
    LookerExplore,
    LookerExploreJoin,
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
