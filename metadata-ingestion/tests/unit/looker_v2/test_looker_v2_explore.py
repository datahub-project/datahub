"""Unit tests for LookerExploreProcessor field extraction and upstream view URNs."""

from __future__ import annotations

from unittest.mock import MagicMock

from datahub.ingestion.source.looker.looker_common import ViewFieldType
from datahub.ingestion.source.looker_v2.looker_v2_explore_processor import (
    LookerExploreProcessor,
)
from tests.unit.looker_v2.conftest import make_ctx


def _make_processor(**overrides: object) -> LookerExploreProcessor:
    ctx = make_ctx(**overrides)
    ctx.config.view_naming_pattern = MagicMock()
    ctx.config.view_naming_pattern.replace_variables.return_value = (
        "test_project.some_view"
    )
    ctx.config.base_folder = None
    return LookerExploreProcessor(ctx)


def _make_explore(
    *,
    dimensions: list | None = None,
    measures: list | None = None,
    parameters: list | None = None,
    view_name: str | None = None,
    name: str | None = None,
    joins: list | None = None,
) -> MagicMock:
    explore = MagicMock()
    explore.name = name
    explore.view_name = view_name

    if dimensions is None and measures is None and parameters is None:
        explore.fields = None
    else:
        explore.fields = MagicMock()
        explore.fields.dimensions = dimensions or []
        explore.fields.measures = measures or []
        explore.fields.parameters = parameters or []

    explore.joins = joins
    return explore


def _make_dimension(name: str, *, dimension_group: str | None = None) -> MagicMock:
    field = MagicMock()
    field.name = name
    field.label_short = None
    field.type = "string"
    field.description = ""
    field.dimension_group = dimension_group
    field.primary_key = False
    field.tags = []
    field.field_group_label = None
    return field


def _make_measure(name: str) -> MagicMock:
    field = MagicMock()
    field.name = name
    field.label_short = None
    field.type = "count"
    field.description = ""
    field.dimension_group = None
    field.tags = []
    field.field_group_label = None
    return field


class TestExtractExploreFields:
    def test_dimension_maps_to_dimension_type(self) -> None:
        processor = _make_processor()
        dim = _make_dimension("orders.id", dimension_group=None)
        explore = _make_explore(dimensions=[dim])

        fields = processor._extract_explore_fields(explore)

        assert len(fields) == 1
        assert fields[0].field_type == ViewFieldType.DIMENSION

    def test_measure_maps_to_measure_type(self) -> None:
        processor = _make_processor()
        measure = _make_measure("orders.count")
        explore = _make_explore(measures=[measure])

        fields = processor._extract_explore_fields(explore)

        assert len(fields) == 1
        assert fields[0].field_type == ViewFieldType.MEASURE

    def test_dimension_group_maps_to_dimension_group_type(self) -> None:
        processor = _make_processor()
        dim = _make_dimension("orders.created_date", dimension_group="created")
        explore = _make_explore(dimensions=[dim])

        fields = processor._extract_explore_fields(explore)

        assert len(fields) == 1
        assert fields[0].field_type == ViewFieldType.DIMENSION_GROUP

    def test_empty_fields_returns_empty_list(self) -> None:
        processor = _make_processor()
        explore = _make_explore()

        fields = processor._extract_explore_fields(explore)

        assert fields == []


class TestGetExploreUpstreamViewUrns:
    def test_base_view_and_join_produce_two_urns(self) -> None:
        processor = _make_processor()

        join = MagicMock()
        join.from_ = None
        join.name = "users"

        explore = _make_explore(view_name="orders", name="orders_explore", joins=[join])

        urns = processor._get_explore_upstream_view_urns(explore, "my_model")

        assert len(urns) == 2
        urn_strings = [str(u) for u in urns]
        assert all("test_project.some_view" in u for u in urn_strings)

    def test_explore_name_used_as_base_view_when_view_name_matches(self) -> None:
        processor = _make_processor()
        explore = _make_explore(view_name="orders", name="orders", joins=None)

        urns = processor._get_explore_upstream_view_urns(explore, "my_model")

        assert len(urns) == 1
        assert "test_project.some_view" in str(urns[0])
