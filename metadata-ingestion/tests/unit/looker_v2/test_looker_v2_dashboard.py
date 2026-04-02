"""Unit tests for LookerDashboardProcessor."""

from typing import cast
from unittest import mock
from unittest.mock import MagicMock

from looker_sdk.sdk.api40.models import Dashboard as LookerAPIDashboard

from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
from datahub.ingestion.source.looker_v2.looker_v2_dashboard_processor import (
    LookerDashboardProcessor,
)
from tests.unit.looker_v2.conftest import make_ctx


def _make_processor(ctx: LookerV2Context) -> LookerDashboardProcessor:
    return LookerDashboardProcessor(
        ctx=ctx,
        folder_proc=MagicMock(),
        explore_registry=MagicMock(),
        reachable_look_registry=set(),
        chart_urns=set(),
    )


def test_chart_pattern_drops_disallowed_chart() -> None:
    """When chart_pattern denies an element, it is skipped and charts_dropped is incremented."""
    ctx = make_ctx()
    cast(MagicMock, ctx.config).chart_pattern.allowed.return_value = False

    processor = _make_processor(ctx)

    element = MagicMock()
    element.id = "42"
    element.query = None
    element.look = None
    element.result_maker = None

    api_dashboard = MagicMock(spec=LookerAPIDashboard)
    api_dashboard.dashboard_elements = [element]

    dashboard = MagicMock()
    dashboard.urn = "urn:li:dashboard:(looker,test)"

    entities: list = []
    extra_mcps: list = []

    processor._process_dashboard_elements(
        api_dashboard, dashboard, entities, extra_mcps
    )

    assert ctx.reporter.charts_dropped == 1
    assert entities == []


def test_reachable_explores_populated_from_element_query() -> None:
    """After _extract_chart_input_fields, ctx.reachable_explores contains the chart ID."""
    ctx = make_ctx(explore_cache={})
    processor = _make_processor(ctx)

    element = MagicMock()
    element.id = "99"
    element.result_maker = None
    element.look = None

    query = MagicMock()
    query.model = "my_model"
    query.view = "my_explore"
    query.dynamic_fields = "[]"
    query.fields = []
    query.filters = {}
    element.query = query

    chart = MagicMock()
    chart.urn = "urn:li:chart:(looker,element_99)"

    with mock.patch.object(processor, "_get_enriched_input_fields", return_value=[]):
        processor._extract_chart_input_fields(element, chart)

    key = ("my_model", "my_explore")
    assert key in ctx.reachable_explores
    assert "chart:99" in ctx.reachable_explores[key]


def test_input_fields_from_query_fields() -> None:
    """_get_input_fields_from_query returns InputFieldElement objects for static query.fields."""
    ctx = make_ctx()
    processor = _make_processor(ctx)

    query = MagicMock()
    query.dynamic_fields = "[]"
    query.fields = ["orders.id", "orders.status"]
    query.filters = {}
    query.model = "my_model"
    query.view = "my_explore"

    result = processor._get_input_fields_from_query(query)

    names = [f.name for f in result]
    assert "orders.id" in names
    assert "orders.status" in names
    assert len(result) == 2
