"""Tests for LookerV2Context shared state dataclass."""

from unittest.mock import MagicMock

from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context


def test_context_defaults_are_empty_collections():
    ctx = LookerV2Context(
        config=MagicMock(),
        looker_api=MagicMock(),
        reporter=MagicMock(),
        pipeline_ctx=MagicMock(),
        platform="looker",
    )
    assert ctx.model_registry == {}
    assert ctx.explore_cache == {}
    assert ctx.folder_registry == {}
    assert ctx.reachable_explores == {}
    assert ctx.pdt_upstream_map == {}
    assert ctx.view_to_explore_map == {}
    assert ctx.view_discovery_result is None
    assert ctx.resolved_project_paths == {}
    assert ctx.parsed_view_file_cache == {}
    assert ctx.dashboards_for_usage == []


def test_context_fields_are_mutable():
    ctx = LookerV2Context(
        config=MagicMock(),
        looker_api=MagicMock(),
        reporter=MagicMock(),
        pipeline_ctx=MagicMock(),
        platform="looker",
    )
    ctx.model_registry["m1"] = MagicMock()
    assert "m1" in ctx.model_registry
