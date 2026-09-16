"""Shared-gate behaviour that both the dbt and Snowflake sources depend on.

The Snowflake-specific tests live in tests/unit/snowflake and cover the
version and probe branches; these cover the tri-state itself, which reads the
same for every source.
"""

from typing import Optional
from unittest import mock

import pytest

from datahub.ingestion.source.common.semantic_model_gate import (
    resolve_emit_semantic_model_entities,
)


def _oss_graph() -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.server_config.is_datahub_cloud = False
    graph.server_config.service_version = "1.7.0"
    return graph


def _cloud_graph() -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.server_config.is_datahub_cloud = True
    graph.server_config.service_version = "2.1.0"
    graph.server_config.supports_feature.return_value = True
    graph.get_config.return_value = {"featureFlags": {"metricsEnabled": True}}
    return graph


@pytest.mark.parametrize("graph_factory", [lambda: None, _oss_graph])
def test_unset_and_explicit_false_give_different_reasons(graph_factory) -> None:  # type: ignore[no-untyped-def]
    """Both stay off, but for different reasons and with different advice.

    Unset would have auto-enabled against a capable managed server, so an
    operator asking "why am I not getting metrics" needs to be told which of
    the two they are looking at.
    """
    graph = graph_factory()
    unset = resolve_emit_semantic_model_entities(graph, None)
    explicit_false = resolve_emit_semantic_model_entities(graph, False)

    assert unset.enabled is False
    assert explicit_false.enabled is False
    assert unset.reason != explicit_false.reason
    assert "force-off" in explicit_false.reason
    assert "sets it to true" in unset.reason


def test_force_off_reads_the_same_everywhere() -> None:
    """An explicit false is one state, so it should not be described three ways."""
    reasons = {
        resolve_emit_semantic_model_entities(graph, False).reason
        for graph in (None, _oss_graph(), _cloud_graph())
    }
    assert len(reasons) == 1


@pytest.mark.parametrize(
    "recipe_value,expected",
    [(None, True), (True, True), (False, False)],
)
def test_capable_server_honours_every_tri_state(
    recipe_value: Optional[bool], expected: bool
) -> None:
    decision = resolve_emit_semantic_model_entities(_cloud_graph(), recipe_value)
    assert decision.enabled is expected
    assert decision.is_saas is True
