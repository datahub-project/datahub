from typing import Optional
from unittest.mock import MagicMock

from datahub.configuration.common import GraphError
from datahub.ingestion.source.snowflake.snowflake_semantic_model_gate import (
    resolve_emit_semantic_model_entities,
)
from datahub.utilities.server_config_util import RestServiceConfig


def _server_config(is_cloud: bool, version: str) -> RestServiceConfig:
    return RestServiceConfig(
        raw_config={
            "datahub": {"serverEnv": "prod" if is_cloud else "core"},
            "versions": {"acryldata/datahub": {"version": version}},
        }
    )


def _make_graph(
    *,
    is_cloud: bool,
    version: str,
    metrics_flags: Optional[dict] = None,
    metrics_error: Optional[Exception] = None,
) -> MagicMock:
    graph = MagicMock()
    graph.server_config = _server_config(is_cloud, version)
    if metrics_error is not None:
        graph.execute_graphql.side_effect = metrics_error
    else:
        graph.execute_graphql.return_value = {
            "appConfig": {"featureFlags": metrics_flags or {}}
        }
    return graph


def test_saas_metrics_enabled_recipe_none_enables():
    graph = _make_graph(
        is_cloud=True, version="2.1.0", metrics_flags={"metricsEnabled": True}
    )
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
    assert decision.is_saas
    assert decision.metrics_enabled is True
    assert decision.entity_types_capable


def test_saas_metrics_absent_enables():
    # Empty featureFlags → metricsEnabled treated as "not disabled" → allow.
    graph = _make_graph(is_cloud=True, version="2.3.0", metrics_flags={})
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
    assert decision.metrics_enabled is None
    assert decision.entity_types_capable


def test_saas_metrics_false_vetoes_but_stays_capable():
    # Row 2 of the entityTypes matrix: Metrics off → emit legacy, but the server
    # is still version-capable, so entityTypes must stay at 5 (capable=True).
    graph = _make_graph(
        is_cloud=True, version="2.1.0", metrics_flags={"metricsEnabled": False}
    )
    decision = resolve_emit_semantic_model_entities(graph, True)
    assert not decision.enabled
    assert decision.metrics_enabled is False
    assert decision.entity_types_capable


def test_saas_old_version_vetoes_recipe_true():
    graph = _make_graph(
        is_cloud=True, version="2.0.9", metrics_flags={"metricsEnabled": True}
    )
    decision = resolve_emit_semantic_model_entities(graph, True)
    assert not decision.enabled
    assert "below the minimum" in decision.reason
    # Row 4: below the minimum → not capable → entityTypes stays at 3.
    assert not decision.entity_types_capable


def test_saas_recipe_false_forces_off_but_stays_capable():
    # Row 3: recipe force-off → emit legacy, but a version-capable server keeps
    # entityTypes at 5, so the shared definition does not flap vs an emit-on recipe.
    graph = _make_graph(
        is_cloud=True, version="2.5.0", metrics_flags={"metricsEnabled": True}
    )
    decision = resolve_emit_semantic_model_entities(graph, False)
    assert not decision.enabled
    assert "force-off" in decision.reason
    assert decision.entity_types_capable


def test_oss_recipe_true_enables():
    graph = _make_graph(is_cloud=False, version="1.2.0")
    decision = resolve_emit_semantic_model_entities(graph, True)
    assert decision.enabled
    assert not decision.is_saas
    assert decision.entity_types_capable


def test_oss_recipe_false_and_none_disabled():
    graph_false = _make_graph(is_cloud=False, version="1.2.0")
    d_false = resolve_emit_semantic_model_entities(graph_false, False)
    assert not d_false.enabled
    assert not d_false.entity_types_capable

    graph_none = _make_graph(is_cloud=False, version="1.2.0")
    d_none = resolve_emit_semantic_model_entities(graph_none, None)
    assert not d_none.enabled
    assert not d_none.entity_types_capable


def test_saas_metrics_field_undefined_treated_as_absent():
    # Fallback path (graphql-core stripper unavailable): the server rejects the
    # unknown field with FieldUndefined → treated as absent → ON.
    error = GraphError(
        "Validation error: FieldUndefined: Field 'metricsEnabled' is undefined"
    )
    graph = _make_graph(is_cloud=True, version="2.1.0", metrics_error=error)
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled


def test_saas_metrics_field_stripped_resolves_on():
    # Primary path: strip_unsupported_fields removes the unknown field and returns
    # an empty featureFlags (no GraphError) → metricsEnabled is None → ON.
    graph = _make_graph(is_cloud=True, version="2.1.0", metrics_flags={})
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
    assert decision.metrics_enabled is None


def test_saas_metrics_hard_failure_fails_open():
    # A non-FieldUndefined error (auth/network) is not an explicit false, so the
    # probe fails open: metricsEnabled treated as None (not disabled) → ON.
    error = GraphError("Unauthorized: token expired")
    graph = _make_graph(is_cloud=True, version="2.1.0", metrics_error=error)
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
    assert decision.metrics_enabled is None


def test_no_graph_recipe_true_enables_and_none_disabled():
    d_true = resolve_emit_semantic_model_entities(None, True)
    assert d_true.enabled
    assert d_true.entity_types_capable

    d_none = resolve_emit_semantic_model_entities(None, None)
    assert not d_none.enabled
    assert not d_none.entity_types_capable
