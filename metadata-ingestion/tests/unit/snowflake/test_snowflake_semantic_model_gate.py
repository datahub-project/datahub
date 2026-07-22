from typing import Optional
from unittest.mock import MagicMock

from datahub.configuration.common import GraphError
from datahub.ingestion.graph.entity_aspect_specs import EntityAspectSpecs
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


def _supported_specs() -> EntityAspectSpecs:
    return EntityAspectSpecs(
        entity_aspects={
            "semanticModel": {"semanticModelInfo"},
            "metric": {"metricInfo"},
        }
    )


def _make_graph(
    *,
    is_cloud: bool,
    version: str,
    metrics_flags: Optional[dict] = None,
    metrics_error: Optional[Exception] = None,
    specs: Optional[EntityAspectSpecs] = None,
) -> MagicMock:
    graph = MagicMock()
    graph.server_config = _server_config(is_cloud, version)

    if metrics_error is not None:
        graph.execute_graphql.side_effect = metrics_error
    else:
        graph.execute_graphql.return_value = {
            "appConfig": {"featureFlags": metrics_flags or {}}
        }

    graph.get_entity_aspect_specs.return_value = (
        specs if specs is not None else _supported_specs()
    )
    return graph


def test_saas_metrics_enabled_recipe_none_enables():
    graph = _make_graph(
        is_cloud=True, version="2.1.0", metrics_flags={"metricsEnabled": True}
    )
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
    assert decision.is_saas
    assert decision.metrics_enabled is True


def test_saas_metrics_absent_enables():
    # Empty featureFlags → metricsEnabled treated as "not disabled" → allow.
    graph = _make_graph(is_cloud=True, version="2.3.0", metrics_flags={})
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
    assert decision.metrics_enabled is None


def test_saas_metrics_false_vetoes_recipe_true():
    graph = _make_graph(
        is_cloud=True, version="2.1.0", metrics_flags={"metricsEnabled": False}
    )
    decision = resolve_emit_semantic_model_entities(graph, True)
    assert not decision.enabled
    assert decision.metrics_enabled is False


def test_saas_old_version_vetoes_recipe_true():
    graph = _make_graph(
        is_cloud=True, version="2.0.9", metrics_flags={"metricsEnabled": True}
    )
    decision = resolve_emit_semantic_model_entities(graph, True)
    assert not decision.enabled
    assert "below the minimum" in decision.reason


def test_saas_recipe_false_forces_off():
    graph = _make_graph(
        is_cloud=True, version="2.5.0", metrics_flags={"metricsEnabled": True}
    )
    decision = resolve_emit_semantic_model_entities(graph, False)
    assert not decision.enabled
    assert "force-off" in decision.reason


def test_oss_recipe_true_enables():
    graph = _make_graph(is_cloud=False, version="1.2.0")
    decision = resolve_emit_semantic_model_entities(graph, True)
    assert decision.enabled
    assert not decision.is_saas


def test_oss_recipe_false_and_none_disabled():
    graph_false = _make_graph(is_cloud=False, version="1.2.0")
    assert not resolve_emit_semantic_model_entities(graph_false, False).enabled

    graph_none = _make_graph(is_cloud=False, version="1.2.0")
    assert not resolve_emit_semantic_model_entities(graph_none, None).enabled


def test_saas_metrics_field_undefined_treated_as_absent():
    error = GraphError(
        "Validation error: FieldUndefined: Field 'metricsEnabled' is undefined"
    )
    graph = _make_graph(is_cloud=True, version="2.1.0", metrics_error=error)
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled


def test_saas_metrics_hard_failure_fails_closed():
    error = GraphError("Unauthorized: token expired")
    graph = _make_graph(is_cloud=True, version="2.1.0", metrics_error=error)
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert not decision.enabled
    assert "failing closed" in decision.reason


def test_no_graph_recipe_true_enables_and_none_disabled():
    assert resolve_emit_semantic_model_entities(None, True).enabled
    assert not resolve_emit_semantic_model_entities(None, None).enabled


def test_saas_capability_missing_forces_off():
    specs = EntityAspectSpecs(entity_aspects={"dataset": {"datasetProperties"}})
    graph = _make_graph(
        is_cloud=True,
        version="2.1.0",
        metrics_flags={"metricsEnabled": True},
        specs=specs,
    )
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert not decision.enabled
    assert "registry lacks" in decision.reason


def test_saas_specs_unavailable_does_not_block():
    graph = _make_graph(
        is_cloud=True,
        version="2.1.0",
        metrics_flags={"metricsEnabled": True},
        specs=None,
    )
    # specs=None sentinel means "return the supported specs"; override to real None.
    graph.get_entity_aspect_specs.return_value = None
    decision = resolve_emit_semantic_model_entities(graph, None)
    assert decision.enabled
