from typing import Any, Dict, List

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.montecarlo import assertion as mc_assertion
from datahub.ingestion.source.montecarlo.client import MonteCarloClient
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import MconResolver
from datahub.ingestion.source.montecarlo.query_builder import (
    ALERT_TYPE_SPEC,
    COMPARISONS_FIELD,
    CUSTOM_RULE_TYPE_SPEC,
    MONITOR_TYPE_SPEC,
    TYPE_SPECS,
    DriftVerdict,
    FieldSpec,
    IntrospectingQueryBuilder,
    MonteCarloGraphQLType,
    _build_selection_with_subshapes,
    _emit_fallback_selection,
    _parse_type_shape,
    _TypeShape,
    drift_abort_title,
    iter_leaf_field_specs,
    spec_value,
)
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.ingestion.source.montecarlo.source import MonteCarloSource
from tests.unit.montecarlo.static_query_builder import StaticQueryBuilder


def make_config(**overrides: Any) -> MonteCarloSourceConfig:
    base: Dict[str, Any] = {"api_id": "id", "api_token": "token"}
    base.update(overrides)
    return MonteCarloSourceConfig.parse_obj(base)


def _introspect_response(fields_spec: Dict[str, str]) -> Dict[str, Any]:
    """Build a fake __type introspection response. fields_spec maps a field
    name to its underlying GraphQL kind+name, e.g. {"uuid": "SCALAR:String",
    "comparisons": "OBJECT:Comparison"}. SCALAR/ENUM fields are a plain type
    node; OBJECT fields are wrapped in NON_NULL(LIST(OBJECT)) so the
    unwrapper has real ofType chains to walk."""
    fields = []
    for name, spec in fields_spec.items():
        kind, type_name = spec.split(":", 1)
        type_node: Dict[str, Any]
        if kind in ("SCALAR", "ENUM"):
            type_node = {"kind": kind, "name": type_name, "of_type": None}
        else:  # OBJECT / INTERFACE / UNION — wrap as NON_NULL(LIST(...))
            type_node = {
                "kind": "NON_NULL",
                "name": None,
                "of_type": {
                    "kind": "LIST",
                    "name": None,
                    "of_type": {
                        "kind": kind,
                        "name": type_name,
                        "of_type": None,
                    },
                },
            }
        fields.append({"name": name, "type": type_node})
    return {"mc_type": {"fields": fields}}


def _live_monitor_shape(
    *,
    include_priority: bool = True,
    include_severity_alias: bool = False,
    include_where_condition: bool = True,
    include_description: bool = True,
    include_data_quality_dimension: bool = True,
) -> Dict[str, str]:
    """Current-gateway Monitor fields (no customSql). Toggle fields to simulate drift."""
    shape: Dict[str, str] = {
        "uuid": "SCALAR:UUID",
        "name": "SCALAR:String",
        "monitorType": "SCALAR:String",
        "entityMcons": "SCALAR:String",
        "resourceId": "SCALAR:UUID",
        "comparisons": "OBJECT:Comparison",
    }
    if include_description:
        shape["description"] = "SCALAR:String"
    if include_where_condition:
        shape["whereCondition"] = "SCALAR:String"
    if include_data_quality_dimension:
        shape["dataQualityDimension"] = "SCALAR:String"
    if include_priority:
        shape["priority"] = "SCALAR:String"
    if include_severity_alias:
        shape["severity"] = "SCALAR:String"
    return shape


_COMPARISON_DESIRED_FIELDS = tuple(
    child.name for child in COMPARISONS_FIELD.children or ()
)


def _live_comparison_shape() -> Dict[str, str]:
    return {
        "comparisonType": "SCALAR:String",
        "operator": "SCALAR:String",
        "metric": "SCALAR:String",
        "customMetric": "OBJECT:CustomMetric",
        "field": "SCALAR:String",
        "fields": "SCALAR:String",
        "threshold": "SCALAR:Float",
        "upperThreshold": "SCALAR:Float",
        "lowerThreshold": "SCALAR:Float",
    }


def _live_custom_metric_shape() -> Dict[str, str]:
    return {"uuid": "SCALAR:UUID", "metricName": "SCALAR:String"}


def _live_warehouse_table_shape() -> Dict[str, str]:
    return {
        "mcon": "SCALAR:String",
        "fullTableId": "SCALAR:String",
        "warehouse": "OBJECT:Warehouse",
    }


def _live_job_execution_history_log_shape() -> Dict[str, str]:
    return {
        "jobExecutionUuid": "SCALAR:UUID",
        "startTime": "SCALAR:DateTime",
        "endTime": "SCALAR:DateTime",
        "status": "SCALAR:String",
        "exceptions": "SCALAR:String",
        "totalResultCount": "SCALAR:Int",
        "evaluatedRecordCount": "SCALAR:Int",
    }


def _live_metric_shape() -> Dict[str, str]:
    return {
        "metric": "SCALAR:String",
        "value": "SCALAR:Float",
        "field": "SCALAR:String",
        "timestamp": "SCALAR:DateTime",
        "measurementTimestamp": "SCALAR:DateTime",
        "jobExecutionUuid": "SCALAR:UUID",
    }


def _full_nested_responses(
    **overrides: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Introspection map with a complete Comparison / CustomMetric tree so
    check_drift does not treat a thin nested fixture as silent nested drift."""
    responses: Dict[str, Dict[str, Any]] = {
        "Comparison": _introspect_response(_live_comparison_shape()),
        "CustomMetric": _introspect_response(_live_custom_metric_shape()),
        "WarehouseTable": _introspect_response(_live_warehouse_table_shape()),
        "Warehouse": _introspect_response({"connectionType": "SCALAR:String"}),
    }
    responses.update(overrides)
    return responses


def _assert_desired_comparison_tree(query: str) -> None:
    for fname in _COMPARISON_DESIRED_FIELDS:
        assert fname in query


def _builder_with_introspection(
    introspect_responses: Dict[str, Dict[str, Any]],
    fatal_error_types: tuple = (),
) -> IntrospectingQueryBuilder:
    """Build an IntrospectingQueryBuilder whose _call returns canned
    introspection responses keyed by the type name being introspected."""
    calls: Dict[str, int] = {}

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        calls[type_name] = calls.get(type_name, 0) + 1
        if type_name in introspect_responses:
            return introspect_responses[type_name]
        return {"mc_type": {"fields": []}}

    return IntrospectingQueryBuilder(
        fake_call, lambda **kwargs: None, fatal_error_types=fatal_error_types
    )


def test_parse_type_shape_unwraps_list_of_object() -> None:
    response = _introspect_response(
        {"uuid": "SCALAR:UUID", "comparisons": "OBJECT:Comparison"}
    )
    shape = _parse_type_shape(response)
    assert "uuid" in shape.fields
    assert shape.fields["uuid"].is_object is False
    assert shape.fields["uuid"].is_composite is False
    assert shape.fields["comparisons"].is_object is True
    assert shape.fields["comparisons"].is_composite is True
    assert shape.fields["comparisons"].object_type_name == "Comparison"


def test_builder_current_gateway_selection() -> None:
    builder = _builder_with_introspection(
        _full_nested_responses(
            Monitor=_introspect_response(_live_monitor_shape()),
        )
    )
    query = builder.monitors_query()
    assert "customSql" not in query
    assert "uuid" in query
    assert "entityMcons" in query
    assert "comparisons" in query
    assert "whereCondition" in query
    assert "priority" in query
    assert "dataQualityDimension" in query


def test_builder_keeps_all_fields_when_no_drift() -> None:
    builder = _builder_with_introspection(
        _full_nested_responses(
            Monitor=_introspect_response(_live_monitor_shape()),
        )
    )
    query = builder.monitors_query()
    assert "uuid" in query
    _assert_desired_comparison_tree(query)


def test_drift_proceed_on_current_gateway_shape() -> None:
    builder = _builder_with_introspection(
        _full_nested_responses(
            Monitor=_introspect_response(_live_monitor_shape()),
        )
    )
    for strict in (False, True):
        drift = builder.check_drift(
            strict=strict, types=[MonteCarloGraphQLType.MONITOR]
        )
        assert drift.per_type["Monitor"].verdict == DriftVerdict.PROCEED
        assert drift.per_type["Monitor"].missing == []
        assert drift.verdict == DriftVerdict.PROCEED


def test_drift_degraded_when_optional_fields_missing() -> None:
    shape = _live_monitor_shape(include_description=False)
    builder = _builder_with_introspection(
        _full_nested_responses(Monitor=_introspect_response(shape))
    )
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.DEGRADED
    assert "description" in drift.per_type["Monitor"].missing
    assert drift.per_type["Monitor"].critical_missing == []
    assert drift.verdict == DriftVerdict.DEGRADED


def test_drift_abort_when_strict_and_optional_missing() -> None:
    shape = _live_monitor_shape(include_description=False)
    builder = _builder_with_introspection(
        _full_nested_responses(Monitor=_introspect_response(shape))
    )
    drift = builder.check_drift(strict=True, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.ABORT


def test_drift_proceed_when_no_drift() -> None:
    builder = _builder_with_introspection(
        _full_nested_responses(
            Monitor=_introspect_response(_live_monitor_shape()),
        )
    )
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.PROCEED
    assert drift.per_type["Monitor"].missing == []
    assert drift.verdict == DriftVerdict.PROCEED


def test_drift_reports_new_uningested_fields() -> None:
    shape = _live_monitor_shape()
    shape["monitorSqlBlocks"] = "SCALAR:String"
    builder = _builder_with_introspection(
        _full_nested_responses(Monitor=_introspect_response(shape))
    )
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert "monitorSqlBlocks" in drift.per_type["Monitor"].new_fields
    assert drift.per_type["Monitor"].verdict == DriftVerdict.PROCEED


def test_builder_falls_back_when_introspection_fails() -> None:
    warnings: List[str] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("transient introspection blip")

    def warner(**kwargs: Any) -> None:
        warnings.append(kwargs.get("title", ""))

    builder = IntrospectingQueryBuilder(fake_call, warner)
    query = builder.monitors_query()
    assert "uuid" in query
    assert "entityMcons" in query
    assert "customSql" not in query
    assert "severity" not in query
    # Fallback requests the renamed replacement fields, not the removed ones.
    assert "whereCondition" in query
    assert "priority" in query
    # TABLE monitor fields not part of the known drift are kept in the fallback
    # so those monitors still resolve warehouse assets and comparisons.
    assert "resourceId" in query
    assert "comparisons" in query
    # CustomRule / Alert fallbacks keep their nested blocks so a failed
    # parent introspection does not emit no-comparison / no-subTypes records.
    custom_q = builder.custom_rules_query()
    assert "comparisons" in custom_q
    assert "operator" in custom_q
    assert "customMetric" in custom_q
    alert_q = builder.alerts_query()
    assert "subTypes" in alert_q
    assert "assets" in alert_q
    assert any("introspection failed" in w for w in warnings)
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.verdict == DriftVerdict.PROCEED


def test_builder_fallback_does_not_poison_other_types() -> None:
    # A failed __type call for one type must not force the others onto the
    # fallback path or suppress their drift report.
    calls: Dict[str, int] = {}

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        calls[type_name] = calls.get(type_name, 0) + 1
        if type_name == "Monitor":
            raise RuntimeError("transient blip on Monitor only")
        if type_name == "Alert":
            return _introspect_response(
                {
                    "id": "SCALAR:UUID",
                    "type": "SCALAR:String",
                    "subTypes": "SCALAR:String",
                    "severity": "SCALAR:String",
                    "priority": "SCALAR:String",
                    "status": "SCALAR:String",
                    "createdTime": "SCALAR:DateTime",
                    "title": "SCALAR:String",
                    "monitorUuids": "SCALAR:String",
                    "assets": "OBJECT:AlertAsset",
                }
            )
        if type_name == "AlertAsset":
            return _introspect_response({"mcon": "SCALAR:String"})
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    monitor_q = builder.monitors_query()
    # Monitor failed -> fallback (minimal selection, no customSql/severity).
    assert "customSql" not in monitor_q
    # Alert succeeds -> full dynamic selection, not the fallback.
    alert_q = builder.alerts_query()
    assert any(line.strip() == "id" for line in alert_q.splitlines())
    assert "monitorUuids" in alert_q
    drift = builder.check_drift(
        strict=False, types=[MonteCarloGraphQLType.MONITOR, MonteCarloGraphQLType.ALERT]
    )
    assert drift.per_type["Monitor"].verdict == DriftVerdict.PROCEED
    assert drift.per_type["Alert"].verdict == DriftVerdict.PROCEED
    # Monitor failed but Alert was still introspected.
    assert "Alert" in calls


def test_builder_keeps_comparisons_when_nested_introspection_fails() -> None:
    # A failed __type call for Comparison must not drop the parent
    # comparisons field. Monitor introspection succeeded, so the Monitor
    # fallback is not used; the desired Comparison selection is emitted
    # instead of an empty sub-selection.
    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "Monitor":
            return _introspect_response(_live_monitor_shape())
        if type_name == "Comparison":
            raise RuntimeError("transient blip on Comparison only")
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    query = builder.monitors_query()
    # Monitor introspection succeeded; Comparison failure must not drop comparisons.
    assert "dataQualityDimension" in query
    _assert_desired_comparison_tree(query)
    assert "whereCondition" in query
    assert "priority" in query
    assert "uuid" in query
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert not any(
        m.startswith("comparisons.") for m in drift.per_type["Monitor"].missing
    )


def test_custom_rule_keeps_comparisons_when_nested_introspection_fails() -> None:
    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "CustomRule":
            return _introspect_response(
                {
                    "uuid": "SCALAR:UUID",
                    "ruleName": "SCALAR:String",
                    "ruleType": "SCALAR:String",
                    "description": "SCALAR:String",
                    "customSql": "SCALAR:String",
                    "entityMcons": "SCALAR:String",
                    "priority": "SCALAR:String",
                    "comparisons": "OBJECT:Comparison",
                }
            )
        if type_name == "Comparison":
            raise RuntimeError("transient blip on Comparison only")
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    query = builder.custom_rules_query()
    _assert_desired_comparison_tree(query)
    assert "customSql" in query
    assert "priority" in query
    assert "severity" not in query
    assert "uuid" in query


def test_successful_empty_parent_aborts_instead_of_fallback() -> None:
    # A successful empty Monitor shape (null/empty __type) is real drift,
    # not an introspection failure. The gate must ABORT on missing critical
    # fields rather than PROCEED and send the hardcoded fallback.
    builder = _builder_with_introspection({"Monitor": {"mc_type": {"fields": []}}})
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.ABORT
    assert "uuid" in drift.per_type["Monitor"].critical_missing
    assert "entityMcons" in drift.per_type["Monitor"].critical_missing
    assert drift.verdict == DriftVerdict.ABORT


def test_empty_comparison_drops_field_and_reports_nested_drift() -> None:
    # Successful empty Comparison is real nested drift: omit comparisons
    # from the query (empty selection is invalid GraphQL) and surface it
    # on the gate so the run is DEGRADED, not silent PROCEED.
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(_live_monitor_shape()),
            "Comparison": {"mc_type": {"fields": []}},
        }
    )
    query = builder.monitors_query()
    assert "comparisons" not in query
    assert "uuid" in query
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.DEGRADED
    assert any(m.startswith("comparisons.") for m in drift.per_type["Monitor"].missing)
    assert drift.verdict == DriftVerdict.DEGRADED


def test_partial_comparison_reports_nested_drift() -> None:
    # Thin Comparison {comparisonType, operator} used to assert PROCEED
    # while metric / thresholds were silently dropped from the query.
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(_live_monitor_shape()),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    query = builder.monitors_query()
    assert "comparisons" in query
    assert "comparisonType" in query
    assert "operator" in query
    assert "metric" not in query
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.DEGRADED
    assert "comparisons.metric" in drift.per_type["Monitor"].missing


def test_builder_only_introspects_desired_nested_types() -> None:
    # The connector must not introspect object fields it does not request
    # (avoids burning the daily budget on unused nested types).
    introspected: List[str] = []
    monitor_shape = _live_monitor_shape()
    # Add an object field the connector does NOT request.
    monitor_shape["unusedObject"] = "OBJECT:UnusedNestedType"

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        introspected.append(type_name)
        if type_name == "Monitor":
            return _introspect_response(monitor_shape)
        if type_name == "Comparison":
            return _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            )
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    builder.monitors_query()
    assert "Monitor" in introspected
    assert "Comparison" in introspected  # desired nested type
    assert "UnusedNestedType" not in introspected  # not desired -> not introspected


def test_builder_propagates_fatal_introspection_errors() -> None:
    class FakeAuthError(RuntimeError):
        pass

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        raise FakeAuthError("rejected credentials")

    builder = IntrospectingQueryBuilder(
        fake_call, lambda **kwargs: None, fatal_error_types=(FakeAuthError,)
    )
    with pytest.raises(FakeAuthError):
        builder.monitors_query()


def test_call_stub_after_construction_reaches_introspection() -> None:
    client = MonteCarloClient(make_config(), page_size=100, report=None)
    introspection_calls: List[str] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if "mcIntrospect" in query:
            introspection_calls.append(query)
            return {"mc_type": {"fields": []}}
        raise AssertionError(f"unexpected query: {query[:80]!r}")

    client._call = fake_call  # type: ignore[method-assign]
    client.check_schema_drift(strict=False)
    assert introspection_calls


def test_static_query_builder_returns_verbatim_strings() -> None:
    builder = StaticQueryBuilder(
        monitors_query="MONITORS_STUB",
        custom_rules_query="CUSTOM_RULES_STUB",
        alerts_query="ALERTS_STUB",
    )
    assert builder.monitors_query() == "MONITORS_STUB"
    assert builder.custom_rules_query() == "CUSTOM_RULES_STUB"
    assert builder.alerts_query() == "ALERTS_STUB"
    assert builder.check_drift(strict=False).verdict == DriftVerdict.PROCEED


def test_client_check_schema_drift_respects_config_flags() -> None:
    client = MonteCarloClient(
        make_config(include_alerts=False), page_size=100, report=None
    )
    builder = _builder_with_introspection(
        _full_nested_responses(
            Monitor=_introspect_response(_live_monitor_shape()),
            CustomRule=_introspect_response(
                {
                    "uuid": "SCALAR:UUID",
                    "ruleName": "SCALAR:String",
                    "ruleType": "SCALAR:String",
                    "description": "SCALAR:String",
                    "customSql": "SCALAR:String",
                    "entityMcons": "SCALAR:String",
                    "severity": "SCALAR:String",
                    "priority": "SCALAR:String",
                    "comparisons": "OBJECT:Comparison",
                }
            ),
            Alert=_introspect_response({"id": "SCALAR:UUID"}),
        )
    )
    client.set_query_builder(builder)
    drift = client.check_schema_drift(strict=False)
    assert "Alert" not in drift.per_type
    assert "Monitor" in drift.per_type
    assert "CustomRule" in drift.per_type
    assert drift.verdict == DriftVerdict.PROCEED


def _build_source(cfg: MonteCarloSourceConfig) -> MonteCarloSource:
    source = MonteCarloSource.__new__(MonteCarloSource)
    source.config = cfg
    source.report = MonteCarloSourceReport()
    source.client = MonteCarloClient(cfg, page_size=100, report=source.report)
    source.client.set_query_builder(StaticQueryBuilder())
    source.client._client = None  # type: ignore[assignment]

    def _stub_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        # Return valid empty responses for whichever fetch runs after the gate.
        if "getMonitors" in query:
            return {"get_monitors": []}
        if "getCustomRules" in query:
            return {
                "get_custom_rules": {
                    "edges": [],
                    "page_info": {"has_next_page": False},
                }
            }
        return {}

    source.client._call = _stub_call  # type: ignore[method-assign]
    source.resolver = MconResolver(cfg, source.client, source.report)
    source.builder = mc_assertion.MonteCarloAssertionBuilder(
        cfg, source.report, source.resolver
    )
    source.ctx = PipelineContext(run_id="test")  # type: ignore[attr-defined]
    return source


def test_source_gate_aborts_on_critical_drift() -> None:
    cfg = make_config(include_alerts=False)
    source = _build_source(cfg)
    shape = _live_monitor_shape()
    del shape["uuid"]
    source.client.set_query_builder(
        _builder_with_introspection(
            _full_nested_responses(
                Monitor=_introspect_response(shape),
                Comparison=_introspect_response(
                    {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
                ),
                CustomRule=_introspect_response(
                    {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
                ),
            )
        )
    )
    list(source.get_workunits_internal())
    assert source.report.monitors_scanned == 0
    assert len(source.report.failures) == 1
    assert "critical fields missing" in (source.report.failures[0].title or "")
    assert "uuid" in (source.report.failures[0].title or "")


def test_source_gate_proceeds_with_warning_on_degraded_drift() -> None:
    cfg = make_config(include_alerts=False)
    source = _build_source(cfg)
    shape = _live_monitor_shape(include_description=False)
    source.client.set_query_builder(
        _builder_with_introspection(
            _full_nested_responses(
                Monitor=_introspect_response(shape),
                Comparison=_introspect_response(
                    {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
                ),
                CustomRule=_introspect_response(
                    {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
                ),
            )
        )
    )
    list(source.get_workunits_internal())
    assert len(source.report.failures) == 0
    assert source.report.warnings_count >= 1
    joined = " ".join(w.message for w in source.report.warnings)
    assert "description" in joined


def test_drift_abort_when_strict_and_introspection_fails() -> None:
    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("transient introspection blip")

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    drift = builder.check_drift(strict=True, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.ABORT
    assert drift.per_type["Monitor"].introspection_failed


def test_drift_type_absent_does_not_fake_critical_list() -> None:
    builder = _builder_with_introspection({"Monitor": {"mc_type": None}})
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    td = drift.per_type["Monitor"]
    assert td.verdict == DriftVerdict.ABORT
    assert td.type_absent
    assert td.critical_missing == []
    title = drift_abort_title(td, strict=False)
    assert "not found" in title
    assert "uuid" not in title


def test_degraded_message_includes_spec_impact() -> None:
    shape = _live_monitor_shape(
        include_where_condition=False, include_data_quality_dimension=False
    )
    builder = _builder_with_introspection(
        _full_nested_responses(Monitor=_introspect_response(shape))
    )
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    missing = drift.per_type["Monitor"].missing
    assert "whereCondition" in missing
    cfg = make_config(include_alerts=False)
    source = _build_source(cfg)
    source.client.set_query_builder(builder)
    list(source.get_workunits_internal())
    joined = " ".join(w.message for w in source.report.warnings)
    assert "whereCondition (row filter on nativeParameters)" in joined


def test_partial_comparison_drift_message_names_dotted_path() -> None:
    builder = _builder_with_introspection(
        _full_nested_responses(
            Monitor=_introspect_response(_live_monitor_shape()),
            Comparison=_introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        )
    )
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert "comparisons.customMetric" in drift.per_type["Monitor"].missing
    cfg = make_config(include_alerts=False, strict_schema_drift=False)
    source = _build_source(cfg)
    source.client.set_query_builder(builder)
    list(source.get_workunits_internal())
    joined = " ".join(w.message for w in source.report.warnings)
    assert "comparisons.customMetric" in joined


def test_alert_schema_abort_still_runs_monitor_phase() -> None:
    cfg = make_config(include_alerts=True, include_assertions=True)
    source = _build_source(cfg)

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "Monitor":
            return _introspect_response(_live_monitor_shape())
        if type_name == "Comparison":
            return _introspect_response(_live_comparison_shape())
        if type_name == "CustomRule":
            return _introspect_response(
                {
                    "uuid": "SCALAR:UUID",
                    "ruleName": "SCALAR:String",
                    "ruleType": "SCALAR:String",
                    "customSql": "SCALAR:String",
                    "entityMcons": "SCALAR:String",
                    "priority": "SCALAR:String",
                    "comparisons": "OBJECT:Comparison",
                }
            )
        if type_name == "Alert":
            alert_shape = {
                "id": "SCALAR:UUID",
                "createdTime": "SCALAR:DateTime",
                "monitorUuids": "SCALAR:String",
            }
            del alert_shape["monitorUuids"]
            return _introspect_response(alert_shape)
        if type_name == "AlertAsset":
            return _introspect_response({"mcon": "SCALAR:String"})
        if type_name == "SchemaChange":
            return _introspect_response(
                {
                    "mcon": "SCALAR:String",
                    "startTime": "SCALAR:DateTime",
                    "fieldsAdded": "SCALAR:String",
                    "fieldsRemoved": "SCALAR:String",
                    "fieldTypeChanges": "OBJECT:FieldTypeChange",
                }
            )
        if type_name == "FieldTypeChange":
            return _introspect_response(
                {
                    "fieldName": "SCALAR:String",
                    "oldFieldType": "SCALAR:String",
                    "newFieldType": "SCALAR:String",
                }
            )
        if type_name == "WarehouseTable":
            return _introspect_response(_live_warehouse_table_shape())
        if type_name == "Warehouse":
            return _introspect_response({"connectionType": "SCALAR:String"})
        if "getMonitors" in query:
            return {
                "get_monitors": [
                    {
                        "uuid": "mon-1",
                        "name": "m",
                        "monitor_type": "METRIC",
                        "entity_mcons": ["mcon:abc"],
                    }
                ]
            }
        if "getCustomRules" in query:
            return {
                "get_custom_rules": {
                    "edges": [],
                    "page_info": {"has_next_page": False},
                }
            }
        if "getAlerts" in query:
            return {
                "get_alerts": {
                    "edges": [],
                    "page_info": {"has_next_page": False},
                }
            }
        return {}

    source.client.set_query_builder(
        IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    )
    source.client._call = fake_call  # type: ignore[method-assign]
    list(source.get_workunits_internal())
    assert any(
        "no alerts were ingested because of schema drift" in (f.message or "").lower()
        for f in source.report.failures
    )
    assert source.report.monitors_scanned == 1
    assert source.report.alerts_scanned == 0


def test_strict_without_critical_miss_title_not_critical_fields() -> None:
    shape = _live_monitor_shape(include_description=False)
    builder = _builder_with_introspection(
        _full_nested_responses(Monitor=_introspect_response(shape))
    )
    drift = builder.check_drift(strict=True, types=[MonteCarloGraphQLType.MONITOR])
    title = drift_abort_title(drift.per_type["Monitor"], strict=True)
    assert "strict mode" in title
    assert "critical fields missing" not in title


# --- Renamed-field fallback mapping (customSql->whereCondition, severity->priority) ---


def _definition(**kwargs: Any) -> "mc_assertion.MonteCarloAssertionDef":
    from datahub.ingestion.source.montecarlo.client import MonteCarloAssertionDef

    base: Dict[str, Any] = {
        "uuid": "monitor-1",
        "name": "my monitor",
        "entity_mcons": ["mcon:abc"],
    }
    base.update(kwargs)
    return MonteCarloAssertionDef(**base)


def test_native_parameters_maps_priority_to_severity_key() -> None:
    definition = _definition(priority="HIGH")
    params = mc_assertion._native_parameters(definition)
    assert params["severity"] == "HIGH"


def test_spec_value_reads_severity_alias_as_priority() -> None:
    raw: dict[str, object] = {"severity": "CRITICAL"}
    assert (
        spec_value(raw, FieldSpec(name="priority", aliases=("severity",))) == "CRITICAL"
    )


def test_native_parameters_includes_where_condition() -> None:
    # whereCondition is a native MC row-filter; it is carried on
    # nativeParameters (not logic) so the UI can render the filter without
    # misrepresenting it as the monitor's SQL body.
    definition = _definition(where_condition="amount > 100")
    params = mc_assertion._native_parameters(definition)
    assert params["where_condition"] == "amount > 100"


def test_native_parameters_omits_where_condition_when_absent() -> None:
    # _string_map drops None values, so an absent where_condition leaves no
    # empty key on nativeParameters.
    definition = _definition(where_condition=None)
    params = mc_assertion._native_parameters(definition)
    assert "where_condition" not in params


def test_custom_assertion_logic_omits_where_condition() -> None:
    # Row-level filters scope the table, not the executable statement behind
    # an alert. When GraphQL no longer exposes recoverable runnable text,
    # the builder must not promote the filter into the assertion's primary
    # text slot; it keeps the filter on the sidecar map and leaves that slot
    # empty.
    definition = _definition(
        custom_sql=None, where_condition="amount > 100", monitor_type="CUSTOM_SQL"
    )
    info = mc_assertion._make_custom_assertion_info(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)",
        native_type="CUSTOM_SQL",
        definition=definition,
    )
    assert info.logic is None
    assert info.nativeParameters is not None
    assert info.nativeParameters.get("where_condition") == "amount > 100"


def test_custom_assertion_logic_uses_custom_sql_when_present() -> None:
    # On CustomRule (which still exposes customSql), it is the real SQL body
    # and populates logic; whereCondition is ignored for logic even if set.
    definition = _definition(
        custom_sql="SELECT 1", where_condition="x > 0", rule_type="CUSTOM_SQL"
    )
    info = mc_assertion._make_custom_assertion_info(
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)",
        native_type="CUSTOM_SQL",
        definition=definition,
    )
    assert info.logic == "SELECT 1"


def _shape_from_field_names(field_names: Dict[str, str]) -> _TypeShape:
    return _parse_type_shape(_introspect_response(field_names))


def test_drift_proceed_when_only_severity_alias_present() -> None:
    shape = _live_monitor_shape(include_priority=False, include_severity_alias=True)
    builder = _builder_with_introspection(
        _full_nested_responses(Monitor=_introspect_response(shape))
    )
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType.MONITOR])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.PROCEED
    query = builder.monitors_query()
    assert "severity" in query
    assert "priority" not in query


@pytest.mark.parametrize(
    ("type_name", "field_to_drop", "critical_path"),
    [
        ("Monitor", "uuid", "uuid"),
        ("Monitor", "entityMcons", "entityMcons"),
        ("CustomRule", "uuid", "uuid"),
        ("CustomRule", "entityMcons", "entityMcons"),
        ("Alert", "id", "id"),
        ("Alert", "monitorUuids", "monitorUuids"),
        ("Alert", "createdTime", "createdTime"),
    ],
)
def test_drift_abort_when_critical_field_missing(
    type_name: str, field_to_drop: str, critical_path: str
) -> None:
    if type_name == "Monitor":
        shape = _live_monitor_shape()
    elif type_name == "CustomRule":
        shape = {
            "uuid": "SCALAR:UUID",
            "ruleName": "SCALAR:String",
            "ruleType": "SCALAR:String",
            "description": "SCALAR:String",
            "customSql": "SCALAR:String",
            "entityMcons": "SCALAR:String",
            "priority": "SCALAR:String",
            "comparisons": "OBJECT:Comparison",
        }
    else:
        shape = {
            "id": "SCALAR:UUID",
            "type": "SCALAR:String",
            "subTypes": "SCALAR:String",
            "severity": "SCALAR:String",
            "priority": "SCALAR:String",
            "status": "SCALAR:String",
            "createdTime": "SCALAR:DateTime",
            "title": "SCALAR:String",
            "monitorUuids": "SCALAR:String",
            "assets": "OBJECT:AlertAsset",
        }
    del shape[field_to_drop]
    responses = _full_nested_responses(**{type_name: _introspect_response(shape)})
    if type_name == "Alert":
        responses["AlertAsset"] = _introspect_response({"mcon": "SCALAR:String"})
    builder = _builder_with_introspection(responses)
    drift = builder.check_drift(strict=False, types=[MonteCarloGraphQLType(type_name)])
    assert drift.per_type[type_name].verdict == DriftVerdict.ABORT
    assert critical_path in drift.per_type[type_name].critical_missing


def test_monitor_fallback_includes_data_quality_dimension() -> None:
    from datahub.ingestion.source.montecarlo.query_builder import (
        monitors_fallback_query,
    )

    query = monitors_fallback_query()
    assert "dataQualityDimension" in query


def _iter_in_fallback_graphql_names(fields: tuple) -> list:
    names: list = []
    for spec in fields:
        if not spec.in_fallback:
            continue
        if spec.children:
            names.append(spec.name)
            names.extend(_iter_in_fallback_graphql_names(spec.children))
        else:
            names.append(spec.name)
    return names


def test_field_spec_consistency_with_selections() -> None:
    nested_shapes = {
        "Comparison": _shape_from_field_names(_live_comparison_shape()),
        "CustomMetric": _shape_from_field_names(_live_custom_metric_shape()),
        "AlertAsset": _shape_from_field_names({"mcon": "SCALAR:String"}),
    }
    for type_spec in TYPE_SPECS.values():
        fallback = _emit_fallback_selection(type_spec.fields, "")
        for name in _iter_in_fallback_graphql_names(type_spec.fields):
            assert name in fallback, (
                f"{name} missing from {type_spec.type_name} fallback"
            )

        if type_spec.type_name.value == "Monitor":
            live = _shape_from_field_names(_live_monitor_shape())
        elif type_spec.type_name.value == "CustomRule":
            live = _shape_from_field_names(
                {
                    "uuid": "SCALAR:UUID",
                    "ruleName": "SCALAR:String",
                    "ruleType": "SCALAR:String",
                    "description": "SCALAR:String",
                    "customSql": "SCALAR:String",
                    "entityMcons": "SCALAR:String",
                    "priority": "SCALAR:String",
                    "comparisons": "OBJECT:Comparison",
                }
            )
        else:
            live = _shape_from_field_names(
                {
                    "id": "SCALAR:UUID",
                    "type": "SCALAR:String",
                    "subTypes": "SCALAR:String",
                    "severity": "SCALAR:String",
                    "priority": "SCALAR:String",
                    "status": "SCALAR:String",
                    "createdTime": "SCALAR:DateTime",
                    "title": "SCALAR:String",
                    "monitorUuids": "SCALAR:String",
                    "assets": "OBJECT:AlertAsset",
                }
            )
        dynamic = _build_selection_with_subshapes(
            type_spec.fields, live, nested_shapes, indent=""
        )
        for spec in type_spec.fields:
            if spec.children:
                continue
            for gql_name in (spec.name, *spec.aliases):
                if gql_name in live.fields:
                    assert gql_name in dynamic or (
                        spec.name in dynamic or gql_name in dynamic
                    )
                    break

    monitor_leaves = {
        "uuid",
        "name",
        "description",
        "monitorType",
        "whereCondition",
        "entityMcons",
        "resourceId",
        "priority",
        "dataQualityDimension",
    }
    spec_leaves = {
        spec.name for _, spec in iter_leaf_field_specs(MONITOR_TYPE_SPEC.fields)
    }
    assert monitor_leaves <= spec_leaves
    rule_leaves = {
        "uuid",
        "ruleName",
        "ruleType",
        "description",
        "customSql",
        "entityMcons",
        "priority",
    }
    spec_rule_leaves = {
        spec.name for _, spec in iter_leaf_field_specs(CUSTOM_RULE_TYPE_SPEC.fields)
    }
    assert rule_leaves <= spec_rule_leaves
    alert_leaves = {
        "id",
        "type",
        "subTypes",
        "severity",
        "priority",
        "status",
        "createdTime",
        "title",
        "monitorUuids",
        "mcon",
    }
    spec_alert_leaves = {
        spec.name.split(".")[-1]
        for path, spec in iter_leaf_field_specs(ALERT_TYPE_SPEC.fields)
    }
    assert alert_leaves <= spec_alert_leaves


def _live_table_monitor_shape() -> Dict[str, str]:
    return {
        "warehouseUuid": "SCALAR:UUID",
        "assetSelection": "OBJECT:AssetSelection",
    }


def _live_asset_selection_shape() -> Dict[str, str]:
    return {
        "filtersJoiner": "SCALAR:String",
        "exclusionsJoiner": "SCALAR:String",
        "databases": "OBJECT:AssetSelectionDatabase",
        "filters": "INTERFACE:AssetFilterInterface",
        "exclusions": "INTERFACE:AssetFilterInterface",
    }


def _live_asset_filter_shape() -> Dict[str, str]:
    return {
        "type": "SCALAR:String",
        "negated": "SCALAR:Boolean",
        "fullTableId": "SCALAR:String",
    }


def _table_monitor_drift_introspection(
    asset_selection_shape: Dict[str, str],
) -> Dict[str, Dict[str, Any]]:
    return {
        "TableMonitor": _introspect_response(_live_table_monitor_shape()),
        "AssetSelection": _introspect_response(asset_selection_shape),
        "AssetSelectionDatabase": _introspect_response(
            {"name": "SCALAR:String", "schemas": "SCALAR:String"}
        ),
        "AssetFilterInterface": _introspect_response(_live_asset_filter_shape()),
    }


def test_drift_proceed_when_asset_filter_fields_are_interface_kind() -> None:
    builder = _builder_with_introspection(
        _table_monitor_drift_introspection(_live_asset_selection_shape())
    )
    for strict in (False, True):
        drift = builder.check_drift(
            strict=strict,
            types=[
                MonteCarloGraphQLType.TABLE_MONITOR,
                MonteCarloGraphQLType.ASSET_SELECTION,
            ],
        )
        assert drift.per_type["TableMonitor"].verdict == DriftVerdict.PROCEED
        assert drift.per_type["AssetSelection"].verdict == DriftVerdict.PROCEED
        assert drift.per_type["TableMonitor"].missing == []
        assert drift.per_type["AssetSelection"].missing == []


def test_table_monitor_keeps_asset_selection_when_nested_introspection_fails() -> None:
    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "TableMonitor":
            return _introspect_response(_live_table_monitor_shape())
        if type_name == "AssetSelection":
            raise RuntimeError("transient blip on AssetSelection only")
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    query = builder.table_monitor_query()
    assert "warehouseUuid" in query
    assert "assetSelection" in query
    assert "filtersJoiner" in query
    assert "databases" in query
    assert "filters" in query
    drift = builder.check_drift(
        strict=False,
        types=[
            MonteCarloGraphQLType.TABLE_MONITOR,
            MonteCarloGraphQLType.ASSET_SELECTION,
        ],
    )
    assert drift.per_type["TableMonitor"].verdict == DriftVerdict.PROCEED
    assert drift.per_type["AssetSelection"].verdict == DriftVerdict.PROCEED


def test_table_monitor_keeps_databases_when_asset_selection_database_introspection_fails() -> (
    None
):
    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "TableMonitor":
            return _introspect_response(_live_table_monitor_shape())
        if type_name == "AssetSelection":
            return _introspect_response(_live_asset_selection_shape())
        if type_name == "AssetSelectionDatabase":
            raise RuntimeError("transient blip on AssetSelectionDatabase only")
        if type_name == "AssetFilterInterface":
            return _introspect_response(_live_asset_filter_shape())
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    query = builder.table_monitor_query()
    assert "databases" in query
    assert "name" in query
    assert "schemas" in query
    assert "filters" in query
    drift = builder.check_drift(
        strict=False,
        types=[
            MonteCarloGraphQLType.TABLE_MONITOR,
            MonteCarloGraphQLType.ASSET_SELECTION,
        ],
    )
    assert drift.per_type["AssetSelection"].verdict == DriftVerdict.PROCEED


def test_check_schema_drift_skips_run_event_types_without_lookback() -> None:
    introspect_calls: List[str] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if "mcIntrospect" in query:
            introspect_calls.append(variables.get("name", ""))
            return {"mc_type": {"fields": []}}
        return {}

    client = MonteCarloClient(
        make_config(include_alerts=False), page_size=100, report=None
    )
    client.set_query_builder(
        IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    )
    client.check_schema_drift(strict=False)
    assert "JobExecutionHistoryLog" not in introspect_calls
    assert "TableMetricV2" not in introspect_calls


def test_warehouse_table_critical_miss_aborts_monitor_phase() -> None:
    shape = _live_warehouse_table_shape()
    del shape["fullTableId"]
    cfg = make_config(include_alerts=False)
    source = _build_source(cfg)
    source.client.set_query_builder(
        _builder_with_introspection(
            {
                "Monitor": _introspect_response(_live_monitor_shape()),
                "Comparison": _introspect_response(_live_comparison_shape()),
                "CustomRule": _introspect_response(
                    {
                        "uuid": "SCALAR:UUID",
                        "entityMcons": "SCALAR:String",
                        "comparisons": "OBJECT:Comparison",
                    }
                ),
                "WarehouseTable": _introspect_response(shape),
            }
        )
    )
    list(source.get_workunits_internal())
    assert source.report.monitors_scanned == 0
    assert any(
        "critical fields missing" in (f.title or "")
        and "fullTableId" in (f.title or "")
        for f in source.report.failures
    )


def test_schema_change_type_absent_disables_join_with_one_warning() -> None:
    cfg = make_config(include_alerts=True, include_assertions=True)
    source = _build_source(cfg)

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "Monitor":
            return _introspect_response(_live_monitor_shape())
        if type_name == "Comparison":
            return _introspect_response(_live_comparison_shape())
        if type_name == "CustomRule":
            return _introspect_response(
                {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
            )
        if type_name == "Alert":
            return _introspect_response(
                {
                    "id": "SCALAR:UUID",
                    "createdTime": "SCALAR:DateTime",
                    "monitorUuids": "SCALAR:String",
                    "assets": "OBJECT:AlertAsset",
                }
            )
        if type_name == "AlertAsset":
            return _introspect_response({"mcon": "SCALAR:String"})
        if type_name == "SchemaChange":
            return {"mc_type": None}
        if type_name == "WarehouseTable":
            return _introspect_response(_live_warehouse_table_shape())
        if type_name == "Warehouse":
            return _introspect_response({"connectionType": "SCALAR:String"})
        if "getAlerts" in query:
            return {
                "get_alerts": {
                    "edges": [
                        {
                            "node": {
                                "id": "alert-1",
                                "type": "SCHEMA_CHANGES",
                                "created_time": "2024-01-01T00:00:00Z",
                                "monitor_uuids": ["mon-1"],
                                "assets": [{"mcon": "mcon:abc"}],
                            }
                        }
                    ],
                    "page_info": {"has_next_page": False},
                }
            }
        if "getSchemaChanges" in query:
            raise AssertionError("join should be disabled")
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    source.client.set_query_builder(builder)
    source.client._call = fake_call  # type: ignore[method-assign]
    list(source.get_workunits_internal())
    join_warnings = [
        w
        for w in source.report.warnings
        if "Schema-change catalog join disabled" in (w.title or "")
    ]
    assert len(join_warnings) == 1


def test_schema_change_critical_miss_disables_join_with_one_warning() -> None:
    shape = {
        "mcon": "SCALAR:String",
        "startTime": "SCALAR:DateTime",
        "fieldsAdded": "SCALAR:String",
        "fieldsRemoved": "SCALAR:String",
        "fieldTypeChanges": "OBJECT:FieldTypeChange",
    }
    del shape["startTime"]
    cfg = make_config(include_alerts=True, include_assertions=True)
    source = _build_source(cfg)

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "Monitor":
            return _introspect_response(_live_monitor_shape())
        if type_name == "Comparison":
            return _introspect_response(_live_comparison_shape())
        if type_name == "CustomRule":
            return _introspect_response(
                {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
            )
        if type_name == "Alert":
            return _introspect_response(
                {
                    "id": "SCALAR:UUID",
                    "createdTime": "SCALAR:DateTime",
                    "monitorUuids": "SCALAR:String",
                    "assets": "OBJECT:AlertAsset",
                }
            )
        if type_name == "AlertAsset":
            return _introspect_response({"mcon": "SCALAR:String"})
        if type_name == "SchemaChange":
            return _introspect_response(shape)
        if type_name == "WarehouseTable":
            return _introspect_response(_live_warehouse_table_shape())
        if type_name == "Warehouse":
            return _introspect_response({"connectionType": "SCALAR:String"})
        if "getAlerts" in query:
            return {
                "get_alerts": {
                    "edges": [
                        {
                            "node": {
                                "id": "alert-1",
                                "type": "SCHEMA_CHANGES",
                                "created_time": "2024-01-01T00:00:00Z",
                                "monitor_uuids": ["mon-1"],
                                "assets": [{"mcon": "mcon:abc"}],
                            }
                        }
                    ],
                    "page_info": {"has_next_page": False},
                }
            }
        if "getSchemaChanges" in query:
            raise AssertionError("join should be disabled")
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    source.client.set_query_builder(builder)
    source.client._call = fake_call  # type: ignore[method-assign]
    list(source.get_workunits_internal())
    join_warnings = [
        w
        for w in source.report.warnings
        if "Schema-change catalog join disabled" in (w.title or "")
    ]
    assert len(join_warnings) == 1


@pytest.mark.parametrize(
    "drift_type,field_to_drop",
    [
        ("JobExecutionHistoryLog", "jobExecutionUuid"),
        ("TableMetricV2", "metric"),
    ],
)
def test_run_events_type_critical_miss_disables_with_one_warning(
    drift_type: str, field_to_drop: str
) -> None:
    job_shape = _live_job_execution_history_log_shape()
    metric_shape = _live_metric_shape()
    if drift_type == "JobExecutionHistoryLog":
        del job_shape[field_to_drop]
    else:
        del metric_shape[field_to_drop]

    cfg = make_config(include_alerts=False, run_events_lookback_days=7)
    source = _build_source(cfg)

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "Monitor":
            return _introspect_response(_live_monitor_shape())
        if type_name == "Comparison":
            return _introspect_response(_live_comparison_shape())
        if type_name == "CustomRule":
            return _introspect_response(
                {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
            )
        if type_name == "WarehouseTable":
            return _introspect_response(_live_warehouse_table_shape())
        if type_name == "Warehouse":
            return _introspect_response({"connectionType": "SCALAR:String"})
        if type_name == "JobExecutionHistoryLog":
            return _introspect_response(job_shape)
        if type_name == "TableMetricV2":
            return _introspect_response(metric_shape)
        if "getJobExecutions" in query or "getMetricsV4" in query:
            raise AssertionError("run events should be disabled")
        if "getMonitors" in query:
            return {"get_monitors": []}
        if "getCustomRules" in query:
            return {
                "get_custom_rules": {
                    "edges": [],
                    "page_info": {"has_next_page": False},
                }
            }
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    source.client.set_query_builder(builder)
    source.client._call = fake_call  # type: ignore[method-assign]
    list(source.get_workunits_internal())
    assert source.client.run_events_disabled
    run_event_warnings = [
        w for w in source.report.warnings if w.title == "Monitor run events disabled"
    ]
    assert len(run_event_warnings) == 1


def test_table_scope_drift_locks_table_monitor_resolution() -> None:
    report = MonteCarloSourceReport()
    client = MonteCarloClient(make_config(), page_size=100, report=report)
    table_monitor_shape = _live_table_monitor_shape()
    del table_monitor_shape["warehouseUuid"]

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "TableMonitor":
            return _introspect_response(table_monitor_shape)
        if type_name == "AssetSelection":
            return _introspect_response(_live_asset_selection_shape())
        if type_name == "AssetSelectionResult":
            return _introspect_response(
                {"selected": "SCALAR:Boolean", "mcon": "SCALAR:String"}
            )
        if type_name == "AssetFilterInterface":
            return _introspect_response(_live_asset_filter_shape())
        if "getTableMonitor" in query:
            return {
                "get_table_monitor": {"warehouse_uuid": "wh", "asset_selection": {}}
            }
        raise AssertionError(f"unexpected: {query[:60]!r}")

    client.set_query_builder(
        IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    )
    client._call = fake_call  # type: ignore[method-assign]
    mcons = client._resolve_table_monitor_entity_mcons("mon-tbl", None)
    assert mcons == []
    assert report.build_failures == 1
    assert len(report.failures) == 1
    assert "critical fields missing" in (report.failures[0].title or "")
    failure_context = " ".join(report.failures[0].context or [])
    assert "TableMonitor" in failure_context
    assert "warehouseUuid" in failure_context

    client._resolve_table_monitor_entity_mcons("mon-tbl-2", None)
    assert len(report.failures) == 1


def test_table_scope_degraded_warning_names_missing_fields() -> None:
    report = MonteCarloSourceReport()
    client = MonteCarloClient(make_config(), page_size=100, report=report)
    asset_selection_shape = _live_asset_selection_shape()
    del asset_selection_shape["filtersJoiner"]

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        type_name = variables.get("name", "")
        if type_name == "TableMonitor":
            return _introspect_response(_live_table_monitor_shape())
        if type_name == "AssetSelection":
            return _introspect_response(asset_selection_shape)
        if type_name == "AssetSelectionDatabase":
            return _introspect_response(
                {"name": "SCALAR:String", "schemas": "SCALAR:String"}
            )
        if type_name == "AssetSelectionResult":
            return _introspect_response(
                {"selected": "SCALAR:Boolean", "mcon": "SCALAR:String"}
            )
        if type_name == "AssetFilterInterface":
            return _introspect_response(_live_asset_filter_shape())
        if "getTableMonitor" in query:
            return {
                "get_table_monitor": {"warehouse_uuid": "wh", "asset_selection": {}}
            }
        raise AssertionError(f"unexpected: {query[:60]!r}")

    client.set_query_builder(
        IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    )
    client._call = fake_call  # type: ignore[method-assign]
    client._resolve_table_monitor_entity_mcons("mon-tbl", None)
    drift_warnings = [
        w
        for w in report.warnings
        if w.title == "Monte Carlo schema drift (AssetSelection)"
    ]
    assert len(drift_warnings) == 1
    assert "filtersJoiner" in (drift_warnings[0].message or "")
    assert report.build_failures == 0
    assert len(report.failures) == 0
