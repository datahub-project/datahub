from typing import Any, Dict, List

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.montecarlo import assertion as mc_assertion
from datahub.ingestion.source.montecarlo.client import MonteCarloClient
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import MconResolver
from datahub.ingestion.source.montecarlo.query_builder import (
    DriftVerdict,
    IntrospectingQueryBuilder,
    StaticQueryBuilder,
    _parse_type_shape,
)
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.ingestion.source.montecarlo.source import MonteCarloSource


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
        else:  # OBJECT — wrap as NON_NULL(LIST(OBJECT)) like a real connection
            type_node = {
                "kind": "NON_NULL",
                "name": None,
                "of_type": {
                    "kind": "LIST",
                    "name": None,
                    "of_type": {"kind": "OBJECT", "name": type_name, "of_type": None},
                },
            }
        fields.append({"name": name, "type": type_node})
    return {"mc_type": {"fields": fields}}


def _live_monitor_shape(
    *,
    include_custom_sql: bool = True,
    include_severity: bool = True,
    include_priority: bool = True,
    include_where_condition: bool = True,
) -> Dict[str, str]:
    """The live Monitor type's field set. customSql/severity were renamed to
    whereCondition/priority on the current gateway; toggles simulate the drift
    that caused the original 400 (customSql/severity dropped) while keeping the
    replacement fields present by default."""
    shape: Dict[str, str] = {
        "uuid": "SCALAR:UUID",
        "name": "SCALAR:String",
        "description": "SCALAR:String",
        "monitorType": "SCALAR:String",
        "entityMcons": "SCALAR:String",
        "resourceId": "SCALAR:UUID",
        "dataQualityDimension": "SCALAR:String",
        "comparisons": "OBJECT:Comparison",
    }
    if include_custom_sql:
        shape["customSql"] = "SCALAR:String"
    if include_where_condition:
        shape["whereCondition"] = "SCALAR:String"
    if include_severity:
        shape["severity"] = "SCALAR:String"
    if include_priority:
        shape["priority"] = "SCALAR:String"
    return shape


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
    assert shape.fields["comparisons"].is_object is True
    assert shape.fields["comparisons"].object_type_name == "Comparison"


def test_builder_drops_removed_fields_from_query() -> None:
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(
                _live_monitor_shape(include_custom_sql=False, include_severity=False)
            ),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    query = builder.monitors_query()
    assert "customSql" not in query
    assert "severity" not in query
    assert "uuid" in query
    assert "entityMcons" in query
    assert "comparisons" in query
    # The renamed replacement fields are present on the live schema and so are
    # fetched even when customSql/severity were dropped.
    assert "whereCondition" in query
    assert "priority" in query


def test_builder_keeps_all_fields_when_no_drift() -> None:
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(_live_monitor_shape()),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    query = builder.monitors_query()
    assert "customSql" in query
    assert "severity" in query
    assert "uuid" in query


def test_drift_degraded_when_optional_fields_missing() -> None:
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(
                _live_monitor_shape(include_custom_sql=False, include_severity=False)
            ),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    drift = builder.check_drift(strict=False, types=["Monitor"])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.DEGRADED
    assert "customSql" in drift.per_type["Monitor"].missing
    assert "severity" in drift.per_type["Monitor"].missing
    assert drift.per_type["Monitor"].critical_missing == []
    assert drift.verdict == DriftVerdict.DEGRADED


def test_drift_abort_when_critical_field_missing() -> None:
    shape = _live_monitor_shape()
    del shape["uuid"]
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(shape),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    drift = builder.check_drift(strict=False, types=["Monitor"])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.ABORT
    assert "uuid" in drift.per_type["Monitor"].critical_missing
    assert drift.verdict == DriftVerdict.ABORT


def test_drift_abort_when_strict_and_optional_missing() -> None:
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(
                _live_monitor_shape(include_custom_sql=False, include_severity=False)
            ),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    drift = builder.check_drift(strict=True, types=["Monitor"])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.ABORT


def test_drift_proceed_when_no_drift() -> None:
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(_live_monitor_shape()),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    drift = builder.check_drift(strict=False, types=["Monitor"])
    assert drift.per_type["Monitor"].verdict == DriftVerdict.PROCEED
    assert drift.per_type["Monitor"].missing == []
    assert drift.verdict == DriftVerdict.PROCEED


def test_drift_reports_new_uningested_fields() -> None:
    shape = _live_monitor_shape()
    shape["customSqlRuleType"] = "SCALAR:String"
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(shape),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
        }
    )
    drift = builder.check_drift(strict=False, types=["Monitor"])
    assert "customSqlRuleType" in drift.per_type["Monitor"].new_fields
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
    drift = builder.check_drift(strict=False, types=["Monitor"])
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
    assert "id" in alert_q
    assert "monitorUuids" in alert_q
    drift = builder.check_drift(strict=False, types=["Monitor", "Alert"])
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
            return _introspect_response(
                _live_monitor_shape(include_custom_sql=False, include_severity=False)
            )
        if type_name == "Comparison":
            raise RuntimeError("transient blip on Comparison only")
        return {"mc_type": {"fields": []}}

    builder = IntrospectingQueryBuilder(fake_call, lambda **kwargs: None)
    query = builder.monitors_query()
    assert "comparisons" in query
    assert "operator" in query
    assert "metric" in query
    assert "customMetric" in query
    # Parent Monitor was introspected successfully, so drifted fields stay
    # dropped and the renamed replacements stay requested.
    assert "customSql" not in query
    assert "severity" not in query
    assert "whereCondition" in query
    assert "priority" in query
    assert "uuid" in query


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
    assert "comparisons" in query
    assert "operator" in query
    assert "customMetric" in query
    assert "customSql" in query
    assert "uuid" in query


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
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config(include_alerts=False)
    client.page_size = 100
    client.report = None
    builder = _builder_with_introspection(
        {
            "Monitor": _introspect_response(_live_monitor_shape()),
            "Comparison": _introspect_response(
                {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
            ),
            "CustomRule": _introspect_response(
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
            "Alert": _introspect_response({"id": "SCALAR:UUID"}),
        }
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
    source.client = MonteCarloClient.__new__(MonteCarloClient)
    source.client.config = cfg
    source.client.page_size = 100
    source.client.report = source.report
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
            {
                "Monitor": _introspect_response(shape),
                "Comparison": _introspect_response(
                    {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
                ),
                "CustomRule": _introspect_response(
                    {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
                ),
            }
        )
    )
    wus = list(source.get_workunits_internal())
    assert wus == []
    assert len(source.report.failures) == 1
    assert "critical fields missing" in (source.report.failures[0].title or "")


def test_source_gate_proceeds_with_warning_on_degraded_drift() -> None:
    cfg = make_config(include_alerts=False)
    source = _build_source(cfg)
    source.client.set_query_builder(
        _builder_with_introspection(
            {
                "Monitor": _introspect_response(
                    _live_monitor_shape(
                        include_custom_sql=False, include_severity=False
                    )
                ),
                "Comparison": _introspect_response(
                    {"comparisonType": "SCALAR:String", "operator": "SCALAR:String"}
                ),
                "CustomRule": _introspect_response(
                    {"uuid": "SCALAR:UUID", "entityMcons": "SCALAR:String"}
                ),
            }
        )
    )
    list(source.get_workunits_internal())
    # Degraded drift → a warning naming the dropped fields, not a failure.
    assert len(source.report.failures) == 0
    assert source.report.warnings_count >= 1
    joined = " ".join(w.message for w in source.report.warnings)
    assert "customSql" in joined and "severity" in joined


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


def test_native_parameters_severity_falls_back_to_priority() -> None:
    # severity removed from the live schema -> priority carries the value.
    definition = _definition(severity=None, priority="HIGH")
    params = mc_assertion._native_parameters(definition)
    assert params["severity"] == "HIGH"


def test_native_parameters_severity_preferred_over_priority() -> None:
    # On a gateway that still exposes severity, it wins.
    definition = _definition(severity="CRITICAL", priority="HIGH")
    params = mc_assertion._native_parameters(definition)
    assert params["severity"] == "CRITICAL"


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
    # whereCondition is a row-filter WHERE clause on metric/comparison
    # monitors, NOT the monitor's SQL body. It must not be folded into logic
    # (rendering a filter predicate as if it were the monitor SQL). With
    # customSql removed from the Monitor type, the SQL is unrecoverable and
    # logic stays None. The filter is preserved on nativeParameters instead.
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
