from typing import Any, List, Optional, Sequence

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.external_dq.contract import (
    RESULTS_COLUMNS,
    RULES_COLUMNS,
    ResultRow,
    RuleRow,
    parse_result_row,
    parse_rule_row,
)
from datahub.ingestion.source.external_dq.mapper import ExternalDQMapper
from datahub.ingestion.source.external_dq.report import ExternalDQReport
from datahub.ingestion.source.external_dq.types import coerce_value
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionRunEventClass,
    StatusClass,
)
from tests.unit.external_dq._fixtures import result_raw, rule_raw


class FakeLocator:
    def dataset_urn(self, dataset_path: Sequence[str]) -> Optional[str]:
        if dataset_path[0] == "unknown":
            return None
        return make_dataset_urn("databricks", ".".join(dataset_path))

    def field_urn(self, dataset_urn: str, column_path: str) -> str:
        return make_schema_field_urn(dataset_urn, column_path)


def rule(**overrides: Any) -> RuleRow:
    raw = rule_raw(**overrides)
    return parse_rule_row(
        {c.name: coerce_value(raw[c.name], c.logical_type) for c in RULES_COLUMNS}, {}
    )


def result(**overrides: Any) -> ResultRow:
    raw = result_raw(**overrides)
    return parse_result_row(
        {c.name: coerce_value(raw[c.name], c.logical_type) for c in RESULTS_COLUMNS}, {}
    )


def make_mapper(platform_instance: Optional[str] = None) -> ExternalDQMapper:
    return ExternalDQMapper(
        platform="databricks",
        platform_instance=platform_instance,
        rule_namespace="default",
        locator=FakeLocator(),
        report=ExternalDQReport(),
        source_report=SourceReport(),
    )


def test_identity_ignores_dataset_and_depends_on_instance() -> None:
    mapper = make_mapper()
    renamed = list(
        mapper.map_rules([rule(dataset_path=["main", "sales", "orders_v2"])])
    )
    original = list(make_mapper().map_rules([rule()]))
    assert renamed[0].entityUrn == original[0].entityUrn
    assert make_mapper("ws2").assertion_urn("r1") != mapper.assertion_urn("r1")


def test_rule_maps_to_info_and_status() -> None:
    mcps = list(make_mapper().map_rules([rule()]))
    info, status = mcps[0].aspect, mcps[1].aspect
    assert isinstance(info, AssertionInfoClass) and isinstance(status, StatusClass)
    assert info.customAssertion is not None
    assert info.customAssertion.nativeType == "completeness"
    assert info.customAssertion.logic == "amount IS NOT NULL"
    assert info.customProperties["severity"] == "HIGH"
    assert status.removed is False


def test_result_inherits_rule_severity_and_snapshots() -> None:
    mapper = make_mapper()
    list(mapper.map_rules([rule()]))
    mcp = mapper.map_result(result())
    assert mcp is not None and isinstance(mcp.aspect, AssertionRunEventClass)
    assert mcp.aspect.result is not None
    assert mcp.aspect.result.severity == "HIGH"
    assert mcp.aspect.result.unexpectedCount == 3
    assert mcp.aspect.result.nativeResults == {
        "operator_snapshot": "NOT_NULL",
        "rule_version_snapshot": "3",
    }


def test_inactive_rule_still_records_history() -> None:
    mapper = make_mapper()
    mcps = list(mapper.map_rules([rule(is_active=False)]))
    status = mcps[1].aspect
    assert isinstance(status, StatusClass) and status.removed is True
    assert mapper.map_result(result()) is not None


def test_unresolved_duplicate_and_unknown_rules_are_counted() -> None:
    mapper = make_mapper()
    emitted: List[Any] = list(
        mapper.map_rules(
            [
                rule(),
                rule(rule_name="dup"),
                rule(rule_id="r2", dataset_path=["unknown"]),
            ]
        )
    )
    assert len(emitted) == 2
    assert mapper.report.rules_duplicate == 1
    assert mapper.report.rules_unresolved_dataset == 1
    assert mapper.map_result(result(rule_id="r2")) is None
    assert mapper.report.results_unknown_rule == 1
