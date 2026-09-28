from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Protocol, Sequence

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.external_dq.builders import (
    StdAssertion,
    build_assertion_run_event,
    build_custom_assertion_info,
    build_status,
    make_external_assertion_urn,
    map_operator,
)
from datahub.ingestion.source.external_dq.contract import ResultRow, RuleRow
from datahub.ingestion.source.external_dq.report import ExternalDQReport
from datahub.metadata.schema_classes import DatasetAssertionScopeClass

_IDENTITY_SOURCE = "external_dq"
DEFAULT_CATEGORY = "External Data Quality"


class DatasetLocator(Protocol):
    """Resolves contract paths to the URNs the host connector itself emits."""

    def dataset_urn(self, dataset_path: Sequence[str]) -> Optional[str]: ...

    def field_urn(self, dataset_urn: str, column_path: str) -> str: ...


@dataclass(frozen=True)
class _BoundRule:
    assertion_urn: str
    dataset_urn: str
    severity: Optional[str]
    active: bool


class ExternalDQMapper:
    """Pure mapping from contract rows to assertion MCPs. No I/O."""

    def __init__(
        self,
        *,
        platform: str,
        platform_instance: Optional[str],
        rule_namespace: str,
        locator: DatasetLocator,
        report: ExternalDQReport,
        source_report: SourceReport,
        category: str = DEFAULT_CATEGORY,
    ) -> None:
        self.platform = platform
        self.platform_instance = platform_instance
        self.rule_namespace = rule_namespace
        self.locator = locator
        self.report = report
        self.source_report = source_report
        self.category = category
        self._rules: Dict[str, _BoundRule] = {}

    def assertion_urn(self, rule_id: str) -> str:
        # The dataset is deliberately not part of the key: a table rename re-points
        # the assertion instead of forking its run history.
        return make_external_assertion_urn(
            {
                "source": _IDENTITY_SOURCE,
                "platform": self.platform,
                "instance": self.platform_instance or "",
                "namespace": self.rule_namespace,
                "rule_id": rule_id,
            }
        )

    def std_for(self, rule: RuleRow) -> StdAssertion:
        scope = (
            DatasetAssertionScopeClass.DATASET_COLUMN
            if rule.column_paths
            else DatasetAssertionScopeClass.DATASET_ROWS
        )
        return map_operator(
            rule.operator or "",
            rule.threshold_min,
            rule.threshold_max,
            rule.threshold_value,
            scope=scope,
        )

    def map_rules(
        self, rules: Iterable[RuleRow]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        for rule in rules:
            if rule.rule_id in self._rules:
                self.report.rules_duplicate += 1
                self.source_report.warning(
                    title="Duplicate external DQ rule_id",
                    message="Only the first row for this rule_id was used.",
                    context=rule.rule_id,
                )
                continue
            dataset_urn = self.locator.dataset_urn(rule.dataset_path)
            if dataset_urn is None:
                self.report.rules_unresolved_dataset += 1
                self.source_report.warning(
                    title="External DQ rule targets a dataset that was not ingested",
                    message="The rule's dataset_path does not match a dataset ingested "
                    "in this run; the rule and its results were skipped.",
                    context=f"{rule.rule_id}: {'.'.join(rule.dataset_path)}",
                )
                continue

            assertion_urn = self.assertion_urn(rule.rule_id)
            self._rules[rule.rule_id] = _BoundRule(
                assertion_urn=assertion_urn,
                dataset_urn=dataset_urn,
                severity=rule.severity,
                active=rule.is_active,
            )
            properties = {
                key: value
                for key, value in (
                    ("rule_id", rule.rule_id),
                    ("rule_namespace", self.rule_namespace),
                    ("rule_description", rule.rule_description),
                    ("dimension", rule.dimension),
                    ("rule_version", rule.rule_version),
                    ("severity", rule.severity),
                )
                if value is not None
            }
            yield build_custom_assertion_info(
                assertion_urn=assertion_urn,
                entity_urn=dataset_urn,
                category=self.category,
                display_name=rule.rule_name,
                native_type=rule.rule_type,
                std=self.std_for(rule),
                field_urns=[
                    self.locator.field_urn(dataset_urn, column)
                    for column in rule.column_paths
                ],
                logic=rule.logic,
                native_parameters=dict(rule.extras) or None,
                external_url=rule.external_url,
                custom_properties=properties,
            )
            yield build_status(assertion_urn, active=rule.is_active)
            self.report.assertions_emitted += 1

    def is_known_rule(self, rule_id: str) -> bool:
        return rule_id in self._rules

    def map_result(self, result: ResultRow) -> Optional[MetadataChangeProposalWrapper]:
        bound = self._rules.get(result.rule_id)
        if bound is None:
            self.report.results_unknown_rule += 1
            return None
        if not bound.active:
            # A run event re-adds the assertion to the dataset's health summary
            # regardless of status.removed, which would un-retire the rule.
            self.report.results_skipped_retired += 1
            return None
        native = {
            key: str(value)
            for key, value in (
                ("operator_snapshot", result.operator_snapshot),
                ("threshold_min_snapshot", result.threshold_min_snapshot),
                ("threshold_max_snapshot", result.threshold_max_snapshot),
                ("threshold_value_snapshot", result.threshold_value_snapshot),
                ("rule_version_snapshot", result.rule_version_snapshot),
            )
            if value is not None
        }
        native.update(result.extras)
        self.report.run_events_emitted += 1
        return build_assertion_run_event(
            assertion_urn=bound.assertion_urn,
            dataset_urn=bound.dataset_urn,
            run_id=result.run_id,
            timestamp_millis=result.executed_at_millis,
            status=result.status,
            warning=result.is_warning,
            severity=result.severity or bound.severity,
            actual_value=result.actual_value,
            row_count=result.evaluated_row_count,
            missing_count=result.missing_row_count,
            unexpected_count=result.failed_row_count,
            external_url=result.external_url,
            error_type=result.error_type,
            error_message=result.error_message,
            native_results=native or None,
        )
