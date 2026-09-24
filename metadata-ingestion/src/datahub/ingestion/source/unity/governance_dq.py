"""Extractor for the in-house Databricks Governance DQ tables (spec §7).

Reads two customer-owned tables (a rule-definition table and a rule-result/run-history
table) and republishes them as DataHub assertions on the shared seam (`unity/assertion.py`),
the same seam used by the data_quality/pipeline_expectations surfaces.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.assertion import (
    build_assertion_run_event,
    build_custom_assertion_info,
    build_status,
    make_urn,
    map_operator,
)
from datahub.ingestion.source.unity.config import GovernanceDQConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import DatasetAssertionScopeClass

logger = logging.getLogger(__name__)

_NATIVE_PARAMETER_FIELDS = ("severity", "rule_version", "dataset_name", "source_format")
_NATIVE_RESULT_FIELDS = (
    "operator_snapshot",
    "threshold_min_snapshot",
    "threshold_max_snapshot",
    "threshold_value_snapshot",
    "rule_version_snapshot",
    "source_format",
)


def _columns(rule: Dict[str, Any]) -> List[str]:
    raw = rule.get("columns") or ""
    if isinstance(raw, list):
        return [c for c in raw if c]
    return [c.strip() for c in str(raw).split(",") if c.strip()]


def _native_strs(row: Dict[str, Any], keys: Iterable[str]) -> Dict[str, str]:
    return {k: str(row[k]) for k in keys if row.get(k) is not None}


def rows_to_workunits(
    rules: List[Dict[str, Any]],
    results: List[Dict[str, Any]],
    resolve_dataset_urn: Callable[[str, str, str], Optional[str]],
    resolve_field_urn: Callable[[str, str], str],
) -> Iterable[MetadataChangeProposalWrapper]:
    """Pure core: rule/result rows -> assertion MCPs. No I/O, fully unit-testable."""
    urn_by_rule: Dict[str, str] = {}
    dataset_urn_by_rule: Dict[str, str] = {}

    for rule in rules:
        dataset_urn = resolve_dataset_urn(
            rule["catalog"], rule["schema"], rule["table"]
        )
        if not dataset_urn:
            continue

        cols = _columns(rule)
        scope = (
            DatasetAssertionScopeClass.DATASET_COLUMN
            if cols
            else DatasetAssertionScopeClass.DATASET_ROWS
        )
        std = map_operator(
            rule.get("operator", ""),
            rule.get("threshold_min"),
            rule.get("threshold_max"),
            rule.get("threshold_value"),
            scope=scope,
        )

        # Governance surface key = rule_id (spec §8), keyed off the dataset urn so it
        # inherits platform_instance/metastore/env; "surface" tag stops cross-surface collisions.
        urn = make_urn(
            {
                "surface": "governance",
                "platform": "databricks",
                "dataset": dataset_urn,
                "rule_id": rule["rule_id"],
            }
        )
        urn_by_rule[rule["rule_id"]] = urn
        dataset_urn_by_rule[rule["rule_id"]] = dataset_urn

        yield build_custom_assertion_info(
            assertion_urn=urn,
            entity_urn=dataset_urn,
            category="Databricks Governance DQ",
            native_type=rule["rule_type"],
            display_name=rule.get("rule_name", rule["rule_id"]),
            std=std,
            field_urns=[resolve_field_urn(dataset_urn, c) for c in cols],
            logic=rule.get("rule_description"),
            native_parameters=_native_strs(rule, _NATIVE_PARAMETER_FIELDS) or None,
        )

        active = bool(rule.get("active", True))
        if not active:
            # Only emit removed=True on retirement; an active rule needs no redundant
            # removed=False status MCP.
            yield build_status(urn, active)

    seen_runs = set()
    for res in results:
        rule_id = res["rule_id"]
        assertion_urn = urn_by_rule.get(rule_id)
        if assertion_urn is None:
            continue
        dedup_key = (assertion_urn, res["run_id"])
        if dedup_key in seen_runs:
            continue
        seen_runs.add(dedup_key)

        yield build_assertion_run_event(
            assertion_urn=assertion_urn,
            dataset_urn=dataset_urn_by_rule[rule_id],
            run_id=res["run_id"],
            timestamp_millis=res["executed_at_millis"],
            status=res["status"],
            warning=bool(res.get("warning", False)),
            severity=res.get("severity"),
            actual_value=res.get("actual_value"),
            row_count=res.get("evaluated_row_count"),
            missing_count=res.get("missing_row_count"),
            unexpected_count=res.get("failed_row_count"),
            external_url=res.get("external_url"),
            error_type=res.get("error_type"),
            error_message=res.get("error_message"),
            native_results=_native_strs(res, _NATIVE_RESULT_FIELDS) or None,
        )


@dataclass(eq=False)
class GovernanceDQExtractor:
    """Thin I/O wrapper: reads the two governance tables off the warehouse and
    delegates to the pure `rows_to_workunits`. All decision logic lives there."""

    config: GovernanceDQConfig
    proxy: UnityCatalogApiProxy
    report: UnityCatalogReport
    platform_instance: Optional[str]
    env: str
    platform: str = "databricks"

    def _resolve_dataset_urn(
        self, catalog: str, schema: str, table: str
    ) -> Optional[str]:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{catalog}.{schema}.{table}",
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.rules_table or not self.config.results_table:
            return

        rules = self.proxy.get_rows_from_table(self.config.rules_table)
        results = self.proxy.get_rows_from_table(self.config.results_table)
        for res in results:
            # The results table stores a `executed_at` timestamp (spec §7); the seam
            # works in epoch millis, so convert once here rather than in the pure core.
            executed_at = res.pop("executed_at", None)
            if executed_at is not None:
                res.setdefault("executed_at_millis", make_ts_millis(executed_at))

        for mcp in rows_to_workunits(
            rules, results, self._resolve_dataset_urn, make_schema_field_urn
        ):
            yield mcp.as_workunit()
