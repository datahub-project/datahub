"""Map Qualytics quality checks onto DataHub assertions.

The differentiating mapper: only this connector can turn Qualytics' 49 rule types into
something DataHub renders on a dataset's Validation tab.

**Shape.** Every check becomes `AssertionTypeClass.CUSTOM` carrying a
`CustomAssertionInfoClass`, with `type="qualytics"` and `nativeType` set to the
Qualytics rule type. That follows both precedents -- dbt's `dbt_tests.py` and
`montecarlo/assertion.py` independently converge on CUSTOM -- and it is the honest
description: these are externally defined and externally evaluated assertions.
`CustomAssertionInfoClass` still carries `scope`, `operator`, `aggregation`,
`parameters`, `fields` and `logic`, so choosing CUSTOM costs no structure. This refines
the planning doc, which described mapping onto the FIELD / VOLUME / FRESHNESS assertion
*types*; the semantic grouping survives, but it drives scope/operator/aggregation
rather than the top-level type.

**Nothing is ever dropped.** A rule type this build has not seen still becomes an
assertion -- with `_NATIVE_` sentinels for operator and aggregation, its properties
preserved in `nativeParameters`, and a report entry so the gap is visible. Silently
losing a customer's quality check is the worst thing this connector could do.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from datahub.emitter.mce_builder import (
    make_assertion_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qualytics.constants import PLATFORM
from datahub.ingestion.source.qualytics.models import QualityCheck
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DataPlatformInstanceClass,
    DatasetAssertionScopeClass,
)

_COLUMN = DatasetAssertionScopeClass.DATASET_COLUMN
_ROWS = DatasetAssertionScopeClass.DATASET_ROWS
_SCHEMA = DatasetAssertionScopeClass.DATASET_SCHEMA

_OP = AssertionStdOperatorClass
_AGG = AssertionStdAggregationClass

# Sentinel for Qualytics semantics with no DataHub equivalent. The real enum member,
# not a bare string, so the aspect stays consistent with what dbt and montecarlo emit
# and the UI renders the native description instead of a blank operator.
_NATIVE_OP = _OP._NATIVE_
_NATIVE_AGG = _AGG._NATIVE_


@dataclass(frozen=True)
class RuleMapping:
    """How one Qualytics rule type projects onto DataHub's assertion vocabulary."""

    scope: str
    operator: str
    aggregation: str
    # The bound the rule type implies when its properties carry none. Without it
    # `unique` renders as "unique proportion equals" with nothing on the right.
    implied_value: str | None = None


# All 49 rule types in the captured spec, grouped by what they assert.
# tests/unit/test_assertion.py checks this table against the spec's RuleType enum, so
# a 50th rule type fails CI rather than quietly taking the custom fallback.
RULE_MAPPINGS: dict[str, RuleMapping] = {
    # Null and emptiness
    "notNull": RuleMapping(_COLUMN, _OP.NOT_NULL, _AGG.IDENTITY),
    "anyNotNull": RuleMapping(_COLUMN, _OP.NOT_NULL, _AGG.IDENTITY),
    "notEmpty": RuleMapping(_COLUMN, _OP.NOT_EQUAL_TO, _AGG.IDENTITY),
    # Uniqueness
    "unique": RuleMapping(_COLUMN, _OP.EQUAL_TO, _AGG.UNIQUE_PROPORTION, "1"),
    "distinctCount": RuleMapping(_COLUMN, _OP.EQUAL_TO, _AGG.UNIQUE_COUNT),
    # Numeric range and aggregate
    "between": RuleMapping(_COLUMN, _OP.BETWEEN, _AGG.IDENTITY),
    "minValue": RuleMapping(_COLUMN, _OP.GREATER_THAN_OR_EQUAL_TO, _AGG.MIN),
    "maxValue": RuleMapping(_COLUMN, _OP.LESS_THAN_OR_EQUAL_TO, _AGG.MAX),
    "greaterThan": RuleMapping(_COLUMN, _OP.GREATER_THAN, _AGG.IDENTITY),
    "lessThan": RuleMapping(_COLUMN, _OP.LESS_THAN, _AGG.IDENTITY),
    "positive": RuleMapping(_COLUMN, _OP.GREATER_THAN, _AGG.IDENTITY, "0"),
    "notNegative": RuleMapping(
        _COLUMN, _OP.GREATER_THAN_OR_EQUAL_TO, _AGG.IDENTITY, "0"
    ),
    "sum": RuleMapping(_COLUMN, _OP.EQUAL_TO, _AGG.SUM),
    # Pattern and PII detection
    "matchesPattern": RuleMapping(_COLUMN, _OP.REGEX_MATCH, _AGG.IDENTITY),
    "containsCreditCard": RuleMapping(_COLUMN, _OP.CONTAIN, _AGG.IDENTITY),
    "containsEmail": RuleMapping(_COLUMN, _OP.CONTAIN, _AGG.IDENTITY),
    "containsUrl": RuleMapping(_COLUMN, _OP.CONTAIN, _AGG.IDENTITY),
    "containsSocialSecurityNumber": RuleMapping(_COLUMN, _OP.CONTAIN, _AGG.IDENTITY),
    # "is" variants assert the whole value matches a format, which none of DataHub's
    # standard operators expresses; the rule type in nativeType carries the meaning.
    "isCreditCard": RuleMapping(_COLUMN, _NATIVE_OP, _AGG.IDENTITY),
    "isAddress": RuleMapping(_COLUMN, _NATIVE_OP, _AGG.IDENTITY),
    # Set membership
    "expectedValues": RuleMapping(_COLUMN, _OP.IN, _AGG.IDENTITY),
    "existsIn": RuleMapping(_COLUMN, _OP.IN, _AGG.IDENTITY),
    "notExistsIn": RuleMapping(_COLUMN, _OP.NOT_IN, _AGG.IDENTITY),
    # requiredValues is the converse of IN: every listed value must appear somewhere in
    # the column. IN would invert the meaning, so it stays native.
    "requiredValues": RuleMapping(_COLUMN, _NATIVE_OP, _AGG.IDENTITY),
    # String length. DataHub has no length aggregation, so the comparison is standard
    # but what is being compared is not.
    "minLength": RuleMapping(_COLUMN, _OP.GREATER_THAN_OR_EQUAL_TO, _NATIVE_AGG),
    "maxLength": RuleMapping(_COLUMN, _OP.LESS_THAN_OR_EQUAL_TO, _NATIVE_AGG),
    # Temporal
    "afterDateTime": RuleMapping(_COLUMN, _OP.GREATER_THAN, _AGG.IDENTITY),
    "beforeDateTime": RuleMapping(_COLUMN, _OP.LESS_THAN, _AGG.IDENTITY),
    "betweenTimes": RuleMapping(_COLUMN, _OP.BETWEEN, _AGG.IDENTITY),
    "notFuture": RuleMapping(_COLUMN, _OP.LESS_THAN_OR_EQUAL_TO, _AGG.IDENTITY),
    # Volume
    "volumetric": RuleMapping(_ROWS, _OP.BETWEEN, _AGG.ROW_COUNT),
    "minPartitionSize": RuleMapping(
        _ROWS, _OP.GREATER_THAN_OR_EQUAL_TO, _AGG.ROW_COUNT
    ),
    "maxPartitionSize": RuleMapping(_ROWS, _OP.LESS_THAN_OR_EQUAL_TO, _AGG.ROW_COUNT),
    "fieldCount": RuleMapping(_SCHEMA, _OP.EQUAL_TO, _AGG.COLUMN_COUNT),
    # Freshness and distribution over time
    "freshness": RuleMapping(_ROWS, _OP.LESS_THAN_OR_EQUAL_TO, _NATIVE_AGG),
    "timeDistributionSize": RuleMapping(_ROWS, _OP.BETWEEN, _AGG.ROW_COUNT),
    # Schema
    "expectedSchema": RuleMapping(_SCHEMA, _OP.EQUAL_TO, _AGG.COLUMNS),
    "isType": RuleMapping(_COLUMN, _OP.EQUAL_TO, _NATIVE_AGG),
    # Expression and metric comparison. The expression itself goes in `logic`.
    "satisfiesExpression": RuleMapping(_ROWS, _NATIVE_OP, _NATIVE_AGG),
    "aggregationComparison": RuleMapping(_ROWS, _NATIVE_OP, _NATIVE_AGG),
    "metric": RuleMapping(_ROWS, _NATIVE_OP, _NATIVE_AGG),
    # Field-to-field comparison. The operator is standard; the right-hand side being
    # another column rather than a literal is what DataHub cannot express.
    "equalTo": RuleMapping(_COLUMN, _OP.EQUAL_TO, _AGG.IDENTITY),
    "equalToField": RuleMapping(_COLUMN, _OP.EQUAL_TO, _NATIVE_AGG),
    "greaterThanField": RuleMapping(_COLUMN, _OP.GREATER_THAN, _NATIVE_AGG),
    "lessThanField": RuleMapping(_COLUMN, _OP.LESS_THAN, _NATIVE_AGG),
    # Cross-dataset and inferential checks -- no DataHub vocabulary for any of these.
    "isReplicaOf": RuleMapping(_ROWS, _NATIVE_OP, _NATIVE_AGG),
    "dataDiff": RuleMapping(_ROWS, _NATIVE_OP, _NATIVE_AGG),
    "entityResolution": RuleMapping(_ROWS, _NATIVE_OP, _NATIVE_AGG),
    "predictedBy": RuleMapping(_COLUMN, _NATIVE_OP, _NATIVE_AGG),
}

# Fallback for a rule type added after this build. Deliberately permissive rather than
# a skip.
_UNKNOWN_RULE = RuleMapping(DatasetAssertionScopeClass.UNKNOWN, _NATIVE_OP, _NATIVE_AGG)

# Qualytics property keys that carry the assertion's bounds, in precedence order.
_VALUE_KEYS = ("value", "pattern", "list", "datetime", "last_value")
_MIN_KEYS = ("min", "min_size", "min_time")
_MAX_KEYS = ("max", "max_size", "max_time")
_LOGIC_KEYS = ("expression", "ref_expression")

# Items kept from a list-valued check property. An expectedValues check can carry
# thousands of members, all of which would otherwise land in both `parameters` and
# `nativeParameters` on every run. Matches profile.py's histogram cap.
MAX_LIST_PARAMETER_ITEMS = 100


class QualyticsAssertionKey(DatahubKey):
    """Identity for a Qualytics quality check as a DataHub assertion.

    ``instance`` is load-bearing, not decoration. Qualytics check ids are per-deployment
    Postgres sequences, so check 42 in one deployment and check 42 in another are
    unrelated. Without the platform instance in the key they would collapse onto one
    assertion URN and silently overwrite each other.
    """

    check_id: str
    instance: str | None = None


def _first_present(properties: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = properties.get(key)
        if value is not None:
            return value
    return None


def _parameter(value: Any) -> AssertionStdParameterClass | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        rendered = ",".join(str(v) for v in value)
        return AssertionStdParameterClass(
            value=rendered, type=AssertionStdParameterTypeClass.LIST
        )
    if isinstance(value, bool):
        # Checked before the numeric branch: bool is a subclass of int in Python, so
        # True would otherwise be reported as the number 1.
        return AssertionStdParameterClass(
            value=str(value), type=AssertionStdParameterTypeClass.STRING
        )
    if isinstance(value, (int, float)):
        return AssertionStdParameterClass(
            value=str(value), type=AssertionStdParameterTypeClass.NUMBER
        )
    return AssertionStdParameterClass(
        value=str(value), type=AssertionStdParameterTypeClass.STRING
    )


def _as_logic(value: object) -> str | None:
    """``logic`` is a string in DataHub's schema; anything else fails serialisation.

    The spec types the expression as a string, but this is the one property that goes
    into a typed aspect field rather than the string map, so it is coerced, not trusted.
    """
    return None if value is None else str(value)


def _value_parameter(properties: dict[str, Any]) -> AssertionStdParameterClass | None:
    """The comparison value, unless it is a list _cap_lists trimmed.

    A trimmed list is left out of the typed parameter, which carries no sign that it
    is partial; ``nativeParameters`` keeps it, with its ``_total_count``.
    """
    for key in _VALUE_KEYS:
        value = properties.get(key)
        if value is not None:
            if f"{key}_total_count" in properties:
                return None
            return _parameter(value)
    return None


def _cap_lists(properties: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Trim list-valued properties to MAX_LIST_PARAMETER_ITEMS.

    A trimmed list gains a ``<key>_total_count`` sibling, so a reader of the assertion
    can see the list is partial rather than mistaking the first hundred for all of it.
    """
    capped: dict[str, Any] = {}
    truncated = False
    for key, value in properties.items():
        if isinstance(value, (list, tuple)) and len(value) > MAX_LIST_PARAMETER_ITEMS:
            capped[key] = list(value)[:MAX_LIST_PARAMETER_ITEMS]
            capped[f"{key}_total_count"] = len(value)
            truncated = True
        else:
            capped[key] = value
    return capped, truncated


def _string_map(properties: dict[str, Any]) -> dict[str, str]:
    """nativeParameters is map[string, string]; coerce and drop nulls."""
    return {k: str(v) for k, v in properties.items() if v is not None}


class AssertionMapper:
    """Builds DataHub assertions from Qualytics quality checks."""

    def __init__(
        self, platform_instance: str | None, report: QualyticsSourceReport
    ) -> None:
        self.platform_instance = platform_instance
        self.report = report
        self._reported_rule_types: set[str] = set()

    def assertion_urn(self, check_id: int) -> str:
        key = QualyticsAssertionKey(
            check_id=str(check_id), instance=self.platform_instance
        )
        return make_assertion_urn(key.guid())

    def workunits(
        self,
        check: QualityCheck,
        dataset_urn: str,
        field_urns: list[str] | None = None,
        external_url: str | None = None,
    ) -> Iterable[MetadataWorkUnit]:
        """The assertion for one quality check: assertionInfo and dataPlatformInstance.

        The same pair Monte Carlo emits. dataPlatformInstance ties the assertion to the
        qualytics platform and, when set, to this Qualytics deployment -- the only place
        the deployment is visible on the assertion, since the URN is an opaque guid.
        """
        assertion_urn = self.assertion_urn(check.id)
        info = self.assertion_info(check, dataset_urn, field_urns, external_url)
        self.report.assertions_emitted += 1
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=info
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(PLATFORM),
                instance=(
                    make_dataplatform_instance_urn(PLATFORM, self.platform_instance)
                    if self.platform_instance
                    else None
                ),
            ),
        ).as_workunit()

    def _mapping(self, rule_type: str) -> RuleMapping:
        mapping = RULE_MAPPINGS.get(rule_type)
        if mapping is not None:
            return mapping

        if rule_type in self._reported_rule_types:
            return _UNKNOWN_RULE
        # Once per rule type, not per check: 500 checks of one new type are one gap.
        self._reported_rule_types.add(rule_type)
        self.report.report_unmapped_rule_type(rule_type)
        self.report.warning(
            title="Unrecognised Qualytics rule type",
            message=(
                "The check is still emitted as a custom assertion with its properties "
                "preserved, but without a DataHub operator or aggregation. The rule "
                "type is newer than this build of the connector."
            ),
            context=f"rule_type={rule_type}",
        )
        return _UNKNOWN_RULE

    @staticmethod
    def _parameters(
        properties: dict[str, Any], mapping: RuleMapping
    ) -> AssertionStdParametersClass | None:
        value = _value_parameter(properties)
        if value is None and mapping.implied_value is not None:
            value = AssertionStdParameterClass(
                value=mapping.implied_value, type=AssertionStdParameterTypeClass.NUMBER
            )
        min_value = _parameter(_first_present(properties, _MIN_KEYS))
        max_value = _parameter(_first_present(properties, _MAX_KEYS))
        if value is None and min_value is None and max_value is None:
            return None
        return AssertionStdParametersClass(
            value=value, minValue=min_value, maxValue=max_value
        )

    def _description(self, check: QualityCheck) -> str:
        """Qualytics descriptions are optional; fall back to something readable.

        Without this the UI shows its generic "A custom externally reported Assertion"
        placeholder, which tells a user nothing about which check failed.
        """
        if check.description:
            return check.description
        fields = ", ".join(f.name for f in check.fields)
        return f"{check.rule_type} on {fields}" if fields else check.rule_type

    def assertion_info(
        self,
        check: QualityCheck,
        dataset_urn: str,
        field_urns: list[str] | None = None,
        external_url: str | None = None,
    ) -> AssertionInfoClass:
        """The assertionInfo aspect for one quality check."""
        mapping = self._mapping(check.rule_type)
        properties, truncated = _cap_lists(check.properties or {})
        if truncated:
            self.report.assertion_parameters_truncated += 1
        fields = field_urns or []

        custom_properties = {
            "qualytics_check_id": str(check.id),
            "rule_type": check.rule_type,
            "inferred": str(check.inferred),
            "coverage": str(check.coverage),
            "weight": str(check.weight),
        }
        if check.status:
            custom_properties["status"] = check.status
        if check.filter:
            custom_properties["filter"] = check.filter

        return AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            # EXTERNAL, as Monte Carlo sets it. Built by hand rather than with
            # make_assertion_source(), which stamps `created` with the wall clock and
            # would turn every run into a new version of every assertionInfo.
            source=AssertionSourceClass(type=AssertionSourceTypeClass.EXTERNAL),
            description=self._description(check),
            externalUrl=external_url,
            customProperties=custom_properties,
            customAssertion=CustomAssertionInfoClass(
                type=PLATFORM,
                entity=dataset_urn,
                field=fields[0] if fields else None,
                fields=fields or None,
                scope=mapping.scope,
                operator=mapping.operator,
                aggregation=mapping.aggregation,
                parameters=self._parameters(properties, mapping),
                nativeType=check.rule_type,
                nativeParameters=_string_map(properties) or None,
                logic=_as_logic(_first_present(properties, _LOGIC_KEYS)),
            ),
        )
