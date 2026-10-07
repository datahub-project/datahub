"""Tests for the Qualytics quality check -> DataHub assertion mapping.

The first test is the important one: it enumerates RuleType from the committed
OpenAPI spec and fails if any value is missing from RULE_MAPPINGS. That is the
tripwire the whole design rests on -- a Qualytics release adding a rule type should
break CI here, not quietly degrade a customer's check in production.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.qualytics.assertion import (
    MAX_LIST_PARAMETER_ITEMS,
    RULE_MAPPINGS,
    AssertionMapper,
)
from datahub.ingestion.source.qualytics.models import QualityCheck
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    CustomAssertionInfoClass,
    DatasetAssertionScopeClass,
)

SPEC = Path(__file__).resolve().parent / "fixtures" / "openapi.json"
URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES.PUBLIC.ORDERS,PROD)"


def _spec_rule_types() -> set[str]:
    spec = json.loads(SPEC.read_text())
    return set(spec["components"]["schemas"]["RuleType"]["enum"])


def _mapper(
    instance: str | None = "acme",
) -> tuple[AssertionMapper, QualyticsSourceReport]:
    report = QualyticsSourceReport()
    return AssertionMapper(instance, report), report


def _custom(info: AssertionInfoClass) -> CustomAssertionInfoClass:
    assert info.customAssertion is not None
    return info.customAssertion


def _params(info: AssertionInfoClass) -> AssertionStdParametersClass:
    params = _custom(info).parameters
    assert params is not None
    return params


def _value(info: AssertionInfoClass) -> AssertionStdParameterClass:
    value = _params(info).value
    assert value is not None
    return value


def _native(info: AssertionInfoClass) -> dict[str, str]:
    return _custom(info).nativeParameters or {}


def _check(**overrides: Any) -> QualityCheck:
    return QualityCheck.model_validate({"id": 1, "rule_type": "notNull", **overrides})


# --- the coverage tripwire ---------------------------------------------------------


def test_every_rule_type_in_the_spec_is_mapped() -> None:
    # If this fails after refreshing tests/unit/qualytics/fixtures/openapi.json, Qualytics has added
    # a rule type. Add it to RULE_MAPPINGS -- do not delete this test.
    missing = _spec_rule_types() - set(RULE_MAPPINGS)

    assert missing == set(), (
        f"{len(missing)} Qualytics rule type(s) have no DataHub mapping: "
        f"{sorted(missing)}. Add them to RULE_MAPPINGS in assertion.py."
    )


def test_no_mapping_exists_for_a_rule_type_the_spec_does_not_have() -> None:
    # Catches typos and rules removed upstream, which would otherwise sit in the
    # table forever looking like coverage.
    stale = set(RULE_MAPPINGS) - _spec_rule_types()

    assert stale == set(), f"RULE_MAPPINGS has entries not in the spec: {sorted(stale)}"


# --- representative mappings -------------------------------------------------------


@pytest.mark.parametrize(
    ("rule_type", "scope", "operator", "aggregation"),
    [
        (
            "notNull",
            DatasetAssertionScopeClass.DATASET_COLUMN,
            AssertionStdOperatorClass.NOT_NULL,
            AssertionStdAggregationClass.IDENTITY,
        ),
        (
            "unique",
            DatasetAssertionScopeClass.DATASET_COLUMN,
            AssertionStdOperatorClass.EQUAL_TO,
            AssertionStdAggregationClass.UNIQUE_PROPORTION,
        ),
        (
            "volumetric",
            DatasetAssertionScopeClass.DATASET_ROWS,
            AssertionStdOperatorClass.BETWEEN,
            AssertionStdAggregationClass.ROW_COUNT,
        ),
        (
            "fieldCount",
            DatasetAssertionScopeClass.DATASET_SCHEMA,
            AssertionStdOperatorClass.EQUAL_TO,
            AssertionStdAggregationClass.COLUMN_COUNT,
        ),
        (
            "matchesPattern",
            DatasetAssertionScopeClass.DATASET_COLUMN,
            AssertionStdOperatorClass.REGEX_MATCH,
            AssertionStdAggregationClass.IDENTITY,
        ),
        (
            "expectedValues",
            DatasetAssertionScopeClass.DATASET_COLUMN,
            AssertionStdOperatorClass.IN,
            AssertionStdAggregationClass.IDENTITY,
        ),
        (
            "notExistsIn",
            DatasetAssertionScopeClass.DATASET_COLUMN,
            AssertionStdOperatorClass.NOT_IN,
            AssertionStdAggregationClass.IDENTITY,
        ),
    ],
)
def test_representative_rules_project_onto_the_expected_vocabulary(
    rule_type: str, scope: str, operator: str, aggregation: str
) -> None:
    mapper, _ = _mapper()

    info = mapper.assertion_info(_check(rule_type=rule_type), URN)

    custom = _custom(info)
    assert custom.scope == scope
    assert custom.operator == operator
    assert custom.aggregation == aggregation


@pytest.mark.parametrize(
    "rule_type",
    ["requiredValues", "isCreditCard", "isAddress", "satisfiesExpression", "dataDiff"],
)
def test_rules_with_no_datahub_equivalent_use_the_native_sentinel(
    rule_type: str,
) -> None:
    # Not a gap: DataHub has no operator for "every listed value must appear" or for
    # a cross-dataset diff. The sentinel is how the UI knows to render the native
    # description rather than an empty operator.
    mapper, report = _mapper()

    info = mapper.assertion_info(_check(rule_type=rule_type), URN)

    custom = _custom(info)
    assert AssertionStdOperatorClass._NATIVE_ in (custom.operator, custom.aggregation)
    # These are mapped, so they must NOT be reported as unmapped.
    assert list(report.unmapped_rule_types) == []


# --- the unknown-rule fallback -----------------------------------------------------


def test_an_unknown_rule_type_still_produces_an_assertion() -> None:
    # The core promise: a rule type newer than this build is degraded, never dropped.
    mapper, report = _mapper()

    first, _ = mapper.workunits(
        _check(rule_type="inventedNextQuarter", properties={"threshold": 5}), URN
    )
    assert isinstance(first.metadata, MetadataChangeProposalWrapper)
    info = first.metadata.aspect
    assert isinstance(info, AssertionInfoClass)

    custom = _custom(info)
    assert custom.nativeType == "inventedNextQuarter"
    assert custom.scope == DatasetAssertionScopeClass.UNKNOWN
    assert custom.operator == AssertionStdOperatorClass._NATIVE_
    # The check's own configuration survives even though we cannot interpret it.
    assert custom.nativeParameters == {"threshold": "5"}
    assert report.assertions_emitted == 1


def test_an_unknown_rule_type_is_reported_so_the_gap_is_visible() -> None:
    mapper, report = _mapper()

    mapper.assertion_info(_check(rule_type="inventedNextQuarter"), URN)

    assert list(report.unmapped_rule_types) == ["inventedNextQuarter"]
    assert any("rule type" in str(w).lower() for w in report.warnings)


# --- identity and namespacing ------------------------------------------------------


def test_assertion_urns_are_stable_across_runs() -> None:
    # Stateful ingestion soft-deletes anything not re-emitted, so an unstable URN
    # would delete and recreate every assertion on every run.
    first, _ = _mapper()
    second, _ = _mapper()

    assert first.assertion_urn(42) == second.assertion_urn(42)


def test_assertion_urns_differ_between_platform_instances() -> None:
    # The single-tenancy hazard. Qualytics check ids are per-deployment Postgres
    # sequences, so check 42 in two deployments is two unrelated checks. Without the
    # instance in the key they collapse onto one URN and overwrite each other.
    acme, _ = _mapper("acme")
    other, _ = _mapper("other")

    assert acme.assertion_urn(42) != other.assertion_urn(42)


def test_different_checks_get_different_urns() -> None:
    mapper, _ = _mapper()

    assert mapper.assertion_urn(1) != mapper.assertion_urn(2)


# --- payload -----------------------------------------------------------------------


def test_the_assertion_targets_the_resolved_dataset_and_its_fields() -> None:
    mapper, _ = _mapper()
    field_urn = f"urn:li:schemaField:({URN},amount)"

    info = mapper.assertion_info(
        _check(rule_type="notNull"), URN, field_urns=[field_urn]
    )

    custom = _custom(info)
    assert custom.entity == URN
    assert custom.field == field_urn
    assert custom.fields == [field_urn]


def test_bounds_are_extracted_into_typed_parameters() -> None:
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(rule_type="between", properties={"min": 0, "max": 100}), URN
    )

    params = _params(info)
    assert params.minValue is not None and params.maxValue is not None
    assert params.minValue.value == "0"
    assert params.minValue.type == AssertionStdParameterTypeClass.NUMBER
    assert params.maxValue.value == "100"


def test_a_list_bound_is_typed_as_a_list_not_stringified_python() -> None:
    # repr() of a Python list would leak brackets and quotes into the UI.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(rule_type="expectedValues", properties={"list": ["US", "CA"]}), URN
    )

    value = _value(info)
    assert value.value == "US,CA"
    assert value.type == AssertionStdParameterTypeClass.LIST


def test_a_boolean_property_is_not_reported_as_the_number_one() -> None:
    # bool is a subclass of int in Python, so an unguarded numeric branch renders
    # True as "True" typed NUMBER.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(rule_type="isType", properties={"value": True}), URN
    )

    value = _value(info)
    assert value.type == AssertionStdParameterTypeClass.STRING


def test_checks_without_bounds_omit_parameters_entirely() -> None:
    mapper, _ = _mapper()

    info = mapper.assertion_info(_check(rule_type="notNull"), URN)

    assert _custom(info).parameters is None


def test_an_expression_check_carries_its_sql_in_logic() -> None:
    # Without this the assertion says "satisfiesExpression" and nothing about what.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(
            rule_type="satisfiesExpression", properties={"expression": "amount > 0"}
        ),
        URN,
    )

    assert _custom(info).logic == "amount > 0"


def test_a_check_without_a_description_gets_a_readable_fallback() -> None:
    # Otherwise the UI renders its generic "A custom externally reported Assertion"
    # placeholder, which identifies nothing.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(rule_type="notNull", fields=[{"id": 1, "name": "amount"}]), URN
    )

    assert info.description == "notNull on amount"


def test_an_explicit_description_is_preferred_over_the_fallback() -> None:
    mapper, _ = _mapper()

    info = mapper.assertion_info(_check(description="Amount must be present"), URN)

    assert info.description == "Amount must be present"


def test_custom_properties_carry_the_qualytics_identity_and_provenance() -> None:
    # qualytics_check_id is what lets an operator find the check back in Qualytics,
    # and `inferred` distinguishes a machine-suggested check from a human-authored one.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(id=77, rule_type="notNull", inferred=True, coverage=0.98), URN
    )

    props = info.customProperties
    assert props["qualytics_check_id"] == "77"
    assert props["rule_type"] == "notNull"
    assert props["inferred"] == "True"
    assert props["coverage"] == "0.98"


def test_native_parameters_drop_nulls_and_stringify_the_rest() -> None:
    # nativeParameters is map[string, string]; a None value would fail serialization.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(
            rule_type="between", properties={"min": 0, "max": None, "inclusive": True}
        ),
        URN,
    )

    native = _native(info)
    assert native == {"min": "0", "inclusive": "True"}


def test_a_huge_value_list_is_capped_and_says_how_big_it_was() -> None:
    # An expectedValues check can carry thousands of members, which would land twice
    # in the aspect on every run. Capped like the profile histograms, with the true
    # size alongside so the partial list is not mistaken for the whole.
    mapper, report = _mapper()
    members = [f"v{i}" for i in range(MAX_LIST_PARAMETER_ITEMS + 50)]

    info = mapper.assertion_info(
        _check(rule_type="expectedValues", properties={"list": members}), URN
    )

    assert _value(info).value.split(",") == members[:MAX_LIST_PARAMETER_ITEMS]
    assert _native(info)["list_total_count"] == str(len(members))
    assert report.assertion_parameters_truncated == 1


def test_a_list_within_the_cap_is_left_alone() -> None:
    mapper, report = _mapper()

    info = mapper.assertion_info(
        _check(rule_type="expectedValues", properties={"list": ["US", "CA"]}), URN
    )

    assert "list_total_count" not in _native(info)
    assert report.assertion_parameters_truncated == 0


def test_a_non_string_expression_still_produces_a_serialisable_assertion() -> None:
    # `logic` is the one property that lands in a typed aspect field. A number there
    # passed the mapper and failed later, at serialisation, taking the batch with it.
    mapper, _ = _mapper()

    info = mapper.assertion_info(
        _check(rule_type="satisfiesExpression", properties={"expression": 42}), URN
    )

    assert _custom(info).logic == "42"
    assert info.validate()
