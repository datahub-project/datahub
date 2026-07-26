"""Golden + behavioral tests over the official Bitol ODCS example corpus.

Every file under `bitol_examples/` is vendored verbatim from the
open-data-contract-standard repository at tag v3.1.0 (docs/examples/...).
Unlike the hand-authored fixtures, none of these were written against this
source's implementation, so they guard the translation against real-world
ODCS shapes: spec-exact quality routing, non-relational servers, the team
object form, and schema-only excerpts.

Several upstream examples do not validate against Bitol's own JSON Schemas
(e.g. `logicalType: timestamp` is not in the v3.0.2 enum, and
`column-completeness` uses the v3.1 `metric` key with a v3.0.2 apiVersion).
The default lenient validation ingests them anyway — which is exactly the
real-world behavior the default exists for — so these tests run with
`strict_validation: False` and tolerate only the lenient-validation notice.
"""

import json
import pathlib
from typing import Any, Dict, List, Optional, Tuple

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-15 12:00:00"

# The only warning the official corpus is allowed to produce: upstream
# examples that fail Bitol's own JSON Schema get the lenient-validation
# notice. Anything else (unknown fields, routing skips) is a regression.
_ALLOWED_WARNING = "ODCS contract has JSON Schema validation issues"

# Examples that materialize logical datasets and have per-file goldens.
# postgresql-adventureworks (68 tables) is covered behaviorally below
# instead of via a multi-megabyte golden.
_GOLDEN_EXAMPLES = [
    "data-types__all-data-types",
    "fundamentals__table-column-description",
    "quality__column-accuracy",
    "quality__column-completeness",
    "quality__column-custom",
    "quality__column-validity",
    "schema__all-schema-types",
    "schema__kafka-schema",
    "schema__kafka-schemaregistry",
    "schema__table-column",
    "schema__table-columns-with-partition",
    "server__kafka-server",
]

# Excerpt examples with an empty/absent `schema` section: nothing to
# materialize, so the source must skip them gracefully (no crash, no
# unknown-field warnings) rather than emit empty shells.
_SCHEMALESS_EXCERPTS = [
    "roles__service-and-operational-roles",
    "server__azure-server",
    "sla__database-table-sla",
    "stakeholders__basic-four-dpo",
]


def _examples_dir(pytestconfig: pytest.Config) -> pathlib.Path:
    return pytestconfig.rootpath / "tests/integration/odcs/bitol_examples"


def _run_example(
    pytestconfig: pytest.Config,
    tmp_path: pathlib.Path,
    fixture: str,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Any, pathlib.Path]:
    """Run one vendored example through a full pipeline (file sink).

    Returns (mces, source_report, output_path).
    """
    output_path = tmp_path / f"{fixture}_mces.json"
    config: Dict[str, Any] = {
        "path": str(_examples_dir(pytestconfig) / f"{fixture}.odcs.yaml"),
        "strict_validation": False,
    }
    if extra_config:
        config.update(extra_config)
    pipeline = Pipeline.create(
        {
            "run_id": f"test-odcs-bitol-{fixture}",
            "source": {"type": "odcs", "config": config},
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    report = pipeline.source.get_report()
    return json.loads(output_path.read_text()), report, output_path


def _assert_only_lenient_validation_warnings(report: Any) -> None:
    for warning in report.warnings:
        assert str(getattr(warning, "title", warning)) == _ALLOWED_WARNING, (
            f"official Bitol example produced an unexpected warning: {warning}"
        )
    assert report.unknown_fields_count == 0


def _quality_assertions(mces: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """assertionInfo aspects minus the DATA_SCHEMA (schema-compliance) ones."""
    return [
        m["aspect"]["json"]
        for m in mces
        if m.get("aspectName") == "assertionInfo"
        and m["aspect"]["json"].get("type") != "DATA_SCHEMA"
    ]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.parametrize("fixture", _GOLDEN_EXAMPLES)
def test_bitol_example_golden(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path, fixture: str
) -> None:
    mces, report, output_path = _run_example(pytestconfig, tmp_path, fixture)
    _assert_only_lenient_validation_warnings(report)
    assert report.contracts_skipped == 0
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=_examples_dir(pytestconfig) / f"{fixture}_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.parametrize("fixture", _SCHEMALESS_EXCERPTS)
def test_bitol_schemaless_excerpt_skips_gracefully(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path, fixture: str
) -> None:
    """Excerpts with `schema: []` (roles / SLA / stakeholders / azure server)
    have nothing to materialize: the contract is skipped without emitting
    dataset shells and without unknown-field warnings — their roles / SLA /
    team-object keys are all recognized as spec-valid."""
    mces, report, _ = _run_example(pytestconfig, tmp_path, fixture)
    # A single schemaless contract emits nothing, so the pipeline's
    # "No metadata was produced" notice is expected here alongside the
    # lenient-validation one.
    allowed = {_ALLOWED_WARNING, "No metadata was produced by the source"}
    for warning in report.warnings:
        assert str(getattr(warning, "title", warning)) in allowed, (
            f"official Bitol example produced an unexpected warning: {warning}"
        )
    assert report.unknown_fields_count == 0
    assert report.contracts_skipped == 1
    assert not [m for m in mces if m.get("entityType") == "dataset"]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_column_accuracy_produces_sql_assertion(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """`type: sql` + `query` + `mustBe: 0` must become a SQL assertion whose
    metric (the query result) is compared EQUAL_TO 0. `dimension` plays no
    functional role — it is preserved as provenance only."""
    mces, _, _ = _run_example(pytestconfig, tmp_path, "quality__column-accuracy")
    quality = _quality_assertions(mces)
    assert len(quality) == 1
    info = quality[0]
    assert info["type"] == "SQL"
    sql = info["sqlAssertion"]
    assert sql["type"] == "METRIC"
    assert sql["operator"] == "EQUAL_TO"
    assert sql["parameters"]["value"] == {"value": "0", "type": "NUMBER"}
    assert (
        sql["statement"]
        == "select count(*) from Air_Quality where DataValue <= 0 or DataValue >= 500"
    )
    assert "urn:li:dataPlatform:odcs" in sql["entity"]
    props = info["customProperties"]
    assert props["odcs.rule.dimension"] == "accuracy"
    assert props["odcs.rule.severity"] == "error"
    assert props["odcs.rule.businessImpact"] == "operational"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_column_completeness_maps_to_not_null(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """`metric: nullValues` + `mustBe: 0` = "no nulls tolerated" — a typed
    NOT_NULL field-values assertion with a zero fail threshold."""
    mces, _, _ = _run_example(pytestconfig, tmp_path, "quality__column-completeness")
    quality = _quality_assertions(mces)
    assert len(quality) == 1
    fva = quality[0]["fieldAssertion"]["fieldValuesAssertion"]
    assert fva["field"]["path"] == "UniqueID"
    assert fva["operator"] == "NOT_NULL"
    assert fva["failThreshold"] == {"type": "COUNT", "value": 0}


@time_machine.travel(FROZEN_TIME, tick=False)
def test_column_validity_maps_to_in_with_percent_threshold(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """`metric: invalidValues` with `arguments.validValues` routes to an IN
    assertion; the integral `mustBeLessOrEqualTo: 5` with `unit: percent` is
    exactly representable as a PERCENTAGE fail threshold."""
    mces, _, _ = _run_example(pytestconfig, tmp_path, "quality__column-validity")
    quality = _quality_assertions(mces)
    assert len(quality) == 1
    fva = quality[0]["fieldAssertion"]["fieldValuesAssertion"]
    assert fva["operator"] == "IN"
    assert json.loads(fva["parameters"]["value"]["value"]) == ["", None, "n/a"]
    assert fva["failThreshold"] == {"type": "PERCENTAGE", "value": 5}


@time_machine.travel(FROZEN_TIME, tick=False)
def test_column_custom_preserves_engine_and_implementation(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """`type: custom` rules keep the vendor engine as the assertion type and
    the implementation verbatim as `logic` — for both the string and the
    dict `implementation` forms the example exercises."""
    mces, _, _ = _run_example(pytestconfig, tmp_path, "quality__column-custom")
    quality = _quality_assertions(mces)
    assert len(quality) == 2
    for info in quality:
        custom = info["customAssertion"]
        assert custom["type"] == "soda"
        assert "duplicate_percent" in custom["logic"]


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.parametrize("fixture", ["schema__kafka-schema", "server__kafka-server"])
def test_kafka_examples_stay_unbound(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path, fixture: str
) -> None:
    """kafka has no platform mapping: the logical dataset is still emitted,
    but no logicalParent link is derived."""
    mces, report, _ = _run_example(pytestconfig, tmp_path, fixture)
    assert not [m for m in mces if m.get("aspectName") == "logicalParent"]
    logical = [
        m
        for m in mces
        if m.get("aspectName") == "datasetProperties"
        and "urn:li:dataPlatform:odcs" in m["entityUrn"]
    ]
    assert logical
    assert report.unmappable_servers == 1


@time_machine.travel(FROZEN_TIME, tick=False)
def test_adventureworks_contract_fans_out(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """The upstream AdventureWorks contract (v3.0.0, 68 tables, no servers):
    every schema entry becomes its own logical dataset with schema metadata
    and a schema-compliance assertion, with no cross-table bleed. Covered
    behaviorally instead of via a very large golden file."""
    mces, report, _ = _run_example(
        pytestconfig, tmp_path, "all__postgresql-adventureworks-contract"
    )
    _assert_only_lenient_validation_warnings(report)
    assert report.contracts_skipped == 0

    logical_urns = {
        m["entityUrn"]
        for m in mces
        if m.get("aspectName") == "datasetProperties"
        and "urn:li:dataPlatform:odcs" in m["entityUrn"]
    }
    assert len(logical_urns) == 68
    schema_aspects = [m for m in mces if m.get("aspectName") == "schemaMetadata"]
    assert len(schema_aspects) == 68
    schema_assertions = [
        m
        for m in mces
        if m.get("aspectName") == "assertionInfo"
        and m["aspect"]["json"].get("type") == "DATA_SCHEMA"
    ]
    assert len(schema_assertions) == 68

    # Spot-check one table end-to-end: department has 4 columns, with
    # departmentid the primary key.
    department = next(m for m in schema_aspects if ".department," in m["entityUrn"])
    fields = {f["fieldPath"]: f for f in department["aspect"]["json"]["fields"]}
    assert set(fields) == {"departmentid", "name", "groupname", "modifieddate"}
    assert fields["departmentid"]["isPartOfKey"]
