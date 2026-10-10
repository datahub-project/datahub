"""Check that no model requires a field the Qualytics spec treats as optional.

A model stricter than the spec is a silent-data-loss bug with a long fuse. Pydantic
raises on the missing field, the object is skipped, and the run still exits 0 -- so the
only symptom is metadata that never appears.

This exists because it happened. ``QualityCheckField`` required ``id``, which the
``/quality-checks`` listing does supply, so every fixture and every mock passed. But the
copy embedded in ``Anomaly.failed_checks[].quality_check.fields`` is the spec's
``FieldStub``, whose sole required property is ``name``. Against a live deployment that
made 255 of every 300 anomalies unparseable, and since ``Anomaly.model_validate`` runs
inside the per-container walk, each failure cost that container its whole anomaly set.
Assertions looked healthy while assertion *results* quietly collapsed.

The spec had said so from the beginning. Nothing was reading it.

The check is deliberately one-directional. Being *laxer* than the spec is the documented
house style -- see the models module docstring on open-ended enums and ``extra="ignore"``
-- so only over-strictness fails here.
"""

import json
from pathlib import Path
from typing import Annotated, Any

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from datahub.ingestion.source.qualytics import models

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "openapi.json"

# (our model, the spec schema the endpoint actually returns). Resolved from the 200
# response of each entry in CONSUMED_PATHS -- see test_api_paths.py for the paths
# themselves. Add a pair whenever a model starts backing a new endpoint.
MODEL_TO_SCHEMA: list[tuple[str, str]] = [
    ("QualityCheck", "GetQualityCheckListing"),
    ("Anomaly", "GetAnomalyListing"),
    ("FieldProfile", "GetFieldProfileListing"),
    ("ContainerProfile", "GetContainerProfile"),
    ("TableContainer", "GetTableContainer"),
    ("FileContainer", "GetFileContainer"),
    ("JdbcDatastore", "GetJdbcDatastore"),
    ("DfsDatastore", "GetDfsDatastore"),
    ("NativeDatastore", "GetNativeDatastore"),
    ("QualityCheckField", "FieldStub"),
    ("HistogramBucket", "HistogramBucket"),
    # The shapes embedded in an anomaly's failed_checks. Different schemas from the
    # listings above, and the path the live anomaly failures came through.
    ("FailedCheck", "FailedCheckListing"),
    ("QualityCheck", "QualityCheckListing"),
]


@pytest.fixture(scope="module")
def schemas() -> dict[str, Any]:
    # Fail, never skip: a skipped contract test reports green, and a fixture lost in a
    # move would quietly switch off the API-churn tripwire.
    assert FIXTURE.exists(), (
        f"{FIXTURE} is missing; run tests/unit/qualytics/capture_openapi.py"
    )
    spec: dict[str, Any] = json.loads(FIXTURE.read_text())
    loaded: dict[str, Any] = spec["components"]["schemas"]
    return loaded


def _required_wire_names(model: type[BaseModel]) -> set[str]:
    # Alias, not attribute name: `schema_` is sent over the wire as `schema`, and it is
    # the wire name the spec's `required` list refers to.
    return {
        (f.alias or name) for name, f in model.model_fields.items() if f.is_required()
    }


@pytest.mark.parametrize(("model_name", "schema_name"), MODEL_TO_SCHEMA)
def test_model_is_not_stricter_than_spec(
    schemas: dict[str, Any], model_name: str, schema_name: str
) -> None:
    model = getattr(models, model_name)
    schema = schemas.get(schema_name)
    assert schema is not None, (
        f"{schema_name} is not in the captured spec. Either it was renamed upstream or "
        f"MODEL_TO_SCHEMA is stale -- read the spec, do not guess."
    )

    properties = set((schema.get("properties") or {}).keys())
    spec_required = set(schema.get("required", []))
    over_strict = {
        field
        for field in _required_wire_names(model)
        if field not in properties or field not in spec_required
    }

    assert not over_strict, (
        f"{model_name} requires {sorted(over_strict)}, which {schema_name} does not "
        f"guarantee. A payload without them is dropped at parse time and the run still "
        f"reports success. Give each a default instead."
    )


def _nullable(prop: dict[str, Any]) -> bool:
    return prop.get("nullable") is True or any(
        option.get("type") == "null" for option in prop.get("anyOf", [])
    )


@pytest.mark.parametrize(("model_name", "schema_name"), MODEL_TO_SCHEMA)
def test_model_accepts_null_wherever_spec_allows_it(
    schemas: dict[str, Any], model_name: str, schema_name: str
) -> None:
    # The required-field check above cannot see this: a list field with a default is
    # optional, yet still rejects an explicit null. That is how a datastore with
    # `global_tags: null`, or a check with `fields: null`, was dropped whole.
    model = getattr(models, model_name)
    properties = schemas[schema_name].get("properties") or {}

    rejects_null = []
    for name, field in model.model_fields.items():
        wire = field.alias or name
        if wire not in properties or not _nullable(properties[wire]):
            continue
        # Rebuilt from the annotation plus its metadata, so a BeforeValidator such as
        # NullableList's is part of what gets tested. Tuple form for Python 3.10.
        annotation = (
            Annotated[(field.annotation, *field.metadata)]
            if field.metadata
            else field.annotation
        )
        adapter: TypeAdapter[Any] = TypeAdapter(annotation)
        try:
            adapter.validate_python(None)
        except ValidationError:
            rejects_null.append(wire)

    assert not rejects_null, (
        f"{model_name} rejects null for {rejects_null}, which {schema_name} allows. "
        f"Make the field Optional, or a NullableList if it is a list."
    )
