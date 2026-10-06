import json
from typing import Any

import pytest

from datahub.emitter.mcp_builder import (
    StructuredPropertyWriteMode,
    add_structured_properties_to_entity_wu,
)
from datahub.emitter.mcp_patch_builder import UNIT_SEPARATOR
from datahub.metadata.schema_classes import (
    MetadataAttributionClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.metadata.urns import StructuredPropertyUrn
from datahub.specific.dataset import DatasetPatchBuilder

ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
PROP_A = StructuredPropertyUrn.from_string(
    "urn:li:structuredProperty:io.acryl.classification"
)
PROP_B = StructuredPropertyUrn.from_string(
    "urn:li:structuredProperty:io.acryl.retention"
)


def _parse_mcp_aspect(mcp_raw: Any) -> dict:  # type: ignore[return]
    return json.loads(mcp_raw.aspect.value)


def test_default_emits_upsert() -> None:
    """No write_mode → backward-compatible UPSERT (full-aspect replace)."""
    wus = list(
        add_structured_properties_to_entity_wu(ENTITY_URN, {PROP_A: "sensitive"})
    )
    assert wus
    for wu in wus:
        mcp = wu.get_metadata()["metadata"]
        assert mcp.changeType == "UPSERT"


def test_patch_mode_emits_patch() -> None:
    """Explicit PATCH opt-in must use changeType PATCH, never UPSERT."""
    wus = list(
        add_structured_properties_to_entity_wu(
            ENTITY_URN,
            {PROP_A: "sensitive"},
            write_mode=StructuredPropertyWriteMode.PATCH,
        )
    )
    assert wus
    for wu in wus:
        mcp = wu.get_metadata()["metadata"]
        assert mcp.changeType == "PATCH"


def test_patch_targets_only_specified_property() -> None:
    """The patch envelope must contain an op for the requested property only.

    Verifying this at the JSON Patch level proves cross-source persistence: if the
    backend receives a patch that only mentions PROP_A, a concurrently-stored PROP_B
    (set via UI or another pipeline) is untouched.
    """
    wus = list(
        add_structured_properties_to_entity_wu(
            ENTITY_URN,
            {PROP_A: "sensitive"},
            write_mode=StructuredPropertyWriteMode.PATCH,
        )
    )
    assert wus

    aspect_json = _parse_mcp_aspect(wus[0].get_metadata()["metadata"])
    patch_ops = aspect_json.get("patch", aspect_json)
    if isinstance(patch_ops, dict):
        patch_ops = patch_ops.get("patch", [])

    assert len(patch_ops) == 1, f"Expected exactly one patch op, got {len(patch_ops)}"

    path = patch_ops[0]["path"]
    assert PROP_A.urn() in path
    assert PROP_B.urn() not in path


def test_multiple_properties_emit_separate_ops() -> None:
    """Each property in the dict gets its own patch op (or workunit) — not a full replace."""
    wus = list(
        add_structured_properties_to_entity_wu(
            ENTITY_URN,
            {PROP_A: "sensitive", PROP_B: "7 years"},
            write_mode=StructuredPropertyWriteMode.PATCH,
        )
    )
    assert wus

    all_patch_ops = []
    for wu in wus:
        aspect_json = _parse_mcp_aspect(wu.get_metadata()["metadata"])
        ops = aspect_json.get("patch", aspect_json)
        if isinstance(ops, dict):
            ops = ops.get("patch", [])
        all_patch_ops.extend(ops)

    paths = [op["path"] for op in all_patch_ops]
    assert any(PROP_A.urn() in p for p in paths)
    assert any(PROP_B.urn() in p for p in paths)


@pytest.mark.parametrize(
    "value",
    ["string-value", "42", ""],
    ids=["string", "numeric-string", "empty"],
)
def test_patch_value_is_set(value: str) -> None:
    """The patch op for add must carry the supplied value."""
    wus = list(
        add_structured_properties_to_entity_wu(
            ENTITY_URN,
            {PROP_A: value},
            write_mode=StructuredPropertyWriteMode.PATCH,
        )
    )
    assert wus

    aspect_json = _parse_mcp_aspect(wus[0].get_metadata()["metadata"])
    ops = aspect_json.get("patch", aspect_json)
    if isinstance(ops, dict):
        ops = ops.get("patch", [])

    add_ops = [op for op in ops if op.get("op") == "add"]
    assert add_ops
    op_value = add_ops[0]["value"]
    assert op_value is not None


def _assignment(
    property_urn: str, values: list, source: str = ""
) -> StructuredPropertyValueAssignmentClass:
    return StructuredPropertyValueAssignmentClass(
        propertyUrn=property_urn,
        values=values,
        attribution=(
            MetadataAttributionClass(
                source=source, time=0, actor="urn:li:corpuser:__datahub_system"
            )
            if source
            else None
        ),
    )


def test_upsert_manual_keys_on_property_urn_alone() -> None:
    """upsert_structured_property_manual must patch /properties/<urn> as a plain JSON
    Patch (no arrayPrimaryKeys), so the backend's default propertyUrn key replaces any
    existing entry for the property, whatever its attribution source."""
    source = "urn:li:dataHubAction:test"
    builder = DatasetPatchBuilder(ENTITY_URN).upsert_structured_property_manual(
        _assignment(str(PROP_A), ["sensitive"], source)
    )
    mcps = list(builder.build())
    assert len(mcps) == 1
    ops = _parse_mcp_aspect(mcps[0])
    assert ops == [
        {
            "op": "add",
            "path": f"/properties/{PROP_A}",
            "value": {
                "propertyUrn": str(PROP_A),
                "values": [{"string": "sensitive"}],
                "attribution": {
                    "time": 0,
                    "actor": "urn:li:corpuser:__datahub_system",
                    "source": source,
                    "sourceDetail": {},
                },
            },
        }
    ]


def test_upsert_manual_differs_from_set_manual() -> None:
    """set_structured_property_manual keys on (propertyUrn, attribution source) via
    arrayPrimaryKeys; the upsert must not, or it would append a second entry."""
    source = "urn:li:dataHubAction:test"
    set_payload = _parse_mcp_aspect(
        list(
            DatasetPatchBuilder(ENTITY_URN)
            .set_structured_property_manual(_assignment(str(PROP_A), ["x"], source))
            .build()
        )[0]
    )
    assert set_payload["arrayPrimaryKeys"] == {
        "properties": ["propertyUrn", f"attribution{UNIT_SEPARATOR}source"]
    }
    assert set_payload["patch"][0]["path"] == f"/properties/{PROP_A}/{source}"

    upsert_payload = _parse_mcp_aspect(
        list(
            DatasetPatchBuilder(ENTITY_URN)
            .upsert_structured_property_manual(_assignment(str(PROP_A), ["x"], source))
            .build()
        )[0]
    )
    assert isinstance(upsert_payload, list)
    assert upsert_payload[0]["path"] == f"/properties/{PROP_A}"


def test_upsert_manual_multiple_properties_and_no_attribution() -> None:
    builder = (
        DatasetPatchBuilder(ENTITY_URN)
        .upsert_structured_property_manual(_assignment(str(PROP_A), ["a"]))
        .upsert_structured_property_manual(_assignment(str(PROP_B), ["30d"]))
    )
    mcps = list(builder.build())
    assert len(mcps) == 1
    ops = _parse_mcp_aspect(mcps[0])
    assert [op["path"] for op in ops] == [
        f"/properties/{PROP_A}",
        f"/properties/{PROP_B}",
    ]
    assert all("attribution" not in op["value"] for op in ops)
