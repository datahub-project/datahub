import json
from typing import Any

import pytest

from datahub.emitter.mcp_builder import (
    StructuredPropertyWriteMode,
    add_structured_properties_to_entity_wu,
)
from datahub.metadata.urns import StructuredPropertyUrn

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
