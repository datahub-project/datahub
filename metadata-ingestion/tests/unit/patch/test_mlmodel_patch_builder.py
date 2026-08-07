import json
from typing import Any, List

import pytest

from datahub.metadata.schema_classes import (
    MLModelPropertiesClass,
    OwnerClass,
    OwnershipTypeClass,
    TagAssociationClass,
    VersionTagClass,
)
from datahub.specific.mlmodel import MLModelPatchBuilder

MODEL_URN = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,fraud_detector_v3,PROD)"
FEATURE_A = "urn:li:mlFeature:(fraud_detection,transaction_velocity_7d)"
FEATURE_B = "urn:li:mlFeature:(fraud_detection,customer_risk_score)"
PROP_A = "urn:li:structuredProperty:io.acryl.risk_verdict"
PROP_B = "urn:li:structuredProperty:io.acryl.last_checked"


def _patch_ops(mcp: Any) -> List[dict]:
    """Decode the JSON Patch operations carried by a built proposal."""
    value = mcp.aspect.value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    payload = json.loads(value)
    return payload["patch"] if isinstance(payload, dict) else payload


def test_builder_emits_patch_not_upsert() -> None:
    """The reason this builder needs to exist.

    Without it, mlModel aspects are UPSERT-only, and an UPSERT on
    structuredProperties replaces the whole aspect — discarding any property
    the writer did not know about.
    """
    mcps = list(
        MLModelPatchBuilder(MODEL_URN).set_structured_property(PROP_A, "BLOCK").build()
    )

    assert mcps
    for mcp in mcps:
        assert mcp.changeType == "PATCH"
        assert mcp.entityUrn == MODEL_URN


def test_entity_type_is_inferred_from_the_urn() -> None:
    """MetadataPatchProposal is entity-agnostic; mlModel needs no special casing."""
    mcps = list(
        MLModelPatchBuilder(MODEL_URN).set_structured_property(PROP_A, "BLOCK").build()
    )

    assert mcps[0].entityType == "mlModel"


def test_independent_properties_get_their_own_ops() -> None:
    """Two writers, two paths, no read-modify-write in between.

    If the backend receives a patch mentioning only PROP_A, a concurrently
    stored PROP_B — set via the UI or another pipeline — is untouched.
    """
    builder = MLModelPatchBuilder(MODEL_URN)
    builder.set_structured_property(PROP_A, "BLOCK")
    builder.set_structured_property(PROP_B, "2024-01-01T00:00:00Z")

    ops = [op for mcp in builder.build() for op in _patch_ops(mcp)]
    paths = [op["path"] for op in ops]

    assert len(ops) == 2
    assert len(set(paths)) == 2, f"operations collided on one path: {paths}"
    for op in ops:
        assert op["op"] == "add"
        assert op["path"] not in ("", "/", "/properties")


def test_structured_properties_target_the_right_aspect() -> None:
    mcps = list(
        MLModelPatchBuilder(MODEL_URN).set_structured_property(PROP_A, "BLOCK").build()
    )

    assert mcps[0].aspectName == "structuredProperties"


@pytest.mark.parametrize(
    "setter, args, path, expected",
    [
        ("set_name", ("fraud_detector_v3",), "name", "fraud_detector_v3"),
        ("set_description", ("Card fraud scorer.",), "description", "Card fraud scorer."),
        ("set_type", ("gradient-boosted-trees",), "type", "gradient-boosted-trees"),
        (
            "set_external_url",
            ("https://example.com/models/fraud",),
            "externalUrl",
            "https://example.com/models/fraud",
        ),
    ],
)
def test_property_setters_patch_the_expected_path(
    setter: str, args: tuple, path: str, expected: Any
) -> None:
    builder = MLModelPatchBuilder(MODEL_URN)
    getattr(builder, setter)(*args)

    mcps = list(builder.build())
    ops = _patch_ops(mcps[0])

    assert mcps[0].aspectName == MLModelPropertiesClass.ASPECT_NAME
    assert len(ops) == 1
    assert ops[0]["path"].strip("/") == path
    assert ops[0]["value"] == expected


def test_version_is_patched_as_a_version_tag() -> None:
    builder = MLModelPatchBuilder(MODEL_URN)
    builder.set_version(VersionTagClass(versionTag="3.1.0"))

    ops = _patch_ops(list(builder.build())[0])

    assert len(ops) == 1
    assert ops[0]["path"].strip("/") == "version"


def test_ml_features_can_be_added_and_removed_individually() -> None:
    """Adding one feature must not rewrite the whole array."""
    builder = MLModelPatchBuilder(MODEL_URN)
    builder.add_ml_feature(FEATURE_A)
    builder.remove_ml_feature(FEATURE_B)

    ops = [op for mcp in builder.build() for op in _patch_ops(mcp)]
    by_op = {op["op"]: op for op in ops}

    assert set(by_op) == {"add", "remove"}
    assert FEATURE_A in by_op["add"]["path"]
    assert FEATURE_B in by_op["remove"]["path"]


def test_tags_come_from_the_shared_mixins() -> None:
    """The mixins are the point: mlModel gets these for free, as other entities do."""
    builder = MLModelPatchBuilder(MODEL_URN)
    builder.add_tag(TagAssociationClass(tag="urn:li:tag:production"))

    mcps = list(builder.build())

    assert any(mcp.aspectName == "globalTags" for mcp in mcps)
    assert all(mcp.changeType == "PATCH" for mcp in mcps)


def test_ownership_comes_from_the_shared_mixins() -> None:
    builder = MLModelPatchBuilder(MODEL_URN)
    builder.add_owner(
        OwnerClass(owner="urn:li:corpuser:ml_eng_alex", type=OwnershipTypeClass.TECHNICAL_OWNER)
    )

    mcps = list(builder.build())

    assert any(mcp.aspectName == "ownership" for mcp in mcps)
    assert all(mcp.changeType == "PATCH" for mcp in mcps)
