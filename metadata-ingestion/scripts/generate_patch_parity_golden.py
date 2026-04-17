#!/usr/bin/env python3
"""Generate golden JSON files for patch builder parity tests.

Run from the metadata-ingestion directory:
    venv/bin/python scripts/generate_patch_parity_golden.py

The output file is consumed by the Java test:
    entity-registry/src/test/resources/patch_builder_parity.json
"""

import json
from pathlib import Path

from datahub.metadata.schema_classes import (
    DocumentationAssociationClass,
    GlossaryTermAssociationClass,
    MetadataAttributionClass,
    OwnerClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)"
ATTRIBUTION_SOURCE = "urn:li:dataHubAction:testSource"
TYPE_URN = "urn:li:ownershipType:__system__technical_owner"


def extract_patch_payload(builder: DatasetPatchBuilder, aspect_name: str) -> dict:
    """Build MCPs and return the parsed JSON payload for the given aspect."""
    mcps = builder.build()
    for mcp in mcps:
        if mcp.aspectName == aspect_name:
            raw = mcp.aspect.value
            return json.loads(raw)
    raise ValueError(f"No MCP found for aspect {aspect_name}")


def main() -> None:
    result = {}

    # --- Tags: add (no context) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_tag(TagAssociationClass(tag="urn:li:tag:testTag"))
    result["globalTags_add"] = extract_patch_payload(builder, "globalTags")

    # --- Tags: add (with context) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_tag(
        TagAssociationClass(tag="urn:li:tag:testTag", context="testContext")
    )
    result["globalTags_add_with_context"] = extract_patch_payload(builder, "globalTags")

    # --- Tags: remove ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_tag("urn:li:tag:testTag")
    result["globalTags_remove"] = extract_patch_payload(builder, "globalTags")

    # --- Terms: add (no context) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_term(GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:testTerm"))
    result["glossaryTerms_add"] = extract_patch_payload(builder, "glossaryTerms")

    # --- Terms: add (with context) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_term(
        GlossaryTermAssociationClass(
            urn="urn:li:glossaryTerm:testTerm", context="testContext"
        )
    )
    result["glossaryTerms_add_with_context"] = extract_patch_payload(
        builder, "glossaryTerms"
    )

    # --- Terms: remove ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_term("urn:li:glossaryTerm:testTerm")
    result["glossaryTerms_remove"] = extract_patch_payload(builder, "glossaryTerms")

    # --- Ownership: add ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_owner(
        OwnerClass(
            owner="urn:li:corpuser:testUser",
            type=OwnershipTypeClass.DATAOWNER,
        )
    )
    result["ownership_add"] = extract_patch_payload(builder, "ownership")

    # --- Ownership: removeOwner(owner) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_owner("urn:li:corpuser:testUser")
    result["ownership_removeOwner_1"] = extract_patch_payload(builder, "ownership")

    # --- Ownership: removeOwnershipType (type only) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_owner(
        "urn:li:corpuser:testUser",
        owner_type=OwnershipTypeClass.DATAOWNER,
    )
    result["ownership_removeOwnershipType"] = extract_patch_payload(
        builder, "ownership"
    )

    # --- Ownership: removeOwner(owner, type, typeUrn) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_owner(
        "urn:li:corpuser:testUser", OwnershipTypeClass.DATAOWNER, type_urn=TYPE_URN
    )
    result["ownership_removeOwner_3"] = extract_patch_payload(builder, "ownership")

    # --- Documentation: add ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_documentation(
        DocumentationAssociationClass(documentation="Test documentation")
    )
    result["documentation_add"] = extract_patch_payload(builder, "documentation")

    # --- Documentation: remove (all entries) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_documentation()
    result["documentation_remove"] = extract_patch_payload(builder, "documentation")

    # --- Structured properties: add (string, single value) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.set_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty", "testValue"
    )
    result["structuredProperties_add"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    # --- Structured properties: set number (single value) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.set_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty", 42.0
    )
    result["structuredProperties_setNumber"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    # --- Structured properties: set string list ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.set_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty", ["value1", "value2"]
    )
    result["structuredProperties_setStringList"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    # --- Structured properties: set number list ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.set_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty", [42.0, 43.0]
    )
    result["structuredProperties_setNumberList"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    # --- Structured properties: remove ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty"
    )
    result["structuredProperties_remove"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    # =========================================================================
    # Attributed operations
    # =========================================================================
    attribution = MetadataAttributionClass(
        source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        time=0,
    )

    # --- Tags: add with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_tag(
        TagAssociationClass(tag="urn:li:tag:testTag", attribution=attribution)
    )
    result["globalTags_add_attributed"] = extract_patch_payload(builder, "globalTags")

    # --- Tags: remove with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_tag("urn:li:tag:testTag", attribution_source=ATTRIBUTION_SOURCE)
    result["globalTags_remove_attributed"] = extract_patch_payload(
        builder, "globalTags"
    )

    # --- Terms: add with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_term(
        GlossaryTermAssociationClass(
            urn="urn:li:glossaryTerm:testTerm", attribution=attribution
        )
    )
    result["glossaryTerms_add_attributed"] = extract_patch_payload(
        builder, "glossaryTerms"
    )

    # --- Terms: remove with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_term(
        "urn:li:glossaryTerm:testTerm", attribution_source=ATTRIBUTION_SOURCE
    )
    result["glossaryTerms_remove_attributed"] = extract_patch_payload(
        builder, "glossaryTerms"
    )

    # --- Ownership: add with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_owner(
        OwnerClass(
            owner="urn:li:corpuser:testUser",
            type=OwnershipTypeClass.DATAOWNER,
            attribution=attribution,
        )
    )
    result["ownership_add_attributed"] = extract_patch_payload(builder, "ownership")

    # --- Ownership: remove with attribution (fully specified) ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_owner(
        "urn:li:corpuser:testUser",
        owner_type=OwnershipTypeClass.DATAOWNER,
        type_urn="urn:li:ownershipType:__system__technical_owner",
        attribution_source=ATTRIBUTION_SOURCE,
    )
    result["ownership_remove_attributed"] = extract_patch_payload(builder, "ownership")

    # --- Documentation: add with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.add_documentation(
        DocumentationAssociationClass(
            documentation="Test documentation", attribution=attribution
        )
    )
    result["documentation_add_attributed"] = extract_patch_payload(
        builder, "documentation"
    )

    # --- Documentation: remove with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_documentation(attribution_source=ATTRIBUTION_SOURCE)
    result["documentation_remove_attributed"] = extract_patch_payload(
        builder, "documentation"
    )

    # --- Structured properties: add with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.set_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty",
        "testValue",
        attribution_source=ATTRIBUTION_SOURCE,
    )
    result["structuredProperties_add_attributed"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    # --- Structured properties: remove with attribution ---
    builder = DatasetPatchBuilder(DATASET_URN)
    builder.remove_structured_property(
        "urn:li:structuredProperty:io.acryl.testProperty",
        attribution_source=ATTRIBUTION_SOURCE,
    )
    result["structuredProperties_remove_attributed"] = extract_patch_payload(
        builder, "structuredProperties"
    )

    out_path = (
        Path(__file__).resolve().parents[2]
        / "entity-registry"
        / "src"
        / "test"
        / "resources"
        / "patch_builder_parity.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
