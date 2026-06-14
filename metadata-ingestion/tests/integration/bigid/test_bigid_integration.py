"""
Integration tests for the BigID DataHub connector.

Uses mocked BigID API responses (fixtures/) and compares output against
golden files (golden/). To regenerate golden files:

    UPDATE_GOLDEN=1 pytest tests/integration/
"""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import patch

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigid.bigid_api import BigIDClient
from datahub.ingestion.source.bigid.bigid_source import BigIDSource
from datahub.ingestion.source.bigid.bigid_utils import IDSoRAttributeInfo

# Freeze to a known timestamp so AuditStamp / MetadataAttribution fields are deterministic.
FROZEN_TIME = "2024-02-01T12:00:00Z"


def _parse_aspect(mcp: dict[str, Any]) -> dict[str, Any]:
    """Extract the aspect dict from a to_obj() MCP; handles nested JSON string encoding."""
    raw = mcp.get("aspect", {})
    if isinstance(raw, dict) and "value" in raw:
        return json.loads(raw["value"])
    return raw or {}

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN", "").lower() in ("1", "true", "yes")

BASE_CONFIG: dict[str, Any] = {
    "bigid_url": "https://bigid.test.example.com",
    "access_token": "test-token",
    "env": "PROD",
    "create_datasets": False,
    "minimum_confidence_threshold": 0.0,
    "confidence_level_tag": False,
    "domain_mode": "none",
    "owner_type": "user",
    "sync_tags": True,
}


def _load(name: str) -> Any:
    with open(os.path.join(FIXTURES_DIR, name)) as fh:
        return json.load(fh)


def _load_idsor_attrs() -> dict[str, IDSoRAttributeInfo]:
    """Load idsor_attributes.json fixture and convert lists to IDSoRAttributeInfo."""
    raw = _load("idsor_attributes.json")
    return {k: IDSoRAttributeInfo(friendly_name=v[0], glossary_id=v[1]) for k, v in raw.items()}


def _columns_side_effect(object_name: str, source_name: str, fqn: str = "") -> list[dict[str, Any]]:
    fname = (
        f"columns_{source_name.lower().replace('-', '_').replace(' ', '_')}"
        f"_{object_name.lower()}.json"
    )
    path = os.path.join(FIXTURES_DIR, fname)
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return []


def _run_source(config_overrides: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run source with mocked BigID APIs; return sorted list of MCP dicts.

    Time is frozen to FROZEN_TIME so AuditStamp and MetadataAttribution
    timestamps are deterministic across runs.
    """
    config = {**BASE_CONFIG, **(config_overrides or {})}
    ctx = PipelineContext(run_id="integration-test-run")

    with (
        time_machine.travel(FROZEN_TIME, tick=False),
        patch.object(BigIDClient, "get_connections", return_value=_load("ds_connections.json")),
        patch.object(BigIDClient, "get_all_classifications", return_value=_load("all_classifications.json")),
        patch.object(BigIDClient, "get_glossary_items", return_value=_load("business_glossary_items.json")),
        patch.object(BigIDClient, "get_catalog_objects", side_effect=lambda: iter(_load("data_catalog.json"))),
        patch.object(BigIDClient, "get_columns", side_effect=_columns_side_effect),
        patch.object(BigIDClient, "get_idsor_attribute_map", return_value=_load_idsor_attrs()),
        patch.object(BigIDClient, "close"),
    ):
        source = BigIDSource.create(config, ctx)
        mcps = [wu.metadata.to_obj() for wu in source.get_workunits()]

    # Sort for determinism: URN first, then aspect name
    mcps.sort(key=lambda m: (m.get("entityUrn", ""), m.get("aspectName", "")))
    return mcps


# ---------------------------------------------------------------------------
# Golden file test
# ---------------------------------------------------------------------------


def test_bigid_source_golden() -> None:
    """Full end-to-end output must match the golden file exactly."""
    mcps = _run_source()
    golden_path = os.path.join(GOLDEN_DIR, "test_bigid_source.json")

    if UPDATE_GOLDEN:
        os.makedirs(GOLDEN_DIR, exist_ok=True)
        with open(golden_path, "w") as fh:
            json.dump(mcps, fh, indent=2, default=str)
        pytest.skip("Golden file updated — re-run without UPDATE_GOLDEN to verify")

    assert os.path.exists(golden_path), (
        f"Golden file missing: {golden_path}. "
        "Run with UPDATE_GOLDEN=1 to generate it."
    )
    with open(golden_path) as fh:
        golden = json.load(fh)

    assert mcps == golden, (
        "Output differs from golden file. "
        "If the change is intentional, re-run with UPDATE_GOLDEN=1."
    )


# ---------------------------------------------------------------------------
# Structural invariant tests (pass even without a golden file)
# ---------------------------------------------------------------------------


def test_mcp_count_meets_minimum() -> None:
    """Must produce at least as many MCPs as the current golden file."""
    golden_path = os.path.join(GOLDEN_DIR, "test_bigid_source.json")
    if not os.path.exists(golden_path):
        pytest.skip("Golden file missing — run with UPDATE_GOLDEN=1 to generate it")
    mcps = _run_source()
    with open(golden_path) as fh:
        golden_count = len(json.load(fh))
    assert len(mcps) >= golden_count, f"Expected >={golden_count} MCPs, got {len(mcps)}"


def test_glossary_node_emitted() -> None:
    """Root BigID GlossaryNode must always be emitted."""
    mcps = _run_source()
    node_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryNodeInfo"}
    assert "urn:li:glossaryNode:bigid" in node_urns


def test_glossary_terms_emitted() -> None:
    """All three fixture glossary items must produce GlossaryTermInfo MCPs."""
    mcps = _run_source()
    term_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryTermInfo"}
    assert "urn:li:glossaryTerm:bigid.bt_email" in term_urns
    assert "urn:li:glossaryTerm:bigid.bt_full_name" in term_urns
    assert "urn:li:glossaryTerm:bigid.bt_ssn" in term_urns


def test_tag_entities_emitted() -> None:
    """BigID object tags must produce TagProperties entities."""
    mcps = _run_source()
    tag_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "tagProperties"}
    # Classification:PII from CUSTOMERS, DataClass:Internal from documents
    assert any("Classification" in u and "PII" in u for u in tag_urns)
    assert any("DataClass" in u and "Internal" in u for u in tag_urns)


def test_editable_schema_metadata_emitted() -> None:
    """Structured datasets with classified columns must get editableSchemaMetadata."""
    mcps = _run_source()
    enriched = {m["entityUrn"] for m in mcps if m.get("aspectName") == "editableSchemaMetadata"}
    # Both Snowflake CUSTOMERS and Postgres ORDERS have classifiable columns
    assert any("snowflake" in u.lower() or "customers" in u.lower() for u in enriched)
    assert any("postgres" in u.lower() or "orders" in u.lower() for u in enriched)


def test_dataset_profile_emitted() -> None:
    """Structured datasets with column profiles must get datasetProfile MCPs."""
    mcps = _run_source()
    profiles = [m for m in mcps if m.get("aspectName") == "datasetProfile"]
    assert len(profiles) >= 2


def test_platform_instance_not_emitted_without_config() -> None:
    """dataPlatformInstance must only be emitted when platform_instance is configured.
    Emitting with instance=None in enrichment mode would overwrite an existing
    non-null instance set by a native connector.
    """
    mcps = _run_source()
    platform_instance_mcps = [
        m for m in mcps if m.get("aspectName") == "dataPlatformInstance"
    ]
    assert platform_instance_mcps == [], (
        "dataPlatformInstance should not be emitted when no platform_instance is configured; "
        f"got {len(platform_instance_mcps)} unexpected MCPs"
    )


def test_snowflake_urn_is_lowercased() -> None:
    """Snowflake dataset names must be lowercased in the URN."""
    mcps = _run_source()
    snowflake_urns = [
        m["entityUrn"]
        for m in mcps
        if "snowflake" in m.get("entityUrn", "").lower()
        and m.get("entityType") == "dataset"
    ]
    assert snowflake_urns, "No Snowflake dataset URNs found"
    for urn in snowflake_urns:
        dataset_name = urn.split(",")[1] if "," in urn else urn
        assert dataset_name == dataset_name.lower(), (
            f"Snowflake URN name component must be lowercase: {urn}"
        )


def test_unstructured_datasets_have_no_schema_enrichment() -> None:
    """Unstructured sources (SharePoint) must not get editableSchemaMetadata."""
    mcps = _run_source()
    enriched = {m["entityUrn"] for m in mcps if m.get("aspectName") == "editableSchemaMetadata"}
    assert not any("sharepoint" in u.lower() for u in enriched), (
        "Unstructured dataset (SharePoint) should not have editableSchemaMetadata"
    )


def test_confidence_threshold_filters_low_findings() -> None:
    """With threshold=0.75 only HIGH-confidence findings should produce terms."""
    mcps = _run_source({"minimum_confidence_threshold": 0.75})

    # ORDERS customer_email classifier finding is MEDIUM (0.50 < 0.75) — must be filtered.
    # Note: ORDERS still gets editableSchemaMetadata from HIGH-confidence IDSoR findings,
    # so we check term-level filtering rather than MCP absence.
    orders_editable = [
        m for m in mcps
        if m.get("aspectName") == "editableSchemaMetadata"
        and "orders" in m.get("entityUrn", "").lower()
    ]
    if orders_editable:
        aspect = _parse_aspect(orders_editable[0])
        all_term_urns = {
            term["urn"]
            for field in aspect.get("editableSchemaFieldInfo", [])
            for term in field.get("glossaryTerms", {}).get("terms", [])
        }
        # The MEDIUM classifier finding for EMAIL must not appear
        assert "urn:li:glossaryTerm:bigid.bt_email" not in all_term_urns or all(
            # bt_email may appear via IDSoR path 1 (HIGH rank) — that's correct
            term.get("attribution", {}).get("sourceDetail", {}).get("classifier_type") == "idsor_attribute"
            for field in aspect.get("editableSchemaFieldInfo", [])
            for term in field.get("glossaryTerms", {}).get("terms", [])
            if term.get("urn") == "urn:li:glossaryTerm:bigid.bt_email"
        ), "MEDIUM classifier finding for EMAIL must be filtered at threshold=0.75"


def test_global_tags_on_pii_dataset() -> None:
    """Datasets with object tags must have GlobalTags emitted."""
    mcps = _run_source()
    customers_tags = [
        m for m in mcps
        if m.get("aspectName") == "globalTags"
        and "customers" in m.get("entityUrn", "").lower()
    ]
    assert len(customers_tags) == 1


def test_glossary_term_parent_node_is_bigid_root() -> None:
    """Non-IDSoR GlossaryTerms must declare parentNode=urn:li:glossaryNode:bigid."""
    mcps = _run_source()
    for mcp in mcps:
        if mcp.get("aspectName") != "glossaryTermInfo":
            continue
        aspect = _parse_aspect(mcp)
        if aspect.get("customProperties", {}).get("bigid_type") == "idsor_attribute":
            continue  # IDSoR terms (paths 1-3) use bigid.idsor node — tested separately
        assert aspect.get("parentNode") == "urn:li:glossaryNode:bigid", (
            f"GlossaryTerm {mcp['entityUrn']} has wrong parentNode: {aspect.get('parentNode')}"
        )


def test_unlinked_classifier_term_emitted() -> None:
    """Classifiers with no glossary linkage must produce auto-generated GlossaryTerms."""
    mcps = _run_source()
    term_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryTermInfo"}
    assert "urn:li:glossaryTerm:bigid.classifier.phone" in term_urns, (
        "Expected auto-generated GlossaryTerm for unlinked classifier.PHONE"
    )


def test_sync_unlinked_classifiers_false_skips_unlinked() -> None:
    """With sync_unlinked_classifiers=False no auto-generated classifier terms are emitted,
    but linked classifiers still produce editableSchemaMetadata."""
    mcps = _run_source({"sync_unlinked_classifiers": False})
    term_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryTermInfo"}
    assert "urn:li:glossaryTerm:bigid.classifier.phone" not in term_urns, (
        "Unlinked classifier GlossaryTerm should be suppressed when sync_unlinked_classifiers=False"
    )
    # Linked classifiers (EMAIL, FULL_NAME) must still enrich CUSTOMERS
    enriched = {m["entityUrn"] for m in mcps if m.get("aspectName") == "editableSchemaMetadata"}
    assert any("customers" in u.lower() for u in enriched), (
        "Linked classifier findings must still produce editableSchemaMetadata when sync_unlinked_classifiers=False"
    )


def test_unlinked_classifier_term_content() -> None:
    """Auto-generated GlossaryTerm for unlinked classifier.PHONE must have correct content."""
    mcps = _run_source()
    phone_mcp = next(
        (m for m in mcps
         if m.get("entityUrn") == "urn:li:glossaryTerm:bigid.classifier.phone"
         and m.get("aspectName") == "glossaryTermInfo"),
        None,
    )
    assert phone_mcp is not None, "GlossaryTermInfo for classifier.PHONE not found"
    aspect = _parse_aspect(phone_mcp)
    assert aspect.get("name") == "Phone", f"Expected name='Phone', got {aspect.get('name')!r}"
    assert "classifier.PHONE" in aspect.get("definition", ""), (
        "Definition must reference the original classifier name"
    )
    assert aspect.get("parentNode") == "urn:li:glossaryNode:bigid", (
        f"parentNode must be the BigID root node, got {aspect.get('parentNode')!r}"
    )
    custom = aspect.get("customProperties", {})
    assert custom.get("bigid_type") == "classifier"
    assert custom.get("bigid_classifier_name") == "classifier.PHONE"


def test_domain_mode_auto_namespaced_emits_domain_entities() -> None:
    """auto_namespaced mode must produce domainProperties MCPs with bigid.-namespaced URNs."""
    mcps = _run_source({"domain_mode": "auto_namespaced"})
    domain_mcps = [m for m in mcps if m.get("aspectName") == "domainProperties"]
    assert domain_mcps, "Expected at least one domainProperties MCP in auto_namespaced mode"
    domain_urns = {m["entityUrn"] for m in domain_mcps}
    assert any("bigid." in u for u in domain_urns), (
        f"Expected at least one domain URN containing 'bigid.', got: {domain_urns}"
    )


def test_glossary_term_has_domain_aspect_when_auto_namespaced() -> None:
    """In auto_namespaced mode, GlossaryTerm entities must also have a domains aspect MCP."""
    mcps = _run_source({"domain_mode": "auto_namespaced"})
    term_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryTermInfo"}
    domains_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "domains"}
    assert any(u in domains_urns for u in term_urns), (
        "Expected at least one glossaryTerm URN to also have a 'domains' aspect MCP"
    )


# ---------------------------------------------------------------------------
# IDSoR
# ---------------------------------------------------------------------------


def test_idsor_glossary_node_emitted() -> None:
    """bigid.idsor GlossaryNode must be emitted when sync_idsor=True (default)."""
    mcps = _run_source()
    nodes = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryNodeInfo"}
    assert "urn:li:glossaryNode:bigid.idsor" in nodes


def test_idsor_node_suppressed_when_disabled() -> None:
    """bigid.idsor GlossaryNode must not be emitted when sync_idsor=False."""
    mcps = _run_source({"sync_idsor": False})
    nodes = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryNodeInfo"}
    assert "urn:li:glossaryNode:bigid.idsor" not in nodes


def test_idsor_path1_reuses_existing_glossary_term() -> None:
    """customer_email IDSoR attribute maps to bt_email via fixture — must reuse that term, not auto-gen."""
    mcps = _run_source()
    # bt_email term must appear in editableSchemaMetadata for the ORDERS dataset
    orders_editable = [
        m for m in mcps
        if m.get("aspectName") == "editableSchemaMetadata"
        and "orders" in m.get("entityUrn", "").lower()
    ]
    assert orders_editable, "ORDERS dataset must have editableSchemaMetadata"
    aspect = _parse_aspect(orders_editable[0])
    all_term_urns = {
        term["urn"]
        for field in aspect.get("editableSchemaFieldInfo", [])
        for term in field.get("glossaryTerms", {}).get("terms", [])
    }
    assert "urn:li:glossaryTerm:bigid.bt_email" in all_term_urns, (
        "IDSoR customer_email (path 1) must reuse the existing bt_email GlossaryTerm"
    )
    # No auto-gen idsor term should be created for customer_email
    idsor_urns = {u for u in all_term_urns if "bigid.idsor." in u and "customer_email" in u}
    assert not idsor_urns, "Path 1 must NOT create a new idsor term for customer_email"


def test_idsor_path3_autogenerates_country_term() -> None:
    """country IDSoR attribute is not in the fixture map — must auto-generate bigid.idsor.country."""
    mcps = _run_source()
    term_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "glossaryTermInfo"}
    assert "urn:li:glossaryTerm:bigid.idsor.country" in term_urns, (
        "Path 3 IDSoR attribute 'country' must produce an auto-generated GlossaryTerm"
    )
    # Verify content
    country_mcp = next(
        m for m in mcps
        if m.get("entityUrn") == "urn:li:glossaryTerm:bigid.idsor.country"
        and m.get("aspectName") == "glossaryTermInfo"
    )
    aspect = _parse_aspect(country_mcp)
    assert aspect.get("parentNode") == "urn:li:glossaryNode:bigid.idsor"
    assert aspect.get("customProperties", {}).get("bigid_type") == "idsor_attribute"


def test_idsor_disabled_produces_no_idsor_terms() -> None:
    """With sync_idsor=False no bigid.idsor.* GlossaryTerms are emitted."""
    mcps = _run_source({"sync_idsor": False})
    idsor_term_urns = [
        m["entityUrn"] for m in mcps
        if m.get("aspectName") == "glossaryTermInfo"
        and "bigid.idsor." in m.get("entityUrn", "")
    ]
    assert not idsor_term_urns, f"Expected no idsor terms when disabled; got {idsor_term_urns}"


def test_create_datasets_golden() -> None:
    """With create_datasets=True the output must include DatasetProperties and SchemaMetadata MCPs."""
    mcps = _run_source({"create_datasets": True})
    golden_path = os.path.join(GOLDEN_DIR, "test_bigid_source_create_datasets.json")

    if UPDATE_GOLDEN:
        os.makedirs(GOLDEN_DIR, exist_ok=True)
        with open(golden_path, "w") as fh:
            json.dump(mcps, fh, indent=2, default=str)
        pytest.skip("Golden file updated — re-run without UPDATE_GOLDEN to verify")

    assert os.path.exists(golden_path), (
        f"Golden file missing: {golden_path}. "
        "Run with UPDATE_GOLDEN=1 to generate it."
    )
    with open(golden_path) as fh:
        golden = json.load(fh)

    assert mcps == golden, (
        "create_datasets=True output differs from golden file. "
        "If the change is intentional, re-run with UPDATE_GOLDEN=1."
    )


def test_create_datasets_emits_dataset_properties() -> None:
    """create_datasets=True must produce datasetProperties MCPs for structured datasets."""
    mcps = _run_source({"create_datasets": True})
    dataset_props = [m for m in mcps if m.get("aspectName") == "datasetProperties"]
    assert dataset_props, "Expected datasetProperties MCPs when create_datasets=True"


def test_create_datasets_emits_schema_metadata() -> None:
    """create_datasets=True must produce schemaMetadata MCPs for structured datasets with columns."""
    mcps = _run_source({"create_datasets": True})
    schema_mcps = [m for m in mcps if m.get("aspectName") == "schemaMetadata"]
    assert schema_mcps, "Expected schemaMetadata MCPs when create_datasets=True"


def test_create_datasets_false_emits_no_dataset_properties() -> None:
    """create_datasets=False (default) must not emit datasetProperties or schemaMetadata."""
    mcps = _run_source()
    assert not any(m.get("aspectName") == "datasetProperties" for m in mcps), (
        "datasetProperties must not be emitted in enrichment-only mode"
    )
    assert not any(m.get("aspectName") == "schemaMetadata" for m in mcps), (
        "schemaMetadata must not be emitted in enrichment-only mode"
    )


def test_unstructured_enrichment_emits_dataset_level_glossary_terms() -> None:
    """With sync_unstructured_enrichment=True and attribute_details present, emit dataset-level GlossaryTerms."""
    mcps = _run_source({"sync_unstructured_enrichment": True})
    sharepoint_urn = next(
        (m["entityUrn"] for m in mcps if "sharepoint" in m.get("entityUrn", "").lower()
         and m.get("entityType") == "dataset"),
        None,
    )
    assert sharepoint_urn, "SharePoint dataset URN not found"

    terms_mcps = [m for m in mcps if m["entityUrn"] == sharepoint_urn and m.get("aspectName") == "glossaryTerms"]
    assert len(terms_mcps) == 1, f"Expected exactly 1 glossaryTerms MCP on SharePoint dataset, got {len(terms_mcps)}"

    aspect = _parse_aspect(terms_mcps[0])
    term_urns = {t["urn"] for t in aspect.get("terms", [])}
    assert "urn:li:glossaryTerm:bigid.bt_email" in term_urns, (
        "classifier.EMAIL (linked to bt_email) must appear as a dataset-level GlossaryTerm"
    )
    assert "urn:li:glossaryTerm:bigid.classifier.phone" in term_urns, (
        "classifier.PHONE (unlinked) must produce an auto-generated dataset-level GlossaryTerm"
    )


def test_unstructured_enrichment_emits_dataset_profile() -> None:
    """With sync_unstructured_enrichment=True, emit DatasetProfile with total_pii_count as rowCount."""
    mcps = _run_source({"sync_unstructured_enrichment": True})
    sharepoint_urn = next(
        (m["entityUrn"] for m in mcps if "sharepoint" in m.get("entityUrn", "").lower()
         and m.get("entityType") == "dataset"),
        None,
    )
    assert sharepoint_urn, "SharePoint dataset URN not found"

    profile_mcps = [m for m in mcps if m["entityUrn"] == sharepoint_urn and m.get("aspectName") == "datasetProfile"]
    assert len(profile_mcps) == 1, "Expected exactly 1 datasetProfile MCP on SharePoint dataset"

    aspect = _parse_aspect(profile_mcps[0])
    assert aspect.get("rowCount") == 42, f"Expected rowCount=42 (total_pii_count), got {aspect.get('rowCount')}"
    assert aspect.get("columnCount") == 0, "Unstructured dataset must have columnCount=0"
    assert aspect.get("sizeInBytes") == 1024000


def test_unstructured_no_schema_enrichment_even_when_enabled() -> None:
    """Unstructured datasets must never get editableSchemaMetadata, even with sync_unstructured_enrichment=True."""
    mcps = _run_source({"sync_unstructured_enrichment": True})
    enriched = {m["entityUrn"] for m in mcps if m.get("aspectName") == "editableSchemaMetadata"}
    assert not any("sharepoint" in u.lower() for u in enriched), (
        "Unstructured dataset must not get editableSchemaMetadata even when unstructured enrichment is enabled"
    )


def test_unstructured_enrichment_disabled_by_default() -> None:
    """Without sync_unstructured_enrichment=True, unstructured datasets get no GlossaryTerms aspect."""
    mcps = _run_source()
    sharepoint_terms = [
        m for m in mcps
        if "sharepoint" in m.get("entityUrn", "").lower()
        and m.get("aspectName") == "glossaryTerms"
    ]
    assert not sharepoint_terms, (
        "GlossaryTerms must not be emitted for unstructured datasets when sync_unstructured_enrichment=False"
    )


def test_glossary_term_parent_node_for_idsor_is_idsor_node() -> None:
    """All auto-generated IDSoR GlossaryTerms must declare parentNode=bigid.idsor (not bigid root)."""
    mcps = _run_source()
    for mcp in mcps:
        if mcp.get("aspectName") != "glossaryTermInfo":
            continue
        if "bigid.idsor." not in mcp.get("entityUrn", ""):
            continue
        aspect = _parse_aspect(mcp)
        assert aspect.get("parentNode") == "urn:li:glossaryNode:bigid.idsor", (
            f"IDSoR term {mcp['entityUrn']} must have parentNode=bigid.idsor, "
            f"got {aspect.get('parentNode')!r}"
        )


def test_no_duplicate_term_urns_per_field() -> None:
    """Each schema field must not have the same GlossaryTerm URN more than once.

    Regression test for the bug where customer_email received bt_email twice:
    once from the classifier path and once from IDSoR path-1 (which reuses the
    same URN). The fix deduplicates by URN before appending to the terms list.
    """
    mcps = _run_source()
    for mcp in mcps:
        if mcp.get("aspectName") != "editableSchemaMetadata":
            continue
        aspect = _parse_aspect(mcp)
        for field in aspect.get("editableSchemaFieldInfo", []):
            urns = [t["urn"] for t in field.get("glossaryTerms", {}).get("terms", [])]
            assert len(urns) == len(set(urns)), (
                f"Duplicate term URNs on {mcp['entityUrn']} "
                f"field {field['fieldPath']!r}: {urns}"
            )


def test_confidence_level_tag_emits_on_classified_fields() -> None:
    """With confidence_level_tag=True, classified fields get a bigid.confidence: tag."""
    mcps = _run_source({"confidence_level_tag": True})
    found_confidence_tag = False
    for mcp in mcps:
        if mcp.get("aspectName") != "editableSchemaMetadata":
            continue
        aspect = _parse_aspect(mcp)
        for field in aspect.get("editableSchemaFieldInfo", []):
            tag_urns = [t["tag"] for t in field.get("globalTags", {}).get("tags", [])]
            if any("bigid.confidence:" in u for u in tag_urns):
                found_confidence_tag = True
                # Each tag must be one of the known confidence levels
                for u in tag_urns:
                    if "bigid.confidence:" in u:
                        level = u.split("bigid.confidence:")[-1]
                        assert level in ("HIGH", "MEDIUM", "LOW"), (
                            f"Unexpected confidence level in tag URN: {u}"
                        )
    assert found_confidence_tag, (
        "Expected at least one schema field to have a bigid.confidence: tag when confidence_level_tag=True"
    )


def test_confidence_level_tag_not_emitted_by_default() -> None:
    """Without confidence_level_tag=True, no bigid.confidence: tags are emitted."""
    mcps = _run_source()
    for mcp in mcps:
        if mcp.get("aspectName") != "editableSchemaMetadata":
            continue
        aspect = _parse_aspect(mcp)
        for field in aspect.get("editableSchemaFieldInfo", []):
            tag_urns = [t["tag"] for t in field.get("globalTags", {}).get("tags", [])]
            assert not any("bigid.confidence:" in u for u in tag_urns), (
                f"Unexpected confidence tag on field {field['fieldPath']!r}: {tag_urns}"
            )


def test_owner_type_group_uses_corp_group_urn() -> None:
    """With owner_type='group', GlossaryTerm ownership URNs must use urn:li:corpGroup."""
    mcps = _run_source({"owner_type": "group"})
    ownership_mcps = [m for m in mcps if m.get("aspectName") == "ownership"]
    assert ownership_mcps, "Expected at least one ownership MCP when owner_type='group'"
    for mcp in ownership_mcps:
        aspect = _parse_aspect(mcp)
        for owner in aspect.get("owners", []):
            assert owner["owner"].startswith("urn:li:corpGroup:"), (
                f"Expected corpGroup URN with owner_type='group', got {owner['owner']!r}"
            )


def test_owner_type_none_emits_no_ownership() -> None:
    """With owner_type='none', no ownership MCPs are emitted."""
    mcps = _run_source({"owner_type": "none"})
    ownership_mcps = [m for m in mcps if m.get("aspectName") == "ownership"]
    assert not ownership_mcps, (
        f"Expected no ownership MCPs when owner_type='none'; got {len(ownership_mcps)}"
    )


def test_registries_empty_reports_failure() -> None:
    """When both glossary items and classification map fail, report must have registries-empty failure."""
    from datahub.ingestion.source.bigid.bigid_api import BigIDAPIError

    config = {**BASE_CONFIG}
    ctx = PipelineContext(run_id="registries-empty-test")

    with (
        time_machine.travel(FROZEN_TIME, tick=False),
        patch.object(BigIDClient, "get_connections", return_value=[]),
        patch.object(BigIDClient, "get_all_classifications", side_effect=BigIDAPIError("clf fail")),
        patch.object(BigIDClient, "get_glossary_items", side_effect=BigIDAPIError("gloss fail")),
        patch.object(BigIDClient, "get_catalog_objects", side_effect=lambda: iter([])),
        patch.object(BigIDClient, "get_idsor_attribute_map", return_value={}),
        patch.object(BigIDClient, "close"),
    ):
        source = BigIDSource.create(config, ctx)
        list(source.get_workunits())  # drain generator so report is populated
        report = source.get_report()

    failure_keys = [f.message for f in report.failures]
    assert "registries-empty" in failure_keys, (
        f"Expected 'registries-empty' failure in report; got: {failure_keys}"
    )


def test_domain_entities_emit_status_aspect() -> None:
    """In auto_namespaced mode, domain entities must have a status aspect (removed=False)."""
    mcps = _run_source({"domain_mode": "auto_namespaced"})
    domain_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "domainProperties"}
    status_urns = {m["entityUrn"] for m in mcps if m.get("aspectName") == "status"
                   and "urn:li:domain:" in m.get("entityUrn", "")}
    missing = domain_urns - status_urns
    assert not missing, (
        f"Domain entities missing status aspect: {missing}"
    )
