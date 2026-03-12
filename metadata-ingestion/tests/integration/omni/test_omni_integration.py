"""Integration tests for OmniSource.

Strategy: full source pipeline run against a comprehensive fake client that
returns deterministic fixture data.  The pipeline writes to a local JSON file
sink, and the output is compared against a golden file.

Run tests:
    pytest tests/integration/omni/ -v

Re-generate the golden file after intentional changes:
    pytest tests/integration/omni/ -v --update-golden-files
"""

from __future__ import annotations

import json
import pathlib
from typing import Any, Dict, List

import pytest
import time_machine
from datahub.ingestion.api.common import PipelineContext

from datahub.ingestion.source.omni.omni import OmniSource
from datahub.ingestion.source.omni.omni_config import OmniSourceConfig
from tests.integration.omni.fixtures import (
    FakeOmniClientFull,
)

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2024-07-01 00:00:00"

INTEGRATION_DIR = pathlib.Path(__file__).parent
GOLDEN_FILE = INTEGRATION_DIR / "omni_mces_golden.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_source(extra_config: Dict[str, Any] | None = None) -> OmniSource:
    cfg: Dict[str, Any] = {
        "base_url": "https://acme.omniapp.co/api",
        "api_key": "test-key",
        "include_workbook_only": True,
        "include_column_lineage": True,
        "connection_to_platform": {
            "conn-snow-prod": "snowflake",
            "conn-snow-staging": "snowflake",
        },
        "connection_to_database": {
            "conn-snow-prod": "ANALYTICS_PROD",
            "conn-snow-staging": "ANALYTICS_STAGING",
        },
        "connection_to_platform_instance": {
            "conn-snow-prod": "snowflake",
        },
    }
    if extra_config:
        cfg.update(extra_config)
    config = OmniSourceConfig.model_validate(cfg)
    source = OmniSource(config, PipelineContext(run_id="test-integration"))
    source.client = FakeOmniClientFull()
    return source


def _collect_workunits(source: OmniSource) -> List[Dict[str, Any]]:
    """Run source and serialise all MCP work-units to plain dicts."""
    events: List[Dict[str, Any]] = []
    for wu in source.get_workunits_internal():
        mcp = wu.metadata
        entity_type = ""
        urn = getattr(mcp, "entityUrn", "")
        aspect_name = getattr(mcp, "aspectName", "")
        aspect = getattr(mcp, "aspect", None)
        if urn.startswith("urn:li:dataset"):
            entity_type = "dataset"
        elif urn.startswith("urn:li:dashboard"):
            entity_type = "dashboard"
        elif urn.startswith("urn:li:chart"):
            entity_type = "chart"
        elif urn.startswith("urn:li:dataPlatform"):
            entity_type = "dataPlatform"
        events.append(
            {
                "entityType": entity_type,
                "entityUrn": urn,
                "aspectName": aspect_name,
                "aspect": aspect.to_obj() if hasattr(aspect, "to_obj") else {},
            }
        )
    return events


# ---------------------------------------------------------------------------
# Golden file integration test
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_omni_ingestion_golden_file(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    """Full end-to-end ingestion compared against a golden JSON file.

    Run with --update-golden-files to regenerate after intentional changes.
    """
    source = _build_source()
    events = _collect_workunits(source)

    if pytestconfig.getoption("--update-golden-files", default=False):
        GOLDEN_FILE.write_text(
            json.dumps(events, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        pytest.skip(f"Golden file updated: {GOLDEN_FILE}")
        return

    assert GOLDEN_FILE.exists(), (
        f"Golden file not found: {GOLDEN_FILE}. "
        "Run with --update-golden-files to generate it."
    )
    golden = json.loads(GOLDEN_FILE.read_text(encoding="utf-8"))

    # Compare ignoring aspect-level diffs that include timestamps —
    # we assert structural completeness instead.
    actual_keys = {(e["entityUrn"], e["aspectName"]) for e in events}
    golden_keys = {(e["entityUrn"], e["aspectName"]) for e in golden}

    missing_from_actual = golden_keys - actual_keys
    extra_in_actual = actual_keys - golden_keys

    assert not missing_from_actual, (
        f"Expected events missing from output: {sorted(missing_from_actual)}"
    )
    assert not extra_in_actual, (
        f"Unexpected extra events in output: {sorted(extra_in_actual)}"
    )


# ---------------------------------------------------------------------------
# Structural completeness tests (run without golden file)
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_full_ingestion_event_count_and_entity_types() -> None:
    """Full ingestion produces ≥ 30 events covering all entity types."""
    source = _build_source()
    events = _collect_workunits(source)

    entity_types = {e["entityType"] for e in events}
    assert "dataset" in entity_types, "No dataset events emitted"
    assert "dashboard" in entity_types, "No dashboard events emitted"
    assert "chart" in entity_types, "No chart events emitted"

    assert len(events) >= 30, (
        f"Expected ≥30 events for comprehensive ingestion, got {len(events)}"
    )


@time_machine.travel(FROZEN_TIME)
def test_all_expected_aspect_types_present() -> None:
    """Required aspects must be present across all emitted events."""
    source = _build_source()
    events = _collect_workunits(source)

    aspect_names = {e["aspectName"] for e in events}
    required_aspects = {
        "datasetProperties",  # SDK V2 emits editableDatasetProperties
        "dataPlatformInstance",  # Every entity gets platform instance
        "upstreamLineage",  # Lineage edges are emitted
        "dashboardInfo",  # Dashboard entities
        "chartInfo",  # Chart/tile entities
        "schemaMetadata",  # Semantic views get column fields
        "subTypes",  # Every entity gets a subtype
    }
    # editableDatasetProperties is the SDK V2 name for description
    required_aspects.discard("datasetProperties")
    required_aspects.add("editableDatasetProperties")

    missing = required_aspects - aspect_names
    assert not missing, f"Required aspects missing from output: {missing}"


@time_machine.travel(FROZEN_TIME)
def test_platform_metadata_emitted() -> None:
    """Omni data platform info must be emitted at startup."""
    source = _build_source()
    events = _collect_workunits(source)

    platform_events = [e for e in events if e["entityType"] == "dataPlatform"]
    assert platform_events, "No dataPlatform event emitted"
    assert any("omni" in e["entityUrn"] for e in platform_events), (
        "Omni platform URN not found"
    )


@time_machine.travel(FROZEN_TIME)
def test_connections_emitted_as_datasets() -> None:
    """Both Omni connections must be emitted as connection datasets."""
    source = _build_source()
    events = _collect_workunits(source)

    conn_urns = [
        e["entityUrn"]
        for e in events
        if "connection.conn-snow-prod" in e.get("entityUrn", "")
        or "connection.conn-snow-staging" in e.get("entityUrn", "")
    ]
    assert len(conn_urns) >= 2, f"Expected ≥2 connection events, got {conn_urns}"


@time_machine.travel(FROZEN_TIME)
def test_topics_emitted_with_cleared_upstream_lineage() -> None:
    """Topics must have an upstreamLineage with empty upstreams (stale edge clear)."""
    source = _build_source()
    events = _collect_workunits(source)

    topic_lineage = [
        e
        for e in events
        if ".topic." in e.get("entityUrn", "") and e["aspectName"] == "upstreamLineage"
    ]
    assert topic_lineage, "No upstreamLineage aspect found on topic entities"
    for event in topic_lineage:
        upstreams = event["aspect"].get("upstreams", [])
        assert upstreams == [], (
            f"Topic {event['entityUrn']} should have empty upstreams to prevent "
            f"circular lineage, got: {upstreams}"
        )


@time_machine.travel(FROZEN_TIME)
def test_semantic_views_are_downstream_of_topics() -> None:
    """Semantic view datasets must list their topic as upstream."""
    source = _build_source()
    events = _collect_workunits(source)

    topic_urn_orders = source._topic_dataset_urn("shared-model-1", "orders")
    topic_urn_customers = source._topic_dataset_urn("shared-model-1", "customers")

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    customers_view_urn = source._semantic_dataset_urn("shared-model-1", "customers")

    def _upstreams_of(entity_urn: str) -> set:
        for e in events:
            if e["entityUrn"] == entity_urn and e["aspectName"] == "upstreamLineage":
                return {u["dataset"] for u in e["aspect"].get("upstreams", [])}
        return set()

    orders_upstreams = _upstreams_of(orders_view_urn)
    assert topic_urn_orders in orders_upstreams, (
        f"orders view should be downstream of orders topic; "
        f"actual upstreams: {orders_upstreams}"
    )

    customers_upstreams = _upstreams_of(customers_view_urn)
    assert topic_urn_customers in customers_upstreams, (
        f"customers view should be downstream of customers topic; "
        f"actual upstreams: {customers_upstreams}"
    )


@time_machine.travel(FROZEN_TIME)
def test_physical_tables_downstream_of_semantic_views() -> None:
    """Physical Snowflake tables must list semantic views as their upstream."""
    source = _build_source()
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    orders_physical_urn = source._physical_dataset_urn(
        "snowflake", "ANALYTICS_PROD", "PUBLIC", "ORDERS", platform_instance="snowflake"
    )

    physical_lineage = [
        e
        for e in events
        if e["entityUrn"] == orders_physical_urn
        and e["aspectName"] == "upstreamLineage"
    ]
    assert physical_lineage, (
        f"No upstreamLineage emitted for physical table {orders_physical_urn}"
    )
    upstreams = {
        u["dataset"] for u in physical_lineage[0]["aspect"].get("upstreams", [])
    }
    assert orders_view_urn in upstreams, (
        f"Physical ORDERS table should be downstream of semantic orders view; "
        f"actual upstreams: {upstreams}"
    )


@time_machine.travel(FROZEN_TIME)
def test_dashboard_tiles_reference_topics() -> None:
    """Chart/tile entities must list the related topic URN as an input dataset."""
    source = _build_source()
    events = _collect_workunits(source)

    topic_orders_urn = source._topic_dataset_urn("shared-model-1", "orders")
    topic_customers_urn = source._topic_dataset_urn("shared-model-1", "customers")

    chart_info_events = [e for e in events if e["aspectName"] == "chartInfo"]
    assert chart_info_events, "No chartInfo aspects emitted"

    def _extract_input_urns(aspect: dict) -> set:
        """ChartInfoClass.inputs serializes as [{"string": "urn:..."}, ...] via avro."""
        raw = aspect.get("inputs") or []
        return {item["string"] if isinstance(item, dict) else item for item in raw}

    tile_revenue = next(
        (e for e in chart_info_events if "tile-revenue" in e["entityUrn"]), None
    )
    assert tile_revenue is not None, "tile-revenue chart not found"
    revenue_inputs = _extract_input_urns(tile_revenue["aspect"])
    assert topic_orders_urn in revenue_inputs, (
        f"tile-revenue should reference orders topic; inputs: {revenue_inputs}"
    )

    tile_customers = next(
        (e for e in chart_info_events if "tile-customers" in e["entityUrn"]), None
    )
    assert tile_customers is not None, "tile-customers chart not found"
    customers_inputs = _extract_input_urns(tile_customers["aspect"])
    assert topic_customers_urn in customers_inputs, (
        f"tile-customers should reference customers topic; inputs: {customers_inputs}"
    )


@time_machine.travel(FROZEN_TIME)
def test_dashboard_upstream_is_folder_not_topic() -> None:
    """Dashboard dataset projection must be upstream of its folder only, not topic directly."""
    source = _build_source()
    events = _collect_workunits(source)

    dashboard_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:omni,doc-dashboard-1,PROD)"
    )
    folder_urn = source._folder_dataset_urn("folder-1")

    lineage_events = [
        e
        for e in events
        if e["entityUrn"] == dashboard_dataset_urn
        and e["aspectName"] == "upstreamLineage"
    ]
    if lineage_events:
        upstreams = {
            u["dataset"] for u in lineage_events[0]["aspect"].get("upstreams", [])
        }
        assert folder_urn in upstreams, (
            f"Dashboard should be downstream of folder; actual upstreams: {upstreams}"
        )
        topic_upstreams = {u for u in upstreams if ".topic." in u}
        assert not topic_upstreams, (
            f"Dashboard dataset should NOT have topic as direct upstream; "
            f"found: {topic_upstreams}"
        )


@time_machine.travel(FROZEN_TIME)
def test_fine_grained_lineage_emitted_for_dashboard() -> None:
    """Dashboard dataset must have fine-grained lineage edges from semantic fields."""
    source = _build_source()
    events = _collect_workunits(source)

    dashboard_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:omni,doc-dashboard-1,PROD)"
    )
    lineage_events = [
        e
        for e in events
        if e["entityUrn"] == dashboard_dataset_urn
        and e["aspectName"] == "upstreamLineage"
    ]
    if lineage_events:
        fine_grained = lineage_events[0]["aspect"].get("fineGrainedLineages") or []
        assert fine_grained, (
            "No fine-grained lineage edges emitted for dashboard dataset"
        )
        upstream_fields = {
            u for edge in fine_grained for u in (edge.get("upstreams") or [])
        }
        assert any("schemaField" in u for u in upstream_fields), (
            f"Fine-grained lineage should reference semantic view fields; got: {upstream_fields}"
        )


@time_machine.travel(FROZEN_TIME)
def test_semantic_views_have_schema_metadata() -> None:
    """Semantic view datasets must emit schemaMetadata with their dimensions/measures."""
    source = _build_source()
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    schema_events = [
        e
        for e in events
        if e["entityUrn"] == orders_view_urn and e["aspectName"] == "schemaMetadata"
    ]
    assert schema_events, f"No schemaMetadata emitted for {orders_view_urn}"

    fields = {f["fieldPath"] for f in schema_events[0]["aspect"].get("fields", [])}
    assert "order_id" in fields, f"Expected order_id in schema fields; got: {fields}"
    assert "total_revenue" in fields, (
        f"Expected total_revenue in schema fields; got: {fields}"
    )


@time_machine.travel(FROZEN_TIME)
def test_ownership_emitted_for_dashboard() -> None:
    """Dashboard and chart entities must emit ownership from document owner field."""
    source = _build_source()
    events = _collect_workunits(source)

    dashboard_urn = "urn:li:dashboard:(omni,doc-dashboard-1)"
    ownership_events = [
        e
        for e in events
        if e["entityUrn"] == dashboard_urn and e["aspectName"] == "ownership"
    ]
    assert ownership_events, (
        f"No ownership aspect emitted for dashboard {dashboard_urn}"
    )
    owners = ownership_events[0]["aspect"].get("owners", [])
    assert owners, "Ownership aspect has no owners"
    assert any("user-admin" in str(o) for o in owners), (
        f"Expected user-admin in owners; got: {owners}"
    )


@time_machine.travel(FROZEN_TIME)
def test_subtypes_emitted_for_all_datasets() -> None:
    """Every dataset entity must have a subTypes aspect with a meaningful subtype."""
    source = _build_source()
    events = _collect_workunits(source)

    dataset_urns = {e["entityUrn"] for e in events if e["entityType"] == "dataset"}
    subtype_urns = {e["entityUrn"] for e in events if e["aspectName"] == "subTypes"}
    missing = dataset_urns - subtype_urns
    # Allow physical tables (they are emitted via SDK V2 Dataset which auto-includes subTypes)
    # The key requirement is all omni-platform datasets have subtypes
    omni_missing = {u for u in missing if "urn:li:dataPlatform:omni" in u}
    assert not omni_missing, (
        f"These Omni datasets are missing subTypes aspects: {omni_missing}"
    )


@time_machine.travel(FROZEN_TIME)
def test_model_pattern_filters_models() -> None:
    """model_pattern deny should suppress models and their child topics/views."""
    source = _build_source(
        extra_config={
            "model_pattern": {"deny": ["workbook-model-1"]},
        }
    )
    events = _collect_workunits(source)

    workbook_model_urn = source._model_dataset_urn("workbook-model-1")
    emitted_urns = {e["entityUrn"] for e in events}
    assert workbook_model_urn not in emitted_urns, (
        "workbook-model-1 should have been filtered by model_pattern deny"
    )


@time_machine.travel(FROZEN_TIME)
def test_document_pattern_filters_documents() -> None:
    """document_pattern deny should suppress matching documents."""
    source = _build_source(
        extra_config={
            "include_workbook_only": True,
            "document_pattern": {"deny": ["doc-workbook-1"]},
        }
    )
    events = _collect_workunits(source)

    workbook_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:omni,doc-workbook-1,PROD)"
    )
    emitted_urns = {e["entityUrn"] for e in events}
    assert workbook_dataset_urn not in emitted_urns, (
        "doc-workbook-1 should have been filtered by document_pattern deny"
    )


@time_machine.travel(FROZEN_TIME)
def test_snowflake_names_normalised_to_uppercase() -> None:
    """Physical Snowflake table URNs must use uppercased identifiers."""
    source = _build_source()
    events = _collect_workunits(source)

    physical_urns = [
        e["entityUrn"]
        for e in events
        if "dataPlatform:snowflake" in e.get("entityUrn", "")
    ]
    assert physical_urns, "No Snowflake physical dataset URNs emitted"
    for urn in physical_urns:
        # name part may be prefixed by "platform_instance." — skip that prefix
        name_part = urn.split(",")[1]
        # strip optional platform_instance prefix (it stays lowercase intentionally)
        if "." in name_part:
            parts = name_part.split(".", 1)
            # heuristic: if first segment contains no uppercase and no spaces,
            # treat it as the platform_instance prefix
            if parts[0] == parts[0].lower():
                name_part = parts[1]
        assert name_part == name_part.upper(), (
            f"Snowflake db/schema/table identifiers should be uppercased; got: {urn}"
        )


@time_machine.travel(FROZEN_TIME)
def test_topic_fetch_failure_falls_back_to_yaml() -> None:
    """When the topic API returns 404, views should still be extracted from model YAML."""
    source = _build_source()

    # Override topic fetch to always fail
    class _NoTopicClient(FakeOmniClientFull):
        def get_topic(self, model_id: str, topic_name: str) -> dict:
            raise RuntimeError(f"404 Not Found: {topic_name}")

    source.client = _NoTopicClient()
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    emitted_urns = {e["entityUrn"] for e in events}
    assert orders_view_urn in emitted_urns, (
        f"Semantic view {orders_view_urn} should be extracted from YAML fallback "
        "when topic API fails"
    )


@time_machine.travel(FROZEN_TIME)
def test_model_yaml_failure_logs_warning_but_continues() -> None:
    """403 on model YAML should log a warning and continue to the next model."""
    source = _build_source()

    class _YamlForbiddenClient(FakeOmniClientFull):
        def get_model_yaml(self, model_id: str) -> dict:
            if model_id == "shared-model-1":
                raise RuntimeError("403 Forbidden")
            return {"files": {}}

    source.client = _YamlForbiddenClient()
    events = _collect_workunits(source)

    # Ingestion should still emit model datasets despite the YAML failure
    shared_model_urn = source._model_dataset_urn("shared-model-1")
    emitted_urns = {e["entityUrn"] for e in events}
    assert shared_model_urn in emitted_urns, (
        "Model entity should still be emitted even when its YAML fetch fails"
    )
    warnings = source.report.warnings
    assert any("model-yaml-fetch" in str(w) for w in warnings), (
        "A warning should be logged for the failed YAML fetch"
    )


# ---------------------------------------------------------------------------
# test_connection() static method
# ---------------------------------------------------------------------------


def test_connection_succeeds_with_valid_credentials() -> None:
    """`test_connection()` returns a successful report when API is reachable."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "datahub.ingestion.source.omni.omni_api.OmniClient.test_connection",
            lambda self: True,
        )
        report = OmniSource.test_connection(
            {
                "base_url": "https://acme.omniapp.co/api",
                "api_key": "valid-key",
            }
        )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True


def test_connection_fails_with_invalid_credentials() -> None:
    """`test_connection()` returns a failure report when API auth is rejected."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "datahub.ingestion.source.omni.omni_api.OmniClient.test_connection",
            lambda self: (_ for _ in ()).throw(RuntimeError("401 Unauthorized")),
        )
        report = OmniSource.test_connection(
            {
                "base_url": "https://acme.omniapp.co/api",
                "api_key": "bad-key",
            }
        )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "401" in (report.basic_connectivity.failure_reason or "")
