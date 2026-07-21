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

import pathlib
from typing import Any, Dict, List

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.omni.omni import OmniSource
from datahub.ingestion.source.omni.omni_config import OmniSourceConfig
from datahub.testing import mce_helpers
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
    source.client = FakeOmniClientFull()  # type: ignore[assignment]
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
                "aspect": aspect.to_obj()
                if aspect is not None and hasattr(aspect, "to_obj")
                else {},
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
    output_path = tmp_path / "omni_mces.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "omni",
                "config": {
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
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    # inject fake client before running
    assert isinstance(pipeline.source, OmniSource)
    pipeline.source.client = FakeOmniClientFull()  # type: ignore[assignment]
    pipeline.run()
    pipeline.raise_from_status()

    # Full JSON diff — every field in every aspect is compared
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_FILE,
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
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
def test_topics_emit_stale_edge_clear_before_real_lineage() -> None:
    """Topics must emit an empty upstreamLineage first to clear stale edges."""
    source = _build_source()
    events = _collect_workunits(source)

    # Group upstreamLineage MCPs by topic URN, preserving order
    topic_lineage: Dict[str, list] = {}
    for e in events:
        if ".topic." in e.get("entityUrn", "") and e["aspectName"] == "upstreamLineage":
            topic_lineage.setdefault(e["entityUrn"], []).append(e)

    assert topic_lineage, "No upstreamLineage aspect found on topic entities"
    for urn, lineage_events in topic_lineage.items():
        # First MCP should be the stale edge clear (empty upstreams)
        assert lineage_events[0]["aspect"].get("upstreams", []) == [], (
            f"Topic {urn} should emit empty upstreams first to clear stale edges"
        )
        # Second MCP should have real view upstreams
        assert len(lineage_events) >= 2, (
            f"Topic {urn} should emit view upstreams after the clear"
        )
        assert lineage_events[-1]["aspect"].get("upstreams", []) != [], (
            f"Topic {urn} should have view upstreams in final lineage"
        )


@time_machine.travel(FROZEN_TIME)
def test_topics_list_views_as_upstreams() -> None:
    """Topics curate views — each topic must list its views as upstreams."""
    source = _build_source()
    events = _collect_workunits(source)

    topic_urn_orders = source._topic_dataset_urn("shared-model-1", "orders")
    topic_urn_customers = source._topic_dataset_urn("shared-model-1", "customers")

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    order_items_view_urn = source._semantic_dataset_urn("shared-model-1", "order_items")
    customers_view_urn = source._semantic_dataset_urn("shared-model-1", "customers")

    def _upstreams_of(entity_urn: str) -> set:
        result: set = set()
        for e in events:
            if e["entityUrn"] == entity_urn and e["aspectName"] == "upstreamLineage":
                result.update(u["dataset"] for u in e["aspect"].get("upstreams", []))
        return result

    orders_topic_upstreams = _upstreams_of(topic_urn_orders)
    assert orders_view_urn in orders_topic_upstreams, (
        f"orders topic should list orders view as upstream; "
        f"actual upstreams: {orders_topic_upstreams}"
    )
    assert order_items_view_urn in orders_topic_upstreams, (
        f"orders topic should list order_items view as upstream; "
        f"actual upstreams: {orders_topic_upstreams}"
    )

    customers_topic_upstreams = _upstreams_of(topic_urn_customers)
    assert customers_view_urn in customers_topic_upstreams, (
        f"customers topic should list customers view as upstream; "
        f"actual upstreams: {customers_topic_upstreams}"
    )


@time_machine.travel(FROZEN_TIME)
def test_semantic_views_upstream_of_physical_tables() -> None:
    """Semantic views must list their physical source tables as upstreams."""
    source = _build_source()
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    orders_physical_urn = source._physical_dataset_urn(
        "snowflake", "ANALYTICS_PROD", "PUBLIC", "ORDERS", platform_instance="snowflake"
    )

    view_lineage = [
        e
        for e in events
        if e["entityUrn"] == orders_view_urn and e["aspectName"] == "upstreamLineage"
    ]
    assert view_lineage, (
        f"No upstreamLineage emitted for semantic view {orders_view_urn}"
    )
    upstreams = {u["dataset"] for u in view_lineage[-1]["aspect"].get("upstreams", [])}
    assert orders_physical_urn in upstreams, (
        f"Semantic orders view should list physical ORDERS table as upstream; "
        f"actual upstreams: {upstreams}"
    )


@time_machine.travel(FROZEN_TIME)
def test_view_to_physical_table_column_lineage() -> None:
    """Semantic view → physical table lineage emits edges only for passthrough fields.

    The orders view has dimensions (order_id, customer_id, created_at) which are
    passthrough and should produce edges. The measure (total_revenue = SUM(amount))
    is computed and must be skipped — ``total_revenue`` does not exist on the
    physical table.
    """
    source = _build_source()
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    orders_physical_urn = source._physical_dataset_urn(
        "snowflake", "ANALYTICS_PROD", "PUBLIC", "ORDERS", platform_instance="snowflake"
    )

    view_lineage = [
        e
        for e in events
        if e["entityUrn"] == orders_view_urn and e["aspectName"] == "upstreamLineage"
    ]
    assert view_lineage, f"No upstreamLineage emitted for {orders_view_urn}"

    fine_grained = view_lineage[-1]["aspect"].get("fineGrainedLineages") or []
    assert len(fine_grained) == 3, (
        f"Expected 3 fine-grained edges (3 passthrough dimensions, measure skipped); "
        f"got {len(fine_grained)}"
    )

    edges = {(edge["upstreams"][0], edge["downstreams"][0]) for edge in fine_grained}
    passthrough_fields = ["created_at", "customer_id", "order_id"]
    expected_edges = {
        (
            f"urn:li:schemaField:({orders_physical_urn},{field})",
            f"urn:li:schemaField:({orders_view_urn},{field})",
        )
        for field in passthrough_fields
    }
    assert edges == expected_edges, (
        f"View → physical column lineage mismatch.\n"
        f"Missing: {expected_edges - edges}\n"
        f"Extra: {edges - expected_edges}"
    )


@time_machine.travel(FROZEN_TIME)
def test_computed_measures_skipped_in_view_physical_lineage() -> None:
    """Computed measures (with SQL expressions) must not produce phantom edges.

    total_revenue = SUM(amount) should not create a physical.total_revenue edge
    because that column does not exist on the physical table.
    """
    source = _build_source()
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    orders_physical_urn = source._physical_dataset_urn(
        "snowflake", "ANALYTICS_PROD", "PUBLIC", "ORDERS", platform_instance="snowflake"
    )

    view_lineage = [
        e
        for e in events
        if e["entityUrn"] == orders_view_urn and e["aspectName"] == "upstreamLineage"
    ]
    fine_grained = view_lineage[-1]["aspect"].get("fineGrainedLineages") or []

    phantom_urn = f"urn:li:schemaField:({orders_physical_urn},total_revenue)"
    upstream_urns = {edge["upstreams"][0] for edge in fine_grained}
    assert phantom_urn not in upstream_urns, (
        "Computed measure total_revenue should not produce a physical column edge"
    )

    assert source.report.view_to_physical_column_lineage_skipped_computed > 0


@time_machine.travel(FROZEN_TIME)
def test_view_to_physical_column_lineage_disabled_when_flag_off() -> None:
    """No fine-grained lineage on view → physical when include_column_lineage=False."""
    source = _build_source(extra_config={"include_column_lineage": False})
    events = _collect_workunits(source)

    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    view_lineage = [
        e
        for e in events
        if e["entityUrn"] == orders_view_urn and e["aspectName"] == "upstreamLineage"
    ]
    assert view_lineage, f"No upstreamLineage emitted for {orders_view_urn}"
    fine_grained = view_lineage[-1]["aspect"].get("fineGrainedLineages") or []
    assert not fine_grained, (
        f"Fine-grained lineage should be empty when include_column_lineage=False; "
        f"got {len(fine_grained)} edges"
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
    """Dashboard must have fine-grained lineage mapping semantic view fields to dashboard fields.

    Tile "Revenue by Month" references orders.created_at and orders.total_revenue.
    Tile "Top Customers" references customers.customer_id and customers.lifetime_value.
    Each produces a fine-grained edge: upstream = semantic view schemaField,
    downstream = dashboard schemaField named <view>.<field>.
    """
    source = _build_source()
    events = _collect_workunits(source)

    dashboard_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:omni,doc-dashboard-1,PROD)"
    )
    orders_view_urn = source._semantic_dataset_urn("shared-model-1", "orders")
    customers_view_urn = source._semantic_dataset_urn("shared-model-1", "customers")

    lineage_events = [
        e
        for e in events
        if e["entityUrn"] == dashboard_dataset_urn
        and e["aspectName"] == "upstreamLineage"
    ]
    assert lineage_events, "No upstreamLineage emitted for dashboard dataset"

    fine_grained = lineage_events[0]["aspect"].get("fineGrainedLineages") or []
    assert len(fine_grained) == 4, (
        f"Expected 4 fine-grained lineage edges (one per dashboard field); got {len(fine_grained)}"
    )

    # Collect all edges as (upstream_urn, downstream_urn) pairs
    edges = {(edge["upstreams"][0], edge["downstreams"][0]) for edge in fine_grained}

    # Verify each semantic view field maps to its dashboard field
    expected_edges = {
        (
            f"urn:li:schemaField:({orders_view_urn},created_at)",
            f"urn:li:schemaField:({dashboard_dataset_urn},orders.created_at)",
        ),
        (
            f"urn:li:schemaField:({orders_view_urn},total_revenue)",
            f"urn:li:schemaField:({dashboard_dataset_urn},orders.total_revenue)",
        ),
        (
            f"urn:li:schemaField:({customers_view_urn},customer_id)",
            f"urn:li:schemaField:({dashboard_dataset_urn},customers.customer_id)",
        ),
        (
            f"urn:li:schemaField:({customers_view_urn},lifetime_value)",
            f"urn:li:schemaField:({dashboard_dataset_urn},customers.lifetime_value)",
        ),
    }
    assert edges == expected_edges, (
        f"Fine-grained lineage edges don't match expected.\n"
        f"Missing: {expected_edges - edges}\n"
        f"Extra: {edges - expected_edges}"
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
    """Physical Snowflake table URNs in lineage must use uppercased identifiers.

    Physical datasets are not emitted as entities (BI connector only references them
    via lineage), so we check the upstream lineage of semantic views.
    """
    source = _build_source()
    events = _collect_workunits(source)

    # Collect all Snowflake physical URNs from upstream lineage
    physical_urns = set()
    for e in events:
        if e["aspectName"] == "upstreamLineage":
            upstreams = e["aspect"].get("upstreams", [])
            for upstream in upstreams:
                urn = upstream.get("dataset", "")
                if "dataPlatform:snowflake" in urn:
                    physical_urns.add(urn)

    assert physical_urns, "No Snowflake physical dataset URNs in lineage"
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


# ---------------------------------------------------------------------------
# test_connection() static method
# ---------------------------------------------------------------------------


def test_connection_succeeds_with_valid_credentials() -> None:
    """`test_connection()` returns a successful report when API is reachable."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "datahub.ingestion.source.omni.omni_api.OmniClient.test_connection",
            lambda self: None,
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
