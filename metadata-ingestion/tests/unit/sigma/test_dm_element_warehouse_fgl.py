"""Unit tests for warehouse-passthrough FineGrainedLineage on DM elements.

Coverage:
  - _try_emit_warehouse_passthrough_fgl: all pre-flight failure modes
  - _try_emit_warehouse_passthrough_fgl: Snowflake and Redshift resolution
  - _try_emit_warehouse_passthrough_fgl: convert_urns_to_lowercase=False override
  - _try_emit_warehouse_passthrough_fgl: dedup via emitted_pairs
  - _build_dm_element_fine_grained_lineages: counter shift (deferred → resolved)
  - _build_dm_element_fine_grained_lineages: diamond case (two downstream columns,
    same upstream schemaField → two FGLs, one upstream)
  - _build_dm_element_fine_grained_lineages: mixed intra-DM + warehouse refs
  - URN identity: schemaField parent Dataset URN matches entity-level warehouse URN

Counters verified:
  data_model_element_fgl_warehouse_resolved
  data_model_element_fgl_warehouse_passthrough_deferred
"""

import datetime as dt
from typing import Dict, List, Optional, Set
from unittest.mock import patch

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import (
    SigmaSourceConfig,
    WarehouseConnectionConfig,
)
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import (
    SigmaSource,
    _WarehouseTableRef,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.com.linkedin.pegasus2avro.dataset import FineGrainedLineageClass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SF_CONN_ID = "conn-sf-001"
_RS_CONN_ID = "conn-rs-001"
_UNMAPPABLE_CONN_ID = "conn-oracle-001"

_SF_RECORD = SigmaConnectionRecord(
    connection_id=_SF_CONN_ID,
    name="Prod Snowflake",
    sigma_type="snowflake",
    datahub_platform="snowflake",
    host="example.snowflakecomputing.com",
    account="example",
    is_mappable=True,
)
_RS_RECORD = SigmaConnectionRecord(
    connection_id=_RS_CONN_ID,
    name="Prod Redshift",
    sigma_type="redshift",
    datahub_platform="redshift",
    host="cluster.redshift.amazonaws.com",
    is_mappable=True,
)
_UNMAPPABLE_RECORD = SigmaConnectionRecord(
    connection_id=_UNMAPPABLE_CONN_ID,
    name="Oracle (unsupported)",
    sigma_type="oracle",
    datahub_platform="",
    is_mappable=False,
)

# Snowflake warehouse table fixture
_SF_URL_ID = "7k3e6T4RK9oix71Nm2umE6"
_SF_INODE_SOURCE = f"inode-{_SF_URL_ID}"
_SF_REF = _WarehouseTableRef(
    connection_id=_SF_CONN_ID,
    db="PROD_DB",
    schema="PUBLIC",
    table="CUSTOMERS",
)
_SF_WAREHOUSE_MAP: Dict[str, _WarehouseTableRef] = {_SF_URL_ID: _SF_REF}

# Expected entity-level URN emitted by the warehouse-upstream resolver
_SF_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod_db.public.customers,PROD)"
)

# Redshift warehouse table fixture
_RS_URL_ID = "3KaiZnkNI1mqAKABVqD6Vy"
_RS_INODE_SOURCE = f"inode-{_RS_URL_ID}"
_RS_REF = _WarehouseTableRef(
    connection_id=_RS_CONN_ID,
    db="analytics",
    schema="demo_schema",
    table="base_table",
)
_RS_WAREHOUSE_MAP: Dict[str, _WarehouseTableRef] = {_RS_URL_ID: _RS_REF}
_RS_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:redshift,"
    "analytics.demo_schema.base_table,PROD)"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(
    extra_records: Optional[List[SigmaConnectionRecord]] = None,
    conn_overrides: Optional[Dict[str, WarehouseConnectionConfig]] = None,
) -> SigmaSource:
    config = SigmaSourceConfig.model_validate(
        {"client_id": "test", "client_secret": "test"}
    )
    if conn_overrides:
        config.connection_to_platform_map = conn_overrides
    ctx = PipelineContext(run_id="t4b2-unit")
    with patch.object(SigmaAPI, "_generate_token"):
        source = SigmaSource(config=config, ctx=ctx)
    records = [_SF_RECORD, _RS_RECORD, _UNMAPPABLE_RECORD] + (extra_records or [])
    source.connection_registry = SigmaConnectionRegistry(
        by_id={r.connection_id: r for r in records}
    )
    return source


def _column(column_id: str, name: str, formula: Optional[str]) -> SigmaDataModelColumn:
    return SigmaDataModelColumn(columnId=column_id, name=name, formula=formula)


def _element(
    element_id: str,
    name: str,
    columns: List[SigmaDataModelColumn],
    source_ids: Optional[List[str]] = None,
) -> SigmaDataModelElement:
    return SigmaDataModelElement(
        elementId=element_id,
        name=name,
        columns=[c.model_dump() for c in columns],
        source_ids=source_ids or [],
    )


def _dm(elements: Optional[List[SigmaDataModelElement]] = None) -> SigmaDataModel:
    now = dt.datetime.now(dt.timezone.utc)
    return SigmaDataModel(
        dataModelId="dm-test",
        name="Test DM",
        createdAt=now,
        updatedAt=now,
        elements=elements or [],
    )


def _build_fgls(
    source: SigmaSource,
    element: SigmaDataModelElement,
    warehouse_map: Optional[Dict[str, _WarehouseTableRef]] = None,
    element_name_to_eids: Optional[Dict[str, List[str]]] = None,
    elementId_to_dataset_urn: Optional[Dict[str, str]] = None,
    entity_level_upstream_urns: Optional[Set[str]] = None,
    upstream_elements: Optional[List[SigmaDataModelElement]] = None,
) -> List[FineGrainedLineageClass]:
    all_elements = [element] + (upstream_elements or [])
    urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{element.elementId},PROD)"
    return source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=urn,
        element_name_to_eids=element_name_to_eids or {},
        elementId_to_dataset_urn=elementId_to_dataset_urn or {},
        entity_level_upstream_urns=entity_level_upstream_urns or set(),
        data_model=_dm(all_elements),
        warehouse_url_id_map=warehouse_map or {},
    )


def _try_emit(
    source: SigmaSource,
    column: SigmaDataModelColumn,
    element: SigmaDataModelElement,
    warehouse_map: Dict[str, _WarehouseTableRef],
    downstream_field: Optional[str] = None,
) -> Optional[FineGrainedLineageClass]:
    elem_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{element.elementId},PROD)"
    return source._try_emit_warehouse_passthrough_fgl(
        column=column,
        element=element,
        downstream_field=downstream_field
        or builder.make_schema_field_urn(elem_urn, column.name),
        warehouse_url_id_map=warehouse_map,
    )


# ---------------------------------------------------------------------------
# Pre-flight failure tests
# ---------------------------------------------------------------------------


class TestTryEmitPreflightFailures:
    def test_column_id_no_inode_prefix(self):
        source = _make_source()
        col = _column("bare-col-id", "email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        assert _try_emit(source, col, elem, _SF_WAREHOUSE_MAP) is None

    def test_column_id_no_slash(self):
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}", "email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        assert _try_emit(source, col, elem, _SF_WAREHOUSE_MAP) is None

    def test_column_id_url_id_not_in_source_ids(self):
        source = _make_source()
        col = _column("inode-OTHER_URL_ID/EMAIL", "email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        assert _try_emit(source, col, elem, _SF_WAREHOUSE_MAP) is None

    def test_url_id_not_in_warehouse_map(self):
        """url_id matches source_ids but /files lookup failed (not in map)."""
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        assert _try_emit(source, col, elem, {}) is None

    def test_unmappable_connection(self):
        source = _make_source()
        url_id = "unmappable-url-id"
        wh_map = {
            url_id: _WarehouseTableRef(
                connection_id=_UNMAPPABLE_CONN_ID,
                db="DB",
                schema="SCH",
                table="TBL",
            )
        }
        col = _column(f"inode-{url_id}/COL", "col", "[TBL/col]")
        elem = _element("el-1", "TBL", [col], [f"inode-{url_id}"])
        assert _try_emit(source, col, elem, wh_map) is None

    def test_connection_not_in_registry(self):
        source = _make_source()
        url_id = "unknown-conn-url"
        wh_map = {
            url_id: _WarehouseTableRef(
                connection_id="conn-not-in-registry",
                db="DB",
                schema="SCH",
                table="TBL",
            )
        }
        col = _column(f"inode-{url_id}/COL", "col", "[TBL/col]")
        elem = _element("el-1", "TBL", [col], [f"inode-{url_id}"])
        assert _try_emit(source, col, elem, wh_map) is None


# ---------------------------------------------------------------------------
# Resolution tests
# ---------------------------------------------------------------------------


class TestTryEmitResolution:
    def test_snowflake_lowercases_column(self):
        """Snowflake columnId is UPPERCASE; emitted schemaField must be lowercase."""
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        result = _try_emit(source, col, elem, _SF_WAREHOUSE_MAP)

        assert result is not None
        assert result.upstreams is not None
        assert len(result.upstreams) == 1
        expected_upstream = builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        assert result.upstreams[0] == expected_upstream

    def test_redshift_preserves_lowercase_column(self):
        """Redshift columnId is already lowercase; no double-lowercasing."""
        source = _make_source()
        col = _column(f"inode-{_RS_URL_ID}/age", "Age", "[base_table/Age]")
        elem = _element("el-1", "base_table", [col], [_RS_INODE_SOURCE])
        result = _try_emit(source, col, elem, _RS_WAREHOUSE_MAP)

        assert result is not None
        assert result.upstreams is not None
        expected_upstream = builder.make_schema_field_urn(_RS_DATASET_URN, "age")
        assert result.upstreams[0] == expected_upstream

    def test_convert_urns_to_lowercase_false_preserves_case(self):
        """When convert_urns_to_lowercase=False, both the dataset and column
        identifiers preserve their original casing from the API."""
        override = WarehouseConnectionConfig.model_validate(
            {"convert_urns_to_lowercase": False}
        )
        source = _make_source(conn_overrides={_SF_CONN_ID: override})
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        result = _try_emit(source, col, elem, _SF_WAREHOUSE_MAP)

        assert result is not None
        assert result.upstreams is not None
        # With lowercase=False the dataset URN also preserves case (UPPERCASE for
        # Snowflake since that's what Sigma's API returns in the /files path).
        uppercase_dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "PROD_DB.PUBLIC.CUSTOMERS,PROD)"
        )
        expected_upstream = builder.make_schema_field_urn(
            uppercase_dataset_urn, "EMAIL"
        )
        assert result.upstreams[0] == expected_upstream


# ---------------------------------------------------------------------------
# Integration with _build_dm_element_fine_grained_lineages
# ---------------------------------------------------------------------------


class TestBuildFglWarehouseIntegration:
    def test_counter_shift_resolved_not_deferred(self):
        """Successful resolution bumps _warehouse_resolved and does NOT bump
        _warehouse_passthrough_deferred."""
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element(
            "el-customers",
            "CUSTOMERS",
            [col],
            [_SF_INODE_SOURCE],
        )
        # element_name_to_eids must include the element itself so the self-strip
        # logic fires (candidate_eids non-empty, all equal element.elementId).
        name_to_eids = {"customers": [elem.elementId]}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids=name_to_eids,
        )

        assert len(fgls) == 1
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )

    def test_deferred_when_no_warehouse_source(self):
        """When the element has no warehouse-backed inode, deferred counter bumps."""
        source = _make_source()
        col = _column("bare-col-id", "Email", "[CUSTOMERS/Email]")
        elem = _element("el-customers", "CUSTOMERS", [col], [])  # no source_ids
        name_to_eids = {"customers": [elem.elementId]}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map={},
            element_name_to_eids=name_to_eids,
        )

        assert fgls == []
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 1
        )
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 0

    def test_diamond_two_downstream_columns_same_upstream(self):
        """Two columns on the same element both reference the same warehouse column.
        Each produces a distinct FGL (different downstream schemaField), but the
        upstream schemaField URN is the same — emitted_pairs must not suppress."""
        source = _make_source()
        col_a = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]")
        col_b = _column(
            f"inode-{_SF_URL_ID}/EMAIL", "Contact Email", "[CUSTOMERS/Email]"
        )
        elem = _element(
            "el-customers",
            "CUSTOMERS",
            [col_a, col_b],
            [_SF_INODE_SOURCE],
        )
        name_to_eids = {"customers": [elem.elementId]}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids=name_to_eids,
        )

        assert len(fgls) == 2
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 2
        upstream_fields = {fgl.upstreams[0] for fgl in fgls if fgl.upstreams}
        # Same upstream schemaField for both
        assert len(upstream_fields) == 1
        downstream_fields = {fgl.downstreams[0] for fgl in fgls if fgl.downstreams}
        assert len(downstream_fields) == 2

    def test_same_ref_repeated_in_formula_deduplicated(self):
        """Multiple bracket refs to the same [CUSTOMERS/Email] in one formula
        produce only one FGL entry (dedup via emitted_pairs)."""
        source = _make_source()
        # Formula references the same column three times
        col = _column(
            f"inode-{_SF_URL_ID}/EMAIL",
            "Email",
            'If([CUSTOMERS/Email] = "", "unknown", [CUSTOMERS/Email])',
        )
        elem = _element(
            "el-customers",
            "CUSTOMERS",
            [col],
            [_SF_INODE_SOURCE],
        )
        name_to_eids = {"customers": [elem.elementId]}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids=name_to_eids,
        )

        assert len(fgls) == 1
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1
        # Dedup must not inflate _passthrough_deferred.
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )

    def test_mixed_intra_dm_and_warehouse_refs(self):
        """An element with one intra-DM ref and one warehouse-passthrough ref
        produces both an intra-DM FGL and a warehouse FGL."""
        source = _make_source()
        # Upstream intra-DM element
        upstream_id = "el-upstream"
        upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,el-upstream,PROD)"
        upstream_elem = _element(
            upstream_id,
            "UPSTREAM",
            [_column("up-x", "x", None)],
        )
        # Column a: intra-DM ref → [UPSTREAM/x]
        col_intra = _column("col-a", "a", "[UPSTREAM/x]")
        # Column b: warehouse-passthrough → [CUSTOMERS/Email]
        col_wh = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element(
            "el-customers",
            "CUSTOMERS",
            [col_intra, col_wh],
            [_SF_INODE_SOURCE],
        )
        name_to_eids = {
            "customers": [elem.elementId],
            "upstream": [upstream_id],
        }
        id_to_urn = {upstream_id: upstream_urn}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids=name_to_eids,
            elementId_to_dataset_urn=id_to_urn,
            entity_level_upstream_urns={upstream_urn},
            upstream_elements=[upstream_elem],
        )

        # One intra-DM FGL + one warehouse FGL
        assert len(fgls) == 2
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1
        # data_model_element_fgl_emitted counts ALL FGLs (intra-DM + warehouse)
        assert source.reporter.data_model_element_fgl_emitted == 2

    def test_multiple_columns_all_resolved(self):
        """All columns on a warehouse-passthrough element emit FGL entries."""
        source = _make_source()
        columns = [
            _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]"),
            _column(
                f"inode-{_SF_URL_ID}/FIRST_NAME",
                "First Name",
                "[CUSTOMERS/First Name]",
            ),
            _column(
                f"inode-{_SF_URL_ID}/CUSTOMER_ID",
                "Customer Id",
                "[CUSTOMERS/Customer Id]",
            ),
        ]
        elem = _element(
            "el-customers",
            "CUSTOMERS",
            columns,
            [_SF_INODE_SOURCE],
        )
        name_to_eids = {"customers": [elem.elementId]}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids=name_to_eids,
        )

        assert len(fgls) == 3
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 3
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )
        # Upstream column names are lowercased (Snowflake)
        upstream_cols = {
            fgl.upstreams[0].rsplit(",", 1)[-1].rstrip(")")
            for fgl in fgls
            if fgl.upstreams
        }
        assert upstream_cols == {"email", "first_name", "customer_id"}

    def test_formula_source_is_warehouse_table_name_not_element_name(self):
        """Element named differently from its warehouse table still resolves
        via columnId when formula uses the warehouse table name as source.

        e.g. element "CUSTOMER Summary" with formula "[CUSTOMERS/Email]" where
        "CUSTOMERS" is the warehouse table name, not the element name.
        """
        source = _make_source()
        col = _column(
            f"inode-{_SF_URL_ID}/EMAIL",
            "Email",
            "[CUSTOMERS/Email]",  # source = warehouse table name, not element name
        )
        elem = _element(
            "el-summary",
            "CUSTOMER Summary",  # element name differs from formula source
            [col],
            [_SF_INODE_SOURCE],
        )
        # No DM element named "CUSTOMERS" exists — candidate_eids will be empty
        name_to_eids = {"customer summary": [elem.elementId]}

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids=name_to_eids,
        )

        assert len(fgls) == 1
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1
        assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
        assert fgls[0].upstreams is not None
        expected_upstream = builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        assert fgls[0].upstreams[0] == expected_upstream
