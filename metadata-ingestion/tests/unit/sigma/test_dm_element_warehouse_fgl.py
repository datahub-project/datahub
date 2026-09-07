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
from unittest.mock import MagicMock, patch

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
from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs
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

_DOWNSTREAM_FIELD = builder.make_schema_field_urn(
    "urn:li:dataset:(urn:li:dataPlatform:sigma,e1,PROD)", "some_column"
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
        discovered_upstreams=set(),
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


class TestNoBracketRefWarehouseFgl:
    """Columns whose formula yields no bracket refs.

    Sigma returns ``formula: ""`` for a plain pass-through column, and a constant
    expression parses to zero refs. Both cases must still resolve warehouse
    lineage from ``columnId``, which needs no formula.
    """

    def test_empty_formula_emits_warehouse_fgl(self):
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "")
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1
        assert source.reporter.data_model_element_fgl_no_ref_warehouse_unresolved == 0

    def test_none_formula_emits_warehouse_fgl(self):
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", None)
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1

    def test_constant_formula_emits_warehouse_fgl(self):
        """A non-empty formula with no bracket refs takes the same path."""
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", '"n/a"')
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1

    def test_no_formula_and_failed_warehouse_resolve_is_counted(self):
        """Empty formula + inode columnId but no warehouse map: counted, not silent.

        The counter must not leak into _passthrough_deferred, which stays a
        formula-bearing failure signal so its baseline remains comparable.
        """
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "")
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map={})

        assert fgls == []
        # inode columnId that failed to resolve is actionable, so it must NOT
        # land in the expected-volume bucket.
        assert source.reporter.data_model_element_fgl_no_ref_warehouse_unresolved == 1
        assert source.reporter.data_model_element_fgl_no_ref_unresolved == 0
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 0

    def test_intra_dm_ref_does_not_also_emit_warehouse_edge(self):
        """The post-loop append is gated on zero refs, not on warehouse_consumed.

        One column carries BOTH an intra-DM formula ref and an inode columnId that
        would resolve. Only the intra-DM edge is correct: gating on
        "no warehouse append happened" would add a second, wrong upstream, because
        intra-DM resolution never sets warehouse_consumed.
        """
        source = _make_source()
        upstream_id = "el-upstream"
        upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,el-upstream,PROD)"
        upstream_elem = _element(upstream_id, "UPSTREAM", [_column("up-x", "x", None)])
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[UPSTREAM/x]")
        elem = _element(
            "el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE, upstream_id]
        )

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={
                "customers": [elem.elementId],
                "upstream": [upstream_id],
            },
            elementId_to_dataset_urn={upstream_id: upstream_urn},
            entity_level_upstream_urns={upstream_urn, _SF_DATASET_URN},
            upstream_elements=[upstream_elem],
        )

        assert len(fgls) == 1
        assert fgls[0].upstreams == [builder.make_schema_field_urn(upstream_urn, "x")]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 0

    def test_parameter_only_formula_still_resolves_warehouse(self):
        """Refs that all skip resolution are the same blind spot as no refs.

        `[P_Region]` parses to a ref, so a `not refs` gate would leave the
        columnId-driven warehouse edge computed and discarded with no counter.
        """
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[P_Region]")
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1

    def test_bare_sibling_ref_formula_still_resolves_warehouse(self):
        """A bare `[col]` intra-element ref skips resolution the same way."""
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "Upper([Other Column])")
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1

    def test_transitively_sourced_element_accepts_warehouse_column(self):
        """Element declaring only cross-DM sources still resolves its columns.

        The warehouse table is declared by the producer element in the other
        Data Model, so requiring the inode in THIS element's source_ids rejects
        every column -- the same shape of mistake as gating join-chain
        resolution on Sigma's direct /lineage list.
        """
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "")
        elem = _element(
            "el-consumer",
            "Consumer",
            [col],
            ["producer-dm/suffix"],  # cross-DM only: no inode declared
        )

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]
        assert source.reporter.dm_element_warehouse_transitive_inode_accepted == 1

    def test_element_declaring_a_different_inode_is_still_rejected(self):
        """Genuine payload drift must stay rejected.

        The element declares its own inode and the column names a different
        one -- that is not transitive sourcing, so the guard still applies.
        """
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "")
        elem = _element("el-x", "X", [col], [_RS_INODE_SOURCE])

        fgls = _build_fgls(
            source, elem, warehouse_map={**_SF_WAREHOUSE_MAP, **_RS_WAREHOUSE_MAP}
        )

        assert fgls == []
        assert source.reporter.dm_element_warehouse_transitive_inode_accepted == 0
        assert (
            source.reporter.warehouse_passthrough_miss_reasons.get(
                "url_id_not_in_element_source_ids"
            )
            == 1
        )

    def test_ref_naming_warehouse_table_resolves_with_opaque_column_id(self):
        """The observed 'Dim mapping' shape: opaque columnId, table-named ref.

        Every column carries an opaque columnId and a formula
        '[WAREHOUSE_TABLE_A/Col Id]'. The columnId path
        cannot help (nothing to parse) and no element bears the table's name, so
        these refs previously resolved to nothing at all.
        """
        source = _make_source()
        col = _column("opaque-col-1", "Customer Id", "[CUSTOMERS/Customer Id]")
        elem = _element("el-mapping", "Dim mapping", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert len(fgls) == 1
        # Display name "Customer Id" -> native CUSTOMER_ID -> snowflake lowercase.
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "customer_id")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 1
        # Reduced confidence: the table match is exact, the column name inferred.
        assert fgls[0].confidenceScore == 0.5

    def test_element_named_after_its_own_table_resolves_by_name(self):
        """A single-element DM named after the warehouse table it reads.

        Every column's formula names the element's own name, so every intra-DM
        candidate is a self-reference. That branch used to dead-end: with no
        cross-DM sources and a columnId that is not ``inode-<urlId>/<NATIVE>``
        there was no pre-built warehouse FGL, and the ref was dropped -- the
        Data Model emitted table-level lineage and no column lineage at all,
        which is exactly what a customer reported.
        """
        source = _make_source()
        col = _column("el-self/CUSTOMER_ID", "Customer Id", "[CUSTOMERS/Customer Id]")
        # The element's NAME is the table name -- that is what makes the ref
        # look like a self-reference.
        elem = _element("el-self", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            # Without this the ref does not look like a self-reference and the
            # test passes for the wrong reason, via the no-candidate branch.
            element_name_to_eids={"customers": ["el-self"]},
        )

        assert len(fgls) == 1
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "customer_id")
        ]
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 1
        assert source.reporter.data_model_element_fgl_self_named_no_passthrough == 1
        # The columnId carried the native name, so nothing was inferred.
        assert fgls[0].confidenceScore == 1.0

    def test_columnid_native_name_beats_the_display_name_convention(self):
        """A renamed column breaks the display-name convention silently.

        "Cust ID" would derive CUST_ID, which the table does not have, while
        the columnId already states the real name. Reading it also means the
        edge is exact rather than inferred, so it is not scored down.
        """
        source = _make_source()
        col = _column("el-self/CUSTOMER_ID", "Cust ID", "[CUSTOMERS/Cust ID]")
        elem = _element("el-self", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={"customers": ["el-self"]},
        )

        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "customer_id")
        ]
        assert fgls[0].confidenceScore == 1.0

    def test_opaque_column_id_still_falls_back_to_the_display_name(self):
        """With no native name to read, the inference stays -- and stays 0.5."""
        source = _make_source()
        col = _column("opaque-col-9", "Customer Id", "[CUSTOMERS/Customer Id]")
        elem = _element("el-self", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={"customers": ["el-self"]},
        )

        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "customer_id")
        ]
        assert fgls[0].confidenceScore == 0.5

    def test_self_named_element_still_prefers_the_columnid_derived_edge(self):
        """The name-derived edge is a fallback, never a replacement.

        With an inode-shaped columnId the warehouse FGL is exact, so it must
        win and the lower-confidence name-derived path must not also fire.
        """
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element("el-self", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={"customers": ["el-self"]},
        )

        assert len(fgls) == 1
        assert fgls[0].confidenceScore == 1.0
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 0

    def test_ref_naming_undeclared_warehouse_table_is_not_resolved(self):
        """Only tables the element actually declares may be matched."""
        source = _make_source()
        col = _column("opaque", "Email", "[SOME_OTHER_TABLE/Email]")
        elem = _element("el-x", "X", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert fgls == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 0


class TestDirectWarehouseUrlIdLookup:
    """Recovery when a Data Model's /lineage omits a table its elements use.

    Sigma reports fewer type=table rows than its own elements reference, so the
    url_id is asked for directly via /v2/files/{urlId}. A 404 there is decisive
    rather than inconclusive: the referenced table has been deleted from Sigma,
    and no lookup can supply coordinates for an object that no longer exists.
    """

    def _source_with_file(self, entry):
        source = _make_source()
        source.sigma_api = MagicMock()
        source.sigma_api.get_file_metadata_by_url_id.return_value = entry
        return source

    def test_url_id_absent_from_dm_map_is_recovered(self):
        source = self._source_with_file(
            {
                "urlId": "missingUrlId",
                "id": "inode-uuid",
                "name": "CUSTOMERS",
                "path": "Connection Root/PROD_DB/PUBLIC",
            }
        )
        # The DM map holds a different table, from a single known connection.
        urn = source._resolve_dm_element_warehouse_upstream(
            url_id_suffix="missingUrlId", warehouse_map=_SF_WAREHOUSE_MAP
        )
        assert urn == _SF_DATASET_URN
        assert source.reporter.dm_element_warehouse_recovered_by_url_id_lookup == 1

    def test_unknown_url_id_is_reported_as_unresolvable(self):
        source = self._source_with_file(None)
        assert (
            source._resolve_dm_element_warehouse_upstream(
                url_id_suffix="neverListed", warehouse_map=_SF_WAREHOUSE_MAP
            )
            is None
        )
        assert source.reporter.dm_element_warehouse_url_id_unresolvable == 1
        assert source.reporter.dm_element_warehouse_recovered_by_url_id_lookup == 0

    def test_repeated_url_id_costs_one_call(self):
        source = self._source_with_file(None)
        for _ in range(3):
            source._resolve_dm_element_warehouse_upstream(
                url_id_suffix="sameOne", warehouse_map=_SF_WAREHOUSE_MAP
            )
        assert source.sigma_api.get_file_metadata_by_url_id.call_count == 1

    def test_ambiguous_connection_refuses_to_guess(self):
        """Two connections in the DM map: attributing the table would be a guess."""
        source = self._source_with_file(
            {"urlId": "u", "id": "i", "name": "T", "path": "Connection Root/D/S"}
        )
        mixed = {**_SF_WAREHOUSE_MAP, **_RS_WAREHOUSE_MAP}
        assert (
            source._resolve_dm_element_warehouse_upstream(
                url_id_suffix="u", warehouse_map=mixed
            )
            is None
        )
        assert source.reporter.dm_element_warehouse_connection_ambiguous == 1
        source.sigma_api.get_file_metadata_by_url_id.assert_not_called()


class TestGlobalWarehouseNameIndex:
    """A formula names a warehouse table nothing in the Data Model declares.

    Sigma under-reports an element's tables the same way it under-reports a
    Data Model's: an element can declare inode A while its formula references
    table B, with B appearing in neither the element's source_ids nor the Data
    Model's /lineage. The tenant-wide /v2/files listing is the only place B is
    described, so it is consulted by NAME -- but only as a last resort, and only
    when the match is unambiguous.
    """

    def _source_with_files(self, entries):
        source = _make_source()
        source.sigma_api = MagicMock()
        source.sigma_api.list_warehouse_table_files.return_value = entries
        return source

    def _resolve(self, source, *, ref_source, ref_column, source_ids, allow=True):
        fgls: List[FineGrainedLineageClass] = []
        resolved = source._try_resolve_warehouse_table_name_ref(
            ref=extract_bracket_refs(f"[{ref_source}/{ref_column}]")[0],
            element=_element("e1", "Some Element", [], source_ids=source_ids),
            # No column: this helper exercises TABLE resolution, so the native
            # column name has to come from the display name.
            column=None,
            downstream_field=_DOWNSTREAM_FIELD,
            warehouse_url_id_map=_SF_WAREHOUSE_MAP,
            emitted_pairs=set(),
            fgls=fgls,
            allow_global_name_index=allow,
        )
        return resolved, fgls

    def test_undeclared_table_resolved_by_name(self):
        source = self._source_with_files(
            [
                {
                    "urlId": "otherUrlId",
                    "id": "inode-other",
                    "name": "ORDERS",
                    "path": "Connection Root/PROD_DB/PUBLIC",
                }
            ]
        )
        resolved, fgls = self._resolve(
            source,
            ref_source="ORDERS",
            ref_column="Order Id",
            # Declares a DIFFERENT table -- the whole point of the fallback.
            source_ids=[_SF_INODE_SOURCE],
        )
        assert resolved
        assert fgls[0].upstreams == [
            builder.make_schema_field_urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
                "prod_db.public.orders,PROD)",
                "order_id",
            )
        ]
        # Both table and column are inferred, so this scores below a declared
        # table's name-derived edge (0.5).
        assert fgls[0].confidenceScore == 0.3
        assert source.reporter.dm_element_warehouse_name_index_resolved == 1
        assert (
            source.reporter.data_model_element_fgl_warehouse_global_name_resolved == 1
        )

    def test_same_name_in_two_schemas_is_refused(self):
        source = self._source_with_files(
            [
                {
                    "urlId": "a",
                    "id": "i1",
                    "name": "ORDERS",
                    "path": "Connection Root/PROD_DB/SALES",
                },
                {
                    "urlId": "b",
                    "id": "i2",
                    "name": "ORDERS",
                    "path": "Connection Root/PROD_DB/MARKETING",
                },
            ]
        )
        resolved, fgls = self._resolve(
            source,
            ref_source="ORDERS",
            ref_column="Order Id",
            source_ids=[_SF_INODE_SOURCE],
        )
        assert not resolved
        assert fgls == []
        assert source.reporter.dm_element_warehouse_name_index_ambiguous == 1

    def test_collision_broken_by_the_data_models_own_schema(self):
        """One of the same-named tables sits in a schema this DM demonstrably reads."""
        source = self._source_with_files(
            [
                {
                    "urlId": "a",
                    "id": "i1",
                    "name": "ORDERS",
                    # PROD_DB/PUBLIC is where _SF_WAREHOUSE_MAP's table lives.
                    "path": "Connection Root/PROD_DB/PUBLIC",
                },
                {
                    "urlId": "b",
                    "id": "i2",
                    "name": "ORDERS",
                    "path": "Connection Root/OTHER_DB/MARKETING",
                },
            ]
        )
        resolved, fgls = self._resolve(
            source,
            ref_source="ORDERS",
            ref_column="Order Id",
            source_ids=[_SF_INODE_SOURCE],
        )
        assert resolved
        assert "prod_db.public.orders" in fgls[0].upstreams[0]
        assert source.reporter.dm_element_warehouse_name_index_resolved == 1

    def test_element_with_no_inode_never_triggers_the_listing(self):
        """An element reading only from other Data Models is not a warehouse reader."""
        source = self._source_with_files(
            [
                {
                    "urlId": "a",
                    "id": "i1",
                    "name": "ORDERS",
                    "path": "Connection Root/PROD_DB/PUBLIC",
                }
            ]
        )
        resolved, _ = self._resolve(
            source,
            ref_source="ORDERS",
            ref_column="Order Id",
            source_ids=["otherDmUrlId/element-1"],
        )
        assert not resolved
        source.sigma_api.list_warehouse_table_files.assert_not_called()

    def test_disabled_flag_skips_the_index_entirely(self):
        """The caller tries every exact path before paying for the listing."""
        source = self._source_with_files(
            [
                {
                    "urlId": "a",
                    "id": "i1",
                    "name": "ORDERS",
                    "path": "Connection Root/PROD_DB/PUBLIC",
                }
            ]
        )
        resolved, _ = self._resolve(
            source,
            ref_source="ORDERS",
            ref_column="Order Id",
            source_ids=[_SF_INODE_SOURCE],
            allow=False,
        )
        assert not resolved
        source.sigma_api.list_warehouse_table_files.assert_not_called()

    def test_unlisted_table_counted_as_a_miss(self):
        source = self._source_with_files([])
        resolved, _ = self._resolve(
            source,
            ref_source="NOT_A_TABLE",
            ref_column="Some Column",
            source_ids=[_SF_INODE_SOURCE],
        )
        assert not resolved
        assert source.reporter.dm_element_warehouse_name_index_miss == 1


class TestColumnLevelDirectLookupRecovery:
    """A column's table missing from the DM map must still reach /files/{urlId}.

    The recovery was originally wired only into the entity-level upstream path,
    so a Data Model gained a table-level edge while the column that motivated
    the lookup stayed unresolved. On one tenant that left 1,305 columns behind.
    """

    def test_column_resolves_via_direct_lookup_when_map_lacks_the_url_id(self):
        source = _make_source()
        source.sigma_api = MagicMock()
        source.sigma_api.get_file_metadata_by_url_id.return_value = {
            "urlId": "otherUrlId",
            "id": "inode-other",
            "name": "ORDERS",
            "path": "Connection Root/PROD_DB/PUBLIC",
        }
        col = _column("inode-otherUrlId/ORDER_ID", "Order Id", None)
        elem = _element("e1", "E1", [col], ["inode-otherUrlId"])
        fgl = _try_emit(source, col, elem, _SF_WAREHOUSE_MAP)
        assert fgl is not None
        assert fgl.upstreams == [
            builder.make_schema_field_urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
                "prod_db.public.orders,PROD)",
                "order_id",
            )
        ]
        assert source.reporter.dm_element_warehouse_column_recovered_by_lookup == 1

    def test_unresolvable_url_id_still_counted_as_a_map_miss(self):
        source = _make_source()
        source.sigma_api = MagicMock()
        source.sigma_api.get_file_metadata_by_url_id.return_value = None
        col = _column("inode-nope/ORDER_ID", "Order Id", None)
        elem = _element("e1", "E1", [col], ["inode-nope"])
        assert _try_emit(source, col, elem, _SF_WAREHOUSE_MAP) is None
        assert (
            source.reporter.warehouse_passthrough_miss_reasons[
                "url_id_not_in_warehouse_map"
            ]
            == 1
        )
