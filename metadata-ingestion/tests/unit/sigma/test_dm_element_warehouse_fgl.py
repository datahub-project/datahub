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
  - _build_dm_element_fine_grained_lineages: a column with no ref to follow
    (only an empty formula gets the columnId edge)
  - _try_emit_warehouse_table_name_fgl: a ref naming a warehouse table the
    element reads, for a column with no inode columnId

Counters verified:
  data_model_element_fgl_warehouse_resolved
  data_model_element_fgl_warehouse_passthrough_deferred
  data_model_element_fgl_warehouse_table_name_resolved
  data_model_element_fgl_warehouse_table_name_declined
"""

import datetime as dt
from typing import Dict, List, Optional, Set
from unittest.mock import patch

import pytest

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
    ConnectionPath,
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
_SF_INODE_ID = "f09fe362-828a-42e6-9f8f-3f0feeb2fb3e"
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

    def test_url_id_in_neither_the_map_nor_files(self):
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        with patch.object(source.sigma_api, "get_file_metadata", return_value=None):
            assert _try_emit(source, col, elem, {}) is None

    def _try_emit_via_files(
        self, source: SigmaSource, connection_id: str
    ) -> Optional[FineGrainedLineageClass]:
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "email", "[CUSTOMERS/Email]")
        elem = _element("el-1", "CUSTOMERS", [col], [_SF_INODE_SOURCE])
        entry = {"id": _SF_INODE_ID, "type": "table", "name": "CUSTOMERS"}
        conn_path = ConnectionPath(
            connection_id=connection_id, path=["PROD_DB", "PUBLIC", "CUSTOMERS"]
        )
        with (
            patch.object(source.sigma_api, "get_file_metadata", return_value=entry),
            patch.object(
                source.sigma_api, "get_connection_path", return_value=conn_path
            ),
        ):
            return _try_emit(source, col, elem, {})

    def test_url_id_missing_from_the_map_is_found_via_files(self):
        fgl = self._try_emit_via_files(_make_source(), _SF_CONN_ID)
        assert fgl is not None
        assert fgl.upstreams == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "email")
        ]

    def test_url_id_missing_from_the_map_on_an_unmappable_connection(self):
        # The mappable Snowflake connection in the registry is not borrowed.
        assert self._try_emit_via_files(_make_source(), _UNMAPPABLE_CONN_ID) is None

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


# An empty formula, a formula whose source is the warehouse table rather than
# an element, and one naming the element itself; all read the columnId and
# must count it the same way.
_FORMULAS_READING_THE_COLUMN_ID = [
    pytest.param(None, id="empty-formula"),
    pytest.param("[CUSTOMERS_TABLE/Email]", id="table-formula"),
    pytest.param("[Customers/Email]", id="self-named-formula"),
]
_SELF_NAME_TO_EIDS = {"customers": ["el-customers"]}


class TestColumnWithNoRefToFollow:
    """A column with an empty formula gets its edge from columnId alone."""

    @pytest.mark.parametrize(
        "formula", [None, "", "  "], ids=["absent", "empty", "whitespace"]
    )
    def test_an_inode_column_id_alone_gives_the_warehouse_edge(
        self, formula: Optional[str]
    ) -> None:
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", formula)
        elem = _element("el-customers", "Customers", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert [f.upstreams for f in fgls] == [
            [builder.make_schema_field_urn(_SF_DATASET_URN, "email")]
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 1

    @pytest.mark.parametrize(
        "formula",
        ["42", "[P_Markup]", "[Cost] * 1.2"],
        ids=["constant", "parameter", "bare-ref-to-another-column"],
    )
    def test_a_non_empty_formula_without_a_ref_gets_no_edge(self, formula: str) -> None:
        # The formula no longer reads the column its columnId names, so the
        # columnId edge would be wrong.
        source = _make_source()
        cols = [
            _column(f"inode-{_SF_URL_ID}/PRICE", "Price", formula),
            _column(f"inode-{_SF_URL_ID}/COST", "Cost", "[CUSTOMERS/Cost]"),
        ]
        elem = _element("el-customers", "Customers", cols, [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert [f.downstreams for f in fgls] == [
            [
                builder.make_schema_field_urn(
                    "urn:li:dataset:(urn:li:dataPlatform:sigma,el-customers,PROD)",
                    "Cost",
                )
            ]
        ]

    def test_an_opaque_column_id_gets_no_edge(self) -> None:
        # A sibling has a column of the same name; matching on name alone
        # would fabricate an edge to it.
        source = _make_source()
        sibling_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,el-sibling,PROD)"
        sibling = _element("el-sibling", "Sibling", [_column("s-email", "Email", None)])
        col = _column("opaque-col-id", "Email", None)
        elem = _element("el-customers", "Customers", [col], ["el-sibling"])

        fgls = _build_fgls(
            source,
            elem,
            element_name_to_eids={"sibling": ["el-sibling"]},
            elementId_to_dataset_urn={"el-sibling": sibling_urn},
            entity_level_upstream_urns={sibling_urn},
            upstream_elements=[sibling],
        )

        assert fgls == []
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )

    @pytest.mark.parametrize(
        "column_id",
        [
            "opaque-col-id",
            "inode-OtherUrlId0000000001/EMAIL",
            f"inode-{_SF_URL_ID}/",
            _SF_INODE_SOURCE,
        ],
        ids=["opaque", "not-own-source", "no-column", "no-slash"],
    )
    @pytest.mark.parametrize("formula", _FORMULAS_READING_THE_COLUMN_ID)
    def test_a_column_id_not_naming_an_own_source_column_is_not_counted(
        self, column_id: str, formula: Optional[str]
    ) -> None:
        source = _make_source()
        # As the element-level upstream lookup has done by now in a real run.
        source._warehouse_ref_by_url_id[_SF_URL_ID] = None
        col = _column(column_id, "Email", formula)
        elem = _element("el-customers", "Customers", [col], [_SF_INODE_SOURCE])

        with patch.object(source.sigma_api, "get_file_metadata") as files:
            fgls = _build_fgls(
                source,
                elem,
                warehouse_map={},
                element_name_to_eids=_SELF_NAME_TO_EIDS,
            )

        assert fgls == []
        files.assert_not_called()
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )

    @pytest.mark.parametrize("formula", _FORMULAS_READING_THE_COLUMN_ID)
    def test_an_unresolvable_inode_column_id_is_counted(
        self, formula: Optional[str]
    ) -> None:
        source = _make_source()
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", formula)
        elem = _element("el-customers", "Customers", [col], [_SF_INODE_SOURCE])

        with patch.object(source.sigma_api, "get_file_metadata", return_value=None):
            fgls = _build_fgls(
                source,
                elem,
                warehouse_map={},
                element_name_to_eids=_SELF_NAME_TO_EIDS,
            )

        assert fgls == []
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 1
        )

    @pytest.mark.parametrize("formula", _FORMULAS_READING_THE_COLUMN_ID)
    def test_an_ingested_sigma_dataset_column_id_is_not_counted(
        self, formula: Optional[str]
    ) -> None:
        source = _make_source()
        sd_url_id = "sdUrlId0000000000000001"
        source.sigma_dataset_urn_by_url_id[sd_url_id] = (
            "urn:li:dataset:(urn:li:dataPlatform:sigma,sd-1,PROD)"
        )
        col = _column(f"inode-{sd_url_id}/EMAIL", "Email", formula)
        elem = _element("el-customers", "Customers", [col], [f"inode-{sd_url_id}"])

        fgls = _build_fgls(
            source, elem, warehouse_map={}, element_name_to_eids=_SELF_NAME_TO_EIDS
        )

        assert fgls == []
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )

    def test_a_duplicate_name_keeps_the_formula_row_and_drops_the_empty_twin(
        self,
    ) -> None:
        # Pins dedup: the row with a formula wins, so the empty-formula twin's
        # columnId edge is not emitted.
        source = _make_source()
        cols = [
            _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", None),
            _column("calc-email", "Email", "42"),
        ]
        elem = _element("el-customers", "Customers", cols, [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert fgls == []
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 0

    def test_a_whitespace_formula_does_not_outrank_an_empty_twin(self) -> None:
        # Both rows have no formula, so columnId breaks the tie and the inode
        # row's edge is emitted.
        source = _make_source()
        cols = [
            _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", None),
            _column("z-blank-email", "Email", "  "),
        ]
        elem = _element("el-customers", "Customers", cols, [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert [f.upstreams for f in fgls] == [
            [builder.make_schema_field_urn(_SF_DATASET_URN, "email")]
        ]

    def test_a_column_with_a_sibling_ref_gets_no_extra_warehouse_edge(self) -> None:
        source = _make_source()
        upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,el-upstream,PROD)"
        upstream = _element("el-upstream", "Upstream", [_column("up-x", "x", None)])
        col = _column(f"inode-{_SF_URL_ID}/EMAIL", "Email", "[Upstream/x]")
        elem = _element(
            "el-customers", "Customers", [col], [_SF_INODE_SOURCE, "el-upstream"]
        )

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={"upstream": ["el-upstream"]},
            elementId_to_dataset_urn={"el-upstream": upstream_urn},
            entity_level_upstream_urns={upstream_urn},
            upstream_elements=[upstream],
        )

        assert [f.upstreams for f in fgls] == [
            [builder.make_schema_field_urn(upstream_urn, "x")]
        ]
        assert source.reporter.data_model_element_fgl_warehouse_resolved == 0


class TestExplicitCaseFlagScope:
    """The DM passthrough route must ignore an explicitly-set
    convert_urns_to_lowercase on non-Snowflake platforms -- table and column
    alike -- because only the Sigma Dataset route opts in.

    Deliberately exercises _try_emit_warehouse_passthrough_fgl rather than
    _normalize_warehouse_identifier: a helper-level assertion passes whatever
    the route passes for `explicit`, which is how a column-side leak survived a
    review round.
    """

    def test_explicit_flag_moves_neither_table_nor_column(self) -> None:
        # Mixed-case table, unlike _RS_REF, so a regression shows both halves of
        # the schemaField URN side by side rather than only the column.
        mixed_ref = _WarehouseTableRef(
            connection_id=_RS_CONN_ID,
            db="Analytics",
            schema="Demo",
            table="BaseTable",
        )
        source = _make_source(
            conn_overrides={
                _RS_CONN_ID: WarehouseConnectionConfig.model_validate(
                    {"convert_urns_to_lowercase": True}
                )
            }
        )
        col = _column(f"inode-{_RS_URL_ID}/CustomerId", "Customer Id", None)
        elem = _element("el-1", "BaseTable", [col], [_RS_INODE_SOURCE])
        result = _try_emit(source, col, elem, {_RS_URL_ID: mixed_ref})

        assert result is not None and result.upstreams is not None
        assert result.upstreams[0] == builder.make_schema_field_urn(
            "urn:li:dataset:(urn:li:dataPlatform:redshift,Analytics.Demo.BaseTable,PROD)",
            "CustomerId",
        )


class TestWarehouseTableNameRef:
    """A ref naming a warehouse table the element reads, on a column with no
    ``inode-<url_id>/<COL>`` columnId."""

    @staticmethod
    def _upstreams(fgls: List[FineGrainedLineageClass]) -> List[tuple]:
        return [(f.upstreams, f.confidenceScore) for f in fgls]

    def test_a_calculated_column_gets_a_derived_column_edge(self) -> None:
        source = _make_source()
        col = _column("opaque-1", "Upper Email", "Upper([Customers/Email Address])")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email_address")], 0.5)
        ]
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 1
        assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0

    def test_a_self_named_element_uses_the_native_name_in_its_column_id(
        self,
    ) -> None:
        """Sigma spells this pass-through's columnId ``<url_id>/<NATIVE>``."""
        source = _make_source()
        col = _column(f"{_SF_URL_ID}/E_MAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={"customers": [elem.elementId]},
        )

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "e_mail")], 1.0)
        ]
        assert (
            source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
        )

    def test_each_column_a_formula_names_gets_an_edge(self) -> None:
        source = _make_source()
        col = _column(
            "opaque-1", "Name", 'Concat([CUSTOMERS/First Name], " ", [CUSTOMERS/Last])'
        )
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert sorted(u for f in fgls for u in f.upstreams or []) == [
            builder.make_schema_field_urn(_SF_DATASET_URN, "first_name"),
            builder.make_schema_field_urn(_SF_DATASET_URN, "last"),
        ]

    def test_a_case_preserving_platform_gets_no_derived_column(self) -> None:
        """The derived name's case is a guess, so it is only used where
        identifiers are lowercased."""
        source = _make_source()
        col = _column("opaque-1", "Upper Id", "Upper([base_table/Customer Id])")
        elem = _element("el-summary", "Summary", [col], [_RS_INODE_SOURCE])

        assert _build_fgls(source, elem, warehouse_map=_RS_WAREHOUSE_MAP) == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 1

    @pytest.mark.parametrize(
        "ref",
        ["[CUSTOMERS/Revenue (USD)]", "[CUSTOMERS/Rev\\/Cost]", "[CUSTOMERS/9 Day]"],
    )
    def test_a_derived_name_that_is_no_identifier_gets_no_edge(self, ref: str) -> None:
        source = _make_source()
        col = _column("opaque-1", "Value", f"Sum({ref})")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        assert _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP) == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 1

    def test_a_case_preserving_platform_still_gets_an_exact_column(self) -> None:
        source = _make_source()
        col = _column(
            f"{_RS_URL_ID}/customer_id", "Customer Id", "[base_table/Customer Id]"
        )
        elem = _element("el-base", "base_table", [col], [_RS_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_RS_WAREHOUSE_MAP,
            element_name_to_eids={"base_table": [elem.elementId]},
        )

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_RS_DATASET_URN, "customer_id")], 1.0)
        ]

    def test_a_column_id_naming_the_element_is_not_exact(self) -> None:
        source = _make_source()
        col = _column("el-summary/E_MAIL", "Email", "[CUSTOMERS/Email]")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    def test_a_native_name_with_a_slash_is_still_exact(self) -> None:
        source = _make_source()
        col = _column(f"{_SF_URL_ID}/REV/COST", "Cost", "[CUSTOMERS/Cost]")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "rev/cost")], 1.0)
        ]

    def test_a_calculated_column_id_is_not_a_native_name(self) -> None:
        """Only a formula that is just the ref passes the column through."""
        source = _make_source()
        col = _column("el-summary/UPPER_EMAIL", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    def test_an_inode_column_id_is_left_to_the_passthrough_path(self) -> None:
        """A second ref on a passthrough column gains no derived edge."""
        source = _make_source()
        col = _column(
            f"inode-{_SF_URL_ID}/EMAIL",
            "Email",
            "Coalesce([CUSTOMERS/Email], [CUSTOMERS/Alt])",
        )
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 1.0)
        ]
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 0

    def test_a_table_the_element_does_not_read_gets_no_edge(self) -> None:
        source = _make_source()
        col = _column("opaque-1", "Email", "[ORDERS/Email]")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        assert _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP) == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 0

    def test_two_read_tables_with_the_name_get_no_edge(self) -> None:
        other = _WarehouseTableRef(
            connection_id=_SF_CONN_ID, db="PROD_DB", schema="STAGING", table="CUSTOMERS"
        )
        source = _make_source()
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element(
            "el-summary", "Summary", [col], [_SF_INODE_SOURCE, "inode-other"]
        )

        fgls = _build_fgls(
            source, elem, warehouse_map={**_SF_WAREHOUSE_MAP, "other": other}
        )

        assert fgls == []

    def test_a_guessed_column_on_a_self_named_element(self) -> None:
        source = _make_source()
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element("el-customers", "CUSTOMERS", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(
            source,
            elem,
            warehouse_map=_SF_WAREHOUSE_MAP,
            element_name_to_eids={"customers": [elem.elementId]},
        )

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    def test_the_table_name_matches_case_insensitively(self) -> None:
        source = _make_source()
        col = _column("opaque-1", "Email", "Upper([customers/Email])")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    def test_a_three_part_ref_gets_no_edge(self) -> None:
        """``[Element/Relationship/Column]``: the column belongs to the
        relationship's target table, not the one the ref names."""
        source = _make_source()
        outside = _column("opaque-1", "Email", "Upper([CUSTOMERS/Orders/Email])")
        summary = _element("el-summary", "Summary", [outside], [_SF_INODE_SOURCE])
        self_named = _column("opaque-2", "Total", "[CUSTOMERS/Orders/Total]")
        customers = _element(
            "el-customers", "CUSTOMERS", [self_named], [_SF_INODE_SOURCE]
        )

        assert _build_fgls(source, summary, warehouse_map=_SF_WAREHOUSE_MAP) == []
        assert (
            _build_fgls(
                source,
                customers,
                warehouse_map=_SF_WAREHOUSE_MAP,
                element_name_to_eids={"customers": [customers.elementId]},
            )
            == []
        )
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 2

    def test_a_source_whose_name_is_unknown_blocks_the_edge(self) -> None:
        """The table that resolved is not known to be the only one by that name."""
        source = _make_source()
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element(
            "el-summary", "Summary", [col], [_SF_INODE_SOURCE, "inode-gone"]
        )

        with patch.object(source.sigma_api, "get_file_metadata", return_value=None):
            fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert fgls == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 1

    def test_a_sigma_dataset_by_the_same_name_blocks_the_edge(self) -> None:
        source = _make_source()
        source.sigma_dataset_urn_by_url_id["sd-1"] = "urn:li:dataset:sd-1"
        source.sigma_dataset_name_by_url_id["sd-1"] = "Customers"
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element(
            "el-summary", "Summary", [col], [_SF_INODE_SOURCE, "inode-sd-1"]
        )

        assert _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP) == []

    def test_a_sigma_dataset_by_another_name_does_not_block_the_edge(self) -> None:
        source = _make_source()
        source.sigma_dataset_urn_by_url_id["sd-1"] = "urn:li:dataset:sd-1"
        source.sigma_dataset_name_by_url_id["sd-1"] = "Orders"
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element(
            "el-summary", "Summary", [col], [_SF_INODE_SOURCE, "inode-sd-1"]
        )

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    def test_a_sigma_dataset_on_the_tables_own_inode_does_not_block_the_edge(
        self,
    ) -> None:
        source = _make_source()
        source.sigma_dataset_name_by_url_id[_SF_URL_ID] = "Customers"
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    @staticmethod
    def _with_cross_dm_customers(source: SigmaSource, columns: Dict[str, str]) -> str:
        other = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-other.el-cust,PROD)"
        source.dm_element_urn_by_name = {"dm-other": {"customers": [other]}}
        source.dm_element_urn_to_cols = {other: columns}
        return other

    def test_another_data_models_element_by_the_name_keeps_its_edge(self) -> None:
        """Both paths keep the checked cross-DM edge over the table's guess."""
        source = _make_source()
        other = self._with_cross_dm_customers(source, {"email": "Email"})
        sources = [_SF_INODE_SOURCE, "dm-other/el-cust"]
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        summary = _element("el-summary", "Summary", [col], sources)
        customers = _element("el-customers", "CUSTOMERS", [col], sources)

        expected = [([builder.make_schema_field_urn(other, "Email")], 1.0)]
        assert (
            self._upstreams(
                _build_fgls(source, summary, warehouse_map=_SF_WAREHOUSE_MAP)
            )
            == expected
        )
        assert (
            self._upstreams(
                _build_fgls(
                    source,
                    customers,
                    warehouse_map=_SF_WAREHOUSE_MAP,
                    element_name_to_eids={"customers": [customers.elementId]},
                )
            )
            == expected
        )
        assert source.reporter.data_model_element_fgl_warehouse_table_name_resolved == 0
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 0

    def test_another_data_models_element_by_the_name_blocks_the_table(self) -> None:
        """Its column is not there, but the name is still ambiguous."""
        source = _make_source()
        self._with_cross_dm_customers(source, {"phone": "Phone"})
        sources = [_SF_INODE_SOURCE, "dm-other/el-cust"]
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        summary = _element("el-summary", "Summary", [col], sources)
        customers = _element("el-customers", "CUSTOMERS", [col], sources)

        assert _build_fgls(source, summary, warehouse_map=_SF_WAREHOUSE_MAP) == []
        assert (
            _build_fgls(
                source,
                customers,
                warehouse_map=_SF_WAREHOUSE_MAP,
                element_name_to_eids={"customers": [customers.elementId]},
            )
            == []
        )
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 2

    def test_an_unloaded_data_model_blocks_the_table(self) -> None:
        """Its element names are unknown, so one may be the ref's."""
        source = _make_source()
        sources = [_SF_INODE_SOURCE, "dm-gone/el-x"]
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        summary = _element("el-summary", "Summary", [col], sources)
        customers = _element("el-customers", "CUSTOMERS", [col], sources)

        assert _build_fgls(source, summary, warehouse_map=_SF_WAREHOUSE_MAP) == []
        assert (
            _build_fgls(
                source,
                customers,
                warehouse_map=_SF_WAREHOUSE_MAP,
                element_name_to_eids={"customers": [customers.elementId]},
            )
            == []
        )
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 2

    def test_a_loaded_data_model_without_the_name_keeps_the_edge(self) -> None:
        source = _make_source()
        source.dm_element_urn_by_name = {
            "dm-other": {"orders": ["urn:li:dataset:dm-other.el-orders"]}
        }
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element(
            "el-summary", "Summary", [col], [_SF_INODE_SOURCE, "dm-other/el-orders"]
        )

        fgls = _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP)

        assert self._upstreams(fgls) == [
            ([builder.make_schema_field_urn(_SF_DATASET_URN, "email")], 0.5)
        ]

    def test_unlowercased_snowflake_gets_no_derived_column(self) -> None:
        override = WarehouseConnectionConfig.model_validate(
            {"convert_urns_to_lowercase": False}
        )
        source = _make_source(conn_overrides={_SF_CONN_ID: override})
        # A ref repeated in one formula is declined once.
        col = _column(
            "opaque-1", "Email", "Coalesce([CUSTOMERS/Email], Upper([CUSTOMERS/Email]))"
        )
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        assert _build_fgls(source, elem, warehouse_map=_SF_WAREHOUSE_MAP) == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 1

    def test_an_unmappable_connection_gets_no_edge(self) -> None:
        table = _WarehouseTableRef(
            connection_id=_UNMAPPABLE_CONN_ID, db="DB", schema="S", table="CUSTOMERS"
        )
        source = _make_source()
        col = _column("opaque-1", "Email", "Upper([CUSTOMERS/Email])")
        elem = _element("el-summary", "Summary", [col], [_SF_INODE_SOURCE])

        assert _build_fgls(source, elem, warehouse_map={_SF_URL_ID: table}) == []
        assert source.reporter.data_model_element_fgl_warehouse_table_name_declined == 1
