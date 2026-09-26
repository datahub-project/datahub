"""Unit tests for the chart InputFields resolver helpers.

Cases cover probe-derived chart formulas and resolver behavior.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.data_classes import (
    DataModelElementUpstream,
    DatasetUpstream,
    Element,
    Page,
    WarehouseTableUpstream,
    Workbook,
)
from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    extract_bracket_refs,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource, _workbook_dm_url_ids
from datahub.metadata.schema_classes import InputFieldsClass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_element(
    element_id: str,
    name: str,
    columns: Optional[List[str]] = None,
) -> Element:
    return Element(
        elementId=element_id,
        name=name,
        url=f"https://example.com/{element_id}",
        type="table",
        columns=columns or [],
    )


def _make_element_with_formula(
    element_id: str,
    name: str,
    col_formulas: Dict[str, Optional[str]],
) -> Element:
    """Build an Element whose columns carry formula data (dict format)."""
    raw_columns = [
        {"name": col, "formula": formula} for col, formula in col_formulas.items()
    ]
    return Element(
        elementId=element_id,
        name=name,
        url=f"https://example.com/{element_id}",
        type="table",
        columns=raw_columns,  # type: ignore[arg-type]
    )


def _make_source(config_overrides: Optional[dict] = None) -> SigmaSource:
    """Create a minimal SigmaSource with a mocked API for unit testing."""
    config_dict = {
        "client_id": "test",
        "client_secret": "test",
        **(config_overrides or {}),
    }
    config = SigmaSourceConfig.model_validate(config_dict)
    source = SigmaSource.__new__(SigmaSource)
    source.config = config
    source.reporter = MagicMock()
    source.reporter.chart_input_fields_resolved = 0
    source.reporter.chart_input_fields_self_ref_fallback = 0
    source.reporter.chart_input_fields_skipped_parameter = 0
    source.reporter.chart_input_fields_skipped_sibling = 0
    source.reporter.chart_input_fields_case_mismatch = 0
    source.reporter.chart_input_fields_column_not_found = 0
    source.reporter.chart_input_fields_multi_segment_refused = 0
    source.reporter.chart_input_fields_join_chain_resolved = 0
    source.reporter.chart_input_fields_loaded_dm_resolved = 0
    source.reporter.chart_input_fields_sibling_inherited = 0
    source.reporter.chart_input_fields_warehouse_column_bridged = 0
    source.reporter.chart_input_fields_warehouse_column_bridge_unresolved = 0
    source.reporter.chart_input_fields_multi_ref_extra = 0
    source.reporter.chart_input_fields_warehouse_qualified = 0
    source.reporter.chart_input_fields_warehouse_qualified_via_workbook_index = 0
    # T4.C: sigma_api is needed by _gen_pages_workunit →
    # _build_workbook_warehouse_table_index → get_workbook_lineage.
    source.sigma_api = MagicMock()
    source.sigma_api.get_workbook_lineage = MagicMock(return_value=[])
    source._workbook_customsql_registered_urns = set()
    source._workbook_customsql_formula_fields = {}
    source._dm_element_field_paths = {}
    source._folded_index_memo = None
    source._dm_key_by_element_urn = {}
    source._dm_element_source_urns = {}
    source.dm_element_urn_by_name = {}
    source._bridge_unresolved_warned = set()
    return source


# ---------------------------------------------------------------------------
# _build_workbook_element_index
# ---------------------------------------------------------------------------


def _make_workbook_with_elements(pages_elements: List[List[Element]]) -> Workbook:
    pages = []
    for i, elements in enumerate(pages_elements):
        page = Page(
            pageId=f"page{i}",
            name=f"Page {i}",
            elements=elements,
        )
        pages.append(page)
    return Workbook(
        workbookId="wb1",
        name="Test WB",
        ownerId="o1",
        createdBy="o1",
        updatedBy="o1",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        url="https://example.com/wb",
        path="root",
        latestVersion=1,
        pages=pages,
    )


class TestBuildWorkbookElementIndex:
    def test_single_element(self) -> None:
        e = _make_element("e1", "My Table")
        wb = _make_workbook_with_elements([[e]])
        idx = SigmaSource._build_workbook_element_index(wb)
        assert idx == {"My Table": [e]}

    def test_name_collision_three_elements(self) -> None:
        """Live probe found 3 elements named 'random data model' in one workbook."""
        e1 = _make_element("AAOgK0f3ag", "random data model")
        e2 = _make_element("k7i_W7UYCg", "random data model")
        e3 = _make_element("lBjhSbH_Jp", "random data model")
        wb = _make_workbook_with_elements([[e1, e2], [e3]])
        idx = SigmaSource._build_workbook_element_index(wb)
        assert set(elem.elementId for elem in idx["random data model"]) == {
            "AAOgK0f3ag",
            "k7i_W7UYCg",
            "lBjhSbH_Jp",
        }

    def test_cross_page_elements(self) -> None:
        e1 = _make_element("e1", "Source")
        e2 = _make_element("e2", "Downstream")
        wb = _make_workbook_with_elements([[e1], [e2]])
        idx = SigmaSource._build_workbook_element_index(wb)
        assert "Source" in idx
        assert "Downstream" in idx


# ---------------------------------------------------------------------------
# _build_element_warehouse_table_index
# ---------------------------------------------------------------------------


class TestBuildElementWarehouseTableIndex:
    def test_direct_warehouse_entry(self) -> None:
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,FIVETRAN.LOG.FIVETRAN_LOG__CONNECTOR_STATUS,PROD)"
        idx = SigmaSource._build_element_warehouse_table_index({urn: []})
        assert idx["FIVETRAN_LOG__CONNECTOR_STATUS"] == [urn]

    def test_sigma_dataset_with_warehouse_entry(self) -> None:
        sigma_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,abc123,PROD)"
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.PETS,PROD)"
        idx = SigmaSource._build_element_warehouse_table_index({sigma_urn: [wh_urn]})
        assert idx["PETS"] == [wh_urn]

    def test_sigma_dataset_without_warehouse_entry_is_not_indexed(self) -> None:
        dm_element_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:sigma,dmId.elementId,PROD)"
        )
        idx = SigmaSource._build_element_warehouse_table_index({dm_element_urn: []})
        assert idx == {}

    def test_case_insensitive_key(self) -> None:
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.my_table,PROD)"
        idx = SigmaSource._build_element_warehouse_table_index({urn: []})
        # Key is always uppercased.
        assert "MY_TABLE" in idx

    def test_empty_inputs(self) -> None:
        assert SigmaSource._build_element_warehouse_table_index({}) == {}

    def test_invalid_urn_is_skipped_with_debug_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            idx = SigmaSource._build_element_warehouse_table_index(
                {"not-a-dataset-urn": []}
            )

        assert idx == {}
        assert "Skipping invalid dataset URN" in caplog.text

    def test_collision_two_urns_same_short_name(self) -> None:
        """Two tables in different schemas with the same leaf name produce a list of 2."""
        urn1 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA1.ORDERS,PROD)"
        urn2 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA2.ORDERS,PROD)"
        idx = SigmaSource._build_element_warehouse_table_index({urn1: [], urn2: []})
        assert len(idx["ORDERS"]) == 2
        assert set(idx["ORDERS"]) == {urn1, urn2}


# ---------------------------------------------------------------------------
# _resolve_chart_formula_upstream
# ---------------------------------------------------------------------------


def _make_ref(
    source: str,
    column: Optional[str],
    is_parameter: bool = False,
) -> BracketRef:
    """Build a ref the way the scanner would, from the formula text.

    Previously this used the four-argument constructor, which had to guess
    ``segments`` from the first-slash split -- so a multi-segment column would
    have produced a ref whose segmentation disagreed with its own text. It also
    set ``raw`` without brackets, contradicting that field's contract.

    ``is_parameter`` is kept for call-site readability; the scanner derives it
    from the ``P_`` prefix, and the assert pins the two in agreement.
    """
    body = f"{source}/{column}" if column else source
    ref = BracketRef.from_body(body)
    assert ref.is_parameter == is_parameter, (
        f"scanner derived is_parameter={ref.is_parameter} for {body!r}, "
        f"but the call site says {is_parameter}"
    )
    return ref


class TestResolveChartFormulaUpstream:
    """Test cases drawn from M0 stage-3 probe samples."""

    def setup_method(self) -> None:
        self.src = _make_source()

    # --- parameter ref ---

    def test_parameter_ref_returns_none(self) -> None:
        ref = _make_ref("P_Failure_or_Resync", None, is_parameter=True)
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="e1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )
        assert result is None

    # --- sibling / bare [col] ref ---

    def test_sibling_ref_bare_col_returns_none(self) -> None:
        ref = _make_ref("Log Id", None)
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="1ESOHOLBNY",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )
        assert result is None

    # --- intra-workbook element ref ---

    def test_intra_workbook_single_match(self) -> None:
        """[random data model/Calc] with only AAOgK0f3ag in upstream set."""
        upstream_elem = _make_element("AAOgK0f3ag", "random data model")
        ref = _make_ref("random data model", "Calc")
        chart_urn = "urn:li:chart:(sigma,AAOgK0f3ag)"
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"AAOgK0f3ag"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"random data model": [upstream_elem]},
            element_warehouse_table_index={},
            elementId_to_chart_urn={"AAOgK0f3ag": chart_urn},
        )
        assert result == (chart_urn, "Calc")

    def test_intra_workbook_collision_only_one_in_upstream(self) -> None:
        """Three elements named 'random data model'; only AAOgK0f3ag is upstream."""
        e1 = _make_element("AAOgK0f3ag", "random data model")
        e2 = _make_element("k7i_W7UYCg", "random data model")
        e3 = _make_element("lBjhSbH_Jp", "random data model")
        ref = _make_ref("random data model", "toss_decision")
        chart_urn = "urn:li:chart:(sigma,AAOgK0f3ag)"
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"AAOgK0f3ag"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"random data model": [e1, e2, e3]},
            element_warehouse_table_index={},
            elementId_to_chart_urn={"AAOgK0f3ag": chart_urn},
        )
        assert result == (chart_urn, "toss_decision")

    def test_intra_workbook_collision_no_upstream_match_returns_none(self) -> None:
        """Name collision without lineage-upstream filter -> None (no match)."""
        e1 = _make_element("k7i_W7UYCg", "random data model")
        e2 = _make_element("lBjhSbH_Jp", "random data model")
        ref = _make_ref("random data model", "Calc")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            # Neither collision element is in the upstream set.
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={"random data model": [e1, e2]},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )
        assert result is None

    def _resolve(
        self,
        ref: BracketRef,
        wb_element_index: Dict[str, List[Element]],
        upstream_ids: Optional[set] = None,
        dm_urns: Optional[Dict[str, str]] = None,
        warehouse_index: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[tuple]:
        return self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids=upstream_ids or set(),
            dm_upstream_urn_by_element_name=dm_urns or {},
            wb_element_index=wb_element_index,
            element_warehouse_table_index=warehouse_index or {},
            elementId_to_chart_urn={"sourceElem": "urn:source"},
        )

    def test_case_only_mismatch_resolves_to_the_element(self) -> None:
        # Sigma resolves formula refs case-insensitively.
        elem = _make_element("sourceElem", "T Source", columns=["Col A"])
        result = self._resolve(
            _make_ref("t source", "col a"),
            {"T Source": [elem]},
            upstream_ids={"sourceElem"},
        )
        assert result == ("urn:source", "Col A")

    def test_case_only_mismatch_reaches_dm_element_by_its_own_name(self) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        elem = _make_element("dmElem", "Orders")
        result = self._resolve(
            _make_ref("orders", "Id"), {"Orders": [elem]}, dm_urns={"Orders": dm_urn}
        )
        assert result == (dm_urn, "Id")

    def test_ambiguous_case_insensitive_match_is_refused_and_counted(self) -> None:
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t source,PROD)"
        index = {
            "T Source": [_make_element("sourceElem", "T Source")],
            "T SOURCE": [_make_element("otherElem", "T SOURCE")],
        }
        result = self._resolve(
            _make_ref("t source", "col"),
            index,
            warehouse_index={"T SOURCE": [wh_urn]},
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_case_mismatch == 1

    @pytest.mark.parametrize("ref_source", ["t source", "T Source"])
    def test_lineage_picks_among_case_variants(self, ref_source: str) -> None:
        # The exact spelling must not win over the element lineage names.
        index = {
            "T Source": [_make_element("otherElem", "T Source")],
            "T SOURCE": [_make_element("sourceElem", "T SOURCE")],
        }
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t source,PROD)"
        result = self._resolve(
            _make_ref(ref_source, "col"),
            index,
            upstream_ids={"sourceElem"},
            warehouse_index={"T SOURCE": [wh_urn]},
        )
        assert result == ("urn:source", "col")
        assert self.src.reporter.chart_input_fields_case_mismatch == 0

    @pytest.mark.parametrize(
        ("page_names", "ref_source"),
        [
            (("Orders", "ORDERS"), "Orders"),
            (("Orders", "ORDERS"), "orders"),
            # Neither page spelling is the DM upstream's own.
            (("ORDERS", "orders"), "orders"),
        ],
    )
    def test_dm_lineage_picks_among_case_variants(
        self, page_names: tuple, ref_source: str
    ) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        index = {
            name: [_make_element(f"page{i}", name)] for i, name in enumerate(page_names)
        }
        result = self._resolve(
            _make_ref(ref_source, "Id"), index, dm_urns={"Orders": dm_urn}
        )
        assert result == (dm_urn, "Id")
        assert self.src.reporter.chart_input_fields_case_mismatch == 0

    def test_the_exact_spelling_breaks_a_tie_between_sheet_upstreams(self) -> None:
        index = {
            "Orders": [_make_element("sourceElem", "Orders")],
            "ORDERS": [_make_element("otherElem", "ORDERS")],
        }
        result = self.src._resolve_chart_formula_upstream(
            _make_ref("Orders", "Id"),
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"sourceElem", "otherElem"},
            dm_upstream_urn_by_element_name={},
            wb_element_index=index,
            element_warehouse_table_index={},
            elementId_to_chart_urn={
                "sourceElem": "urn:source",
                "otherElem": "urn:other",
            },
        )
        assert result == ("urn:source", "Id")

    def test_the_exact_spelling_breaks_a_tie_between_dm_upstreams(self) -> None:
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders,PROD)"
        dm_a = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.a,PROD)"
        index = {
            "Orders": [_make_element("a", "Orders")],
            "ORDERS": [_make_element("b", "ORDERS")],
        }
        result = self._resolve(
            _make_ref("Orders", "Id"),
            index,
            dm_urns={
                "Orders": dm_a,
                "ORDERS": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.b,PROD)",
            },
            warehouse_index={"ORDERS": [wh_urn]},
        )
        assert result == (dm_a, "Id")

    def test_the_exact_spelling_wins_when_lineage_picks_nothing(self) -> None:
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t source,PROD)"
        index = {
            "T Source": [_make_element("a", "T Source")],
            "T SOURCE": [_make_element("b", "T SOURCE")],
        }
        result = self._resolve(
            _make_ref("T Source", "col"),
            index,
            warehouse_index={"T SOURCE": [wh_urn]},
        )
        assert result == (wh_urn, "col")
        assert self.src.reporter.chart_input_fields_case_mismatch == 0

    def test_the_chart_itself_is_not_a_case_variant(self) -> None:
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders,PROD)"
        index = {
            "ORDERS": [_make_element("downstreamElem", "ORDERS")],
            "Orders": [_make_element("other", "Orders")],
        }
        # Spelled like neither, so only excluding the chart avoids a refusal.
        result = self._resolve(
            _make_ref("orders", "Id"), index, warehouse_index={"ORDERS": [wh_urn]}
        )
        assert result == (wh_urn, "Id")
        assert self.src.reporter.chart_input_fields_case_mismatch == 0

    def test_a_chart_named_like_its_dm_upstream_still_resolves(self) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        index = {"Orders": [_make_element("downstreamElem", "Orders")]}
        result = self._resolve(
            _make_ref("Orders", "Id"), index, dm_urns={"Orders": dm_urn}
        )
        assert result == (dm_urn, "Id")

    def test_dm_upstreams_off_the_page_differing_in_case_are_refused(self) -> None:
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders,PROD)"
        dm_urns = {
            "Orders": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.a,PROD)",
            "ORDERS": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.b,PROD)",
        }
        result = self._resolve(
            _make_ref("orders", "Id"),
            {},
            dm_urns=dm_urns,
            warehouse_index={"ORDERS": [wh_urn]},
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_case_mismatch == 1

    def test_dm_upstreams_differing_only_in_case_are_refused(self) -> None:
        index = {
            "Orders": [_make_element("pageA", "Orders")],
            "ORDERS": [_make_element("pageB", "ORDERS")],
        }
        dm_urns = {
            "Orders": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.a,PROD)",
            "ORDERS": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.b,PROD)",
        }
        result = self._resolve(_make_ref("orders", "Id"), index, dm_urns=dm_urns)
        assert result is None
        assert self.src.reporter.chart_input_fields_case_mismatch == 1

    def test_a_duplicate_column_id_does_not_shadow_a_real_column(self) -> None:
        elem = _make_element("sourceElem", "Src", columns=["Amount"])
        elem.column_id_by_name = {"Amount": "cid-1", "Hidden": "cid-1"}
        result = self._resolve(
            _make_ref("Src", "cid-1"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result == ("urn:source", "Amount")

    def test_names_fold_by_lower_not_casefold(self) -> None:
        # casefold() would equate "Straße" with "STRASSE".
        elem = _make_element("sourceElem", "STRASSE")
        result = self._resolve(
            _make_ref("Straße", "col"), {"STRASSE": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result is None

    def test_whitespace_mismatch_does_not_match_the_element(self) -> None:
        # Sigma rejects a ref whose name differs only by padding.
        elem = _make_element("sourceElem", "T Source ")
        result = self._resolve(
            _make_ref("T Source", "col"),
            {"T Source ": [elem]},
            upstream_ids={"sourceElem"},
        )
        assert result is None

    def test_sibling_column_id_is_translated_to_its_name(self) -> None:
        elem = _make_element("sourceElem", "Src", columns=["Amount"])
        elem.column_id_by_name = {"Amount": "col-id-1"}
        result = self._resolve(
            _make_ref("Src", "col-id-1"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result == ("urn:source", "Amount")

    def test_column_id_translates_only_to_a_column_the_upstream_has(self) -> None:
        elem = _make_element("sourceElem", "Src", columns=["Amount"])
        elem.column_id_by_name = {"Hidden": "cid-9"}
        result = self._resolve(
            _make_ref("Src", "cid-9"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result is None

    def test_column_id_is_translated_when_columns_are_unknown(self) -> None:
        elem = _make_element("sourceElem", "Src")
        elem.column_id_by_name = {"Amount": "cid-1"}
        result = self._resolve(
            _make_ref("Src", "cid-1"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result == ("urn:source", "Amount")

    def test_a_warehouse_column_id_ref_translates_when_the_upstream_has_it(
        self,
    ) -> None:
        # inode-<id>/<NAME> parses as three segments; the upstream confirms it.
        elem = _make_element("sourceElem", "Src", columns=["AMOUNT"])
        elem.column_id_by_name = {"AMOUNT": "inode-abc/AMOUNT"}
        result = self._resolve(
            _make_ref("Src", "inode-abc/AMOUNT"),
            {"Src": [elem]},
            upstream_ids={"sourceElem"},
        )
        assert result == ("urn:source", "AMOUNT")

    def test_a_slash_in_a_column_name_resolves_against_a_known_schema(self) -> None:
        # Sigma writes [Src/Rev/Cost] unescaped for a column named "Rev/Cost".
        elem = _make_element("sourceElem", "Src", columns=["Rev/Cost"])
        result = self._resolve(
            _make_ref("Src", "Rev/Cost"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result == ("urn:source", "Rev/Cost")
        assert self.src.reporter.chart_input_fields_multi_segment_refused == 0

    @pytest.mark.parametrize("upstream", ["sibling", "dm"])
    def test_a_multi_segment_miss_is_not_a_missing_column(self, upstream: str) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        self.src._dm_element_field_paths[dm_urn] = {"Amount"}
        index = (
            {"Src": [_make_element("sourceElem", "Src", columns=["Amount"])]}
            if upstream == "sibling"
            else {}
        )
        result = self._resolve(
            _make_ref("Src", "Rel/Amount"),
            index,
            upstream_ids={"sourceElem"},
            dm_urns={"Src": dm_urn} if upstream == "dm" else None,
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_multi_segment_refused == 1
        assert self.src.reporter.chart_input_fields_column_not_found == 0

    def test_a_slash_column_resolves_against_a_dm_schema(self) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        self.src._dm_element_field_paths[dm_urn] = {"Rev/Cost"}
        result = self._resolve(
            _make_ref("Orders", "Rev/Cost"), {}, dm_urns={"Orders": dm_urn}
        )
        assert result == (dm_urn, "Rev/Cost")

    @pytest.mark.parametrize("upstream", ["unknown-sibling", "unknown-dm", "warehouse"])
    def test_a_multi_segment_ref_needs_a_known_schema(self, upstream: str) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.src,PROD)"
        index = (
            {"Src": [_make_element("sourceElem", "Src")]}
            if upstream == "unknown-sibling"
            else {}
        )
        result = self._resolve(
            _make_ref("Src", "Rev/Cost"),
            index,
            upstream_ids={"sourceElem"},
            dm_urns={"Src": dm_urn} if upstream == "unknown-dm" else None,
            warehouse_index={"SRC": [wh_urn]} if upstream == "warehouse" else None,
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_multi_segment_refused == 1

    def test_case_variant_dm_upstreams_on_the_page_are_refused(self) -> None:
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders,PROD)"
        result = self._resolve(
            _make_ref("orders", "Id"),
            {"orders": [_make_element("pageElem", "orders")]},
            dm_urns={
                "Orders": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.a,PROD)",
                "ORDERS": "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.b,PROD)",
            },
            warehouse_index={"ORDERS": [wh_urn]},
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_case_mismatch == 1

    @pytest.mark.parametrize("on_page", [True, False])
    def test_case_variant_keys_for_one_dm_element_are_not_ambiguous(
        self, on_page: bool
    ) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.a,PROD)"
        index = {"orders": [_make_element("pageElem", "orders")]} if on_page else {}
        result = self._resolve(
            _make_ref("orders", "Id"),
            index,
            dm_urns={"Orders": dm_urn, "ORDERS": dm_urn},
        )
        assert result == (dm_urn, "Id")
        assert self.src.reporter.chart_input_fields_case_mismatch == 0

    def test_sibling_column_absent_upstream_is_refused(self) -> None:
        elem = _make_element("sourceElem", "Src", columns=["Amount"])
        result = self._resolve(
            _make_ref("Src", "Missing"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_column_not_found == 1

    @pytest.mark.parametrize(
        ("ref_column", "expected"),
        [("Amount", "Amount"), ("amount", "amount"), ("AMOUNT", None)],
    )
    def test_column_exact_match_beats_a_case_variant(
        self, ref_column: str, expected: Optional[str]
    ) -> None:
        elem = _make_element("sourceElem", "Src", columns=["Amount", "amount"])
        result = self._resolve(
            _make_ref("Src", ref_column), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result == (("urn:source", expected) if expected else None)

    @pytest.mark.parametrize(
        ("ref_column", "expected"),
        [("Amount", "Amount"), ("amount", "amount"), ("AMOUNT", None)],
    )
    def test_dm_column_exact_match_beats_a_case_variant(
        self, ref_column: str, expected: Optional[str]
    ) -> None:
        # A set: the winner must not depend on string hashing.
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        self.src._dm_element_field_paths[dm_urn] = {"Amount", "amount"}
        result = self._resolve(
            _make_ref("Orders", ref_column), {}, dm_urns={"Orders": dm_urn}
        )
        assert result == ((dm_urn, expected) if expected else None)

    def test_dm_upstream_off_the_page_matches_case_insensitively(self) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        result = self._resolve(
            _make_ref("orders", "Id"), {}, dm_urns={"Orders": dm_urn}
        )
        assert result == (dm_urn, "Id")

    def test_the_folded_index_follows_the_workbook(self) -> None:
        first = {"Src": [_make_element("sourceElem", "Src")]}
        second = {"Other": [_make_element("sourceElem", "Other")]}
        assert self._resolve(_make_ref("src", "A"), first, {"sourceElem"})
        assert self._resolve(_make_ref("other", "A"), second, {"sourceElem"})
        assert self._resolve(_make_ref("src", "A"), second, {"sourceElem"}) is None

    def test_sibling_with_unknown_columns_passes_ref_through(self) -> None:
        elem = _make_element("sourceElem", "Src")
        result = self._resolve(
            _make_ref("Src", "Any"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result == ("urn:source", "Any")

    def test_dm_column_is_checked_against_emitted_schema(self) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        self.src._dm_element_field_paths[dm_urn] = {"Order Id"}
        dm_urns = {"Orders": dm_urn}
        assert self._resolve(_make_ref("Orders", "order id"), {}, dm_urns=dm_urns) == (
            dm_urn,
            "Order Id",
        )
        assert (
            self._resolve(_make_ref("Orders", "Missing"), {}, dm_urns=dm_urns) is None
        )

    def test_dm_column_passes_through_when_schema_unknown(self) -> None:
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.elem,PROD)"
        result = self._resolve(
            _make_ref("Orders", "Anything"), {}, dm_urns={"Orders": dm_urn}
        )
        assert result == (dm_urn, "Anything")

    def test_three_segment_ref_is_refused(self) -> None:
        # [Element/Relationship/Column] must not become a dangling edge to
        # a column named "Relationship/Column".
        elem = _make_element("sourceElem", "Src")
        result = self._resolve(
            _make_ref("Src", "Rel/Col"), {"Src": [elem]}, upstream_ids={"sourceElem"}
        )
        assert result is None
        assert self.src.reporter.chart_input_fields_multi_segment_refused == 1

    def test_exact_workbook_name_without_lineage_match_falls_through_to_warehouse(
        self,
    ) -> None:
        """When a workbook element name matches but none satisfy the lineage filter,
        fall through to warehouse-table resolution so the formula ref can still
        resolve (e.g. a sibling element that shares its name with the warehouse
        table it wraps)."""
        upstream_elem = _make_element("sourceElem", "Orders")
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        ref = _make_ref("Orders", "id")

        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={"Orders": [upstream_elem]},
            element_warehouse_table_index={"ORDERS": [wh_urn]},
            elementId_to_chart_urn={"sourceElem": "urn:source"},
        )

        assert result == (wh_urn, "id")

    def test_self_reference_is_excluded_from_workbook_element_matches(self) -> None:
        """A self-loop in Sigma lineage must not resolve to the chart itself."""
        self_elem = _make_element("chartElem", "Orders")
        ref = _make_ref("Orders", "id")

        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="chartElem",
            chart_upstream_element_ids={"chartElem"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"Orders": [self_elem]},
            element_warehouse_table_index={},
            elementId_to_chart_urn={"chartElem": "urn:self"},
        )

        assert result is None

    def test_intra_workbook_collision_ambiguous_multiple_upstream_matches_returns_none(
        self,
    ) -> None:
        """If >1 collision elements are all in the upstream set -> ambiguous -> None."""
        e1 = _make_element("k7i_W7UYCg", "random data model")
        e2 = _make_element("lBjhSbH_Jp", "random data model")
        ref = _make_ref("random data model", "Calc")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"k7i_W7UYCg", "lBjhSbH_Jp"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"random data model": [e1, e2]},
            element_warehouse_table_index={},
            elementId_to_chart_urn={"k7i_W7UYCg": "urn:a", "lBjhSbH_Jp": "urn:b"},
        )
        assert result is None

    def test_filtered_upstream_element_does_not_fall_through_to_warehouse(
        self,
    ) -> None:
        """A workbook match without a chart URN falls back to DM lookup, then None."""
        upstream_elem = _make_element("filtered-pivot", "Orders")
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        ref = _make_ref("Orders", "order_id")

        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"filtered-pivot"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"Orders": [upstream_elem]},
            element_warehouse_table_index={"ORDERS": [wh_urn]},
            elementId_to_chart_urn={},
        )

        assert result is None

    # --- warehouse table ref ---

    def test_warehouse_table_ref_resolves(self) -> None:
        """[FIVETRAN_LOG__CONNECTOR_STATUS/Connector Health] -> warehouse URN."""
        wh_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,FIVETRAN.LOG.FIVETRAN_LOG__CONNECTOR_STATUS,PROD)"
        ref = _make_ref("FIVETRAN_LOG__CONNECTOR_STATUS", "Connector Health")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="4Buu0C7LnB",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={"FIVETRAN_LOG__CONNECTOR_STATUS": [wh_urn]},
            elementId_to_chart_urn={},
        )
        assert result == (wh_urn, "Connector Health")

    def test_warehouse_table_ref_case_insensitive(self) -> None:
        """Warehouse table index lookup is case-insensitive."""
        wh_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.MY_TABLE,PROD)"
        )
        ref = _make_ref("my_table", "col")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="e1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={"MY_TABLE": [wh_urn]},
            elementId_to_chart_urn={},
        )
        assert result == (wh_urn, "col")

    def test_warehouse_table_ambiguous_collision_returns_none(self) -> None:
        """Two warehouse URNs with the same leaf name -> ambiguous -> None."""
        urn1 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA1.ORDERS,PROD)"
        urn2 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA2.ORDERS,PROD)"
        ref = _make_ref("ORDERS", "id")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="e1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={"ORDERS": [urn1, urn2]},
            elementId_to_chart_urn={},
        )
        assert result is None

    def test_warehouse_table_ambiguous_collision_logs_candidates(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        urn1 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA1.ORDERS,PROD)"
        urn2 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA2.ORDERS,PROD)"
        ref = _make_ref("ORDERS", "id")

        with caplog.at_level(logging.DEBUG):
            result = self.src._resolve_chart_formula_upstream(
                ref,
                chart_element_id="e1",
                chart_upstream_element_ids=set(),
                dm_upstream_urn_by_element_name={},
                wb_element_index={},
                element_warehouse_table_index={"ORDERS": [urn1, urn2]},
                elementId_to_chart_urn={},
            )

        assert result is None
        assert "Ambiguous warehouse table formula ref" in caplog.text
        assert urn1 in caplog.text
        assert urn2 in caplog.text

    # --- unresolvable ref ---

    def test_unresolvable_ref_returns_none(self) -> None:
        """[NonexistentSource/col] matches neither index -> None."""
        ref = _make_ref("NonexistentSource", "col")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="e1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )
        assert result is None

    def test_dm_upstream_not_in_wb_element_index_resolved_via_step_3c(self) -> None:
        """Step 3c: DM element is an upstream but not a page element -> dm_urn returned."""
        dm_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-x.elem-y,PROD)"
        ref = _make_ref("Orders", "Order Id")
        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="chart-1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={"Orders": dm_urn},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )
        assert result == (dm_urn, "Order Id")

    # --- sibling refs from M0 sample formulas ---

    def test_sibling_refs_from_failures_formula(self) -> None:
        """CountDistinctIf([Log Id], [status] = ...) has two sibling refs."""
        formula = 'CountDistinctIf([Log Id], [status] = "FAILURE" or [status] = "FAILURE_WITH_TASK")'
        refs = extract_bracket_refs(formula)
        assert all(r.column is None for r in refs)
        for ref in refs:
            result = self.src._resolve_chart_formula_upstream(
                ref,
                chart_element_id="1ESOHOLBNY",
                chart_upstream_element_ids=set(),
                dm_upstream_urn_by_element_name={},
                wb_element_index={},
                element_warehouse_table_index={},
                elementId_to_chart_urn={},
            )
            assert result is None

    def test_selected_metric_param_and_siblings(self) -> None:
        """Switch([P_Failure_or_Resync], ...) has param + sibling refs -> all None."""
        formula = 'Switch([P_Failure_or_Resync], "Failure", [Failures], "Resync", [Forced Resyncs])'
        refs = extract_bracket_refs(formula)
        for ref in refs:
            result = self.src._resolve_chart_formula_upstream(
                ref,
                chart_element_id="1ESOHOLBNY",
                chart_upstream_element_ids=set(),
                dm_upstream_urn_by_element_name={},
                wb_element_index={},
                element_warehouse_table_index={},
                elementId_to_chart_urn={},
            )
            assert result is None


class TestGenElementsWorkunitInputFields:
    def test_resolved_input_field_preserves_string_native_data_type(self) -> None:
        src = _make_source()
        src.dataset_upstream_urn_mapping = {}
        workbook = _make_workbook_with_elements([])
        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        )
        chart = _make_element_with_formula(
            "chart-1",
            "Orders Chart",
            {"Order Id": "[ORDERS/ORDER_ID]"},
        )
        src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({warehouse_urn: []}, [])
        )

        workunits = list(
            src._gen_elements_workunit(
                elements=[chart],
                workbook=workbook,
                all_input_fields=[],
                paths=[],
                elementId_to_chart_urn={},
                wb_element_index={},
                wb_warehouse_table_index=None,
            )
        )

        input_fields_aspects = [
            aspect
            for wu in workunits
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
        ]
        assert len(input_fields_aspects) == 1
        schema_field = input_fields_aspects[0].fields[0].schemaField
        assert schema_field is not None
        assert schema_field.nativeDataType == "String"

    def test_duplicate_formula_refs_emit_one_input_field(self) -> None:
        src = _make_source()
        src.dataset_upstream_urn_mapping = {}
        workbook = _make_workbook_with_elements([])
        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        )
        chart = _make_element_with_formula(
            "chart-1",
            "Orders Chart",
            {"Order Id": "[ORDERS/ORDER_ID] + [ORDERS/ORDER_ID]"},
        )
        all_input_fields: List = []
        src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({warehouse_urn: []}, [])
        )

        workunits = list(
            src._gen_elements_workunit(
                elements=[chart],
                workbook=workbook,
                all_input_fields=all_input_fields,
                paths=[],
                elementId_to_chart_urn={},
                wb_element_index={},
                wb_warehouse_table_index=None,
            )
        )

        input_fields_aspects = [
            aspect
            for wu in workunits
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
        ]
        assert len(input_fields_aspects) == 1
        assert len(input_fields_aspects[0].fields) == 1
        assert len(all_input_fields) == 1
        assert src.reporter.chart_input_fields_multi_ref_extra == 0

    def test_multi_ref_distinct_upstreams_emit_two_input_fields(self) -> None:
        """[ORDERS/col] + [CUSTOMERS/id]: two distinct refs → 2 fields, multi_ref_extra=1."""
        src = _make_source()
        src.dataset_upstream_urn_mapping = {}
        orders_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        )
        customers_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.CUSTOMERS,PROD)"
        )
        chart = _make_element_with_formula(
            "chart-1",
            "Multi Chart",
            {"Calc": "[ORDERS/col] + [CUSTOMERS/id]"},
        )
        src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({orders_urn: [], customers_urn: []}, [])
        )

        workunits = list(
            src._gen_elements_workunit(
                elements=[chart],
                workbook=_make_workbook_with_elements([]),
                all_input_fields=[],
                paths=[],
                elementId_to_chart_urn={},
                wb_element_index={},
                wb_warehouse_table_index=None,
            )
        )

        input_fields_aspects = [
            aspect
            for wu in workunits
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
        ]
        assert len(input_fields_aspects) == 1
        assert len(input_fields_aspects[0].fields) == 2
        assert src.reporter.chart_input_fields_resolved == 1
        assert src.reporter.chart_input_fields_multi_ref_extra == 1

    def test_multi_ref_duplicate_upstream_emits_one_input_field(self) -> None:
        """[ORDERS/col] + [ORDERS/col]: same ref twice → 1 field, multi_ref_extra=0."""
        src = _make_source()
        src.dataset_upstream_urn_mapping = {}
        orders_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        )
        chart = _make_element_with_formula(
            "chart-1",
            "Dup Ref Chart",
            {"Calc": "[ORDERS/col] + [ORDERS/col]"},
        )
        src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({orders_urn: []}, [])
        )

        workunits = list(
            src._gen_elements_workunit(
                elements=[chart],
                workbook=_make_workbook_with_elements([]),
                all_input_fields=[],
                paths=[],
                elementId_to_chart_urn={},
                wb_element_index={},
                wb_warehouse_table_index=None,
            )
        )

        input_fields_aspects = [
            aspect
            for wu in workunits
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
        ]
        assert len(input_fields_aspects) == 1
        assert len(input_fields_aspects[0].fields) == 1
        assert src.reporter.chart_input_fields_resolved == 1
        assert src.reporter.chart_input_fields_multi_ref_extra == 0

    def test_dashboard_input_fields_dedup_across_charts(self) -> None:
        src = _make_source()
        src.dataset_upstream_urn_mapping = {}
        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        )
        page = Page(
            pageId="page-1",
            name="Dashboard",
            elements=[
                _make_element_with_formula(
                    "chart-1",
                    "Orders Chart 1",
                    {"Order Id": "[ORDERS/ORDER_ID]"},
                ),
                _make_element_with_formula(
                    "chart-2",
                    "Orders Chart 2",
                    {"Order Id": "[ORDERS/ORDER_ID]"},
                ),
            ],
        )
        workbook = _make_workbook_with_elements([page.elements])
        src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({warehouse_urn: []}, [])
        )

        workunits = list(src._gen_pages_workunit(workbook, paths=[]))

        input_fields_aspects = [
            aspect
            for wu in workunits
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
        ]
        assert len(input_fields_aspects) == 3
        dashboard_input_fields = input_fields_aspects[-1]
        assert len(dashboard_input_fields.fields) == 1

    def test_column_id_by_name_bridges_display_name_to_native(self) -> None:
        """column_id_by_name drives column_native_names so the emitted URN uses the
        warehouse-native column name (lowercased), not the Sigma display name."""
        src = _make_source()
        src.dataset_upstream_urn_mapping = {}
        src._wb_url_id_to_conn_id = {}  # default lowercase=True
        warehouse_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"
        )
        # Formula uses Sigma display name "Customer Id"; columnId maps it to native.
        chart = _make_element_with_formula(
            "chart-1",
            "Orders Chart",
            {"Customer Id": "[ORDERS/Customer Id]"},
        )
        chart.column_id_by_name = {"Customer Id": "inode-tbl-abc/CUSTOMER_ID"}
        upstream = WarehouseTableUpstream(url_id="tbl-abc", name="ORDERS")
        chart.upstream_sources = {"inode-tbl-abc": upstream}
        src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({warehouse_urn: []}, [])
        )

        workunits = list(
            src._gen_elements_workunit(
                elements=[chart],
                workbook=_make_workbook_with_elements([]),
                all_input_fields=[],
                paths=[],
                elementId_to_chart_urn={},
                wb_element_index={},
                wb_warehouse_table_index=None,
            )
        )

        input_fields_aspects = [
            aspect
            for wu in workunits
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
        ]
        assert len(input_fields_aspects) == 1
        field_urn = input_fields_aspects[0].fields[0].schemaFieldUrn
        # Bridge must translate "Customer Id" → "customer_id" (inode-tbl-abc/CUSTOMER_ID, lowercase).
        assert field_urn.endswith(",customer_id)")


class TestBridgeWarehouseColumnName:
    """Unit tests for _bridge_warehouse_column_name."""

    def setup_method(self) -> None:
        self.src = _make_source()

    WAREHOUSE_URN = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "warehouse_coffee_company.public.customer_visits,PROD)"
    )
    SIGMA_DM_URN = (
        "urn:li:dataset:(urn:li:dataPlatform:sigma,"
        "b584ddca-0000-0000-0000-000000000001.elem,PROD)"
    )

    def test_warehouse_urn_bridges_display_name_to_native(self) -> None:
        result = self.src._bridge_warehouse_column_name(
            upstream_urn=self.WAREHOUSE_URN,
            sigma_display_name="Customer Id",
            column_native_names={"Customer Id": "customer_id"},
        )
        assert result == "customer_id"
        assert self.src.reporter.chart_input_fields_warehouse_column_bridged == 1
        assert (
            self.src.reporter.chart_input_fields_warehouse_column_bridge_unresolved == 0
        )

    def test_sigma_urn_leaves_display_name_unchanged(self) -> None:
        result = self.src._bridge_warehouse_column_name(
            upstream_urn=self.SIGMA_DM_URN,
            sigma_display_name="Customer Id",
            column_native_names={"Customer Id": "customer_id"},
        )
        assert result == "Customer Id"
        assert self.src.reporter.chart_input_fields_warehouse_column_bridged == 0

    def test_warehouse_urn_no_native_name_increments_unresolved(self) -> None:
        result = self.src._bridge_warehouse_column_name(
            upstream_urn=self.WAREHOUSE_URN,
            sigma_display_name="Missing Col",
            column_native_names={"Other Col": "other_col"},
        )
        assert result == "Missing Col"
        assert (
            self.src.reporter.chart_input_fields_warehouse_column_bridge_unresolved == 1
        )
        assert self.src.reporter.chart_input_fields_warehouse_column_bridged == 0

    def test_empty_native_names_skips_bridge(self) -> None:
        result = self.src._bridge_warehouse_column_name(
            upstream_urn=self.WAREHOUSE_URN,
            sigma_display_name="Customer Id",
            column_native_names={},
        )
        assert result == "Customer Id"
        assert self.src.reporter.chart_input_fields_warehouse_column_bridged == 0
        assert (
            self.src.reporter.chart_input_fields_warehouse_column_bridge_unresolved == 0
        )

    def test_already_native_name_does_not_double_count(self) -> None:
        """When display name already equals native name, bridged counter stays 0."""
        result = self.src._bridge_warehouse_column_name(
            upstream_urn=self.WAREHOUSE_URN,
            sigma_display_name="visit_id",
            column_native_names={"visit_id": "visit_id"},
        )
        assert result == "visit_id"
        assert self.src.reporter.chart_input_fields_warehouse_column_bridged == 0


_JOIN_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.join,PROD)"
_MIDDLE_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.middle,PROD)"
_OWNER_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.owner,PROD)"
_UNJOINED_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.unjoined,PROD)"


class TestChartRefStrategies:
    def setup_method(self) -> None:
        self.src = _make_source()
        # A Data Model whose join element joins Middle and Owner El; Unjoined
        # is in the same model, has the column, and is not joined in.
        self.src._dm_key_by_element_urn = {_JOIN_URN: "dm1", _OWNER_URN: "dm1"}
        self.src.dm_element_urn_by_name = {
            "dm1": {
                "join el": [_JOIN_URN],
                "middle": [_MIDDLE_URN],
                "owner el": [_OWNER_URN],
                "unjoined": [_UNJOINED_URN],
            }
        }
        self.src._dm_element_source_urns = {_JOIN_URN: {_MIDDLE_URN, _OWNER_URN}}
        self.src._dm_element_field_paths = {
            _JOIN_URN: {"Key"},
            _OWNER_URN: {"Sku"},
            _UNJOINED_URN: {"Sku"},
        }

    def _resolve(
        self,
        body: str,
        dm_urns: Optional[Dict[str, str]] = None,
        wb_element_index: Optional[Dict[str, List[Element]]] = None,
        workbook_dm_url_ids: frozenset = frozenset(),
    ) -> Optional[tuple]:
        return self.src._resolve_chart_formula_upstream(
            BracketRef.from_body(body),
            chart_element_id="chart",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name=dm_urns or {},
            wb_element_index=wb_element_index or {},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
            workbook_dm_url_ids=workbook_dm_url_ids,
        )

    @pytest.mark.parametrize(
        "body",
        ["Join El/Owner El/Sku", "Join El/Middle/Owner El/Sku", "join el/owner el/sku"],
    )
    def test_join_chain_resolves_to_the_owning_sibling(self, body: str) -> None:
        result = self._resolve(body, dm_urns={"Join El": _JOIN_URN})
        assert result == (_OWNER_URN, "Sku")
        assert self.src.reporter.chart_input_fields_join_chain_resolved == 1

    @pytest.mark.parametrize(
        ("body", "dm_urns", "names", "paths"),
        [
            # The first segment is not a DM upstream of the chart.
            ("Other/Owner El/Sku", {"Join El": _JOIN_URN}, None, None),
            # The owner does not have the column.
            ("Join El/Owner El/Nope", {"Join El": _JOIN_URN}, None, None),
            # Two siblings share the owner's name.
            (
                "Join El/Owner El/Sku",
                {"Join El": _JOIN_URN},
                {"join el": [_JOIN_URN], "owner el": [_OWNER_URN, _MIDDLE_URN]},
                None,
            ),
            # A relationship: the owner is not joined into the join element.
            ("Join El/Unjoined/Sku", {"Join El": _JOIN_URN}, None, None),
            # A middle segment is not joined in.
            ("Join El/Unjoined/Owner El/Sku", {"Join El": _JOIN_URN}, None, None),
            # The owner's schema is unknown.
            (
                "Join El/Owner El/Sku",
                {"Join El": _JOIN_URN},
                None,
                {_JOIN_URN: {"Key"}},
            ),
        ],
        ids=[
            "not-an-upstream",
            "column-absent",
            "owner-ambiguous",
            "relationship",
            "middle-not-joined",
            "schema-unknown",
        ],
    )
    def test_join_chain_is_refused_unless_the_owner_is_certain(
        self,
        body: str,
        dm_urns: Dict[str, str],
        names: Optional[Dict[str, List[str]]],
        paths: Optional[Dict[str, set]],
    ) -> None:
        if names is not None:
            self.src.dm_element_urn_by_name = {"dm1": names}
        if paths is not None:
            self.src._dm_element_field_paths = paths
        assert self._resolve(body, dm_urns=dm_urns) is None
        assert self.src.reporter.chart_input_fields_multi_segment_refused == 1

    def test_loaded_data_model_element_resolves_when_one_owns_the_column(self) -> None:
        result = self._resolve("owner el/Sku", workbook_dm_url_ids=frozenset({"dm1"}))
        assert result == (_OWNER_URN, "Sku")
        assert self.src.reporter.chart_input_fields_loaded_dm_resolved == 1

    @pytest.mark.parametrize(
        ("body", "loaded", "extra_dm"),
        [
            ("owner el/Sku", frozenset(), False),
            ("owner el/Nope", frozenset({"dm1"}), False),
            ("owner el/Sku", frozenset({"dm1", "dm2"}), True),
        ],
        ids=["model-not-loaded", "column-absent", "two-owners"],
    )
    def test_loaded_data_model_lookup_refuses_a_guess(
        self, body: str, loaded: frozenset, extra_dm: bool
    ) -> None:
        if extra_dm:
            other = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm2.owner,PROD)"
            self.src.dm_element_urn_by_name["dm2"] = {"owner el": [other]}
            self.src._dm_element_field_paths[other] = {"Sku"}
        assert self._resolve(body, workbook_dm_url_ids=loaded) is None

    def test_the_charts_own_source_skips_the_loaded_data_model_lookup(self) -> None:
        # A chart reading a Sigma Dataset named like a loaded DM element.
        result = self.src._resolve_chart_formula_upstream(
            BracketRef.from_body("Owner El/Sku"),
            chart_element_id="chart",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
            workbook_dm_url_ids=frozenset({"dm1"}),
            chart_source_names=frozenset({"owner el"}),
        )
        assert result is None

    def test_a_page_element_name_skips_the_loaded_data_model_lookup(self) -> None:
        index = {"Owner El": [_make_element("pageElem", "Owner El")]}
        result = self._resolve(
            "owner el/Sku",
            wb_element_index=index,
            workbook_dm_url_ids=frozenset({"dm1"}),
        )
        assert result is None

    def test_the_workbook_models_come_from_every_page_element(self) -> None:
        loader = _make_element("loader", "Loader")
        loader.upstream_sources = {
            "dm1/x": DataModelElementUpstream(name="Join El", data_model_url_id="dm1")
        }
        index = {"Loader": [loader], "Other": [_make_element("other", "Other")]}
        assert _workbook_dm_url_ids(index) == frozenset({"dm1"})

    def _chart_fields(self, elements: List[Element], chart_id: str) -> List[str]:
        self.src.dataset_upstream_urn_mapping = {}
        self.src._get_element_input_details = MagicMock(  # type: ignore[method-assign]
            return_value=({}, [])
        )
        workbook = _make_workbook_with_elements([elements])
        return [
            field.schemaFieldUrn
            for wu in self.src._gen_pages_workunit(workbook, paths=[])
            if (aspect := wu.get_aspect_of_type(InputFieldsClass)) is not None
            and chart_id in wu.get_urn()
            for field in aspect.fields
        ]

    def test_the_workbook_models_reach_the_resolver(self) -> None:
        chart = _make_element_with_formula(
            "chart-1", "Chart", {"Sku": "[Owner El/Sku]"}
        )
        loader = _make_element("loader", "Loader")
        loader.upstream_sources = {
            "dm1/x": DataModelElementUpstream(name="Join El", data_model_url_id="dm1")
        }
        assert self._chart_fields([chart, loader], "chart-1") == [
            f"urn:li:schemaField:({_OWNER_URN},Sku)"
        ]

    def test_a_charts_own_dataset_source_reaches_the_resolver(self) -> None:
        chart = _make_element_with_formula(
            "chart-1", "Chart", {"Sku": "[Owner El/Sku]"}
        )
        chart.upstream_sources = {"ds": DatasetUpstream(name="Owner El")}
        loader = _make_element("loader", "Loader")
        loader.upstream_sources = {
            "dm1/x": DataModelElementUpstream(name="Join El", data_model_url_id="dm1")
        }
        assert self._chart_fields([chart, loader], "chart-1") == [
            "urn:li:schemaField:(urn:li:chart:(sigma,chart-1),Sku)"
        ]


_WH_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders,PROD)"


class TestSiblingInheritance:
    def _fields(self, formulas: Dict[str, Optional[str]]) -> List[str]:
        self.src = _make_source()
        element = _make_element_with_formula("chart-1", "Chart", formulas)
        fields = self.src._build_element_input_fields(
            element=element,
            chart_urn="urn:li:chart:(sigma,chart-1)",
            chart_upstream_eids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={"ORDERS": [_WH_URN]},
            elementId_to_chart_urn={},
        )
        self.last_fields = fields
        return [f.schemaFieldUrn for f in fields]

    def test_a_derived_column_inherits_through_siblings(self) -> None:
        # Listed before what they derive from, so a second pass is needed.
        urns = self._fields(
            {
                "Doubled": "[Total] * 2",
                "Total": "Sum([Amount])",
                "Amount": "[ORDERS/Amount]",
            }
        )
        wh_field = f"urn:li:schemaField:({_WH_URN},Amount)"
        assert urns == [wh_field, wh_field, wh_field]
        reporter = self.src.reporter
        assert reporter.chart_input_fields_sibling_inherited == 2
        assert reporter.chart_input_fields_resolved == 3
        assert reporter.chart_input_fields_skipped_sibling == 0

    def test_a_column_fed_by_two_siblings_gets_both_upstreams(self) -> None:
        urns = self._fields(
            {
                "Cost": "[ORDERS/Cost]",
                "Price": "[ORDERS/Price]",
                "Margin": "[Price] - [Cost]",
            }
        )
        assert sorted(urns[2:]) == sorted(
            [
                f"urn:li:schemaField:({_WH_URN},Price)",
                f"urn:li:schemaField:({_WH_URN},Cost)",
            ]
        )
        assert self.src.reporter.chart_input_fields_multi_ref_extra == 1

    @pytest.mark.parametrize(
        "order",
        [
            ("Base", "Cost", "Price", "Margin"),
            ("Margin", "Price", "Base", "Cost"),
        ],
    )
    def test_inheritance_does_not_depend_on_column_order(self, order: tuple) -> None:
        formulas = {
            "Base": "[ORDERS/Base]",
            "Cost": "[ORDERS/Cost]",
            "Price": "[Base] * 1.1",
            "Margin": "[Price] - [Cost]",
        }
        self._fields({name: formulas[name] for name in order})
        by_column: Dict[str, set] = {}
        for field in self.last_fields:
            assert field.schemaField is not None
            by_column.setdefault(field.schemaField.fieldPath, set()).add(
                field.schemaFieldUrn
            )
        assert by_column["Margin"] == {
            f"urn:li:schemaField:({_WH_URN},Base)",
            f"urn:li:schemaField:({_WH_URN},Cost)",
        }

    def test_inherited_fields_sit_next_to_their_column(self) -> None:
        self._fields(
            {
                "Margin": "[Price] - [Cost]",
                "Cost": "[ORDERS/Cost]",
                "Price": "[ORDERS/Price]",
            }
        )
        assert [f.schemaField.fieldPath for f in self.last_fields if f.schemaField] == [
            "Margin",
            "Margin",
            "Cost",
            "Price",
        ]

    def test_a_deep_chain_resolves(self) -> None:
        formulas: Dict[str, Optional[str]] = {
            f"C{i}": f"[C{i + 1}] + 1" for i in range(1, 8)
        }
        formulas["C8"] = "[ORDERS/Amount]"
        urns = self._fields(formulas)
        assert set(urns) == {f"urn:li:schemaField:({_WH_URN},Amount)"}
        assert self.src.reporter.chart_input_fields_sibling_inherited == 7

    def test_a_parameter_does_not_block_inheritance(self) -> None:
        urns = self._fields(
            {"Amount": "[ORDERS/Amount]", "Scaled": "[Amount] * [P_Rate]"}
        )
        assert urns[1] == f"urn:li:schemaField:({_WH_URN},Amount)"
        reporter = self.src.reporter
        assert reporter.chart_input_fields_self_ref_fallback == 0
        assert reporter.chart_input_fields_resolved == 2

    def test_a_cycle_terminates_unresolved(self) -> None:
        urns = self._fields({"A": "[B]", "B": "[A]"})
        assert self.src.reporter.chart_input_fields_sibling_inherited == 0
        assert len(urns) == 2

    def test_an_unresolved_sibling_leaves_the_self_reference(self) -> None:
        urns = self._fields({"Loose": "[Missing] + 1"})
        assert urns == ["urn:li:schemaField:(urn:li:chart:(sigma,chart-1),Loose)"]
        assert self.src.reporter.chart_input_fields_skipped_sibling == 1
        assert self.src.reporter.chart_input_fields_sibling_inherited == 0
