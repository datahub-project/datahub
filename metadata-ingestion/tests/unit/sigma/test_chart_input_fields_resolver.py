"""Unit tests for the chart InputFields resolver helpers.

Cases cover probe-derived chart formulas and resolver behavior.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import (
    Element,
    Page,
    WarehouseTableUpstream,
    Workbook,
)
from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    extract_bracket_refs,
)
from datahub.ingestion.source.sigma.sigma import (
    SigmaSource,
    _UnresolvedChartColumn,
)
from datahub.metadata.schema_classes import InputFieldClass, InputFieldsClass

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
    # __new__ skips __init__, so attributes a real instance always
    # has must be set here or diagnostics reading them raise.
    source._current_workbook = None
    source.reporter = MagicMock()
    source.reporter.chart_input_fields_resolved = 0
    source.reporter.chart_input_fields_self_ref_fallback = 0
    source.reporter.chart_ref_source_normalized_match = 0
    source.reporter.chart_ref_source_normalized_ambiguous = 0
    source.reporter.chart_input_fields_skipped_parameter = 0
    source.reporter.chart_input_fields_skipped_sibling = 0
    source.reporter.chart_input_fields_case_mismatch = 0
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
    source._bridge_unresolved_warned = set()
    # Memos for maps derived from the per-workbook indexes.
    source._normalized_index_memo = None
    source._known_dm_element_index = None
    source.dm_element_urn_by_name = {}
    source._chart_cols_memo = None
    source._pending_schema_probe = []
    source._known_id_spaces = {}
    source._dm_column_owner = {}
    source._dm_url_id_by_id = {}
    source._unknown_head_ids = {}
    source._dm_id_by_url_id = {}
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
    raw = f"{source}/{column}" if column else source
    return BracketRef(raw=raw, source=source, column=column, is_parameter=is_parameter)


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

    def test_case_mismatched_workbook_element_ref_resolves_to_the_element(
        self,
    ) -> None:
        """A case-only difference resolves to the element, not the warehouse table.

        Sigma element names carry leading/trailing whitespace, non-breaking
        spaces and case differences from what a formula ref spells, leaving the
        element sitting in the index unreachable. The normalized lookup finds
        it; note the element still wins over the same-named warehouse table.
        """
        upstream_elem = _make_element("sourceElem", "T Source")
        wh_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.T SOURCE,PROD)"
        )
        ref = _make_ref("t source", "col")

        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"sourceElem"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"T Source": [upstream_elem]},
            element_warehouse_table_index={"T SOURCE": [wh_urn]},
            elementId_to_chart_urn={"sourceElem": "urn:source"},
        )

        assert result == ("urn:source", "col")
        assert self.src.reporter.chart_ref_source_normalized_match == 1
        assert self.src.reporter.chart_input_fields_case_mismatch == 0

    def test_normalized_ref_source_ambiguity_is_not_guessed(self) -> None:
        """Two distinct element names collapsing to one normalized key stay unresolved."""
        ref = _make_ref("shared", "col")

        result = self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id="downstreamElem",
            chart_upstream_element_ids={"a", "b"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={
                "Shared": [_make_element("a", "Shared")],
                "Shared\xa0": [_make_element("b", "Shared\xa0")],
            },
            element_warehouse_table_index={},
            elementId_to_chart_urn={"a": "urn:a", "b": "urn:b"},
        )

        assert result is None
        assert self.src.reporter.chart_ref_source_normalized_ambiguous == 1
        assert self.src.reporter.chart_ref_source_normalized_match == 0

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


# ---------------------------------------------------------------------------
# _resolve_chart_join_chain_ref
# ---------------------------------------------------------------------------


class TestNameInLoadedDataModelOutcomes:
    """Split "the name exists in a model this workbook loads" by ownership.

    That bucket held 1,065 refs on one tenant and the number alone cannot be
    acted on: it is consistent with a candidate list that is too narrow, with
    names that are genuinely ambiguous, and with pure coincidence -- which need
    three different responses. The dev tenant has no instance of this case at
    all, so these paths are unreachable there and only a test can show they
    fire.
    """

    _URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.a,PROD)"
    _OTHER = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.b,PROD)"

    def _resolve(self, *, urns: List[str], cols: List[str]) -> SigmaSource:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.dm_element_urn_by_name = {"dm1": {"DIM_A": list(urns)}}
        src.dm_element_urn_to_cols = {u: {c.lower(): c for c in cols} for u in urns}
        src.dm_key_by_element_urn = {}
        src._resolve_chart_formula_upstream(
            _make_ref("DIM_A", "Col A"),
            chart_element_id="e1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
            workbook_dm_url_ids=frozenset({"dm1"}),
        )
        return src

    def test_one_owner_means_the_candidate_list_was_too_narrow(self) -> None:
        src = self._resolve(urns=[self._URN], cols=["Col A"])
        assert src.reporter.chart_ref_name_in_loaded_dm_outcomes == {"unique_owner": 1}

    def test_several_owners_cannot_be_resolved_by_name_at_all(self) -> None:
        src = self._resolve(urns=[self._URN, self._OTHER], cols=["Col A"])
        assert src.reporter.chart_ref_name_in_loaded_dm_outcomes == {
            "several_owners": 1
        }

    def test_no_owner_means_the_name_match_is_a_coincidence(self) -> None:
        src = self._resolve(urns=[self._URN], cols=["Unrelated"])
        assert src.reporter.chart_ref_name_in_loaded_dm_outcomes == {
            "no_candidate_owns_the_column": 1
        }

    def test_the_outcome_is_counted_without_debug_logging(self) -> None:
        """The counter must not depend on the log level.

        The detail line is DEBUG-gated; an earlier draft gated the counter with
        it, which would have reported 0 on any run without --debug.
        """
        logging.disable(logging.CRITICAL)
        try:
            src = self._resolve(urns=[self._URN], cols=["Col A"])
        finally:
            logging.disable(logging.NOTSET)
        assert src.reporter.chart_ref_name_in_loaded_dm_outcomes == {"unique_owner": 1}


class TestSchemaMeasurementRecordsFailures:
    """Every /schema outcome is bucketed and sampled, not just the useful one.

    Sampling only the successes answers "how many would this fix" and nothing
    else. The likely result is that it fixes less than hoped, and the next
    question is then what /schema holds for those columns instead -- which
    without the failing shapes costs another full run to answer.
    """

    def _measure(self, formula: Any, *, reason: str = "some_reason") -> SigmaSource:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_schema.return_value = {
            "sheets": {"sheet1": {"columns": {"col1": {"formula": formula}}}}
        }
        workbook = _make_workbook_with_elements([[_make_element("e1", "El")]])
        src._measure_schema_resolvable_refs(
            workbook,
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="col1",
                    reasons=frozenset({reason}),
                )
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )
        return src

    def _measure_unknown_head(
        self, *, head: str, second: str, owner_columns: Dict[str, str]
    ) -> SigmaSource:
        """Probe one 2-segment ref whose HEAD is in no id space this run holds."""
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_schema.return_value = {
            "sheets": {
                "sheet1": {
                    "columns": {
                        "col1": {
                            "formula": {
                                "type": "nameRef",
                                "path": [head, second],
                            }
                        }
                    }
                }
            }
        }
        element = _make_element("e1", "El", columns=list(owner_columns))
        element.column_id_by_name = dict(owner_columns)
        workbook = _make_workbook_with_elements([[element]])
        src._measure_schema_resolvable_refs(
            workbook,
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="col1",
                    reasons=frozenset({"some_reason"}),
                )
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )
        return src

    def test_path1_that_is_a_workbook_column_is_no_longer_unknown(self) -> None:
        """The 7,663-column finding.

        _dm_column_owner holds Data Model columns only, so a path[1] that is a
        WORKBOOK column id was reported as "unknown_column" without ever being
        looked up. The owning element makes the ref resolvable without ever
        identifying the head.
        """
        src = self._measure_unknown_head(
            head="notAnIdWeHold", second="wbCol1", owner_columns={"Amount": "wbCol1"}
        )

        kinds = src.reporter.chart_ref_schema_unknown_head_kinds
        (key,) = list(kinds)
        assert "p1=workbook_column_owner_differs" in key, key
        assert "p1=unknown_column" not in key

    def test_a_bare_warehouse_native_name_is_not_called_an_unknown_column(self) -> None:
        """Sigma writes a warehouse column here as a bare native name.

        Filing ORDER_NUMBER as "unknown_column" overstated how much is
        unidentifiable -- it is not a Sigma id at all, and it is well named.
        """
        src = self._measure_unknown_head(
            head="notAnIdWeHold", second="ORDER_NUMBER", owner_columns={}
        )

        (key,) = list(src.reporter.chart_ref_schema_unknown_head_kinds)
        assert "p1=warehouse_native_name" in key, key

    def test_an_opaque_unknown_path1_is_still_reported_unknown(self) -> None:
        """The residual must stay visible, or the two fixes above would hide it."""
        src = self._measure_unknown_head(
            head="notAnIdWeHold",
            second="_K2Iau-Uzf",
            # A populated map the id is absent from, so the lookup is real --
            # with no map at all "not_checked" would be the honest answer.
            owner_columns={"Amount": "someOtherColumnId"},
        )

        (key,) = list(src.reporter.chart_ref_schema_unknown_head_kinds)
        assert "p1=unknown_column" in key, key

    def test_sheet_element_fanout_is_measured_from_viz_sheet_id(self) -> None:
        """Whether a sheet maps to one chart or several decides cross_sheet.

        The mapping lives at elements[<id>].viz.sheetId. Reading `sheetId` from
        the top of the element instead reported 0 sheets and made the
        ambiguity look unmeasurable.
        """
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_schema.return_value = {
            "sheets": {
                "one": {"columns": {"col1": {"formula": None}}},
                "many": {"columns": {}},
            },
            "elements": {
                "eA": {"viz": {"sheetId": "one"}},
                "eB": {"viz": {"sheetId": "many"}},
                "eC": {"viz": {"sheetId": "many"}},
            },
        }
        workbook = _make_workbook_with_elements([[_make_element("e1", "El")]])
        # The probe short-circuits with nothing unresolved -- correctly, since
        # it costs an API call -- so give it one column to work on.
        src._measure_schema_resolvable_refs(
            workbook,
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="col1",
                    reasons=frozenset({"some_reason"}),
                )
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )

        assert src.reporter.chart_ref_schema_sheet_element_fanout == {"1": 1, "2+": 1}

    def test_a_rare_outcome_is_still_sampled_beside_a_flood(self) -> None:
        """The regression this change exists for.

        One shared LossyList reservoir-samples ~10 entries across all outcomes,
        so a minority outcome draws slots in proportion to its share. On a full
        customer run `join_chain` was 263 of ~16,236 unresolvable columns --
        expected yield 0.16 -- and returned ZERO samples, leaving the one gap
        that is entirely ours with a count and no evidence. Per-outcome budgets
        make the rare one survive regardless of how much else there is.
        """
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.sigma_api = MagicMock()
        # 200 local_only columns and a single 3-segment join chain.
        columns: Dict[str, Any] = {
            f"c{i}": {"formula": {"type": "nameRef", "path": ["sibling"]}}
            for i in range(200)
        }
        columns["rare"] = {"formula": {"type": "nameRef", "path": ["a", "b", "Col"]}}
        src.sigma_api.get_workbook_schema.return_value = {
            "sheets": {"sheet1": {"columns": columns}}
        }
        workbook = _make_workbook_with_elements([[_make_element("e1", "El")]])
        src._measure_schema_resolvable_refs(
            workbook,
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column=f"Col {cid}",
                    column_id=cid,
                    reasons=frozenset({"some_reason"}),
                )
                for cid in columns
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )

        by_outcome = src.reporter.chart_ref_schema_samples_by_outcome
        assert src.reporter.chart_ref_schema_local_only == 200
        assert src.reporter.chart_ref_schema_join_chain == 1
        assert len(by_outcome["join_chain"]) == 1, (
            "the 1-in-201 outcome must still be sampled; a shared reservoir "
            f"would very likely drop it. got {dict(by_outcome)}"
        )
        assert by_outcome["local_only"], "the common outcome is sampled too"

    def test_a_local_only_column_is_sampled_with_its_paths(self) -> None:
        src = self._measure({"type": "nameRef", "path": ["someOtherColumn"]})

        assert src.reporter.chart_ref_schema_local_only == 1
        assert src.reporter.chart_ref_schema_outcomes_by_reason == {
            "local_only::some_reason": 1
        }
        (sample,) = list(src.reporter.chart_ref_schema_samples_by_outcome["local_only"])
        assert "someOtherColumn" in sample
        assert "some_reason" in sample

    def test_a_one_segment_inode_ref_is_warehouse_not_local(self) -> None:
        """Sigma writes a warehouse column as ONE segment, not two.

        Keying the inode test on a two-segment path filed 23 of 83 one-segment
        refs on a real tenant as local columns, which reports the opposite of
        what they say. The shape here is copied from a live /schema response.
        """
        src = self._measure(
            {"type": "nameRef", "path": ["inode-Kq7dR2mXbT9nZv4LsW6yHc/COST"]}
        )

        assert src.reporter.chart_ref_schema_warehouse_resolvable == 1
        assert src.reporter.chart_ref_schema_local_only == 0

    def test_a_data_model_element_ref_is_not_reported_as_local(self) -> None:
        """A "<dataModelId>/<elementId>" head names a Data Model element by id.

        It fell into the old catch-all else, so a ref that states a dependency
        was counted as one that denies having any.
        """
        src = self._measure(
            {"type": "nameRef", "path": ["Vb3TgN8kQm5RzXc7WdLp2J/Zt4KpQ7nWx", "Col B"]}
        )

        assert src.reporter.chart_ref_schema_dm_element == 1
        assert src.reporter.chart_ref_schema_local_only == 0

    def test_an_unrecognised_head_is_reported_with_the_raw_id(self) -> None:
        """An unknown id space must surface the head, not be silently dropped.

        The head is the only thing that can identify which id space it belongs
        to, and it is opaque, so the counter alone says nothing actionable.
        """
        src = self._measure({"type": "nameRef", "path": ["qQ1zXvB2pL", "Col B"]})

        assert src.reporter.chart_ref_schema_unknown_head == 1
        assert list(src.reporter.chart_ref_schema_unknown_head_samples) == [
            "qQ1zXvB2pL"
        ]
        # No Data Model pass ran, so most spaces were never built. That must say
        # so explicitly, not plain "none", which would claim the head was
        # checked against a universe this run never assembled.
        (kind,) = src.reporter.chart_ref_schema_unknown_head_kinds
        assert kind.startswith("space=none_dm_pass_skipped ")

    def test_a_head_matching_a_known_id_space_is_named(self) -> None:
        """A head in a space this run already walked is named, not described.

        This is the identification a standalone probe of /schema cannot make:
        the head is never defined inside the document that cites it, so only a
        run that has also walked datasets and Data Models can place it.
        """
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src._known_id_spaces = {
            "sigma_dataset_url_id": {"someDataset"},
            "data_model_element_id": {"qQ1zXvB2pL"},
        }
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_schema.return_value = {
            "sheets": {
                "sheet1": {
                    "type": "levelTable",
                    "columns": {
                        "col1": {
                            "formula": {
                                "type": "nameRef",
                                "path": ["qQ1zXvB2pL", "Col B"],
                            }
                        }
                    },
                }
            }
        }
        src._measure_schema_resolvable_refs(
            _make_workbook_with_elements([[_make_element("e1", "El")]]),
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="col1",
                    reasons=frozenset({"some_reason"}),
                )
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )

        (kind,) = src.reporter.chart_ref_schema_unknown_head_kinds
        assert kind.startswith("space=data_model_element_id ")
        assert "sheet=levelTable" in kind

    def test_path1_identifies_the_head_even_when_the_head_is_in_no_space(
        self,
    ) -> None:
        """path[1] is a column id, and this run knows every Data Model column.

        So the element owning that column says what the ref points at even
        when the head matches nothing -- which is the case on every tenant
        measured so far, and would otherwise be the end of the enquiry.
        """
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src._known_id_spaces = {"data_model_id": {"dm1"}}
        src._dm_column_owner = {"colB": "ownerElement"}
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_schema.return_value = {
            "sheets": {
                "sheet1": {
                    "columns": {
                        "col1": {
                            "formula": {
                                "type": "nameRef",
                                "path": ["qQ1zXvB2pL", "colB"],
                            }
                        }
                    }
                }
            }
        }
        src._measure_schema_resolvable_refs(
            _make_workbook_with_elements([[_make_element("e1", "El")]]),
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="col1",
                    reasons=frozenset({"some_reason"}),
                )
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )

        (kind,) = src.reporter.chart_ref_schema_unknown_head_kinds
        # The head is in no space, but the column it names is owned by an
        # element that is NOT the head -- so heads are not element ids here,
        # and the ref points at that owner.
        assert "space=none " in kind
        assert "p1=dm_column_owner_differs" in kind
        assert src.reporter.chart_ref_schema_unknown_head_distinct == 1

    def test_a_cross_sheet_ref_outranks_a_warehouse_ref_in_the_same_formula(
        self,
    ) -> None:
        """A formula holding both shapes is filed under the more specific one.

        Live /schema writes cross-sheet refs whose TARGET is itself warehouse-
        qualified ("<sheetId>", "inode-<urlId>/<COL>"), so an unordered test
        would classify by dict iteration order.
        """
        src = self._measure(
            {
                "type": "binOp",
                "args": [
                    {
                        "type": "nameRef",
                        "path": ["sheet1", "inode-Kq7dR2mXbT9nZv4LsW6yHc/ORDER_NUMBER"],
                    },
                    {"type": "nameRef", "path": ["inode-Kq7dR2mXbT9nZv4LsW6yHc/COST"]},
                ],
            }
        )

        assert src.reporter.chart_ref_schema_cross_sheet_resolvable == 1
        assert src.reporter.chart_ref_schema_warehouse_resolvable == 0

    def test_a_column_absent_from_schema_is_sampled_too(self) -> None:
        """The two endpoints disagreeing about the workbook is a finding."""
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_schema.return_value = {"sheets": {}}
        workbook = _make_workbook_with_elements([[_make_element("e1", "El")]])
        src._measure_schema_resolvable_refs(
            workbook,
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="missing",
                    reasons=frozenset({"some_reason"}),
                )
            ],
            schema=src.sigma_api.get_workbook_schema(None),
        )

        assert src.reporter.chart_ref_schema_column_absent == 1
        assert src.reporter.chart_ref_schema_outcomes_by_reason == {
            "column_absent::some_reason": 1
        }
        assert (
            len(src.reporter.chart_ref_schema_samples_by_outcome["column_absent"]) == 1
        )

    def test_a_resolvable_column_is_sampled_under_its_own_outcome(self) -> None:
        src = self._measure(
            {"type": "nameRef", "path": ["sheet1", "col1"]},
            reason="element_named_but_not_a_lineage_upstream",
        )

        assert src.reporter.chart_ref_schema_cross_sheet_resolvable == 1
        assert src.reporter.chart_ref_schema_resolvable_by_reason == {
            "element_named_but_not_a_lineage_upstream": 1
        }
        # Sampled under cross_sheet, and crucially not pooled with the
        # failures -- a shared reservoir starved join_chain to zero.
        assert list(src.reporter.chart_ref_schema_samples_by_outcome) == ["cross_sheet"]
        # Present in the by-outcome map too, so one map covers every column.
        assert src.reporter.chart_ref_schema_outcomes_by_reason == {
            "cross_sheet::element_named_but_not_a_lineage_upstream": 1
        }

    def test_refs_nested_under_an_operator_are_still_found(self) -> None:
        """Sigma nests refs under binOp/callOp/path, so the tree must be walked.

        Reading only the top level would report no_refs for every computed
        column and understate the endpoint.
        """
        src = self._measure(
            {
                "type": "binOp",
                "op": "*",
                "x": {"type": "nameRef", "path": ["sheet1", "col1"]},
                "y": {"type": "const", "val": 2},
            }
        )

        assert src.reporter.chart_ref_schema_cross_sheet_resolvable == 1


class TestChartColumnAccountingCheck:
    """The counters reconcile themselves, so a future gap is one line to find.

    Both identities here were originally derived by hand from two 100MB logs,
    twice, because nothing in the report said they were broken -- every counter
    that fired looked healthy and the residual was only visible as a constant
    234 across runs whose totals differed. Checking them in-run turns that into
    a number in the next report.
    """

    def _source(self) -> SigmaSource:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        return src

    def test_balanced_counters_report_reconciles(self) -> None:
        src = self._source()
        r = src.reporter
        r.chart_input_fields_self_ref_fallback = 10
        r.chart_input_fields_formulas_not_fetched = 4
        r.chart_input_fields_self_ref_no_formula = 3
        r.chart_input_fields_self_ref_unresolved_refs = 3
        r.chart_ref_miss_reasons = {"source_name_unknown_to_this_workbook": 3}

        src.get_report()

        assert r.chart_column_accounting_check == {"reconciles": 1}

    def test_an_unbalanced_fallback_split_reports_the_residual(self) -> None:
        src = self._source()
        r = src.reporter
        r.chart_input_fields_self_ref_fallback = 10
        r.chart_input_fields_formulas_not_fetched = 4
        r.chart_input_fields_self_ref_no_formula = 3
        r.chart_input_fields_self_ref_unresolved_refs = 1  # two columns missing
        r.chart_ref_miss_reasons = {"source_name_unknown_to_this_workbook": 1}

        src.get_report()

        assert r.chart_column_accounting_check["fallback_split_residual"] == 2
        assert r.chart_column_accounting_check["reconciles"] == 0

    def test_columns_with_no_recorded_cause_are_reported(self) -> None:
        """The exact shape of the bug this check exists for."""
        src = self._source()
        r = src.reporter
        r.chart_input_fields_self_ref_fallback = 5
        r.chart_input_fields_formulas_not_fetched = 0
        r.chart_input_fields_self_ref_no_formula = 0
        r.chart_input_fields_self_ref_unresolved_refs = 5
        r.chart_ref_miss_reasons = {"source_name_unknown_to_this_workbook": 3}

        src.get_report()

        assert r.chart_column_accounting_check["unattributed_columns"] == 2
        assert r.chart_column_accounting_check["reconciles"] == 0

    def test_the_check_does_not_read_its_own_previous_result(self) -> None:
        """get_report() runs repeatedly -- the periodic report calls it.

        Deriving "reconciles" from whether the dict was empty made the second
        call see the first call's own output and report a failure with no
        residual behind it. A dev tenant that reconciles perfectly showed
        ``{'reconciles': 0}`` and nothing else.
        """
        src = self._source()
        r = src.reporter
        r.chart_input_fields_self_ref_fallback = 3
        r.chart_input_fields_self_ref_unresolved_refs = 3
        r.chart_ref_miss_reasons = {"source_name_unknown_to_this_workbook": 3}

        src.get_report()
        src.get_report()
        src.get_report()

        assert r.chart_column_accounting_check == {"reconciles": 1}

    def test_the_synthetic_sub_keys_are_not_double_counted(self) -> None:
        """They split an existing reason; counting them would mask a real gap.

        Three refs, all the same cause, each also filed under one sub-key. A
        naive sum reads 6 and would hide a shortfall of up to three columns.
        """
        src = self._source()
        r = src.reporter
        r.chart_input_fields_self_ref_fallback = 3
        r.chart_input_fields_self_ref_unresolved_refs = 3
        r.chart_ref_miss_reasons = {
            "source_name_unknown_to_this_workbook": 3,
            "unknown_source_absent_from_this_workbooks_data_models": 2,
            "unknown_source_but_name_exists_in_a_data_model_this_workbook_loads": 1,
        }

        src.get_report()

        assert r.chart_column_accounting_check == {"reconciles": 1}


class TestEveryUnresolvedRefIsAttributable:
    """``chart_ref_miss_reasons`` must account for every ref that fails.

    The breakdown exists so an operator can say which unresolved refs are a
    connector problem and which are not. That argument only holds if it
    reconciles against its own total -- and it did not: a join-chain ref whose
    every split failed incremented ``chart_join_chain_dangling_suppressed`` and
    returned, recording no reason. On two consecutive tenant runs that left 234
    columns unattributable, invisible because the counter it DID increment
    looked healthy.
    """

    def _resolve(self, formula: str) -> Tuple[SigmaSource, Optional[Tuple[str, str]]]:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src.dm_element_urn_to_cols = {}
        src.dm_element_urn_by_name = {}
        src.dm_key_by_element_urn = {}
        ref = extract_bracket_refs(formula)[0]
        result = src._resolve_chart_ref(
            ref,
            chart_element_id="chart-1",
            chart_upstream_element_ids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )
        return src, result

    def test_a_join_chain_ref_with_no_valid_split_records_a_reason(self) -> None:
        src, result = self._resolve("[Joined/Element B/Col K]")

        assert result is None
        assert src.reporter.chart_join_chain_dangling_suppressed == 1
        assert src.reporter.chart_ref_miss_reasons.get("join_chain_no_valid_split") == 1

    def test_a_two_segment_ref_is_still_attributed_by_the_ordinary_path(
        self,
    ) -> None:
        """The <=2 branch must keep its own, more specific reasons."""
        src, result = self._resolve("[Element B/Col K]")

        assert result is None
        assert src.reporter.chart_join_chain_dangling_suppressed == 0
        assert "join_chain_no_valid_split" not in src.reporter.chart_ref_miss_reasons
        # One ref, one reason -- plus the synthetic sub-key that splits the
        # "unknown source" case, which is why the breakdown is not a flat sum.
        assert src.reporter.chart_ref_miss_reasons == {
            "source_name_unknown_to_this_workbook": 1,
            "unknown_source_absent_from_this_workbooks_data_models": 1,
        }


class TestChartJoinChainRef:
    """[JoinElement/SourceElement/Column] on the chart path.

    The first-slash split reads the column as "SourceElement/Column", which no
    upstream has, so the InputField emitted is dangling. Every split is tried
    and validated against the candidate upstream's real column list instead.
    """

    def _source(self):
        source = _make_source()
        source.reporter.chart_join_chain_resolved = 0
        source.reporter.chart_join_chain_unresolved = 0
        source.reporter.chart_join_chain_upstream_schema_unavailable = 0
        source.reporter.chart_join_chain_sibling_resolved = 0
        source.reporter.chart_join_chain_sibling_dm_unknown = 0
        source.reporter.chart_join_chain_sibling_ambiguous = 0
        source.reporter.chart_join_chain_sibling_column_absent = 0
        source.dm_element_urn_to_cols = {}
        source.dm_element_urn_by_name = {}
        source.dm_key_by_element_urn = {}
        return source

    def _resolve(self, source, formula, *, elements, upstream_ids, chart_urns):
        ref = extract_bracket_refs(formula)[0]
        index: Dict[str, List[Element]] = {}
        for element in elements:
            index.setdefault(element.name, []).append(element)
        return source._resolve_chart_join_chain_ref(
            ref,
            chart_element_id="chart-1",
            chart_upstream_element_ids=set(upstream_ids),
            dm_upstream_urn_by_element_name={},
            wb_element_index=index,
            element_warehouse_table_index={},
            elementId_to_chart_urn=chart_urns,
        )

    def test_owning_element_is_the_segment_before_the_column(self) -> None:
        source = self._source()
        join = _make_element("e-join", "Joined", ["Col K"])
        owner = _make_element("e-owner", "Element B", ["Col K"])
        result = self._resolve(
            source,
            "[Joined/Element B/Col K]",
            elements=[join, owner],
            upstream_ids=["e-join", "e-owner"],
            chart_urns={
                "e-join": "urn:li:chart:(sigma,join)",
                "e-owner": "urn:li:chart:(sigma,owner)",
            },
        )
        assert result == ("urn:li:chart:(sigma,owner)", "Col K")
        assert source.reporter.chart_join_chain_resolved == 1

    def test_candidate_whose_column_is_absent_is_rejected(self) -> None:
        """The join element resolves, but does not have the named column."""
        source = self._source()
        # Neither the join element nor the named source has the column, so
        # no candidate split validates.
        join = _make_element("e-join", "Joined", ["Other"])
        owner = _make_element("e-owner", "Element B", ["Other"])
        result = self._resolve(
            source,
            "[Joined/Element B/Col K]",
            elements=[join, owner],
            upstream_ids=["e-join", "e-owner"],
            chart_urns={
                "e-join": "urn:li:chart:(sigma,join)",
                "e-owner": "urn:li:chart:(sigma,owner)",
            },
        )
        assert result is None
        assert source.reporter.chart_join_chain_unresolved == 1

    def test_single_slash_ref_is_left_to_the_legacy_path(self) -> None:
        source = self._source()
        owner = _make_element("e-owner", "Element B", ["Col K"])
        result = self._resolve(
            source,
            "[Element B/Col K]",
            elements=[owner],
            upstream_ids=["e-owner"],
            chart_urns={"e-owner": "urn:li:chart:(sigma,owner)"},
        )
        assert result is None
        assert source.reporter.chart_join_chain_resolved == 0
        assert source.reporter.chart_join_chain_unresolved == 0

    def test_join_element_supplies_the_column_when_the_owner_is_unreachable(
        self,
    ) -> None:
        """The shape that made this resolve 0 of 832 on a live tenant.

        The owning element is internal to a Data Model, so it is not a workbook
        element and resolves to nothing. The join element IS reachable and
        carries the joined column in its own output, so the edge lands there
        rather than being lost.
        """
        source = self._source()
        join = _make_element("e-join", "Joined", ["Col K", "Other"])
        result = self._resolve(
            source,
            "[Joined/Element B/Col K]",
            elements=[join],  # 'Element B' deliberately absent
            upstream_ids=["e-join"],
            chart_urns={"e-join": "urn:li:chart:(sigma,join)"},
        )
        assert result == ("urn:li:chart:(sigma,join)", "Col K")
        assert source.reporter.chart_join_chain_resolved == 1

    def test_owning_element_still_wins_when_it_is_reachable(self) -> None:
        """The join element is a fallback, never a preference."""
        source = self._source()
        join = _make_element("e-join", "Joined", ["Col K"])
        owner = _make_element("e-owner", "Element B", ["Col K"])
        result = self._resolve(
            source,
            "[Joined/Element B/Col K]",
            elements=[join, owner],
            upstream_ids=["e-join", "e-owner"],
            chart_urns={
                "e-join": "urn:li:chart:(sigma,join)",
                "e-owner": "urn:li:chart:(sigma,owner)",
            },
        )
        assert result == ("urn:li:chart:(sigma,owner)", "Col K")

    def test_join_element_without_the_column_resolves_nothing(self) -> None:
        """Schema validation still gates the fallback -- no dangling field."""
        source = self._source()
        join = _make_element("e-join", "Joined", ["Unrelated"])
        result = self._resolve(
            source,
            "[Joined/Element B/Col K]",
            elements=[join],
            upstream_ids=["e-join"],
            chart_urns={"e-join": "urn:li:chart:(sigma,join)"},
        )
        assert result is None
        assert source.reporter.chart_join_chain_unresolved == 1

    def test_column_casing_is_normalised_to_the_upstream_spelling(self) -> None:
        source = self._source()
        join = _make_element("e-join", "Joined", [])
        owner = _make_element("e-owner", "Element B", ["Col K"])
        result = self._resolve(
            source,
            "[Joined/Element B/col k]",
            elements=[join, owner],
            upstream_ids=["e-join", "e-owner"],
            chart_urns={
                "e-join": "urn:li:chart:(sigma,join)",
                "e-owner": "urn:li:chart:(sigma,owner)",
            },
        )
        assert result == ("urn:li:chart:(sigma,owner)", "Col K")


class TestJoinChainResolvesThroughDataModelSiblings:
    """[JoinElement/JoinedTable/Column] where the joined table is internal.

    The dominant real shape: on one tenant 806 of 832 multi-segment refs failed
    with exactly this signature -- the middle segment reported "no upstream"
    while the first segment resolved but lacked the column. The chart declares
    only the join element as its upstream; the table joined into it is a
    sibling element of the same Data Model and is invisible from the chart's
    own indices.
    """

    _JOIN_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.join,PROD)"
    _SIBLING_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm1.dim,PROD)"

    def _source(self, *, sibling_cols=None, sibling_urns=None):
        source = _make_source()
        for counter in (
            "chart_join_chain_resolved",
            "chart_join_chain_unresolved",
            "chart_join_chain_upstream_schema_unavailable",
            "chart_join_chain_sibling_resolved",
            "chart_join_chain_sibling_dm_unknown",
            "chart_join_chain_sibling_ambiguous",
            "chart_join_chain_sibling_column_absent",
        ):
            setattr(source.reporter, counter, 0)
        # The join element resolves, but its own output does not carry the
        # referenced column -- which is why the ref names the joined table.
        source.dm_element_urn_to_cols = {
            self._JOIN_URN: {"join key": "Join Key"},
            self._SIBLING_URN: {c.lower(): c for c in (sibling_cols or ["Col A"])},
        }
        source.dm_key_by_element_urn = {self._JOIN_URN: "dm1"}
        source.dm_element_urn_by_name = {
            "dm1": {"dim_a": sibling_urns or [self._SIBLING_URN]}
        }
        return source

    def _resolve(self, source, formula):
        ref = extract_bracket_refs(formula)[0]
        return source._resolve_chart_join_chain_ref(
            ref,
            chart_element_id="chart-1",
            chart_upstream_element_ids=set(),
            # The join element is a DM upstream, not a workbook page element.
            dm_upstream_urn_by_element_name={"Joined": self._JOIN_URN},
            wb_element_index={},
            element_warehouse_table_index={},
            elementId_to_chart_urn={},
        )

    def test_middle_segment_resolves_to_a_sibling_of_the_same_data_model(
        self,
    ) -> None:
        source = self._source()
        result = self._resolve(source, "[Joined/DIM_A/Col A]")
        assert result == (self._SIBLING_URN, "Col A")
        assert source.reporter.chart_join_chain_sibling_resolved == 1
        # Counted in the headline counter too, so the two stay comparable.
        assert source.reporter.chart_join_chain_resolved == 1
        assert source.reporter.chart_join_chain_unresolved == 0

    def test_sigma_join_count_label_is_stripped(self) -> None:
        """Sigma writes the joined node as "DIM_A + 3", not as its name."""
        source = self._source()
        result = self._resolve(source, "[Joined/DIM_A + 3/Col A]")
        assert result == (self._SIBLING_URN, "Col A")

    def test_sibling_without_the_column_emits_nothing(self) -> None:
        """Schema validation gates this path too -- never a dangling field."""
        source = self._source(sibling_cols=["Unrelated"])
        result = self._resolve(source, "[Joined/DIM_A/Col A]")
        assert result is None
        assert source.reporter.chart_join_chain_sibling_column_absent == 1
        assert source.reporter.chart_join_chain_unresolved == 1

    def test_duplicate_sibling_names_are_refused_not_guessed(self) -> None:
        source = self._source(
            sibling_urns=[self._SIBLING_URN, "urn:li:dataset:(other,x,PROD)"]
        )
        result = self._resolve(source, "[Joined/DIM_A/Col A]")
        assert result is None
        assert source.reporter.chart_join_chain_sibling_ambiguous == 1

    def test_first_segment_outside_a_data_model_has_no_siblings(self) -> None:
        """A chart or warehouse upstream has no Data Model to search."""
        source = self._source()
        source.dm_key_by_element_urn = {}
        result = self._resolve(source, "[Joined/DIM_A/Col A]")
        assert result is None
        assert source.reporter.chart_join_chain_sibling_dm_unknown == 1


class TestJoinChainProbeDoesNotInflateCounters:
    """Speculative candidate splits must not be counted as if they were refs.

    _resolve_chart_join_chain_ref tries up to 2N-3 splits through
    _resolve_chart_formula_upstream. Letting each attempt bump the name-matching
    counters would make them measure attempts rather than refs -- one 4-segment
    ref could bump the same counter five times.
    """

    def test_probing_does_not_bump_name_matching_counters(self) -> None:
        source = _make_source()
        source.reporter.chart_ref_source_normalized_match = 0
        source.reporter.chart_ref_source_near_miss = 0
        source.reporter.chart_join_chain_resolved = 0
        source.reporter.chart_join_chain_unresolved = 0
        source.reporter.chart_join_chain_upstream_schema_unavailable = 0
        source.dm_element_urn_to_cols = {}
        # Element name differs from every candidate only by case/whitespace, so
        # each probe would take the normalized path and count.
        elem = _make_element("e-join", "Joined ", ["Col K"])
        ref = extract_bracket_refs("[joined/Element B/Col K]")[0]
        source._resolve_chart_join_chain_ref(
            ref,
            chart_element_id="chart-1",
            chart_upstream_element_ids={"e-join"},
            dm_upstream_urn_by_element_name={},
            wb_element_index={"Joined ": [elem]},
            element_warehouse_table_index={},
            elementId_to_chart_urn={"e-join": "urn:li:chart:(sigma,join)"},
        )
        assert source.reporter.chart_ref_source_normalized_match == 0
        assert source.reporter.chart_ref_source_near_miss == 0


class TestChartRefMissIsAttributedToACause:
    """A miss must say WHICH step gave up.

    ``chart_input_fields_self_ref_unresolved_refs`` reached 17,944 on one tenant
    (2026-09) while concentrating in only 87 distinct source names -- so the
    bucket is a handful of causes, and a single number could not tell which.
    """

    def setup_method(self) -> None:
        self.src = _make_source()
        # The shared fixture mocks the reporter; these assertions read counters.
        self.src.reporter = SigmaSourceReport()

    def _resolve(self, ref, **kwargs):
        return self.src._resolve_chart_formula_upstream(
            ref,
            chart_element_id=kwargs.pop("chart_element_id", "e1"),
            chart_upstream_element_ids=kwargs.pop("chart_upstream_element_ids", set()),
            dm_upstream_urn_by_element_name=kwargs.pop(
                "dm_upstream_urn_by_element_name", {}
            ),
            wb_element_index=kwargs.pop("wb_element_index", {}),
            element_warehouse_table_index=kwargs.pop(
                "element_warehouse_table_index", {}
            ),
            elementId_to_chart_urn=kwargs.pop("elementId_to_chart_urn", {}),
            **kwargs,
        )

    def test_unknown_source_absent_from_the_run_is_separated_from_a_scope_miss(
        self,
    ) -> None:
        """Two different bugs that used to be one number.

        A name nothing in the run defines means the run never saw that element.
        A name another Data Model DOES define means our lookup scope was too
        narrow -- fixable here, unlike the first.
        """
        assert self._resolve(_make_ref("NeverSeen", "col")) is None
        reasons = self.src.reporter.chart_ref_miss_reasons
        assert reasons["source_name_unknown_to_this_workbook"] == 1
        assert reasons["unknown_source_absent_from_this_workbooks_data_models"] == 1

        # The element exists but does NOT have the referenced column, so the
        # last-resort global-name step refuses it and the miss still lands in
        # the "scope was too narrow" bucket rather than resolving.
        self.src.dm_element_urn_by_name = {"dm-a": {"KnownElsewhere": ["urn:x"]}}
        self.src.dm_element_urn_to_cols = {"urn:x": {"other": "Other"}}
        self.src._known_dm_element_index = None
        assert (
            self._resolve(
                _make_ref("KnownElsewhere", "col"),
                workbook_dm_url_ids=frozenset({"dm-a"}),
            )
            is None
        )
        assert (
            self.src.reporter.chart_ref_miss_reasons[
                "unknown_source_but_name_exists_in_a_data_model_this_workbook_loads"
            ]
            == 1
        )

    def test_speculative_candidate_splits_do_not_inflate_the_reasons(self) -> None:
        """A join-chain ref probes up to 2N-3 splits through this resolver.

        Counting each probe would report several misses for one ref, which is
        what made the aggregate unreadable in the first place.
        """
        assert self._resolve(_make_ref("NeverSeen", "col"), count=False) is None
        assert self.src.reporter.chart_ref_miss_reasons == {}

        # Same ref, counted: proves the empty dict above is the gate working,
        # not the recording being absent altogether.
        assert self._resolve(_make_ref("NeverSeen", "col")) is None
        assert self.src.reporter.chart_ref_miss_reasons != {}


class TestFetchFailureIsNotReportedAsMissingFormula:
    """A workbook whose /columns call aborted has no formulas THROUGH OUR FAULT.

    On one tenant (2026-09) 12 workbooks aborted having retrieved zero entries,
    and every column in them was counted under
    ``chart_input_fields_self_ref_no_formula`` -- which reads as "Sigma has
    nothing to give" and hid a fetch failure behind an upstream limitation.
    """

    def setup_method(self) -> None:
        self.src = _make_source()
        self.src.reporter = SigmaSourceReport()

    def _count(self, *, formulas_incomplete: bool) -> None:
        self.src._count_unresolved_chart_column(
            element=_make_element("e1", "Chart", ["col"]),
            column="col",
            refs=[],
            all_param=False,
            all_sibling=False,
            all_unresolvable_mixed=False,
            formulas_incomplete=formulas_incomplete,
        )

    def test_a_fetched_workbook_with_no_formula_stays_in_the_original_bucket(
        self,
    ) -> None:
        self._count(formulas_incomplete=False)
        assert self.src.reporter.chart_input_fields_self_ref_no_formula == 1
        assert self.src.reporter.chart_input_fields_formulas_not_fetched == 0

    def test_an_aborted_workbook_is_attributed_to_the_fetch_not_to_sigma(self) -> None:
        self._count(formulas_incomplete=True)
        assert self.src.reporter.chart_input_fields_formulas_not_fetched == 1
        assert self.src.reporter.chart_input_fields_self_ref_no_formula == 0
        # Still one column in the fallback bucket either way: the split is a
        # sub-category, so the per-element invariant is unchanged.
        assert self.src.reporter.chart_input_fields_self_ref_fallback == 1


class TestWorkbookSourcesMeasurement:
    """What GET /workbooks/{id}/sources would explain, measured before built.

    The counters that matter are the ones read when the hypothesis is WRONG:
    a declared-element set that /lineage already covered means the endpoint
    adds nothing, and that has to be legible directly rather than inferred
    from a small "explains" number.
    """

    def _measure(
        self,
        entries: Any,
        *,
        column_owner: Optional[Dict[str, str]] = None,
        lineage_dm_url_ids: FrozenSet[str] = frozenset(),
        url_id_by_id: Optional[Dict[str, str]] = None,
    ) -> SigmaSource:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src._dm_column_owner = column_owner or {}
        src._dm_url_id_by_id = url_id_by_id or {}
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_sources.return_value = entries
        src._measure_workbook_sources(
            _make_workbook_with_elements([[_make_element("e1", "El")]]),
            [
                _UnresolvedChartColumn(
                    element_id="e1",
                    column="Col",
                    column_id="colB",
                    reasons=frozenset({"some_reason"}),
                )
            ],
            lineage_dm_url_ids,
        )
        return src

    def test_a_declared_element_owning_the_column_is_counted_as_explained(
        self,
    ) -> None:
        src = self._measure(
            [{"type": "data-model", "dataModelId": "dm1", "elementIds": ["ownerEl"]}],
            column_owner={"colB": "ownerEl"},
        )

        assert src.reporter.chart_ref_sources_explains == 1
        assert src.reporter.chart_ref_sources_explains_by_reason == {"some_reason": 1}
        assert src.reporter.chart_ref_sources_outcomes_by_reason == {
            "explains::some_reason": 1
        }

    def test_a_source_list_lineage_already_covered_reports_nothing_new(self) -> None:
        """The case that kills the idea, and it must be legible as such.

        sources declaring only Data Models the workbook's own lineage already
        reached states nothing new, however many entries it returns.
        """
        src = self._measure(
            [{"type": "data-model", "dataModelId": "dm1", "elementIds": ["a", "b"]}],
            lineage_dm_url_ids=frozenset({"dm1-url"}),
            url_id_by_id={"dm1": "dm1-url"},
        )

        assert src.reporter.workbook_sources_dm_elements_declared == 2
        assert src.reporter.workbook_sources_dm_elements_new_vs_lineage == 0

    def test_an_element_absent_from_lineage_is_the_recovered_set(self) -> None:
        src = self._measure(
            [{"type": "data-model", "dataModelId": "dm1", "elementIds": ["a", "b"]}],
            lineage_dm_url_ids=frozenset({"other-url"}),
            url_id_by_id={"dm1": "dm1-url"},
        )

        assert src.reporter.workbook_sources_dm_elements_new_vs_lineage == 2

    def test_the_undocumented_type_vocabulary_is_recorded(self) -> None:
        src = self._measure(
            [
                {"type": "data-model", "dataModelId": "dm1", "elementIds": []},
                {"type": "table", "inodeId": "inode-abc"},
            ]
        )

        assert src.reporter.workbook_sources_entry_types == {
            "data-model": 1,
            "table": 1,
        }
        assert src.reporter.workbook_sources_warehouse_entries == 1

    def test_a_failed_fetch_measures_nothing_and_never_raises(self) -> None:
        src = self._measure(None)

        assert src.reporter.workbook_sources_workbooks_read == 0
        assert src.reporter.chart_ref_sources_explains == 0
        assert src.reporter.chart_ref_sources_owner_not_declared == 0

    def test_a_column_with_no_known_owner_is_not_read_as_undeclared(self) -> None:
        """ "Owner unknown" and "owner not declared" are different findings.

        Pooling them would let a gap in the Data Model column universe read as
        evidence against the endpoint.
        """
        src = self._measure(
            [{"type": "data-model", "dataModelId": "dm1", "elementIds": ["ownerEl"]}]
        )

        assert src.reporter.chart_ref_sources_column_owner_unknown == 1
        assert src.reporter.chart_ref_sources_owner_not_declared == 0

    def test_a_workbook_with_no_unresolved_columns_still_measures_its_sources(
        self,
    ) -> None:
        """The vocabulary and the lineage delta are tenant properties.

        Sampling them only where the connector already fails would understate
        both and answer a narrower question than the one being asked.
        """
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src._dm_column_owner = {}
        src._dm_url_id_by_id = {"dm1": "dm1-url"}
        src.sigma_api = MagicMock()
        src.sigma_api.get_workbook_sources.return_value = [
            {"type": "data-model", "dataModelId": "dm1", "elementIds": ["a"]}
        ]

        src._measure_workbook_sources(
            _make_workbook_with_elements([[_make_element("e1", "El")]]),
            [],
            frozenset(),
        )

        assert src.reporter.workbook_sources_workbooks_read == 1
        assert src.reporter.workbook_sources_dm_elements_new_vs_lineage == 1


class TestUnknownHeadRecheck:
    """A head missed early must not be reported as unidentifiable.

    `space=` is decided when a head is first met, against a set that is still
    filling as workbooks are walked. The re-check is the correction.
    """

    def _source(self, *, heads: Dict[str, int], spaces: Dict[str, set]) -> SigmaSource:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src._unknown_head_ids = dict(heads)
        src._known_id_spaces = dict(spaces)
        return src

    def test_a_head_seen_before_its_workbook_was_walked_is_recovered(self) -> None:
        src = self._source(
            heads={"lateHead": 5},
            spaces={"workbook_element_id_seen_so_far": {"lateHead"}},
        )
        src._recheck_unknown_heads()

        assert src.reporter.chart_ref_schema_unknown_head_recheck_heads == {
            "workbook_element_id_seen_so_far": 1
        }
        # The columns figure is what says how much lineage is at stake.
        assert src.reporter.chart_ref_schema_unknown_head_recheck_columns == {
            "workbook_element_id_seen_so_far": 5
        }

    def test_a_genuinely_absent_head_stays_unidentified(self) -> None:
        src = self._source(
            heads={"reallyUnknown": 3},
            spaces={"workbook_element_id_seen_so_far": {"somethingElse"}},
        )
        src._recheck_unknown_heads()

        assert src.reporter.chart_ref_schema_unknown_head_recheck_heads == {
            "still_unidentified": 1
        }
        assert src.reporter.chart_ref_schema_unknown_head_recheck_columns == {
            "still_unidentified": 3
        }

    def test_repeated_calls_do_not_accumulate(self) -> None:
        """get_report() runs repeatedly during a run.

        The sibling accounting check had exactly this bug: it read back its own
        previous output, so the first call was right and every one after it was
        nonsense. Build fresh, assign once.
        """
        src = self._source(
            heads={"lateHead": 5},
            spaces={"workbook_element_id_seen_so_far": {"lateHead"}},
        )
        src._recheck_unknown_heads()
        first = dict(src.reporter.chart_ref_schema_unknown_head_recheck_columns)
        src._recheck_unknown_heads()
        src._recheck_unknown_heads()

        assert src.reporter.chart_ref_schema_unknown_head_recheck_columns == first, (
            "the re-check must be idempotent"
        )

    def test_nothing_is_reported_when_no_head_was_unknown(self) -> None:
        src = self._source(heads={}, spaces={"any": {"x"}})
        src._recheck_unknown_heads()

        assert src.reporter.chart_ref_schema_unknown_head_recheck_heads == {}


class TestSchemaCrossSheetResolver:
    """The ID-based cross-sheet rule, cross-validated on 609 real dev columns.

    Fixtures mirror the real /schema shape: sheets keyed by id, and
    elements[<id>].viz.sheetId as the sheet -> element mapping.
    """

    _SHEETS: Dict[str, Any] = {
        "upSheet": {"columns": {}},
        "downSheet": {"columns": {}},
    }
    _ELEMENTS = {
        "upEl": {"viz": {"sheetId": "upSheet"}},
        "downEl": {"viz": {"sheetId": "downSheet"}},
    }

    def _resolve(
        self,
        path: List[str],
        *,
        elements: Optional[Dict[str, Any]] = None,
        names: Optional[Dict[Tuple[str, str], str]] = None,
        urns: Optional[Dict[str, str]] = None,
    ) -> Tuple[Optional[Tuple[str, str]], SigmaSource]:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        got = src._resolve_schema_cross_sheet_ref(
            path,
            sheets=self._SHEETS,
            elements=elements if elements is not None else self._ELEMENTS,
            column_name_by_element_column=(
                names if names is not None else {("upEl", "c1"): "Order Number"}
            ),
            elementId_to_chart_urn=(
                urns if urns is not None else {"upEl": "urn:li:chart:(sigma,upEl)"}
            ),
        )
        return got, src

    def test_a_sheet_ref_resolves_through_viz_sheet_id(self) -> None:
        got, _ = self._resolve(["upSheet", "c1"])
        assert got == ("urn:li:chart:(sigma,upEl)", "Order Number")

    def test_the_column_name_comes_from_the_resolved_element(self) -> None:
        """(elementId, columnId) -> name is a function; columnId -> owner is not.

        /columns lists one columnId under every element that surfaces it, so
        resolving path[1] to "its owner" picked an arbitrary winner and
        disagreed with the name-based resolver 39% of the time.
        """
        got, _ = self._resolve(
            ["upSheet", "shared"],
            names={("upEl", "shared"): "Mine", ("downEl", "shared"): "Theirs"},
        )
        assert got == ("urn:li:chart:(sigma,upEl)", "Mine")

    def test_several_elements_on_one_sheet_is_refused(self) -> None:
        got, src = self._resolve(
            ["upSheet", "c1"],
            elements={
                "upEl": {"viz": {"sheetId": "upSheet"}},
                "alsoUp": {"viz": {"sheetId": "upSheet"}},
            },
        )
        assert got is None
        assert src.reporter.chart_ref_schema_cross_sheet_sheet_ambiguous == 1

    def test_a_column_absent_from_that_element_is_refused(self) -> None:
        got, src = self._resolve(["upSheet", "notAColumn"])
        assert got is None
        assert src.reporter.chart_ref_schema_cross_sheet_column_unknown == 1

    def test_an_element_filtered_from_chart_emission_is_refused(self) -> None:
        got, src = self._resolve(["upSheet", "c1"], urns={})
        assert got is None
        assert src.reporter.chart_ref_schema_cross_sheet_no_chart_urn == 1

    def test_a_head_that_is_not_a_sheet_is_not_this_handler(self) -> None:
        got, src = self._resolve(["inode-abc", "ORDER_NUMBER"])
        assert got is None
        # Not a refusal -- a different head shape, for a different handler.
        assert src.reporter.chart_ref_schema_cross_sheet_sheet_ambiguous == 0
        assert src.reporter.chart_ref_schema_cross_sheet_column_unknown == 0

    def test_a_one_segment_path_is_not_this_handler(self) -> None:
        got, _ = self._resolve(["justAColumn"])
        assert got is None


class TestSchemaTakesPrecedenceOverNameMatching:
    """/schema states upstreams by ID, so it gets first refusal.

    The name path matches a DISPLAY NAME, and names repeat across elements --
    the reason name matching was built and deleted once already. These pin the
    three comparison outcomes, because the precedence was justified on 609
    columns from a 7-workbook tenant and governs ~437,000 on the customer's.
    """

    _DOWN = "urn:li:chart:(sigma,downEl)"
    _UP = "urn:li:chart:(sigma,upEl)"

    def _run(
        self,
        *,
        formula: Any,
        current_urn: Optional[str] = None,
        elements: Optional[Dict[str, Any]] = None,
        schema_present: bool = True,
    ) -> Tuple[List[Any], SigmaSource, Dict[str, List[InputFieldClass]]]:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        # "Amount" is the column under test; "Other" must survive the re-emit.
        fields = {
            self._DOWN: [
                InputFieldClass(
                    schemaFieldUrn=current_urn
                    or builder.make_schema_field_urn(self._DOWN, "Amount"),
                    schemaField=src._make_string_schema_field("Amount"),
                ),
                InputFieldClass(
                    schemaFieldUrn=builder.make_schema_field_urn(self._DOWN, "Other"),
                    schemaField=src._make_string_schema_field("Other"),
                ),
            ],
            self._UP: [],
        }
        up = _make_element("upEl", "Up", columns=["Amount"])
        up.column_id_by_name = {"Amount": "upAmount"}
        down = _make_element("downEl", "Down", columns=["Amount", "Other"])
        down.column_id_by_name = {"Amount": "downCol", "Other": "otherCol"}
        schema = {
            "sheets": {
                "upSheet": {"columns": {}},
                "downSheet": {"columns": {"downCol": {"formula": formula}}},
            },
            "elements": elements
            if elements is not None
            else {
                "upEl": {"viz": {"sheetId": "upSheet"}},
                "downEl": {"viz": {"sheetId": "downSheet"}},
            },
        }
        wus = list(
            src._apply_schema_resolution(
                _make_workbook_with_elements([[up, down]]),
                schema=schema if schema_present else None,
                fields_by_chart_urn=fields,
                chart_urn_by_element_id={"upEl": self._UP, "downEl": self._DOWN},
            )
        )
        return wus, src, fields

    _CROSS_SHEET = {"type": "nameRef", "path": ["upSheet", "upAmount"]}

    def test_a_self_reference_is_replaced_by_the_stated_edge(self) -> None:
        wus, src, _ = self._run(formula=self._CROSS_SHEET)

        assert len(wus) == 1
        by_path = {
            f.schemaField.fieldPath: f.schemaFieldUrn
            for f in wus[0].metadata.aspect.fields
        }
        assert by_path["Amount"] == builder.make_schema_field_urn(self._UP, "Amount")
        assert src.reporter.chart_input_fields_recovered_from_schema == 1

    def test_the_reemit_carries_every_field_not_just_the_changed_one(self) -> None:
        """InputFields is full-replace -- a partial re-emit would DELETE the rest."""
        wus, _, _ = self._run(formula=self._CROSS_SHEET)

        paths = {f.schemaField.fieldPath for f in wus[0].metadata.aspect.fields}
        assert paths == {"Amount", "Other"}

    def test_agreement_with_the_name_path_is_counted_and_changes_nothing(self) -> None:
        wus, src, _ = self._run(
            formula=self._CROSS_SHEET,
            current_urn=builder.make_schema_field_urn(self._UP, "Amount"),
        )

        assert wus == [], "nothing to re-emit when both paths already agree"
        assert src.reporter.chart_ref_schema_agrees_with_name_path == 1
        assert src.reporter.chart_ref_schema_disagrees_with_name_path == 0

    def test_on_disagreement_the_id_answer_wins_and_is_sampled(self) -> None:
        """The whole point of the precedence -- and it must stay auditable."""
        wrong = builder.make_schema_field_urn(
            "urn:li:chart:(sigma,somewhereElse)", "Amount"
        )
        wus, src, _ = self._run(formula=self._CROSS_SHEET, current_urn=wrong)

        by_path = {
            f.schemaField.fieldPath: f.schemaFieldUrn
            for f in wus[0].metadata.aspect.fields
        }
        assert by_path["Amount"] == builder.make_schema_field_urn(self._UP, "Amount")
        assert src.reporter.chart_ref_schema_disagrees_with_name_path == 1
        (sample,) = list(src.reporter.chart_ref_schema_disagreement_samples)
        assert "name_path=" in sample and "id_path=" in sample

    def test_when_schema_is_silent_the_name_path_stands(self) -> None:
        wus, src, fields = self._run(
            formula={"type": "nameRef", "path": ["notASheet", "x"]},
            current_urn=builder.make_schema_field_urn(
                "urn:li:chart:(sigma,keepMe)", "Amount"
            ),
        )

        assert wus == []
        assert fields[self._DOWN][0].schemaFieldUrn.endswith("keepMe),Amount)")
        assert src.reporter.chart_ref_schema_no_id_path == 1
        # "Other" has no /schema entry at all -- a different finding, kept
        # separate so "described but unresolvable" is not pooled with
        # "not described".
        assert src.reporter.chart_ref_schema_column_not_described == 1

    def test_an_ambiguous_sheet_changes_nothing(self) -> None:
        wus, src, _ = self._run(
            formula=self._CROSS_SHEET,
            elements={
                "upEl": {"viz": {"sheetId": "upSheet"}},
                "alsoUp": {"viz": {"sheetId": "upSheet"}},
                "downEl": {"viz": {"sheetId": "downSheet"}},
            },
        )

        assert wus == []
        assert src.reporter.chart_ref_schema_cross_sheet_sheet_ambiguous == 1

    def test_no_schema_means_no_change_and_no_crash(self) -> None:
        wus, src, _ = self._run(formula=self._CROSS_SHEET, schema_present=False)

        assert wus == []
        assert src.reporter.chart_ref_schema_no_id_path == 0


class TestDmElementHeadShape:
    """Measurement only: which half of a <dmUrlId>/<elementId> head resolves.

    path[1] is a display name on the customer tenant and an opaque column id on
    ours, so a handler must try both. These pin that each case is told apart --
    a single "dm_element: 4,163" cannot say which handler to write.
    """

    def _measure(
        self,
        *,
        head: str,
        second: str,
        url_map: Optional[Dict[str, str]] = None,
        cols: Optional[Dict[str, Dict[str, str]]] = None,
        owners: Optional[Dict[str, str]] = None,
    ) -> SigmaSource:
        src = _make_source()
        src.reporter = SigmaSourceReport()
        src._dm_id_by_url_id = url_map if url_map is not None else {"u1": "dm-1"}
        src.dm_element_urn_to_cols = cols if cols is not None else {}
        src._dm_column_owner = owners if owners is not None else {}
        src._describe_dm_element_head(head, [[head, second]])
        return src

    _URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-1.el1,PROD)"

    def test_an_unknown_data_model_url_id_is_named(self) -> None:
        src = self._measure(head="nope/el1", second="x")
        assert src.reporter.chart_ref_schema_dm_element_shape == {
            "dm_url_id_unknown": 1
        }

    def test_an_element_absent_from_the_run_is_named(self) -> None:
        """True for BOTH dev heads, which is why dev cannot validate this shape."""
        src = self._measure(head="u1/el1", second="x")
        assert src.reporter.chart_ref_schema_dm_element_shape == {
            "dm_element_not_in_run": 1
        }

    def test_path1_as_a_display_name_is_named(self) -> None:
        src = self._measure(
            head="u1/el1",
            second="Account Type Name",
            cols={self._URN: {"account type name": "Account Type Name"}},
        )
        assert src.reporter.chart_ref_schema_dm_element_shape == {
            "path1_is_a_column_name": 1
        }

    def test_path1_as_a_column_id_of_that_element_is_named(self) -> None:
        src = self._measure(
            head="u1/el1",
            second="-huDtVJMTb",
            cols={self._URN: {"other": "Other"}},
            owners={"-huDtVJMTb": "el1"},
        )
        assert src.reporter.chart_ref_schema_dm_element_shape == {
            "path1_is_a_column_id_of_this_element": 1
        }

    def test_a_column_id_owned_by_another_element_is_kept_separate(self) -> None:
        """Different finding, different fix -- it must not pool with the above."""
        src = self._measure(
            head="u1/el1",
            second="someCol",
            cols={self._URN: {"other": "Other"}},
            owners={"someCol": "adifferentElement"},
        )
        assert src.reporter.chart_ref_schema_dm_element_shape == {
            "path1_is_a_column_id_of_another_element": 1
        }

    def test_an_unrecognised_path1_is_named(self) -> None:
        src = self._measure(
            head="u1/el1", second="???", cols={self._URN: {"other": "Other"}}
        )
        assert src.reporter.chart_ref_schema_dm_element_shape == {
            "path1_unrecognised": 1
        }


class TestChartGranularityOutcome:
    """Charts, not columns, are the unit a customer reports a problem in.

    Every other counter on this path is per column, so "437,000 columns
    resolved" could coexist with a specific chart having none and the report
    could not say so. On 2026-09-11 three reported chart URNs had to be placed
    in a workbook by bisecting emission-order dashboard URNs in a 105MB log,
    because nothing recorded the outcome per chart.
    """

    def setup_method(self) -> None:
        self.reporter = SigmaSourceReport()

    def _note(self, *, total: int, self_ref: int, causes: Dict[str, int]) -> None:
        self.reporter.note_chart_column_lineage_outcome(
            chart_element_id="e1",
            workbook_id="wb-1",
            workbook_name="A Workbook",
            total_columns=total,
            self_ref_columns=self_ref,
            causes=causes,
        )

    def test_a_fully_resolved_chart_is_not_filed_as_a_problem(self) -> None:
        self._note(total=3, self_ref=0, causes={})
        assert self.reporter.charts_with_column_lineage == 1
        assert self.reporter.charts_with_no_column_lineage == 0
        assert self.reporter.charts_with_partial_column_lineage == 0

    def test_a_partly_resolved_chart_is_its_own_bucket(self) -> None:
        """Invisible in both other buckets, and the one a user calls "flaky"."""
        self._note(total=3, self_ref=1, causes={"no_formula": 1})
        assert self.reporter.charts_with_partial_column_lineage == 1
        assert self.reporter.charts_with_no_column_lineage == 0

    def test_a_chart_with_no_upstream_at_all_is_filed_and_sampled(self) -> None:
        self._note(total=2, self_ref=2, causes={"no_formula": 2})
        assert self.reporter.charts_with_no_column_lineage == 1
        assert self.reporter.charts_with_no_column_lineage_by_cause == {"no_formula": 1}
        sample = list(self.reporter.charts_with_no_column_lineage_samples["no_formula"])
        assert len(sample) == 1
        # The workbook is the whole point: without it a chart URN from a ticket
        # cannot be placed in a workbook from the report at all.
        assert "workbook=wb-1" in sample[0]
        assert "element=e1" in sample[0]

    def test_the_dominant_cause_wins_when_a_chart_has_several(self) -> None:
        self._note(total=5, self_ref=5, causes={"no_formula": 1, "unresolved_refs": 4})
        assert self.reporter.charts_with_no_column_lineage_by_cause == {
            "unresolved_refs": 1
        }

    def test_each_cause_gets_its_own_sample_list(self) -> None:
        """One shared reservoir is proportional BY DESIGN, so the rare cause --
        always the interesting one -- can never be evidenced. That exact bug
        returned 0 samples for a 263-column population on a previous run."""
        self._note(total=1, self_ref=1, causes={"no_formula": 1})
        self._note(total=1, self_ref=1, causes={"unresolved_refs": 1})
        assert set(self.reporter.charts_with_no_column_lineage_samples) == {
            "no_formula",
            "unresolved_refs",
        }

    def test_a_cause_free_chart_is_still_attributed(self) -> None:
        """A silent path must not produce an unlabelled chart; that is how the
        234-column residual stayed unattributable across two full runs."""
        self._note(total=1, self_ref=1, causes={})
        assert self.reporter.charts_with_no_column_lineage_by_cause == {
            "unattributed": 1
        }


class TestNoFormulaBucketCarriesEvidence:
    """chart_input_fields_self_ref_no_formula was the last silent bucket here.

    It is where three reported chart URNs landed, and a bare count could not
    distinguish "Sigma had no formula for this column" from "this element got
    no formulas at all while its workbook's /columns call succeeded".
    """

    def setup_method(self) -> None:
        self.src = _make_source()
        self.src.reporter = SigmaSourceReport()

    def test_the_sample_records_the_elements_formula_coverage(self) -> None:
        element = _make_element_with_formula("e1", "Chart", {"a": None, "b": "[Src/x]"})
        self.src._count_unresolved_chart_column(
            element=element,
            column="a",
            refs=[],
            all_param=False,
            all_sibling=False,
            all_unresolvable_mixed=False,
            formulas_incomplete=False,
        )
        samples = list(self.src.reporter.chart_no_formula_samples)
        assert len(samples) == 1
        # 1 of 2 columns carries a formula: the gap is this COLUMN, not the
        # element. The opposite reading (0/N) is the one worth acting on.
        assert "element_columns_with_formulas=1/2" in samples[0]
        assert "column='a'" in samples[0]

    def test_an_aborted_fetch_is_not_sampled_here(self) -> None:
        """It has its own counter; mixing the two would re-create the bug that
        reported our fetch failure as an upstream limitation."""
        self.src._count_unresolved_chart_column(
            element=_make_element("e1", "Chart", ["a"]),
            column="a",
            refs=[],
            all_param=False,
            all_sibling=False,
            all_unresolvable_mixed=False,
            formulas_incomplete=True,
        )
        assert list(self.src.reporter.chart_no_formula_samples) == []
        assert self.src.reporter.chart_input_fields_formulas_not_fetched == 1
