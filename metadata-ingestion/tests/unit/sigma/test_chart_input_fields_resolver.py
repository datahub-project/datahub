"""Unit tests for the chart InputFields resolver helpers.

Cases cover probe-derived chart formulas and resolver behavior.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.data_classes import Element, Page, Workbook
from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    extract_bracket_refs,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
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

    def test_case_mismatched_workbook_element_ref_is_diagnosed(self) -> None:
        """Workbook element names are exact-case; near misses are counted."""
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

        assert result is None
        assert self.src.reporter.chart_input_fields_case_mismatch == 1

    def test_exact_workbook_name_without_lineage_match_does_not_fallback_to_warehouse(
        self,
    ) -> None:
        """An exact workbook name match must satisfy the lineage filter."""
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

        assert result is None

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
