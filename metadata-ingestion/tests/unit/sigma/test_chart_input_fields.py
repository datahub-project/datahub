"""Unit tests for Sigma chart InputFields formula resolution (T1.8 combined fix).

Covers:
  1. DataModelElementUpstream resolves to Dataset URN (not dropped as unresolved).
  2. Columns without formulas emit a self-ref fallback entry (not empty InputFields).
  3. Columns with only parameter/sibling refs emit a self-ref fallback entry.
  4. reporter.chart_input_fields_self_ref_fallback exists and is incremented.
  5. Counter invariant: resolved + self_ref_fallback + skipped_parameter
     + skipped_sibling == total chart columns processed.
"""

from typing import Dict, List, Optional
from unittest.mock import patch

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import (
    DataModelElementUpstream,
    Element,
    Page,
    Workbook,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.schema_classes import InputFieldsClass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(
    dm_element_urn_by_name: Optional[Dict] = None,
    dm_container_urn_by_url_id: Optional[Dict] = None,
) -> SigmaSource:
    """Build a SigmaSource instance with the API mocked out."""
    config = SigmaSourceConfig(
        client_id="test",
        client_secret="test",
        api_url="https://aws-api.sigmacomputing.com/v2",
    )
    ctx = PipelineContext(run_id="test")

    with patch.object(SigmaAPI, "_generate_token", return_value=None):
        source = SigmaSource(config, ctx)

    if dm_element_urn_by_name is not None:
        source.dm_element_urn_by_name = dm_element_urn_by_name
    if dm_container_urn_by_url_id is not None:
        source.dm_container_urn_by_url_id = dm_container_urn_by_url_id

    return source


def _make_element(
    element_id: str,
    name: str,
    columns: List[str],
    column_formulas: Optional[Dict[str, Optional[str]]] = None,
    upstream_sources: Optional[Dict] = None,
) -> Element:
    elem = Element(
        elementId=element_id,
        name=name,
        url=f"https://example.com/{element_id}",
        columns=columns,
    )
    if column_formulas is not None:
        elem.column_formulas = column_formulas
    if upstream_sources is not None:
        elem.upstream_sources = upstream_sources
    return elem


def _collect_input_fields(
    source: SigmaSource, elements: List[Element], workbook: Workbook
) -> Dict[str, InputFieldsClass]:
    """Run _gen_elements_workunit and collect {chart_urn: InputFieldsClass}."""
    elementId_to_chart_urn: Dict[str, str] = {
        elem.elementId: builder.make_chart_urn(
            platform="sigma",
            platform_instance=None,
            name=elem.elementId,
        )
        for elem in elements
    }
    wb_element_index = SigmaSource._build_workbook_element_index(workbook)

    all_input_fields: List = []
    result: Dict[str, InputFieldsClass] = {}

    for wu in source._gen_elements_workunit(
        elements=elements,
        workbook=workbook,
        all_input_fields=all_input_fields,
        paths=[],
        elementId_to_chart_urn=elementId_to_chart_urn,
        wb_element_index=wb_element_index,
    ):
        aspect = wu.get_aspect_of_type(InputFieldsClass)
        if aspect is not None:
            urn = wu.metadata.entityUrn  # type: ignore[union-attr]
            assert isinstance(urn, str)
            result[urn] = aspect

    return result


def _make_workbook(elements: List[Element]) -> Workbook:
    page = Page(pageId="page-1", name="Page 1")
    page.elements = elements
    wb = Workbook(
        workbookId="wb-1",
        name="Test Workbook",
        ownerId="u1",
        createdBy="u1",
        updatedBy="u1",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        url="https://example.com/wb-1",
        path="Test",
        latestVersion=1,
    )
    wb.pages = [page]
    return wb


def _chart_urn(element_id: str) -> str:
    return builder.make_chart_urn(
        platform="sigma", platform_instance=None, name=element_id
    )


def _schema_field_urn(entity_urn: str, field: str) -> str:
    return builder.make_schema_field_urn(entity_urn, field)


# ---------------------------------------------------------------------------
# Test 1: reporter has chart_input_fields_self_ref_fallback counter
# ---------------------------------------------------------------------------


class TestReporterHasSelfRefFallbackCounter:
    def test_self_ref_fallback_counter_exists_on_report(self) -> None:
        report = SigmaSourceReport()
        # The attribute must exist (will fail before implementation).
        assert hasattr(report, "chart_input_fields_self_ref_fallback"), (
            "SigmaSourceReport must have chart_input_fields_self_ref_fallback counter"
        )
        assert report.chart_input_fields_self_ref_fallback == 0


# ---------------------------------------------------------------------------
# Test 2: Columns without formulas emit self-ref entries (not empty InputFields)
# ---------------------------------------------------------------------------


class TestNoFormulaColumnsSelfRefFallback:
    def test_columns_with_no_formula_emit_self_ref_input_field_per_column(
        self,
    ) -> None:
        source = _make_source()
        elem = _make_element(
            element_id="noFormula01",
            name="No Formula",
            columns=["col_a", "col_b"],
            column_formulas={},  # No formulas at all.
        )
        workbook = _make_workbook([elem])
        result = _collect_input_fields(source, [elem], workbook)

        chart_urn = _chart_urn("noFormula01")
        assert chart_urn in result
        fields = result[chart_urn].fields

        assert len(fields) == 2, (
            "Should emit one InputField per column even without formulas"
        )
        field_urns = {f.schemaFieldUrn for f in fields}
        assert _schema_field_urn(chart_urn, "col_a") in field_urns
        assert _schema_field_urn(chart_urn, "col_b") in field_urns

    def test_columns_with_no_formula_increment_self_ref_fallback_counter(
        self,
    ) -> None:
        source = _make_source()
        elem = _make_element(
            element_id="noFormula01",
            name="No Formula",
            columns=["col_a", "col_b"],
            column_formulas={},
        )
        workbook = _make_workbook([elem])
        _collect_input_fields(source, [elem], workbook)

        assert source.reporter.chart_input_fields_self_ref_fallback == 2
        assert source.reporter.chart_input_fields_resolved == 0

    def test_schemaField_fieldPath_is_chart_own_column_name(self) -> None:
        source = _make_source()
        elem = _make_element(
            element_id="noFormula01",
            name="No Formula",
            columns=["my_column"],
            column_formulas={},
        )
        workbook = _make_workbook([elem])
        result = _collect_input_fields(source, [elem], workbook)

        chart_urn = _chart_urn("noFormula01")
        fields = result[chart_urn].fields
        assert len(fields) == 1
        assert fields[0].schemaField is not None
        assert fields[0].schemaField.fieldPath == "my_column"


# ---------------------------------------------------------------------------
# Test 3: Parameter/sibling-only formulas emit self-ref entries
# ---------------------------------------------------------------------------


class TestParamSiblingOnlyFormulas:
    def test_parameter_only_column_emits_self_ref_entry(self) -> None:
        source = _make_source()
        elem = _make_element(
            element_id="paramElem01",
            name="Param Element",
            columns=["target"],
            column_formulas={"target": "[P_Monthly_Target]"},
        )
        workbook = _make_workbook([elem])
        result = _collect_input_fields(source, [elem], workbook)

        chart_urn = _chart_urn("paramElem01")
        fields = result[chart_urn].fields
        assert len(fields) == 1
        assert fields[0].schemaFieldUrn == _schema_field_urn(chart_urn, "target")
        assert source.reporter.chart_input_fields_skipped_parameter == 1

    def test_sibling_only_column_emits_self_ref_entry(self) -> None:
        source = _make_source()
        elem = _make_element(
            element_id="siblingElem01",
            name="Sibling Element",
            columns=["derived"],
            column_formulas={"derived": "[target]"},
        )
        workbook = _make_workbook([elem])
        result = _collect_input_fields(source, [elem], workbook)

        chart_urn = _chart_urn("siblingElem01")
        fields = result[chart_urn].fields
        assert len(fields) == 1
        assert fields[0].schemaFieldUrn == _schema_field_urn(chart_urn, "derived")
        assert source.reporter.chart_input_fields_skipped_sibling == 1


# ---------------------------------------------------------------------------
# Test 4: DataModelElementUpstream resolves to Dataset URN
# ---------------------------------------------------------------------------

DM_URL_ID = "dm-abc-123"
DM_ELEMENT_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-abc-123.elem01,PROD)"
)


class TestDmElementUpstreamResolvesToDatasetUrn:
    def _build_source_with_dm(self) -> SigmaSource:
        """Source whose DM lookup maps DM_URL_ID -> {"my dm element": [DM_ELEMENT_DATASET_URN]}."""
        return _make_source(
            dm_element_urn_by_name={
                DM_URL_ID: {"my dm element": [DM_ELEMENT_DATASET_URN]},
            },
            dm_container_urn_by_url_id={DM_URL_ID: "urn:li:container:dm-abc-123"},
        )

    def test_dm_element_formula_ref_resolves_to_dataset_urn(self) -> None:
        """Chart with [My DM Element/col] should get schemaFieldUrn on DM element Dataset."""
        source = self._build_source_with_dm()

        # "My DM Element" is a workbook page element backed by the DM.
        dm_page_elem = _make_element(
            element_id="elem01",
            name="My DM Element",
            columns=["col"],
            column_formulas={},
            upstream_sources={},
        )
        # Chart referencing the DM element via formula.
        chart_elem = _make_element(
            element_id="chartElem01",
            name="My Chart",
            columns=["chart_col"],
            column_formulas={"chart_col": "[My DM Element/col]"},
            upstream_sources={
                "dm_node": DataModelElementUpstream(
                    name="My DM Element",
                    data_model_url_id=DM_URL_ID,
                )
            },
        )
        workbook = _make_workbook([dm_page_elem, chart_elem])
        result = _collect_input_fields(source, [dm_page_elem, chart_elem], workbook)

        chart_urn = _chart_urn("chartElem01")
        assert chart_urn in result
        fields = result[chart_urn].fields

        assert len(fields) == 1, (
            "Chart with resolvable DM element ref should emit exactly one InputField"
        )
        expected_upstream_field_urn = _schema_field_urn(DM_ELEMENT_DATASET_URN, "col")
        assert fields[0].schemaFieldUrn == expected_upstream_field_urn, (
            f"schemaFieldUrn should point to DM element Dataset URN, "
            f"got {fields[0].schemaFieldUrn!r}"
        )
        assert fields[0].schemaField is not None
        assert fields[0].schemaField.fieldPath == "chart_col"

    def test_dm_element_ref_increments_resolved_counter(self) -> None:
        source = self._build_source_with_dm()

        dm_page_elem = _make_element("elem01", "My DM Element", ["col"], {}, {})
        chart_elem = _make_element(
            element_id="chartElem01",
            name="My Chart",
            columns=["chart_col"],
            column_formulas={"chart_col": "[My DM Element/col]"},
            upstream_sources={
                "dm_node": DataModelElementUpstream(
                    name="My DM Element",
                    data_model_url_id=DM_URL_ID,
                )
            },
        )
        workbook = _make_workbook([dm_page_elem, chart_elem])
        _collect_input_fields(source, [dm_page_elem, chart_elem], workbook)

        # chartElem01.chart_col resolves to DM element Dataset URN.
        assert source.reporter.chart_input_fields_resolved == 1
        # dm_page_elem.col has no formula → self-ref fallback.
        assert source.reporter.chart_input_fields_self_ref_fallback == 1

    def test_dm_element_ref_not_in_lineage_falls_back_to_self_ref(self) -> None:
        """Chart formula refs a workbook element that IS named in the workbook
        but has NO DataModelElementUpstream linking it to the DM. Without the
        upstream registration, the ref is unresolvable and should fall back to
        self-ref (preserving the column in V2) rather than emitting nothing.
        """
        source = self._build_source_with_dm()

        dm_page_elem = _make_element("elem01", "My DM Element", ["col"], {}, {})
        chart_elem = _make_element(
            element_id="chartElem01",
            name="My Chart",
            columns=["chart_col"],
            column_formulas={"chart_col": "[My DM Element/col]"},
            upstream_sources={},  # No upstream declaration.
        )
        workbook = _make_workbook([dm_page_elem, chart_elem])
        result = _collect_input_fields(source, [dm_page_elem, chart_elem], workbook)

        chart_urn = _chart_urn("chartElem01")
        fields = result[chart_urn].fields

        assert len(fields) == 1, (
            "Unresolvable formula ref should still emit one InputField (self-ref fallback)"
        )
        assert fields[0].schemaFieldUrn == _schema_field_urn(chart_urn, "chart_col")
        # dm_page_elem.col (no formula) + chart_elem.chart_col (unresolvable) = 2.
        assert source.reporter.chart_input_fields_self_ref_fallback == 2
        assert source.reporter.chart_input_fields_resolved == 0


# ---------------------------------------------------------------------------
# Test 5: Counter invariant
# ---------------------------------------------------------------------------


class TestCounterInvariant:
    def test_counter_sum_equals_total_columns(self) -> None:
        """resolved + self_ref_fallback + skipped_parameter + skipped_sibling == column count."""
        source = _make_source(
            dm_element_urn_by_name={
                DM_URL_ID: {"DM Elem": [DM_ELEMENT_DATASET_URN]},
            },
            dm_container_urn_by_url_id={DM_URL_ID: "urn:li:container:dm-abc-123"},
        )

        dm_elem = _make_element("dmElem", "DM Elem", ["col"], {}, {})

        # Chart 1: 1 resolved (DM element ref), 1 self-ref (no formula)
        chart1 = _make_element(
            "chart1",
            "Chart1",
            columns=["resolved_col", "no_formula_col"],
            column_formulas={"resolved_col": "[DM Elem/col]"},
            upstream_sources={
                "dm": DataModelElementUpstream(
                    name="DM Elem", data_model_url_id=DM_URL_ID
                )
            },
        )

        # Chart 2: 1 skipped_parameter, 1 skipped_sibling
        chart2 = _make_element(
            "chart2",
            "Chart2",
            columns=["param_col", "sibling_col"],
            column_formulas={
                "param_col": "[P_Budget]",
                "sibling_col": "[param_col]",
            },
            upstream_sources={},
        )

        workbook = _make_workbook([dm_elem, chart1, chart2])
        # All elements are processed — dm_elem is a page element too.
        total_chart_columns = (
            len(dm_elem.columns) + len(chart1.columns) + len(chart2.columns)
        )

        _collect_input_fields(source, [dm_elem, chart1, chart2], workbook)

        r = source.reporter
        counter_sum = (
            r.chart_input_fields_resolved
            + r.chart_input_fields_self_ref_fallback
            + r.chart_input_fields_skipped_parameter
            + r.chart_input_fields_skipped_sibling
        )
        assert counter_sum == total_chart_columns, (
            f"Counter invariant broken: {r.chart_input_fields_resolved} resolved "
            f"+ {r.chart_input_fields_self_ref_fallback} self_ref_fallback "
            f"+ {r.chart_input_fields_skipped_parameter} skipped_param "
            f"+ {r.chart_input_fields_skipped_sibling} skipped_sibling "
            f"= {counter_sum} != {total_chart_columns} total chart columns"
        )
