"""Sigma chart InputFields must not be replaced by a poorer aspect.

Element ids repeat across duplicated workbooks and a chart URN is built from
the element id alone, so two workbooks can land on one URN. InputFields is
full-replace, so without a guard the last workbook processed wins even when it
resolved fewer columns.
"""

from typing import Dict, List, Optional
from unittest.mock import patch

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.data_classes import (
    DataModelElementUpstream,
    Element,
    Page,
    Workbook,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    InputFieldClass,
    InputFieldsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

SOURCE_ELEMENT_ID = "srcElem01"
CHART_ELEMENT_ID = "chartElem01"
DM_URL_ID = "dm-1"
DM_ELEMENT_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-1.srcElem01,PROD)"
)
UPSTREAM_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.t,PROD)"


def _make_source() -> SigmaSource:
    config = SigmaSourceConfig(
        client_id="test",
        client_secret="test",
        api_url="https://aws-api.sigmacomputing.com/v2",
    )
    with patch.object(SigmaAPI, "_generate_token", return_value=None):
        source = SigmaSource(config, PipelineContext(run_id="test"))
    source.dm_element_urn_by_name = {DM_URL_ID: {"source": [DM_ELEMENT_DATASET_URN]}}
    source.dm_container_urn_by_url_id = {DM_URL_ID: "urn:li:container:dm-1"}
    return source


def _chart_urn(element_id: str) -> str:
    return builder.make_chart_urn(
        platform="sigma", platform_instance=None, name=element_id
    )


def _make_workbook(workbook_id: str, elements: List[Element]) -> Workbook:
    page = Page(pageId=f"{workbook_id}-page", name="Page 1")
    page.elements = elements
    wb = Workbook(
        workbookId=workbook_id,
        name=f"Workbook {workbook_id}",
        ownerId="u1",
        createdBy="u1",
        updatedBy="u1",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        url=f"https://example.com/{workbook_id}",
        path="Test",
        latestVersion=1,
    )
    wb.pages = [page]
    return wb


def _elements(*, chart_has_formula: bool) -> List[Element]:
    """A data-model-backed element plus a chart that references it.

    With the formula the chart column resolves onto the data model element's
    Dataset URN; without it -- the shape a workbook whose /columns call failed
    produces -- the column falls back to a self-reference.
    """
    source_element = Element(
        elementId=SOURCE_ELEMENT_ID,
        name="Source",
        url="https://example.com/src",
        columns=["col"],
    )
    source_element.column_formulas = {}
    source_element.upstream_sources = {}
    chart_element = Element(
        elementId=CHART_ELEMENT_ID,
        name="Chart",
        url="https://example.com/chart",
        columns=["c"],
    )
    chart_element.column_formulas = {"c": "[Source/col]"} if chart_has_formula else {}
    chart_element.upstream_sources = {
        "dm_node": DataModelElementUpstream(name="Source", data_model_url_id=DM_URL_ID)
    }
    return [source_element, chart_element]


def _run_workbook(
    source: SigmaSource, workbook: Workbook
) -> Dict[str, InputFieldsClass]:
    """Emit one workbook's charts and collect {chart_urn: aspect}."""
    elements = workbook.pages[0].elements
    emitted: Dict[str, InputFieldsClass] = {}
    for wu in source._gen_elements_workunit(
        elements=elements,
        workbook=workbook,
        all_input_fields=[],
        paths=[],
        elementId_to_chart_urn={e.elementId: _chart_urn(e.elementId) for e in elements},
        wb_element_index=SigmaSource._build_workbook_element_index(workbook),
        wb_warehouse_table_index=None,
    ):
        aspect = wu.get_aspect_of_type(InputFieldsClass)
        if aspect is not None:
            urn = wu.metadata.entityUrn  # type: ignore[union-attr]
            assert isinstance(urn, str)
            emitted[urn] = aspect
    return emitted


def _resolved_urns(aspect: InputFieldsClass, chart_urn: str) -> List[str]:
    self_ref_prefix = f"urn:li:schemaField:({chart_urn},"
    return [
        f.schemaFieldUrn
        for f in aspect.fields
        if f.schemaFieldUrn and not f.schemaFieldUrn.startswith(self_ref_prefix)
    ]


class TestADuplicateWorkbookCannotOverwriteRicherLineage:
    def test_the_poorer_copy_is_refused(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)

        rich = _run_workbook(
            source, _make_workbook("wb-1", _elements(chart_has_formula=True))
        )
        assert len(_resolved_urns(rich[chart_urn], chart_urn)) == 1

        poor = _run_workbook(
            source, _make_workbook("wb-2", _elements(chart_has_formula=False))
        )

        assert chart_urn not in poor
        assert source.reporter.chart_input_fields_regressive_emission_skipped == 1
        # The source element resolves nothing either way, so it is unaffected.
        assert _chart_urn(SOURCE_ELEMENT_ID) in poor

    def test_the_richer_copy_does_replace_the_poorer_one(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)

        _run_workbook(
            source, _make_workbook("wb-1", _elements(chart_has_formula=False))
        )
        second = _run_workbook(
            source, _make_workbook("wb-2", _elements(chart_has_formula=True))
        )

        assert len(_resolved_urns(second[chart_urn], chart_urn)) == 1
        assert source.reporter.chart_input_fields_regressive_emission_skipped == 0

    def test_an_equally_good_copy_is_still_emitted(self) -> None:
        """The customSQL drain re-emits the same chart; it must not be refused."""
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)

        _run_workbook(source, _make_workbook("wb-1", _elements(chart_has_formula=True)))
        second = _run_workbook(
            source, _make_workbook("wb-2", _elements(chart_has_formula=True))
        )

        assert chart_urn in second
        assert source.reporter.chart_input_fields_regressive_emission_skipped == 0

    def test_a_refused_chart_still_contributes_to_the_page_aspect(self) -> None:
        """The dashboard aspect is a union over the page, so it is unaffected."""
        source = _make_source()
        _run_workbook(source, _make_workbook("wb-1", _elements(chart_has_formula=True)))

        page_fields: List[InputFieldClass] = []
        workbook = _make_workbook("wb-2", _elements(chart_has_formula=False))
        list(
            source._gen_elements_workunit(
                elements=workbook.pages[0].elements,
                workbook=workbook,
                all_input_fields=page_fields,
                paths=[],
                elementId_to_chart_urn={
                    e.elementId: _chart_urn(e.elementId)
                    for e in workbook.pages[0].elements
                },
                wb_element_index=SigmaSource._build_workbook_element_index(workbook),
                wb_warehouse_table_index=None,
            )
        )

        assert source.reporter.chart_input_fields_regressive_emission_skipped == 1
        assert any(
            f.schemaField is not None and f.schemaField.fieldPath == "c"
            for f in page_fields
        )


class TestSelfReferencesAreNotLineage:
    def test_a_chart_of_pure_self_references_never_outranks_a_resolved_one(
        self,
    ) -> None:
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        self_refs = [
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(chart_urn, name),
                schemaField=SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            )
            for name in ("a", "b", "c")
        ]
        assert SigmaSource._resolved_field_count(chart_urn, self_refs) == 0


class TestTheCustomSqlDrainIsGuardedToo:
    """The drain runs after every workbook, so it is the last word on a chart."""

    def _drain_aspect(
        self, source: SigmaSource, chart_urn: str, upstream_column: Optional[str]
    ) -> Optional[InputFieldsClass]:
        source._workbook_customsql_registered_urns.add(chart_urn)
        aspect = UpstreamLineage(
            upstreams=[
                Upstream(
                    dataset=UPSTREAM_DATASET_URN,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        builder.make_schema_field_urn(
                            UPSTREAM_DATASET_URN, upstream_column
                        )
                    ],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(chart_urn, upstream_column)
                    ],
                )
            ]
            if upstream_column
            else None,
        )
        mcp = source._build_workbook_chart_input_fields_mcp(chart_urn, aspect)
        if mcp is None:
            return None
        assert isinstance(mcp.aspect, InputFieldsClass)
        return mcp.aspect

    def test_a_poorer_drain_aspect_is_refused(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        assert self._drain_aspect(source, chart_urn, "col") is not None

        assert self._drain_aspect(source, chart_urn, None) is None
        assert source.reporter.chart_input_fields_regressive_emission_skipped == 1

    def test_a_refused_drain_aspect_is_not_yielded(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        source._chart_best_input_fields[chart_urn] = 5
        source._workbook_customsql_registered_urns.add(chart_urn)

        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=UpstreamLineage(upstreams=[], fineGrainedLineages=None),
        )
        assert source._rewrite_fgl_downstreams(mcp) is None
