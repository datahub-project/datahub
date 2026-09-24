"""Sigma chart InputFields must not be replaced by a poorer aspect.

Element ids repeat across duplicated workbooks and a chart URN is built from
the element id alone, so two workbooks can land on one URN. InputFields is
full-replace, so without a guard the last workbook processed wins even when it
resolved fewer columns.
"""

from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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


def _page_of_two_charts(first: int, second: int) -> List[Element]:
    """A page whose two charts resolve `first` and `second` columns.

    Every column carries the same data-model ref, so a chart's resolved count
    is just how many columns it has.
    """
    source_element = Element(
        elementId=SOURCE_ELEMENT_ID,
        name="Source",
        url="https://example.com/src",
        columns=["col"],
    )
    source_element.column_formulas = {}
    source_element.upstream_sources = {}

    charts = []
    for n, resolved in enumerate((first, second)):
        chart = Element(
            elementId=f"chartElem0{n}",
            name=f"Chart {n}",
            url=f"https://example.com/chart{n}",
            columns=[f"c{i}" for i in range(resolved)],
        )
        chart.column_formulas = {f"c{i}": "[Source/col]" for i in range(resolved)}
        chart.upstream_sources = {
            "dm_node": DataModelElementUpstream(
                name="Source", data_model_url_id=DM_URL_ID
            )
        }
        charts.append(chart)
    return [source_element, *charts]


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
        assert source.reporter.input_fields_regressive_emission_skipped == 1
        # The counter alone cannot be acted on; a default INFO run has no logs,
        # and an element id is not something a Sigma admin can search for. The
        # two labels must not swap: they say which copy to keep.
        assert [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if f"entity={chart_urn} kept=1 kept_from=wb-1 refused=0 "
            "refused_from=wb-2" in s
        ]
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
        assert source.reporter.input_fields_regressive_emission_skipped == 0

    def test_an_equally_good_copy_is_still_emitted(self) -> None:
        """Only a strictly poorer copy is refused."""
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)

        _run_workbook(source, _make_workbook("wb-1", _elements(chart_has_formula=True)))
        second = _run_workbook(
            source, _make_workbook("wb-2", _elements(chart_has_formula=True))
        )

        assert chart_urn in second
        assert source.reporter.input_fields_regressive_emission_skipped == 0

    def test_a_refused_chart_still_contributes_to_the_page_aspect(self) -> None:
        """Within one workbook the page aspect is a union over its charts."""
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

        assert source.reporter.input_fields_regressive_emission_skipped == 1
        assert any(
            f.schemaField is not None and f.schemaField.fieldPath == "c"
            for f in page_fields
        )


class TestTheBarOnlyEverRises:
    def test_a_refused_copy_does_not_lower_the_bar(self) -> None:
        """Otherwise a third, middling copy displaces the richest one."""
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)

        rich = [
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(
                    UPSTREAM_DATASET_URN, name
                ),
                schemaField=SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            )
            for name in ("a", "b")
        ]
        assert source._chart_input_fields_mcp(chart_urn, rich, "wb-1") is not None
        assert source._chart_input_fields_mcp(chart_urn, [], "wb-2") is None
        assert source._chart_input_fields_mcp(chart_urn, rich[:1], "wb-3") is None
        assert source.reporter.input_fields_regressive_emission_skipped == 2

    def test_an_accepted_tie_moves_the_label(self) -> None:
        """The label must name whoever wrote the aspect that is there."""
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        rich = [
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(UPSTREAM_DATASET_URN, "a"),
                schemaField=SchemaFieldClass(
                    fieldPath="a",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            )
        ]
        assert source._chart_input_fields_mcp(chart_urn, rich, "wb-1") is not None
        assert source._chart_input_fields_mcp(chart_urn, rich, "wb-2") is not None
        assert source._chart_input_fields_mcp(chart_urn, [], "wb-3") is None

        assert [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if "kept=1 kept_from=wb-2 refused=0 refused_from=wb-3" in s
        ]

    def test_the_bar_does_not_carry_across_runs(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        source._best_input_fields_resolved[chart_urn] = (5, "wb-1")

        source.sigma_api = MagicMock()
        source.sigma_api.get_sigma_entities.return_value = []
        source.sigma_api.fill_workspaces.return_value = None
        list(source.get_workunits_internal())

        assert source._best_input_fields_resolved == {}


class TestThePageDashboardIsGuardedToo:
    """Page ids collide across duplicated workbooks exactly as element ids do."""

    def _page_aspect(
        self, source: SigmaSource, workbook: Workbook
    ) -> Optional[InputFieldsClass]:
        for wu in source._gen_pages_workunit(workbook, paths=[]):
            aspect = wu.get_aspect_of_type(InputFieldsClass)
            if aspect is not None and str(wu.metadata.entityUrn).startswith(  # type: ignore[union-attr]
                "urn:li:dashboard:"
            ):
                return aspect
        return None

    def test_a_poorer_copy_of_a_page_is_refused(self) -> None:
        source = _make_source()
        shared_page = Page(pageId="shared-page", name="Page 1")

        rich_workbook = _make_workbook("wb-1", _elements(chart_has_formula=True))
        rich_workbook.pages[0].pageId = shared_page.pageId
        rich = self._page_aspect(source, rich_workbook)
        assert rich is not None

        poor_workbook = _make_workbook("wb-2", _elements(chart_has_formula=False))
        poor_workbook.pages[0].pageId = shared_page.pageId

        assert self._page_aspect(source, poor_workbook) is None
        assert [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if s.startswith("entity=urn:li:dashboard:")
            and "kept=1 kept_from=wb-1 refused=0 refused_from=wb-2" in s
        ]

    def _page(
        self, source: SigmaSource, workbook_id: str, first: int, second: int
    ) -> Optional[InputFieldsClass]:
        workbook = _make_workbook(workbook_id, _page_of_two_charts(first, second))
        workbook.pages[0].pageId = "shared-page"
        return self._page_aspect(source, workbook)

    def test_a_page_that_lost_one_charts_lineage_is_refused(self) -> None:
        """Scored by the SUM of its charts: under max these two would tie.

        This is the shape a partial /columns fetch produces -- some charts on
        the page keep their formulas, some do not.
        """
        source = _make_source()

        assert self._page(source, "wb-1", first=1, second=1) is not None
        assert self._page(source, "wb-2", first=1, second=0) is None

    def test_a_refused_charts_columns_still_count_towards_its_page(self) -> None:
        """They are still in the page union, so they belong in the page score.

        wb-2 loses on the second chart but makes it up on the first, so its
        page is exactly as rich as wb-1's and must still be emitted.
        """
        source = _make_source()

        assert self._page(source, "wb-1", first=0, second=3) is not None
        assert self._page(source, "wb-2", first=2, second=1) is not None
        # Only the second chart was refused, not the page.
        assert source.reporter.input_fields_regressive_emission_skipped == 1
        assert not [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if "urn:li:dashboard:" in s
        ]

    def test_an_equally_good_copy_of_a_page_is_still_emitted(self) -> None:
        source = _make_source()
        first = _make_workbook("wb-1", _elements(chart_has_formula=True))
        first.pages[0].pageId = "shared-page"
        second = _make_workbook("wb-2", _elements(chart_has_formula=True))
        second.pages[0].pageId = "shared-page"

        assert self._page_aspect(source, first) is not None
        assert self._page_aspect(source, second) is not None


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


class TestTheScoreCountsColumnsNotEntries:
    """A formula naming several upstream columns emits one entry per reference."""

    def _fields(
        self, columns_to_reference_counts: Dict[str, int]
    ) -> List[InputFieldClass]:
        return [
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(
                    UPSTREAM_DATASET_URN, f"{column}_{i}"
                ),
                schemaField=SchemaFieldClass(
                    fieldPath=column,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            )
            for column, refs in columns_to_reference_counts.items()
            for i in range(refs)
        ]

    def test_one_column_resolved_four_ways_scores_one(self) -> None:
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        assert SigmaSource._resolved_field_count(chart_urn, self._fields({"a": 4})) == 1

    def test_a_multi_reference_column_cannot_outrank_three_resolved_ones(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)

        three = self._fields({"a": 1, "b": 1, "c": 1})
        assert source._chart_input_fields_mcp(chart_urn, three, "wb-1") is not None

        one_column_four_refs = self._fields({"a": 4})
        assert (
            source._chart_input_fields_mcp(chart_urn, one_column_four_refs, "wb-2")
            is None
        )
        assert source.reporter.input_fields_regressive_emission_skipped == 1


class TestARefusedCopyIsNotFedBackThroughTheDrain:
    def test_the_stash_keeps_the_accepted_copys_fields(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        source._workbook_customsql_registered_urns.add(chart_urn)

        _run_workbook(source, _make_workbook("wb-1", _elements(chart_has_formula=True)))
        stashed_after_accept = source._workbook_customsql_formula_fields[chart_urn]

        _run_workbook(
            source, _make_workbook("wb-2", _elements(chart_has_formula=False))
        )

        assert source.reporter.input_fields_regressive_emission_skipped == 1
        assert (
            source._workbook_customsql_formula_fields[chart_urn] is stashed_after_accept
        )
        assert (
            SigmaSource._resolved_field_count(
                chart_urn, source._workbook_customsql_formula_fields[chart_urn]
            )
            == 1
        )


class TestTheCustomSqlDrainIsGuardedToo:
    """The drain runs after every workbook, so it is the last word on a chart."""

    def _drain_aspect(
        self,
        source: SigmaSource,
        chart_urn: str,
        upstream_column: Optional[str],
        claimed_by: str = "customsql-drain:snowflake/PROD/inst",
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
        mcp = source._build_workbook_chart_input_fields_mcp(
            chart_urn, aspect, claimed_by
        )
        if mcp is None:
            return None
        assert isinstance(mcp.aspect, InputFieldsClass)
        return mcp.aspect

    def test_a_poorer_drain_aspect_is_refused(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        assert self._drain_aspect(source, chart_urn, "col") is not None

        assert self._drain_aspect(source, chart_urn, None) is None
        assert source.reporter.input_fields_regressive_emission_skipped == 1
        assert [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if "refused_from=customsql-drain:snowflake/PROD/inst" in s
        ]

    def test_a_fallback_only_drain_is_not_counted_as_column_lineage(self) -> None:
        """The flag is read before the fallback merge appends to the same list."""
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        source._workbook_customsql_formula_fields[chart_urn] = [
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(chart_urn, "c"),
                schemaField=SchemaFieldClass(
                    fieldPath="c",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                ),
            )
        ]

        aspect = self._drain_aspect(source, chart_urn, None)

        assert aspect is not None and len(aspect.fields) == 1
        assert source.reporter.workbook_customsql_upstream_emitted == 1
        assert source.reporter.workbook_customsql_column_lineage_emitted == 0

    def test_two_aggregators_are_told_apart(self) -> None:
        """A customSQL chart's element aspects tie at 0, so they record no
        sample -- a drain-vs-drain refusal is the only entry there is."""
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        prod = "customsql-drain:snowflake/PROD/inst"
        dev = "customsql-drain:snowflake/DEV/inst"

        assert self._drain_aspect(source, chart_urn, "col", prod) is not None
        assert self._drain_aspect(source, chart_urn, None, dev) is None

        assert [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if f"kept_from={prod} refused=0 refused_from={dev}" in s
        ]

    def test_a_refused_drain_aspect_is_not_counted_as_emitted(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        self._drain_aspect(source, chart_urn, "col")
        self._drain_aspect(source, chart_urn, None)

        assert source.reporter.workbook_customsql_upstream_emitted == 1
        assert source.reporter.workbook_customsql_column_lineage_emitted == 1

    def test_a_refused_drain_aspect_is_not_yielded(self) -> None:
        source = _make_source()
        chart_urn = _chart_urn(CHART_ELEMENT_ID)
        source._best_input_fields_resolved[chart_urn] = (5, "wb-1")
        source._workbook_customsql_registered_urns.add(chart_urn)

        mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=UpstreamLineage(upstreams=[], fineGrainedLineages=None),
        )
        assert source._rewrite_fgl_downstreams(mcp, "customsql-drain:test") is None

    def test_the_drain_skips_a_refused_aspect_and_keeps_draining(self) -> None:
        """A refusal must not abort the rest of the aggregator's drain.

        The drain wraps gen_metadata in `except Exception`, so yielding a
        refused (None) aspect would raise inside that guard and silently drop
        every later customSQL chart and DM element on the platform.
        """
        source = _make_source()
        refused_urn = _chart_urn(CHART_ELEMENT_ID)
        source._best_input_fields_resolved[refused_urn] = (5, "wb-1")
        source._workbook_customsql_registered_urns.add(refused_urn)
        healthy_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm-1.other,PROD)"

        aggregator = MagicMock()
        aggregator.gen_metadata.return_value = [
            MetadataChangeProposalWrapper(
                entityUrn=refused_urn,
                aspect=UpstreamLineage(upstreams=[], fineGrainedLineages=None),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=healthy_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=UPSTREAM_DATASET_URN,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                    ],
                    fineGrainedLineages=None,
                ),
            ),
        ]
        aggregator.report.views_parse_failures = {}
        aggregator.report.num_views_failed = 0
        source._sql_aggregators = {("snowflake", "PROD", "inst"): aggregator}  # type: ignore[dict-item]

        emitted = [wu.metadata.entityUrn for wu in source._drain_sql_aggregators()]  # type: ignore[union-attr]

        assert emitted == [healthy_urn]
        assert source.reporter.input_fields_regressive_emission_skipped == 1
        # The drain labels the refusal with the aggregator it came from.
        assert [
            s
            for s in source.reporter.input_fields_regressive_emission_samples
            if "refused_from=customsql-drain:snowflake/PROD/inst" in s
        ]
        assert not [
            entry
            for entry in source.reporter.warnings
            if "aggregator drain failed" in (entry.title or "")
        ]
