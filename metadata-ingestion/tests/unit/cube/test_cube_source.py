from typing import Dict, Iterator, List
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.cube.config import CubeSourceConfig
from datahub.ingestion.source.cube.cube import CubeSource
from datahub.ingestion.source.cube.cube_semantic_model import CubeSemanticModelMapper
from datahub.ingestion.source.cube.models import (
    CloudDataSource,
    CubeEntity,
    CubeFolder,
    CubeHierarchy,
    CubeJoin,
    CubeMember,
    CubeReport,
    CubeWorkbook,
)
from datahub.metadata.schema_classes import (
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
)
from datahub.sdk.container import Container


def _source(**overrides: object) -> CubeSource:
    base: Dict[str, object] = {
        "api_url": "https://demo.cubecloud.dev/cubejs-api",
        "api_token": "t",
    }
    base.update(overrides)
    config = CubeSourceConfig.model_validate(base)
    return CubeSource(config, PipelineContext(run_id="test"))


def _aspects(wus: List[MetadataWorkUnit]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    for wu in wus:
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        if aspect is not None:
            out[type(aspect).__name__] = aspect
    return out


def test_deployment_url_derived_from_api_url() -> None:
    assert _source()._deployment_url() == "https://demo.cubecloud.dev"
    assert (
        _source(deployment_url="https://ui.example.com/")._deployment_url()
        == "https://ui.example.com"
    )


def test_meta_mapping_emits_owner_tag_and_term() -> None:
    source = _source(
        meta_mapping={
            "owner": {
                "match": ".*",
                "operation": "add_owner",
                "config": {"owner_type": "user"},
            },
            "certified": {
                "match": True,
                "operation": "add_tag",
                "config": {"tag": "certified"},
            },
            "glossary": {
                "match": ".*",
                "operation": "add_term",
                "config": {"term": "Revenue"},
            },
        }
    )
    entity = CubeEntity(
        name="orders",
        meta={
            "owner": "jdoe@example.com",
            "certified": True,
            "glossary": "Revenue",
        },
    )

    aspects = _aspects(list(source._emit_entity_meta(entity, "urn:li:dataset:test")))
    assert isinstance(aspects.get("OwnershipClass"), OwnershipClass)
    assert isinstance(aspects.get("GlobalTagsClass"), GlobalTagsClass)
    assert isinstance(aspects.get("GlossaryTermsClass"), GlossaryTermsClass)


def test_datahub_meta_block_emits_domain() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders",
        meta={"datahub": {"domain": "analytics"}},
    )

    aspects = _aspects(list(source._emit_entity_meta(entity, "urn:li:dataset:test")))
    domains = aspects.get("DomainsClass")
    assert isinstance(domains, DomainsClass)
    assert domains.domains == ["urn:li:domain:analytics"]


def test_pattern_domain_assignment() -> None:
    source = _source(
        domain={"urn:li:domain:analytics": {"allow": ["orders.*"]}},
    )
    entity = CubeEntity(name="orders")

    aspects = _aspects(list(source._emit_entity_meta(entity, "urn:li:dataset:test")))
    domains = aspects.get("DomainsClass")
    assert isinstance(domains, DomainsClass)
    assert domains.domains == ["urn:li:domain:analytics"]


def test_no_meta_emits_nothing() -> None:
    source = _source()
    entity = CubeEntity(name="orders")
    assert list(source._emit_entity_meta(entity, "urn:li:dataset:test")) == []


def test_temporal_dimension_is_tagged() -> None:
    source = _source()
    field = source._build_schema_field(
        CubeMember(name="created_at", is_measure=False, is_temporal=True)
    )
    assert field.globalTags is not None
    tags = {t.tag for t in field.globalTags.tags}
    assert "urn:li:tag:Dimension" in tags
    assert "urn:li:tag:Temporal" in tags


def test_column_meta_mapping_tags_and_terms_field() -> None:
    source = _source(
        column_meta_mapping={
            "pii": {
                "match": True,
                "operation": "add_tag",
                "config": {"tag": "pii"},
            },
            "term": {
                "match": ".*",
                "operation": "add_term",
                "config": {"term": "CustomerData"},
            },
        }
    )
    field = source._build_schema_field(
        CubeMember(
            name="email",
            is_measure=False,
            meta={"pii": True, "term": "CustomerData"},
        )
    )
    assert field.globalTags is not None
    assert any(t.tag == "urn:li:tag:pii" for t in field.globalTags.tags)
    assert field.glossaryTerms is not None
    assert any("CustomerData" in t.urn for t in field.glossaryTerms.terms)


def test_view_uses_semantic_model_subtype() -> None:
    source = _source()
    container = Container(source._container_key, display_name="demo")
    lineage_builder = MagicMock()
    lineage_builder.build.return_value = None

    view_aspects = _aspects(
        list(
            source._emit_entity(
                CubeEntity(name="orders_view", is_view=True),
                container,
                lineage_builder,
            )
        )
    )
    cube_aspects = _aspects(
        list(
            source._emit_entity(
                CubeEntity(name="orders"),
                container,
                lineage_builder,
            )
        )
    )
    assert view_aspects["SubTypesClass"].typeNames == [  # type: ignore[attr-defined]
        DatasetSubTypes.SEMANTIC_MODEL
    ]
    assert cube_aspects["SubTypesClass"].typeNames == [  # type: ignore[attr-defined]
        DatasetSubTypes.CUBE
    ]


def test_chart_inputs_use_logical_datasets_when_sm_enabled() -> None:
    source = _source(emit_semantic_model_entities=True, platform_instance="cube_demo")
    source._sm_mapper = MagicMock()
    source._sm_mapper.view_chart_inputs = {
        "orders_view": [
            "urn:li:dataset:(urn:li:dataPlatform:cube,cube_demo.orders_view.orders,PROD)"
        ]
    }
    chart = source._build_chart(
        CubeReport(
            id=1,
            public_id="rpt1",
            name="r1",
            referenced_entities=["orders_view", "orders"],
        )
    )
    aspects = _aspects(list(chart.as_workunits()))
    info = aspects["ChartInfoClass"]
    assert set(info.inputs or []) == {  # type: ignore[attr-defined]
        "urn:li:dataset:(urn:li:dataPlatform:cube,cube_demo.orders_view.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:cube,cube_demo.orders,PROD)",
    }


def test_emit_semantic_model_entities_skips_view_dataset() -> None:
    source = _source(emit_semantic_model_entities=True)
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
    )
    cube = CubeEntity(name="orders")
    with (
        patch.object(source.api_client, "get_entities", return_value=[cube, view]),
        patch.object(source.api_client, "get_reports", return_value=[]),
        patch.object(source.api_client, "get_workbooks", return_value=[]),
        patch.object(source.api_client, "get_data_sources", return_value=[]),
    ):
        urns = {wu.get_urn() for wu in source.get_workunits_internal()}

    assert "urn:li:dataset:(urn:li:dataPlatform:cube,orders_view,PROD)" not in urns
    assert any("semanticModel" in urn and "orders_view" in urn for urn in urns)
    assert "urn:li:dataset:(urn:li:dataPlatform:cube,orders,PROD)" in urns
    assert source.report.semantic_models_emitted == 1


def test_empty_view_does_not_emit_orphan_semantic_model_meta() -> None:
    source = _source(
        emit_semantic_model_entities=True,
        meta_mapping={
            "owner": {
                "match": ".*",
                "operation": "add_owner",
                "config": {"owner_type": "user"},
            }
        },
    )
    view = CubeEntity(
        name="empty_view",
        is_view=True,
        meta={"owner": "jdoe@example.com"},
    )
    with (
        patch.object(source.api_client, "get_entities", return_value=[view]),
        patch.object(source.api_client, "get_reports", return_value=[]),
        patch.object(source.api_client, "get_workbooks", return_value=[]),
        patch.object(source.api_client, "get_data_sources", return_value=[]),
    ):
        urns = {wu.get_urn() for wu in source.get_workunits_internal()}

    assert not any("semanticModel" in urn for urn in urns)
    assert "urn:li:dataset:(urn:li:dataPlatform:cube,empty_view,PROD)" not in urns
    assert source.report.semantic_models_emitted == 0


def test_chart_inputs_omit_skipped_semantic_model_view() -> None:
    source = _source(emit_semantic_model_entities=True, platform_instance="cube_demo")
    source._sm_mapper = MagicMock()
    source._sm_mapper.view_chart_inputs = {"empty_view": []}
    chart = source._build_chart(
        CubeReport(
            id=1,
            public_id="rpt1",
            name="r1",
            referenced_entities=["empty_view", "orders"],
        )
    )
    aspects = _aspects(list(chart.as_workunits()))
    info = aspects["ChartInfoClass"]
    assert set(info.inputs or []) == {  # type: ignore[attr-defined]
        "urn:li:dataset:(urn:li:dataPlatform:cube,cube_demo.orders,PROD)",
    }


def test_chart_input_omits_dangling_urn_when_semantic_model_emission_crashes() -> None:
    # Regression: if CubeSemanticModelMapper.emit() raises partway through for
    # a view, the per-view except handler used to leave
    # view_chart_inputs[view.name] unset entirely, so _chart_input_urns fell
    # back to a plain dataset-mode URN -- one that semantic-model mode never
    # emits for any view, successful or not. A chart referencing that view
    # would silently point at a dataset URN guaranteed to 404 forever.
    source = _source(emit_semantic_model_entities=True)
    view = CubeEntity(name="broken_view", is_view=True)
    good_cube = CubeEntity(name="orders")

    def crashing_emit(
        self: CubeSemanticModelMapper, entity: CubeEntity, cubes_by_name: dict
    ) -> Iterator[MetadataWorkUnit]:
        raise ValueError("boom")
        yield  # pragma: no cover - unreachable; keeps this a generator function

    with (
        patch.object(source.api_client, "get_entities", return_value=[good_cube, view]),
        patch.object(source.api_client, "get_reports", return_value=[]),
        patch.object(source.api_client, "get_workbooks", return_value=[]),
        patch.object(source.api_client, "get_data_sources", return_value=[]),
        patch.object(CubeSemanticModelMapper, "emit", crashing_emit),
    ):
        list(source.get_workunits_internal())

    chart = source._build_chart(
        CubeReport(
            id=1,
            public_id="rpt1",
            name="r1",
            referenced_entities=["broken_view", "orders"],
        )
    )
    aspects = _aspects(list(chart.as_workunits()))
    info = aspects["ChartInfoClass"]
    assert set(info.inputs or []) == {  # type: ignore[attr-defined]
        "urn:li:dataset:(urn:li:dataPlatform:cube,orders,PROD)",
    }
    assert "urn:li:dataset:(urn:li:dataPlatform:cube,broken_view,PROD)" not in (
        info.inputs or []  # type: ignore[attr-defined]
    )


def test_chart_input_omits_dangling_urn_for_view_filtered_by_pattern() -> None:
    # Regression: _sm_view_names was built from `to_emit` (post-filter), so a
    # view excluded by view_pattern/include_views/include_hidden never
    # appeared in it, and _chart_input_urns fell through to a plain dataset
    # URN -- dangling in semantic-model mode, same as the crash case above,
    # but reachable on the very first pass with no exception required.
    source = _source(
        emit_semantic_model_entities=True,
        view_pattern={"deny": ["filtered_view"]},
    )
    view = CubeEntity(
        name="filtered_view",
        is_view=True,
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
    )
    good_cube = CubeEntity(name="orders")
    with (
        patch.object(source.api_client, "get_entities", return_value=[good_cube, view]),
        patch.object(source.api_client, "get_reports", return_value=[]),
        patch.object(source.api_client, "get_workbooks", return_value=[]),
        patch.object(source.api_client, "get_data_sources", return_value=[]),
    ):
        list(source.get_workunits_internal())

    chart = source._build_chart(
        CubeReport(
            id=1,
            public_id="rpt1",
            name="r1",
            referenced_entities=["filtered_view", "orders"],
        )
    )
    aspects = _aspects(list(chart.as_workunits()))
    info = aspects["ChartInfoClass"]
    assert set(info.inputs or []) == {  # type: ignore[attr-defined]
        "urn:li:dataset:(urn:li:dataPlatform:cube,orders,PROD)",
    }
    assert "urn:li:dataset:(urn:li:dataPlatform:cube,filtered_view,PROD)" not in (
        info.inputs or []  # type: ignore[attr-defined]
    )


def test_emit_semantic_model_entities_warns_about_ignored_config_options() -> None:
    source = _source(emit_semantic_model_entities=True)
    with (
        patch.object(source.api_client, "get_entities", return_value=[]),
        patch.object(source.api_client, "get_reports", return_value=[]),
        patch.object(source.api_client, "get_workbooks", return_value=[]),
        patch.object(source.api_client, "get_data_sources", return_value=[]),
    ):
        list(source.get_workunits_internal())
    assert any(
        "Some config options do not apply in semantic-model mode" in (w.title or "")
        for w in source.report.warnings
    )


def test_view_definition_uses_sql_for_cube() -> None:
    source = _source()
    entity = CubeEntity(name="orders", sql="SELECT * FROM public.orders")
    view = source._view_definition(entity)
    assert view is not None
    assert view.viewLanguage == "SQL"
    assert view.viewLogic == "SELECT * FROM public.orders"
    assert view.materialized is False


def test_view_definition_reconstructs_includes_for_view() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders_view",
        is_view=True,
        measures=[
            CubeMember(
                name="count", is_measure=True, member_references=["orders.count"]
            )
        ],
        dimensions=[
            CubeMember(
                name="status", is_measure=False, member_references=["orders.status"]
            )
        ],
    )
    view = source._view_definition(entity)
    assert view is not None
    assert view.viewLanguage == "YAML"
    assert view.viewLogic == "includes:\n  - orders.count\n  - orders.status"


def test_view_definition_none_for_cube_without_sql() -> None:
    assert _source()._view_definition(CubeEntity(name="orders")) is None


def test_hidden_cube_filtered_by_default() -> None:
    source = _source()
    hidden = CubeEntity(name="secret", is_hidden=True)
    visible = CubeEntity(name="orders")
    assert source._should_emit(hidden) is False
    assert source._should_emit(visible) is True

    assert _source(include_hidden=True)._should_emit(hidden) is True


def test_hidden_members_excluded_from_schema() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders",
        measures=[CubeMember(name="count", is_measure=True, is_hidden=True)],
        dimensions=[CubeMember(name="status", is_measure=False)],
    )
    fields = source._build_schema_fields(entity)
    assert [f.fieldPath for f in fields] == ["status"]

    fields_all = _source(include_hidden=True)._build_schema_fields(entity)
    assert {f.fieldPath for f in fields_all} == {"count", "status"}


def test_structural_metadata_as_custom_properties() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders",
        file_name="cubes/orders.yml",
        joins=[CubeJoin(name="users", relationship="belongsTo")],
        hierarchies=[CubeHierarchy(name="geo", levels=["country", "city"])],
        folders=[CubeFolder(name="Core", members=["count", "status"])],
        pre_aggregation_names=["main_rollup"],
    )
    props = source._custom_properties(entity)
    assert props["file_name"] == "cubes/orders.yml"
    assert props["join.users"] == "belongsTo"
    assert props["hierarchy.geo"] == "country, city"
    assert props["folder.Core"] == "count, status"
    assert props["pre_aggregations"] == "main_rollup"


def test_measure_json_props() -> None:
    source = _source()
    field = source._build_schema_field(
        CubeMember(
            name="count",
            is_measure=True,
            format="percent",
            drill_members=["id", "status"],
            cumulative=True,
        )
    )
    assert field.jsonProps is not None
    assert '"format": "percent"' in field.jsonProps
    assert "drillMembers" in field.jsonProps

    plain = source._build_schema_field(CubeMember(name="status", is_measure=False))
    assert plain.jsonProps is None


def test_build_chart_sets_inputs_and_owner() -> None:
    source = _source(platform_instance="cube_demo")
    chart = source._build_chart(
        CubeReport(
            id=1,
            public_id="rpt1",
            name="r1",
            title="Report One",
            referenced_entities=["orders_view", "orders"],
            owner_email="a@example.com",
        )
    )
    assert str(chart.urn) == "urn:li:chart:(cube,cube_demo.rpt1)"

    aspects = _aspects(list(chart.as_workunits()))
    info = aspects["ChartInfoClass"]
    assert set(info.inputs or []) == {  # type: ignore[attr-defined]
        "urn:li:dataset:(urn:li:dataPlatform:cube,cube_demo.orders_view,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:cube,cube_demo.orders,PROD)",
    }
    ownership = aspects["OwnershipClass"]
    assert [o.owner for o in ownership.owners] == [  # type: ignore[attr-defined]
        "urn:li:corpuser:a@example.com"
    ]


def test_build_chart_warns_on_malformed_timestamp() -> None:
    # Regression: a malformed updated_at used to be swallowed by
    # _parse_timestamp with no trace anywhere -- last_modified would just be
    # silently absent.
    source = _source(platform_instance="cube_demo")
    chart = source._build_chart(
        CubeReport(id=1, public_id="rpt1", name="r1", updated_at="not-a-timestamp")
    )
    assert chart.last_modified is None
    assert len(source.report.warnings) == 1
    warning = list(source.report.warnings)[0]
    assert warning.title == "Could not parse Cube timestamp"
    assert any("r1" in c for c in warning.context)


def test_warehouse_defaults_autodetected_from_data_sources() -> None:
    # The 'default' data source's type maps to a DataHub platform and its
    # database back-fills warehouse_database.
    source = _source()
    with patch.object(
        source.api_client,
        "get_data_sources",
        return_value=[
            CloudDataSource(name="other", type="snowflake"),
            CloudDataSource(name="default", type="Postgres", database="analytics"),
        ],
    ):
        source._resolve_warehouse_defaults()
    assert source.config.warehouse_platform == "postgres"
    assert source.config.warehouse_database == "analytics"


def test_warehouse_defaults_unmapped_type_warns() -> None:
    # An unrecognized warehouse type must surface a warning, not silently skip lineage.
    source = _source()
    with patch.object(
        source.api_client,
        "get_data_sources",
        return_value=[CloudDataSource(name="default", type="some_new_db")],
    ):
        source._resolve_warehouse_defaults()
    assert source.config.warehouse_platform is None
    assert len(source.report.warnings) == 1


def test_one_bad_entity_does_not_abort_ingestion() -> None:
    # A single failing cube/view must be reported and skipped, not abort the run.
    source = _source()
    good = CubeEntity(name="orders")
    bad = CubeEntity(name="broken")

    def fake_emit(
        entity: CubeEntity, container: object, lineage_builder: object
    ) -> Iterator[MetadataWorkUnit]:
        if entity.name == "broken":
            raise ValueError("boom")
        yield from ()

    with (
        patch.object(source.api_client, "get_entities", return_value=[bad, good]),
        patch.object(source.api_client, "get_reports", return_value=[]),
        patch.object(source.api_client, "get_workbooks", return_value=[]),
        patch.object(source.api_client, "get_data_sources", return_value=[]),
        patch.object(source, "_emit_entity", side_effect=fake_emit),
    ):
        list(source.get_workunits_internal())

    assert len(source.report.warnings) == 1
    assert source.report.cubes_scanned == 2


def test_build_dashboard_links_known_charts() -> None:
    source = _source(platform_instance="cube_demo")
    container = Container(
        source._container_key,
        display_name="demo",
    )
    chart_by_report_id = {
        1: source._build_chart(CubeReport(id=1, public_id="rpt1", name="r1")),
        2: source._build_chart(CubeReport(id=2, public_id="rpt2", name="r2")),
    }
    dashboard = source._build_dashboard(
        CubeWorkbook(
            id=9,
            name="wb",
            title="Workbook",
            owner_email="a@example.com",
            # report 3 was filtered (seen but not built) and must be skipped
            # without a warning.
            report_ids=[1, 2, 3],
        ),
        chart_by_report_id,
        {1, 2, 3},
        container,
    )
    assert str(dashboard.urn) == "urn:li:dashboard:(cube,cube_demo.9)"
    assert len(source.report.warnings) == 0

    info = _aspects(list(dashboard.as_workunits()))["DashboardInfoClass"]
    linked = [edge.destinationUrn for edge in info.chartEdges or []]  # type: ignore[attr-defined]
    assert linked == [
        "urn:li:chart:(cube,cube_demo.rpt1)",
        "urn:li:chart:(cube,cube_demo.rpt2)",
    ]


def test_build_dashboard_warns_on_unknown_report_id() -> None:
    # Regression: a report_id present on the workbook but never returned by
    # get_reports() at all (not filtered, not a build failure) previously
    # vanished from the dashboard with no trace anywhere in the report.
    source = _source(platform_instance="cube_demo")
    container = Container(source._container_key, display_name="demo")
    chart_by_report_id = {
        1: source._build_chart(CubeReport(id=1, public_id="rpt1", name="r1")),
    }
    dashboard = source._build_dashboard(
        CubeWorkbook(
            id=9,
            name="wb",
            title="Workbook",
            report_ids=[1, 99],
        ),
        chart_by_report_id,
        {1},
        container,
    )
    assert len(dashboard.charts or []) == 1
    assert len(source.report.warnings) == 1
    warning = list(source.report.warnings)[0]
    assert warning.title == "Cube workbook references an unknown report"
    assert any("report_id=99" in c for c in warning.context)


def test_build_dashboard_no_warning_when_reports_disabled() -> None:
    # Regression: with include_reports=False, no report ids are ever fetched
    # on purpose, so seen_report_ids stays empty; every workbook report_id
    # used to be (wrongly) treated as "unknown" and warned about.
    source = _source(platform_instance="cube_demo", include_reports=False)
    container = Container(source._container_key, display_name="demo")
    dashboard = source._build_dashboard(
        CubeWorkbook(id=9, name="wb", title="Workbook", report_ids=[1, 2]),
        {},
        set(),
        container,
    )
    assert dashboard.charts == []
    assert len(source.report.warnings) == 0
