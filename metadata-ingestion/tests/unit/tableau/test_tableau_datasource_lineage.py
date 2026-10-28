"""Datasource emission and lineage in TableauSiteSource: embedded vs published
handling, upstream table registration, the upstreamColumns fallback, field
level lineage, Initial SQL merging and the published-datasource query filter.
"""

from typing import Iterable, List, Type, TypeVar
from unittest import mock

from tableauserverclient.models import UserItem

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    InitialSqlLineage,
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ContainerClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OwnershipClass,
    SubTypesClass,
    UpstreamLineageClass,
)
from tests.unit.tableau.test_tableau_config import default_config

T = TypeVar("T")

PROJECT_ID = "project-1"
WORKBOOK = {
    c.ID: "wb-1",
    c.NAME: "Sales WB",
    c.LUID: "wb-luid-1",
    c.PROJECT_NAME: "default",
    c.OWNER: {c.USERNAME: "alice", c.EMAIL: "alice@example.com"},
    c.TAGS: [{c.NAME: "workbook-tag"}],
}
ORDERS_TABLE = {
    c.ID: "table-1",
    c.NAME: "orders",
    c.SCHEMA: "public",
    c.FULL_NAME: "[public].[orders]",
    c.CONNECTION_TYPE: "postgres",
    c.DATABASE: {c.NAME: "dvdrental", c.CONNECTION_TYPE: "postgres"},
    c.COLUMNS_CONNECTION: {c.TOTAL_COUNT: 2},
}
ORDERS_URN = make_dataset_urn_with_platform_instance(
    "postgres", "dvdrental.public.orders", "demo_postgres_instance", "PROD"
)


def _tableau_urn(name: str) -> str:
    return make_dataset_urn_with_platform_instance("tableau", name, None, "PROD")


def _make_source(**overrides: object) -> TableauSiteSource:
    config = TableauConfig.model_validate({**default_config, **overrides})
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = "site-1"
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    source = TableauSiteSource(
        config=config,
        ctx=PipelineContext(run_id="test"),
        platform="tableau",
        site=SiteIdContentUrl(site_id="site-1", site_content_url="site-a"),
        report=TableauSourceReport(),
        server=server,
    )
    source.tableau_project_registry = {
        PROJECT_ID: TableauProject(
            id=PROJECT_ID,
            name="default",
            description="",
            parent_id=None,
            parent_name=None,
            path=["default"],
        )
    }
    source.workbook_project_map = {"wb-luid-1": PROJECT_ID}
    source.datasource_project_map = {"ds-luid-1": PROJECT_ID}
    return source


def _published_datasource(**overrides: object) -> dict:
    datasource: dict = {
        c.ID: "ds-1",
        c.NAME: "Orders DS",
        c.LUID: "ds-luid-1",
        c.TYPE_NAME: c.PUBLISHED_DATA_SOURCE,
        c.PROJECT_NAME: "default",
        c.DESCRIPTION: "orders",
        c.HAS_EXTRACTS: True,
        c.OWNER: {c.USERNAME: "bob", c.EMAIL: "bob@example.com"},
        c.TAGS: [{c.NAME: "ds-tag"}, None],
        c.FIELDS: [],
        c.UPSTREAM_TABLES: [],
        c.UPSTREAM_DATA_SOURCES: [],
    }
    datasource.update(overrides)
    return datasource


def _embedded_datasource(**overrides: object) -> dict:
    datasource: dict = {
        c.ID: "eds-1",
        c.NAME: "Embedded DS",
        c.TYPE_NAME: c.EMBEDDED_DATA_SOURCE,
        c.WORKBOOK: WORKBOOK,
        c.FIELDS: [],
        c.UPSTREAM_TABLES: [],
        c.UPSTREAM_DATA_SOURCES: [],
    }
    datasource.update(overrides)
    return datasource


def _snapshot_aspect(work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]) -> T:
    for wu in work_units:
        if isinstance(wu.metadata, MetadataChangeEvent):
            for aspect in wu.metadata.proposedSnapshot.aspects:
                if isinstance(aspect, aspect_type):
                    return aspect
    raise AssertionError(f"no {aspect_type.__name__} aspect in snapshot")


def _mcp_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    return [
        wu.metadata.aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def test_embedded_datasource_backed_by_a_published_one_defers_table_lineage() -> None:
    source = _make_source()
    datasource = _embedded_datasource(
        upstreamDatasources=[{c.ID: "ds-1"}], upstreamTables=[ORDERS_TABLE]
    )

    result = source._create_upstream_table_lineage(
        datasource, browse_path="default", is_embedded_ds=True
    )

    # The table edge belongs to the published datasource; emitting it here too
    # would show the embedded datasource reading the warehouse directly.
    assert [u.dataset for u in result.upstream_tables] == [_tableau_urn("ds-1")]
    assert source.database_tables == {}
    assert source.datasource_ids_being_used == ["ds-1"]


def test_published_datasource_registers_its_upstream_tables_for_emission() -> None:
    source = _make_source()
    datasource = _published_datasource(upstreamTables=[ORDERS_TABLE])

    result = source._create_upstream_table_lineage(
        datasource, browse_path="default", is_embedded_ds=False
    )

    assert [u.dataset for u in result.upstream_tables] == [ORDERS_URN]
    registered = source.database_tables[ORDERS_URN]
    assert registered.id == "table-1"
    assert registered.num_cols == 2
    assert registered.paths == {"default/Orders DS"}


def test_same_table_from_two_datasources_merges_into_one_entry() -> None:
    source = _make_source()
    # A second Tableau databaseTable instance for the same physical table, this
    # time without column metadata and under a different datasource.
    duplicate = {**ORDERS_TABLE, c.ID: "table-2", c.COLUMNS_CONNECTION: None}
    nameless = {**ORDERS_TABLE, c.ID: "table-3", c.NAME: None}

    source.get_upstream_tables([ORDERS_TABLE], "DS A", "default", is_custom_sql=False)
    upstreams, id_to_urn = source.get_upstream_tables(
        [duplicate, nameless], "DS B", "default", is_custom_sql=False
    )

    assert [u.dataset for u in upstreams] == [ORDERS_URN]
    assert id_to_urn == {"table-2": ORDERS_URN}
    registered = source.database_tables[ORDERS_URN]
    assert registered.id == "table-1"  # the instance that had columns wins
    assert registered.paths == {"default/DS A", "default/DS B"}
    assert source.report.num_upstream_table_skipped_no_name == 1
    assert source.report.num_upstream_table_processed_without_columns == 1


def test_lineage_falls_back_to_upstream_columns_when_upstream_tables_is_empty() -> None:
    source = _make_source()
    datasource = _published_datasource(
        fields=[
            {
                c.ID: "f-1",
                c.NAME: "amount",
                c.TYPE_NAME: c.COLUMN_FIELD,
                c.UPSTREAM_COLUMNS: [
                    {
                        c.NAME: "AMOUNT",
                        c.TABLE: {
                            c.ID: "vct-1",
                            c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                            c.NAME: "orders",
                            c.VIRTUAL_CONNECTION: {c.ID: "vc-1"},
                        },
                    }
                ],
            }
        ]
    )

    result = source._create_upstream_table_lineage(
        datasource, browse_path="default", is_embedded_ds=False
    )

    vc_table_urn = _tableau_urn("vc-1.orders")
    assert [u.dataset for u in result.upstream_tables] == [vc_table_urn]
    # Compared as a set: the fallback path currently yields each column edge
    # twice (once from the fallback itself, once from the regular CLL pass).
    column_edges = {
        (tuple(fgl.upstreams or []), tuple(fgl.downstreams or []))
        for fgl in result.fine_grained_lineages
    }
    assert column_edges == {
        (
            (make_schema_field_urn(vc_table_urn, "AMOUNT"),),
            (make_schema_field_urn(_tableau_urn("ds-1"), "amount"),),
        )
    }
    assert source.report.warnings == []


def test_datasource_without_any_upstream_is_reported() -> None:
    source = _make_source()
    datasource = _published_datasource(
        fields=[{c.ID: "f-1", c.NAME: "amount", c.TYPE_NAME: c.COLUMN_FIELD}]
    )

    result = source._create_upstream_table_lineage(
        datasource, browse_path="default", is_embedded_ds=False
    )

    assert result.upstream_tables == []
    assert [w.title for w in source.report.warnings] == [
        "No upstream table lineage found"
    ]


def test_field_to_field_lineage_records_the_calculation() -> None:
    source = _make_source()
    datasource = _embedded_datasource(
        fields=[
            {
                c.ID: "f-1",
                c.NAME: "double_amount",
                c.TYPE_NAME: c.CALCULATED_FIELD,
                c.FORMULA: "[amount] * 2",
                c.UPSTREAM_FIELDS: [
                    {c.NAME: "amount", c.DATA_SOURCE: {c.ID: "ds-1"}},
                    None,
                    {c.NAME: "orphan", c.DATA_SOURCE: None},
                ],
            },
            {
                c.ID: "f-2",
                c.NAME: "region",
                c.TYPE_NAME: c.COLUMN_FIELD,
                c.UPSTREAM_FIELDS: [{c.NAME: "region", c.DATA_SOURCE: {c.ID: "ds-1"}}],
            },
        ]
    )

    lineages = source.get_upstream_fields_of_field_in_datasource(
        datasource, _tableau_urn("eds-1")
    )

    assert [fgl.upstreams for fgl in lineages] == [
        [make_schema_field_urn(_tableau_urn("ds-1"), "amount")],
        [make_schema_field_urn(_tableau_urn("ds-1"), "region")],
    ]
    calculated, identity = lineages
    assert calculated.transformOperation is not None
    assert "[amount] * 2" in calculated.transformOperation
    assert identity.transformOperation == c.IDENTITY


def test_published_datasource_emits_properties_owner_tags_and_project_container() -> (
    None
):
    source = _make_source(use_email_as_username=True)

    work_units = list(source.emit_datasource(_published_datasource()))

    props = _snapshot_aspect(work_units, DatasetPropertiesClass)
    assert props.name == "Orders DS"
    assert props.customProperties == {c.LUID: "ds-luid-1", c.HAS_EXTRACTS: "True"}
    assert [t.tag for t in _snapshot_aspect(work_units, GlobalTagsClass).tags] == [
        "urn:li:tag:ds-tag"
    ]
    assert [o.owner for o in _snapshot_aspect(work_units, OwnershipClass).owners] == [
        "urn:li:corpuser:bob@example.com"
    ]
    assert _snapshot_aspect(work_units, BrowsePathsClass).paths == [
        "/prod/tableau/default"
    ]
    assert _mcp_aspects(work_units, SubTypesClass)[0].typeNames == [
        "Published Data Source"
    ]
    assert [cn.container for cn in _mcp_aspects(work_units, ContainerClass)] == [
        source.gen_project_key(PROJECT_ID).as_urn()
    ]


def test_embedded_datasource_lives_under_its_workbook() -> None:
    source = _make_source()

    work_units = list(
        source.emit_datasource(
            _embedded_datasource(), workbook=WORKBOOK, is_embedded_ds=True
        )
    )

    # Tags and owner come from the workbook, and the browse path nests under it.
    assert [t.tag for t in _snapshot_aspect(work_units, GlobalTagsClass).tags] == [
        "urn:li:tag:workbook-tag"
    ]
    assert [o.owner for o in _snapshot_aspect(work_units, OwnershipClass).owners] == [
        "urn:li:corpuser:alice"
    ]
    assert _snapshot_aspect(work_units, BrowsePathsClass).paths == [
        "/prod/tableau/default/Sales WB"
    ]
    assert _mcp_aspects(work_units, SubTypesClass)[0].typeNames == [
        "Embedded Data Source"
    ]
    assert [cn.container for cn in _mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]


def test_datasource_in_an_unselected_project_is_not_emitted() -> None:
    source = _make_source()
    source.datasource_project_map = {"ds-luid-1": "project-other"}

    assert list(source.emit_datasource(_published_datasource())) == []


def test_initial_sql_adds_a_custom_property_and_deduplicated_upstreams() -> None:
    source = _make_source(ingest_initial_sql=True)
    staging_urn = make_dataset_urn_with_platform_instance(
        "postgres", "dvdrental.public.staging", "demo_postgres_instance", "PROD"
    )
    initial_sql = InitialSqlLineage(
        upstreams=[
            Upstream(dataset=ORDERS_URN, type=DatasetLineageType.TRANSFORMED),
            Upstream(dataset=staging_urn, type=DatasetLineageType.TRANSFORMED),
        ],
        raw_sql="CREATE TEMP TABLE t AS SELECT * FROM staging",
    )

    with mock.patch.object(source, "get_initial_sql_lineage", return_value=initial_sql):
        work_units = list(
            source.emit_datasource(_published_datasource(upstreamTables=[ORDERS_TABLE]))
        )

    props = _snapshot_aspect(work_units, DatasetPropertiesClass)
    assert props.customProperties is not None
    assert props.customProperties[c.INITIAL_SQL] == initial_sql.raw_sql
    lineage = _mcp_aspects(work_units, UpstreamLineageClass)[0]
    assert [u.dataset for u in lineage.upstreams] == [ORDERS_URN, staging_urn]


def test_field_upstreams_are_merged_into_the_datasource_by_field_id() -> None:
    source = _make_source()
    datasource = _published_datasource(
        fields=[{c.ID: "f-1", c.NAME: "amount"}, {c.ID: "f-2", c.NAME: "region"}]
    )
    upstream_rows = [
        {c.ID: "f-1", c.UPSTREAM_COLUMNS: [{c.NAME: "AMOUNT"}]},
        {c.ID: "f-unknown", c.UPSTREAM_COLUMNS: [{c.NAME: "ignored"}]},
    ]

    with mock.patch.object(
        source, "get_connection_objects", return_value=upstream_rows
    ) as get_objects:
        merged = source.update_datasource_for_field_upstream(
            datasource, field_upstream_query="{ id }", page_size=10
        )

    assert get_objects.call_args.kwargs["query_filter"] == {
        c.ID_WITH_IN: ["f-1", "f-2"]
    }
    assert merged[c.FIELDS] == [
        {c.ID: "f-1", c.NAME: "amount", c.UPSTREAM_COLUMNS: [{c.NAME: "AMOUNT"}]},
        {c.ID: "f-2", c.NAME: "region"},
    ]


def test_published_datasource_query_is_limited_to_referenced_ids_by_default() -> None:
    source = _make_source()
    source.datasource_ids_being_used = ["ds-1", "ds-2"]
    with mock.patch.object(
        source, "get_connection_objects", return_value=[]
    ) as get_objects:
        list(source.emit_published_datasources())
    assert get_objects.call_args.kwargs["query_filter"] == {
        c.ID_WITH_IN: ["ds-1", "ds-2"]
    }

    source = _make_source(emit_all_published_datasources=True)
    source.datasource_ids_being_used = ["ds-1"]
    with mock.patch.object(
        source, "get_connection_objects", return_value=[]
    ) as get_objects:
        list(source.emit_published_datasources())
    assert get_objects.call_args.kwargs["query_filter"] == {}
