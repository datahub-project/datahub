"""Custom SQL datasources, upstream database table emission, workbook
permission properties and multi-site selection in the Tableau source.
"""

from typing import Any, Dict, Iterable, List, Type, TypeVar, cast
from unittest import mock

from tableauserverclient import GroupItem, PermissionsRule, SiteItem
from tableauserverclient.models import UserItem
from tableauserverclient.models.reference_item import ResourceReference

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    DatabaseTable,
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSource,
    TableauSourceReport,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ContainerClass,
    DatasetKeyClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from tests.unit.tableau.test_tableau_config import default_config

T = TypeVar("T")

PROJECT_ID = "project-1"
WORKBOOK = {
    c.ID: "wb-1",
    c.NAME: "Sales WB",
    c.LUID: "wb-luid-1",
    c.PROJECT_NAME: "default",
}
ORDERS_URN = make_dataset_urn_with_platform_instance(
    "postgres", "dvdrental.public.orders", "demo_postgres_instance", "PROD"
)
STAGING_URN = make_dataset_urn_with_platform_instance(
    "postgres", "dvdrental.public.staging", "demo_postgres_instance", "PROD"
)


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
    return source


def _custom_sql(**overrides: object) -> dict:
    """A customSQLTablesConnection node as the Metadata API returns it."""
    csql: dict = {
        c.ID: "csql-1",
        c.NAME: "Custom SQL Query",
        c.QUERY: "SELECT id, amount FROM public.orders WHERE amount >> 10",
        c.CONNECTION_TYPE: "postgres",
        c.DATABASE: {c.NAME: "dvdrental", c.CONNECTION_TYPE: "postgres"},
        c.TABLES: [
            {
                c.ID: "table-1",
                c.NAME: "orders",
                c.SCHEMA: "public",
                c.FULL_NAME: "[public].[orders]",
                c.CONNECTION_TYPE: "postgres",
                c.DATABASE: {c.NAME: "dvdrental", c.CONNECTION_TYPE: "postgres"},
                c.COLUMNS_CONNECTION: {c.TOTAL_COUNT: 2},
            }
        ],
        c.COLUMNS: [
            {
                c.ID: "col-1",
                c.NAME: "amount",
                c.REMOTE_TYPE: "I8",
                "referencedByFields": [
                    {
                        c.DATA_SOURCE: {
                            c.ID: "eds-1",
                            c.NAME: "Embedded DS",
                            c.TYPE_NAME: c.EMBEDDED_DATA_SOURCE,
                            c.WORKBOOK: WORKBOOK,
                        }
                    }
                ],
            }
        ],
    }
    csql.update(overrides)
    return csql


def _snapshot_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    return [
        aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeEvent)
        for aspect in wu.metadata.proposedSnapshot.aspects
        if isinstance(aspect, aspect_type)
    ]


def _mcp_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    return [
        wu.metadata.aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def test_custom_sql_of_an_embedded_datasource_is_placed_under_its_workbook() -> None:
    source = _make_source()

    with mock.patch.object(
        source, "get_connection_objects", return_value=[_custom_sql()]
    ):
        work_units = list(source.emit_custom_sql_datasources())

    assert [cn.container for cn in _mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]
    assert _snapshot_aspects(work_units, BrowsePathsClass)[0].paths == [
        "/prod/tableau/default/Sales WB/Embedded DS"
    ]
    # The dataset is named after the datasource that owns the query, not the query.
    assert (
        _snapshot_aspects(work_units, DatasetPropertiesClass)[0].name == "Embedded DS"
    )
    view = _snapshot_aspects(work_units, ViewPropertiesClass)[0]
    assert view.viewLogic == "SELECT id, amount FROM public.orders WHERE amount > 10"
    assert (
        _snapshot_aspects(work_units, SchemaMetadataClass)[0].fields[0].fieldPath
        == "amount"
    )
    assert _mcp_aspects(work_units, SubTypesClass)[0].typeNames == [
        "View",
        c.CUSTOM_SQL,
    ]
    # Tableau resolved the referenced table, so lineage comes from its metadata.
    lineage = _mcp_aspects(work_units, UpstreamLineageClass)[0]
    assert [u.dataset for u in lineage.upstreams] == [ORDERS_URN]
    assert lineage.fineGrainedLineages is None


def test_custom_sql_without_a_referencing_datasource_is_skipped() -> None:
    source = _make_source()
    orphan = _custom_sql(columns=[])

    with mock.patch.object(source, "get_connection_objects", return_value=[orphan]):
        work_units = list(source.emit_custom_sql_datasources())

    assert _snapshot_aspects(work_units, DatasetPropertiesClass) == []


def test_unresolved_custom_sql_lineage_is_parsed_from_the_query() -> None:
    source = _make_source()
    query = (
        "SELECT o.id, s.amount FROM public.orders o "
        "JOIN public.staging s ON o.id = s.order_id "
        "WHERE s.amount > <Parameters.[Min Amount]>"
    )
    unresolved = _custom_sql(tables=[], query=query)

    with mock.patch.object(source, "get_connection_objects", return_value=[unresolved]):
        work_units = list(source.emit_custom_sql_datasources())

    lineage = _mcp_aspects(work_units, UpstreamLineageClass)[0]
    assert sorted(u.dataset for u in lineage.upstreams) == [ORDERS_URN, STAGING_URN]
    assert lineage.fineGrainedLineages is not None
    assert source.report.num_upstream_fine_grained_lineage == len(
        lineage.fineGrainedLineages
    )
    # Projected columns seen while parsing are remembered for the table's own schema.
    assert source.database_tables[STAGING_URN].parsed_columns == {"amount"}


def test_custom_sql_parsing_is_skipped_unless_unsupported_or_forced() -> None:
    csql = {**_custom_sql(), c.IS_UNSUPPORTED_CUSTOM_SQL: False}
    csql_urn = make_dataset_urn_with_platform_instance(
        "tableau", "csql-1", None, "PROD"
    )

    assert (
        _make_source().parse_custom_sql(
            datasource=csql,
            datasource_urn=csql_urn,
            platform="tableau",
            env="PROD",
            platform_instance=None,
            func_overridden_info=None,
        )
        is None
    )

    forced = _make_source(force_extraction_of_lineage_from_custom_sql_queries=True)
    parsed = forced.parse_custom_sql(
        datasource=csql,
        datasource_urn=csql_urn,
        platform="tableau",
        env="PROD",
        platform_instance=None,
        func_overridden_info=None,
    )
    assert parsed is not None
    assert parsed.result.debug_info.error is None


def test_external_upstream_tables_are_emitted_only_when_enabled() -> None:
    returned_by_tableau = {
        c.ID: "table-1",
        c.IS_EMBEDDED: False,
        c.COLUMNS: [{c.ID: "col-1", c.NAME: "id", c.REMOTE_TYPE: "I8"}],
    }
    table = DatabaseTable(
        urn=ORDERS_URN, id="table-1", num_cols=1, paths={"default/DS"}
    )

    disabled = _make_source(ingest_tables_external=False)
    disabled.database_tables = {ORDERS_URN: table}
    with mock.patch.object(
        disabled, "get_connection_objects", return_value=[returned_by_tableau]
    ):
        assert list(disabled.emit_upstream_tables()) == []

    enabled = _make_source(ingest_tables_external=True)
    enabled.database_tables = {ORDERS_URN: table}
    with mock.patch.object(
        enabled, "get_connection_objects", return_value=[returned_by_tableau]
    ):
        work_units = list(enabled.emit_upstream_tables())
    assert [wu.id for wu in work_units] == [ORDERS_URN]
    assert [
        f.fieldPath
        for f in _snapshot_aspects(work_units, SchemaMetadataClass)[0].fields
    ] == ["id"]
    assert _snapshot_aspects(work_units, BrowsePathsClass)[0].paths == [
        "/prod/tableau/default/DS"
    ]


def test_tables_known_only_from_sql_parsing_are_emitted_with_their_columns() -> None:
    source = _make_source(ingest_tables_external=True)
    source.database_tables = {
        ORDERS_URN: DatabaseTable(
            urn=ORDERS_URN, id="table-1", num_cols=1, parsed_columns={"id", "amount"}
        ),
        STAGING_URN: DatabaseTable(urn=STAGING_URN, parsed_columns={"order_id"}),
    }
    returned_by_tableau = {
        c.ID: "table-1",
        c.IS_EMBEDDED: True,
        c.COLUMNS: [{c.ID: "col-1", c.NAME: "id", c.REMOTE_TYPE: "I8"}],
    }

    with mock.patch.object(
        source, "get_connection_objects", return_value=[returned_by_tableau]
    ):
        work_units = list(source.emit_upstream_tables())

    assert [wu.id for wu in work_units] == [ORDERS_URN, STAGING_URN]
    orders_schema, staging_schema = _snapshot_aspects(work_units, SchemaMetadataClass)
    # Tableau's column metadata wins; parsed columns only fill in what it lacks.
    assert sorted(f.fieldPath for f in orders_schema.fields) == ["amount", "id"]
    assert [f.fieldPath for f in staging_schema.fields] == ["order_id"]


def test_table_without_any_metadata_still_materializes_with_a_key_aspect() -> None:
    source = _make_source(ingest_tables_external=True)
    source.database_tables = {STAGING_URN: DatabaseTable(urn=STAGING_URN)}

    with mock.patch.object(source, "get_connection_objects", return_value=[]):
        work_units = list(source.emit_upstream_tables())

    assert len(_snapshot_aspects(work_units, DatasetKeyClass)) == 1


def _group(group_id: str, name: str) -> GroupItem:
    group = GroupItem(name)
    group._id = group_id
    return group


def _rule(grantee: ResourceReference, capabilities: Dict[str, str]) -> PermissionsRule:
    return PermissionsRule(grantee=grantee, capabilities=capabilities)


def test_workbook_permissions_keep_allowed_capabilities_of_allowed_groups() -> None:
    source = _make_source(
        permission_ingestion={"group_name_pattern": {"deny": ["^Internal.*"]}}
    )
    source.group_map = {
        "g-1": _group("g-1", "Consumers"),
        "g-2": _group("g-2", "Internal Admins"),
    }
    rules = [
        _rule(ResourceReference("g-1", "group"), {"Read": "Allow", "Write": "Deny"}),
        _rule(ResourceReference("g-2", "group"), {"Read": "Allow"}),
        _rule(ResourceReference("g-unknown", "group"), {"Read": "Allow"}),
        _rule(ResourceReference("u-1", "user"), {"Read": "Allow"}),
    ]

    assert source._create_workbook_properties(rules) == {
        "permissions": '[{"group": "Consumers", "capabilities": ["Read"]}]'
    }
    assert source._create_workbook_properties(rules[1:]) is None


def _site(name: str, state: str) -> SiteItem:
    site = SiteItem(name=name, content_url=name.lower())
    # SiteItem only exposes these through private attributes initialised to None.
    site_attrs: Any = cast(Any, site)
    site_attrs._id = f"id-{name.lower()}"
    site_attrs._state = state
    return site


def test_multi_site_ingestion_switches_only_to_allowed_active_sites() -> None:
    config = {
        **default_config,
        "ingest_multiple_sites": True,
        "site_name_pattern": {"deny": ["Archive"]},
    }
    del config["stateful_ingestion"]
    sites = [
        _site("Sales", "Active"),
        _site("Archive", "Active"),
        _site("Old", "Suspended"),
    ]
    server = mock.MagicMock()
    server.is_signed_in.return_value = True

    with (
        mock.patch.object(TableauConfig, "make_tableau_client", return_value=server),
        mock.patch("datahub.ingestion.source.tableau.tableau.report_user_role"),
        mock.patch(
            "datahub.ingestion.source.tableau.tableau.TSC.Pager", return_value=sites
        ),
        mock.patch.object(
            TableauSiteSource, "ingest_tableau_site", return_value=iter(())
        ) as ingest_site,
    ):
        source = TableauSource(
            TableauConfig.model_validate(config), PipelineContext("t")
        )
        list(source.get_workunits_internal())

    assert [call.args[0].name for call in server.auth.switch_site.call_args_list] == [
        "Sales"
    ]
    assert ingest_site.call_count == 1
