"""Custom SQL datasources, upstream database table emission, workbook
permission properties and multi-site selection in the Tableau source.
"""

from typing import Dict
from unittest import mock

from tableauserverclient import PermissionsRule
from tableauserverclient.models.reference_item import ResourceReference

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    DatabaseTable,
    TableauSiteSource,
)
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
from tests.unit.tableau.conftest import (
    ORDERS_URN,
    STAGING_URN,
    WORKBOOK,
    SiteSourceFactory,
    TableauSourceFactory,
    mcp_aspects,
    mock_tableau_server,
    snapshot_aspects,
    tsc_group,
    tsc_site,
)


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


def test_custom_sql_of_an_embedded_datasource_is_placed_under_its_workbook(
    site_source: SiteSourceFactory,
) -> None:
    """The custom SQL dataset is named after the datasource that owns the query,
    nests under that datasource's workbook, and takes its lineage from the
    tables Tableau already resolved rather than from parsing the query."""
    source = site_source(seed_project=True)

    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[_custom_sql()]
    ):
        work_units = list(source.emit_custom_sql_datasources())

    assert [cn.container for cn in mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]
    assert snapshot_aspects(work_units, BrowsePathsClass)[0].paths == [
        "/prod/tableau/default/Sales WB/Embedded DS"
    ]
    assert snapshot_aspects(work_units, DatasetPropertiesClass)[0].name == "Embedded DS"
    view = snapshot_aspects(work_units, ViewPropertiesClass)[0]
    assert view.viewLogic == "SELECT id, amount FROM public.orders WHERE amount > 10"
    assert (
        snapshot_aspects(work_units, SchemaMetadataClass)[0].fields[0].fieldPath
        == "amount"
    )
    assert mcp_aspects(work_units, SubTypesClass)[0].typeNames == [
        "View",
        c.CUSTOM_SQL,
    ]
    lineage = mcp_aspects(work_units, UpstreamLineageClass)[0]
    assert [u.dataset for u in lineage.upstreams] == [ORDERS_URN]
    assert lineage.fineGrainedLineages is None


def test_custom_sql_without_a_referencing_datasource_is_skipped(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True)
    orphan = _custom_sql(columns=[])

    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[orphan]
    ):
        work_units = list(source.emit_custom_sql_datasources())

    assert snapshot_aspects(work_units, DatasetPropertiesClass) == []


def test_unresolved_custom_sql_lineage_is_parsed_from_the_query(
    site_source: SiteSourceFactory,
) -> None:
    """When Tableau could not resolve the query's tables, lineage comes from the
    SQL parser, with Tableau parameters stripped first; columns the parser saw
    are remembered so the table's own schema can be emitted later."""
    source = site_source(seed_project=True)
    query = (
        "SELECT o.id, s.amount FROM public.orders o "
        "JOIN public.staging s ON o.id = s.order_id "
        "WHERE s.amount > <Parameters.[Min Amount]>"
    )
    unresolved = _custom_sql(tables=[], query=query)

    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[unresolved]
    ):
        work_units = list(source.emit_custom_sql_datasources())

    lineage = mcp_aspects(work_units, UpstreamLineageClass)[0]
    assert sorted(u.dataset for u in lineage.upstreams) == [ORDERS_URN, STAGING_URN]
    assert lineage.fineGrainedLineages is not None
    assert source.report.num_upstream_fine_grained_lineage == len(
        lineage.fineGrainedLineages
    )
    assert source.database_tables[STAGING_URN].parsed_columns == {"amount"}


def test_custom_sql_parsing_is_skipped_unless_unsupported_or_forced(
    site_source: SiteSourceFactory,
) -> None:
    csql = {**_custom_sql(), c.IS_UNSUPPORTED_CUSTOM_SQL: False}
    csql_urn = make_dataset_urn_with_platform_instance(
        "tableau", "csql-1", None, "PROD"
    )

    assert (
        site_source(seed_project=True).parse_custom_sql(
            datasource=csql,
            datasource_urn=csql_urn,
            platform="tableau",
            env="PROD",
            platform_instance=None,
            func_overridden_info=None,
        )
        is None
    )

    forced = site_source(
        seed_project=True, force_extraction_of_lineage_from_custom_sql_queries=True
    )
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


def test_external_upstream_tables_are_emitted_only_when_enabled(
    site_source: SiteSourceFactory,
) -> None:
    returned_by_tableau = {
        c.ID: "table-1",
        c.IS_EMBEDDED: False,
        c.COLUMNS: [{c.ID: "col-1", c.NAME: "id", c.REMOTE_TYPE: "I8"}],
    }
    table = DatabaseTable(
        urn=ORDERS_URN, id="table-1", num_cols=1, paths={"default/DS"}
    )

    disabled = site_source(seed_project=True, ingest_tables_external=False)
    disabled.database_tables = {ORDERS_URN: table}
    with mock.patch.object(
        disabled,
        "get_connection_objects",
        autospec=True,
        return_value=[returned_by_tableau],
    ):
        assert list(disabled.emit_upstream_tables()) == []

    enabled = site_source(seed_project=True, ingest_tables_external=True)
    enabled.database_tables = {ORDERS_URN: table}
    with mock.patch.object(
        enabled,
        "get_connection_objects",
        autospec=True,
        return_value=[returned_by_tableau],
    ):
        work_units = list(enabled.emit_upstream_tables())
    assert [wu.id for wu in work_units] == [ORDERS_URN]
    assert [
        f.fieldPath for f in snapshot_aspects(work_units, SchemaMetadataClass)[0].fields
    ] == ["id"]
    assert snapshot_aspects(work_units, BrowsePathsClass)[0].paths == [
        "/prod/tableau/default/DS"
    ]


def test_tables_known_only_from_sql_parsing_are_emitted_with_their_columns(
    site_source: SiteSourceFactory,
) -> None:
    """Tableau's column metadata wins; columns seen by the SQL parser only fill
    in what Tableau lacks, and tables Tableau never listed are still emitted."""
    source = site_source(seed_project=True, ingest_tables_external=True)
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
        source,
        "get_connection_objects",
        autospec=True,
        return_value=[returned_by_tableau],
    ):
        work_units = list(source.emit_upstream_tables())

    assert [wu.id for wu in work_units] == [ORDERS_URN, STAGING_URN]
    orders_schema, staging_schema = snapshot_aspects(work_units, SchemaMetadataClass)
    assert sorted(f.fieldPath for f in orders_schema.fields) == ["amount", "id"]
    assert [f.fieldPath for f in staging_schema.fields] == ["order_id"]


def test_table_without_any_metadata_still_materializes_with_a_key_aspect(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True, ingest_tables_external=True)
    source.database_tables = {STAGING_URN: DatabaseTable(urn=STAGING_URN)}

    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[]
    ):
        work_units = list(source.emit_upstream_tables())

    assert len(snapshot_aspects(work_units, DatasetKeyClass)) == 1


def _rule(grantee: ResourceReference, capabilities: Dict[str, str]) -> PermissionsRule:
    return PermissionsRule(grantee=grantee, capabilities=capabilities)


def test_workbook_permissions_keep_allowed_capabilities_of_allowed_groups(
    site_source: SiteSourceFactory,
) -> None:
    """Only "Allow" capabilities of groups that pass group_name_pattern are kept;
    user grantees and unknown group ids are dropped, and no surviving rule means
    no permissions property at all."""
    source = site_source(
        seed_project=True,
        permission_ingestion={"group_name_pattern": {"deny": ["^Internal.*"]}},
    )
    source.group_map = {
        "g-1": tsc_group("g-1", "Consumers"),
        "g-2": tsc_group("g-2", "Internal Admins"),
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


def test_multi_site_ingestion_switches_only_to_allowed_active_sites(
    tableau_source: TableauSourceFactory,
) -> None:
    """With ingest_multiple_sites the source switches to every site that is
    Active and passes site_name_pattern, and ingests each one in turn."""
    sites = [
        tsc_site("Sales", site_id="id-sales", content_url="sales", state="Active"),
        tsc_site(
            "Archive", site_id="id-archive", content_url="archive", state="Active"
        ),
        tsc_site("Old", site_id="id-old", content_url="old", state="Suspended"),
    ]
    server = mock_tableau_server()
    server.is_signed_in.return_value = True
    source = tableau_source(
        server, ingest_multiple_sites=True, site_name_pattern={"deny": ["Archive"]}
    )

    with (
        mock.patch(
            "datahub.ingestion.source.tableau.tableau.TSC.Pager", return_value=sites
        ),
        mock.patch.object(
            TableauSiteSource,
            "ingest_tableau_site",
            autospec=True,
            return_value=iter(()),
        ) as ingest_site,
    ):
        list(source.get_workunits_internal())

    assert [call.args[0].name for call in server.auth.switch_site.call_args_list] == [
        "Sales"
    ]
    assert ingest_site.call_count == 1
