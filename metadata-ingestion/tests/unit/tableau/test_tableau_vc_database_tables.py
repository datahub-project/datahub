"""Virtual connection tables resolved against Tableau's databaseTables listing:
the multi-key lookup, name matching strategies, URN creation and the column
level lineage emitted for VC tables. Also the malformed-payload handling in VC
reference extraction and lookup.
"""

from typing import Any, Dict, Iterable, List, Optional, Type, TypeVar
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
    SiteIdContentUrl,
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_virtual_connections import (
    DatasourceType,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    UpstreamLineageClass,
)
from tests.unit.tableau.test_tableau_config import default_config

T = TypeVar("T")

ORDERS_TABLE: dict = {
    c.ID: "t-1",
    c.NAME: "orders",
    c.FULL_NAME: "[dvdrental].[public].[orders]",
    c.SCHEMA: "public",
    c.CONNECTION_TYPE: "postgres",
    c.DATABASE: {c.ID: "db-1", c.NAME: "dvdrental", c.CONNECTION_TYPE: "postgres"},
    c.COLUMNS: [{c.NAME: "ID"}, {c.NAME: "amount"}, {c.NAME: None}],
}
# Tableau sometimes returns a complete fullName while leaving schema null.
EVENTS_TABLE: dict = {
    c.ID: "t-2",
    c.NAME: "events",
    c.FULL_NAME: "analytics.web.events",
    c.SCHEMA: None,
    c.CONNECTION_TYPE: "snowflake",
    c.DATABASE: {c.NAME: "analytics", c.CONNECTION_TYPE: "snowflake"},
    c.COLUMNS: [],
}
UNUSABLE_TABLES: List[dict] = [
    {c.ID: "t-3", c.NAME: None, c.DATABASE: {c.CONNECTION_TYPE: "postgres"}},
    {c.ID: "t-4", c.NAME: "no_connection", c.DATABASE: None},
]
ORDERS_URN = make_dataset_urn_with_platform_instance(
    "postgres", "dvdrental.public.orders", "demo_postgres_instance", "PROD"
)
EVENTS_URN = make_dataset_urn_with_platform_instance(
    "snowflake", "analytics.web.events", None, "PROD"
)


def _make_source(**overrides: object) -> TableauSiteSource:
    config = TableauConfig.model_validate(
        {**default_config, "ingest_virtual_connections": True, **overrides}
    )
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = "site-1"
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    return TableauSiteSource(
        config=config,
        ctx=PipelineContext(run_id="test"),
        platform="tableau",
        site=SiteIdContentUrl(site_id="site-1", site_content_url="site-a"),
        report=TableauSourceReport(),
        server=server,
    )


def _connection_objects(
    by_connection_type: Dict[str, List[dict]],
) -> Any:
    def get(**kwargs: Any) -> Iterable[dict]:
        return list(by_connection_type.get(kwargs["connection_type"], []))

    return get


def _mcp_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    return [
        wu.metadata.aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


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


def _source_with_lookup() -> TableauSiteSource:
    source = _make_source()
    with mock.patch.object(
        source,
        "get_connection_objects",
        side_effect=_connection_objects(
            {
                c.DATABASE_TABLES_CONNECTION: [
                    ORDERS_TABLE,
                    EVENTS_TABLE,
                    *UNUSABLE_TABLES,
                ]
            }
        ),
    ) as get_objects:
        source._build_database_tables_lookup()
        source._build_database_tables_lookup()
    # The listing is fetched once per site, however many VCs need it.
    assert get_objects.call_count == 1
    return source


def test_vc_table_names_resolve_to_database_tables_by_several_strategies() -> None:
    source = _source_with_lookup()

    def match(vc_table_name: str) -> Optional[str]:
        table = source._find_matching_database_table(vc_table_name)
        return table[c.ID] if table else None

    assert match("orders") == "t-1"
    assert match("[public].[orders]") == "t-1"
    # Tableau labels VC tables as "TABLE (schema.table)".
    assert match("ORDERS (public.orders)") == "t-1"
    assert match("dvdrental.public.orders") == "t-1"
    # A prefix Tableau never listed still matches on the bare table name.
    assert match("sales.orders") == "t-1"
    # schema.table matches the fully qualified name even when schema was null.
    assert match("web.events") == "t-2"
    assert match("customers") is None
    # Rows without a name or a connection type cannot be matched.
    assert match("no_connection") is None


def test_lookup_is_inert_until_built() -> None:
    source = _make_source()

    assert source._find_matching_database_table("orders") is None
    assert source._create_database_table_urn({c.NAME: "orders", c.DATABASE: None}) is (
        None
    )
    assert (
        source._create_database_table_urn(
            {c.NAME: "orders", c.DATABASE: {c.NAME: "db"}}
        )
        is None
    )


def test_vc_table_lineage_maps_columns_case_insensitively() -> None:
    source = _source_with_lookup()
    source.vc_processor.virtual_connection_ids_being_used = ["vc-1"]
    virtual_connection = {
        c.ID: "vc-1",
        c.NAME: "Sales VC",
        "tables": [
            {
                c.ID: "vct-1",
                c.NAME: "orders",
                c.COLUMNS: [
                    {c.ID: "col-1", c.NAME: "id", c.REMOTE_TYPE: "I8"},
                    {c.ID: "col-2", c.NAME: "amount", c.REMOTE_TYPE: "R8"},
                    {c.ID: "col-3", c.NAME: None},
                ],
            },
            {c.ID: "vct-2", c.NAME: "unknown_table", c.COLUMNS: [{c.NAME: "x"}]},
        ],
    }

    with mock.patch.object(
        source,
        "get_connection_objects",
        side_effect=_connection_objects(
            {c.VIRTUAL_CONNECTIONS_CONNECTION: [virtual_connection]}
        ),
    ):
        work_units = list(source.vc_processor.emit_virtual_connections())

    orders_urn = make_dataset_urn_with_platform_instance(
        "tableau", "vc-1.orders", None, "PROD"
    )
    schemas = {
        s.fields[0].fieldPath: s
        for s in _snapshot_aspects(work_units, SchemaMetadataClass)
    }
    assert [f.fieldPath for f in schemas["id"].fields] == ["id", "amount"]
    assert source.report.num_datasource_field_skipped_no_name == 1

    lineages = _mcp_aspects(work_units, UpstreamLineageClass)
    # Only the table that matched a database table gets upstream lineage.
    assert [u.dataset for lineage in lineages for u in lineage.upstreams] == [
        ORDERS_URN
    ]
    assert lineages[0].fineGrainedLineages is not None
    column_edges = {
        (tuple(fgl.upstreams or []), tuple(fgl.downstreams or []))
        for fgl in lineages[0].fineGrainedLineages
    }
    assert column_edges == {
        (
            (make_schema_field_urn(ORDERS_URN, "ID"),),
            (make_schema_field_urn(orders_urn, "id"),),
        ),
        (
            (make_schema_field_urn(ORDERS_URN, "amount"),),
            (make_schema_field_urn(orders_urn, "amount"),),
        ),
    }


def test_upstream_table_without_a_connection_type_is_counted_not_fatal() -> None:
    source = _make_source()
    broken = {c.ID: "t-9", c.NAME: "orders", c.DATABASE: {c.NAME: "dvdrental"}}

    upstreams, id_to_urn = source.get_upstream_tables(
        [broken, ORDERS_TABLE], "DS", "default", is_custom_sql=False
    )

    assert [u.dataset for u in upstreams] == [ORDERS_URN]
    assert id_to_urn == {"t-1": ORDERS_URN}
    assert source.report.num_upstream_table_failed_generate_reference == 1
    assert [w.title for w in source.report.warnings] == [
        "Potentially Missing Lineage Issue"
    ]


def _vc_table(table_id: str, name: Optional[str]) -> dict:
    return {
        c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
        c.ID: table_id,
        c.NAME: name,
        c.VIRTUAL_CONNECTION: {c.ID: "vc-1"},
    }


def test_vc_references_survive_malformed_upstream_columns() -> None:
    source = _make_source()
    processor = source.vc_processor
    datasource = {
        c.ID: "ds-1",
        c.NAME: "Orders DS",
        c.FIELDS: [
            {c.NAME: "broken", c.UPSTREAM_COLUMNS: "not-a-list"},
            {
                c.NAME: "amount",
                c.UPSTREAM_COLUMNS: [
                    None,
                    {c.NAME: "AMOUNT", c.TABLE: "not-a-dict"},
                    {c.NAME: "AMOUNT", c.TABLE: _vc_table("", "orders")},
                    {c.NAME: "AMOUNT", c.TABLE: _vc_table("vct-1", "orders")},
                ],
            },
            # Tableau can omit the VC table name in field upstream paths.
            {
                c.NAME: "region",
                c.UPSTREAM_COLUMNS: [
                    {c.NAME: "REGION", c.TABLE: _vc_table("vct-2", None)}
                ],
            },
        ],
    }

    processor.process_datasource_for_vc_refs(datasource, DatasourceType.PUBLISHED)

    refs = processor.datasource_vc_relationships["ds-1"]
    assert [(r["field_name"], r["vc_table_id"], r["vc_table_name"]) for r in refs] == [
        ("amount", "vct-1", "orders"),
        ("region", "vct-2", None),
    ]
    assert processor.vc_table_ids_for_lookup == {"vct-1", "vct-2"}
    assert source.report.num_virtual_connections_upstream_columns_malformed == 1
    assert source.report.num_virtual_connections_tables_malformed == 1


def test_vc_lookup_fills_in_table_names_tableau_omitted() -> None:
    source = _make_source()
    processor = source.vc_processor
    processor.vc_table_ids_for_lookup = {"vct-2"}
    processor.datasource_vc_relationships["ds-1"] = [
        {
            "field_name": "region",
            "vc_table_id": "vct-2",
            "vc_table_name": None,
            "column_name": "REGION",
            "vc_id": None,
        }
    ]
    listing = {
        c.ID: "vc-1",
        c.NAME: "Sales VC",
        "tables": [
            "not-a-dict",
            {c.ID: "vct-0", c.NAME: None},
            {c.ID: "vct-2", c.NAME: "regions"},
            {c.ID: "vct-9", c.NAME: "unreferenced"},
        ],
    }

    with mock.patch.object(source, "get_connection_objects", return_value=[listing]):
        processor.lookup_vc_ids_from_table_ids()

    assert processor.vc_table_id_to_vc_id == {"vct-2": "vc-1"}
    assert processor.virtual_connection_ids_being_used == ["vc-1"]

    datasource_urn = make_dataset_urn_with_platform_instance(
        "tableau", "ds-1", None, "PROD"
    )
    result = processor.create_datasource_vc_lineage(datasource_urn)

    assert [fgl.upstreams for fgl in result.fine_grained_lineages] == [
        [
            make_schema_field_urn(
                make_dataset_urn_with_platform_instance(
                    "tableau", "vc-1.regions", None, "PROD"
                ),
                "REGION",
            )
        ]
    ]


def test_parenthesised_field_name_redirects_lineage_to_the_named_vc_table() -> None:
    source = _make_source()
    processor = source.vc_processor
    processor.datasource_vc_relationships["ds-1"] = [
        {
            "field_name": "amount (web.events)",
            "vc_table_id": "vct-1",
            "vc_table_name": "orders",
            "column_name": "AMOUNT",
            "vc_id": "vc-1",
        },
        {
            "field_name": "ts",
            "vc_table_id": "vct-2",
            "vc_table_name": "web.events",
            "column_name": "TS",
            "vc_id": "vc-2",
        },
    ]
    datasource_urn = make_dataset_urn_with_platform_instance(
        "tableau", "ds-1", None, "PROD"
    )

    result = processor.create_datasource_vc_lineage(datasource_urn)

    events_urn = make_dataset_urn_with_platform_instance(
        "tableau", "vc-2.web.events", None, "PROD"
    )
    amount, ts = result.fine_grained_lineages
    assert amount.upstreams == [make_schema_field_urn(events_urn, "AMOUNT")]
    assert amount.transformOperation == "Source: amount from web.events"
    assert ts.upstreams == [make_schema_field_urn(events_urn, "TS")]


def test_malformed_datasource_urn_is_counted_as_a_parsing_error() -> None:
    source = _make_source()

    result = source.vc_processor.create_datasource_vc_lineage("not-a-urn")

    assert result.fine_grained_lineages == []
    assert source.report.num_virtual_connections_lineage_parsing_errors == 1
