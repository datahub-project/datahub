"""Virtual connection tables resolved against Tableau's databaseTables listing:
the multi-key lookup, name matching strategies, URN creation and the column
level lineage emitted for VC tables. Also the malformed-payload handling in VC
reference extraction and lookup.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional
from unittest import mock

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import TableauSiteSource
from datahub.ingestion.source.tableau.tableau_virtual_connections import (
    DatasourceType,
)
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    UpstreamLineageClass,
)
from tests.unit.tableau.conftest import (
    ORDERS_URN,
    SiteSourceFactory,
    mcp_aspects,
    snapshot_aspects,
    tableau_urn,
)

# fullName is three-part here on purpose: a two-part fullName trips the doubled
# schema bug pinned by test_vc_database_table_urn_from_a_two_part_full_name, and
# the matching tests below are about name lookup, not URN building.
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


def _vc_source(site_source: SiteSourceFactory) -> TableauSiteSource:
    return site_source(ingest_virtual_connections=True)


def _connection_objects(
    by_connection_type: Dict[str, List[dict]],
) -> Any:
    def get(**kwargs: Any) -> Iterable[dict]:
        return list(by_connection_type.get(kwargs["connection_type"], []))

    return get


def _source_with_lookup(site_source: SiteSourceFactory) -> TableauSiteSource:
    """A VC-enabled source whose databaseTables lookup has been built from
    ORDERS_TABLE, EVENTS_TABLE and the unusable rows."""
    source = _vc_source(site_source)
    with mock.patch.object(
        source,
        "get_connection_objects",
        autospec=True,
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


def test_vc_table_names_resolve_to_database_tables_by_several_strategies(
    site_source: SiteSourceFactory,
) -> None:
    """VC table names arrive in several shapes (bare, bracketed, Tableau's
    "TABLE (schema.table)" label, fully qualified, foreign prefix); each must
    land on the same databaseTables row."""
    source = _source_with_lookup(site_source)

    def match(vc_table_name: str) -> Optional[str]:
        table = source._find_matching_database_table(vc_table_name)
        return table[c.ID] if table else None

    assert match("orders") == "t-1"
    assert match("[public].[orders]") == "t-1"
    assert match("ORDERS (public.orders)") == "t-1"
    assert match("dvdrental.public.orders") == "t-1"
    # A prefix Tableau never listed still matches on the bare table name.
    assert match("sales.orders") == "t-1"
    # schema.table matches the fully qualified name even when schema was null.
    assert match("web.events") == "t-2"
    assert match("customers") is None
    # Rows without a name or a connection type cannot be matched.
    assert match("no_connection") is None


def test_lookup_is_inert_until_built(site_source: SiteSourceFactory) -> None:
    source = _vc_source(site_source)

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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "_create_database_table_urn hands fullName to get_fully_qualified_table_name "
        "as the table name and the schema separately, so a two-part fullName such "
        "as [public].[orders] builds public.public.orders for three-tier platforms"
    ),
)
def test_vc_database_table_urn_from_a_two_part_full_name(
    site_source: SiteSourceFactory,
) -> None:
    source = _vc_source(site_source)

    assert (
        source._create_database_table_urn(
            {**ORDERS_TABLE, c.FULL_NAME: "[public].[orders]"}
        )
        == ORDERS_URN
    )


def test_vc_table_lineage_maps_columns_case_insensitively(
    site_source: SiteSourceFactory,
) -> None:
    """Only the VC table that matched a databaseTables row gets upstream lineage;
    its columns are matched to the database columns ignoring case, and nameless
    columns are counted and skipped."""
    source = _source_with_lookup(site_source)
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
        autospec=True,
        side_effect=_connection_objects(
            {c.VIRTUAL_CONNECTIONS_CONNECTION: [virtual_connection]}
        ),
    ):
        work_units = list(source.vc_processor.emit_virtual_connections())

    orders_urn = tableau_urn("vc-1.orders")
    schemas = {
        s.fields[0].fieldPath: s
        for s in snapshot_aspects(work_units, SchemaMetadataClass)
    }
    assert [f.fieldPath for f in schemas["id"].fields] == ["id", "amount"]
    assert source.report.num_datasource_field_skipped_no_name == 1

    lineages = mcp_aspects(work_units, UpstreamLineageClass)
    assert [u.dataset for lineage in lineages for u in lineage.upstreams] == [
        ORDERS_URN
    ]
    assert lineages[0].fineGrainedLineages is not None
    assert [
        (fgl.upstreams, fgl.downstreams) for fgl in lineages[0].fineGrainedLineages
    ] == [
        (
            [make_schema_field_urn(ORDERS_URN, "ID")],
            [make_schema_field_urn(orders_urn, "id")],
        ),
        (
            [make_schema_field_urn(ORDERS_URN, "amount")],
            [make_schema_field_urn(orders_urn, "amount")],
        ),
    ]


def test_upstream_table_without_a_connection_type_is_counted_not_fatal(
    site_source: SiteSourceFactory,
) -> None:
    source = _vc_source(site_source)
    broken = {c.ID: "t-9", c.NAME: "orders", c.DATABASE: {c.NAME: "dvdrental"}}

    upstreams, id_to_urn = source.get_upstream_tables(
        [broken, ORDERS_TABLE], "DS", "default", is_custom_sql=False
    )

    assert [u.dataset for u in upstreams] == [ORDERS_URN]
    assert id_to_urn == {"t-1": ORDERS_URN}
    assert source.report.num_upstream_table_failed_generate_reference == 1
    assert "Potentially Missing Lineage Issue" in {
        w.title for w in source.report.warnings
    }


def _vc_table(table_id: str, name: Optional[str]) -> dict:
    return {
        c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
        c.ID: table_id,
        c.NAME: name,
        c.VIRTUAL_CONNECTION: {c.ID: "vc-1"},
    }


def test_vc_references_survive_malformed_upstream_columns(
    site_source: SiteSourceFactory,
) -> None:
    """Null entries, non-dict tables, non-list upstreamColumns and blank table
    ids are counted and skipped without losing the well-formed references in
    the same datasource; a missing VC table name is kept for later lookup."""
    source = _vc_source(site_source)
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


def test_vc_lookup_fills_in_table_names_tableau_omitted(
    site_source: SiteSourceFactory,
) -> None:
    """The VC listing maps referenced table ids to their VC and supplies the
    table name when the datasource's field upstream path left it out; junk
    and unreferenced entries in the listing are ignored."""
    source = _vc_source(site_source)
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

    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[listing]
    ):
        processor.lookup_vc_ids_from_table_ids()

    assert processor.vc_table_id_to_vc_id == {"vct-2": "vc-1"}
    assert processor.virtual_connection_ids_being_used == ["vc-1"]

    result = processor.create_datasource_vc_lineage(tableau_urn("ds-1"))

    assert [fgl.upstreams for fgl in result.fine_grained_lineages] == [
        [make_schema_field_urn(tableau_urn("vc-1.regions"), "REGION")]
    ]


def test_parenthesised_field_name_redirects_lineage_to_the_named_vc_table(
    site_source: SiteSourceFactory,
) -> None:
    """A field named "column (table)" points at that table rather than the
    table its upstream path listed, provided another reference knows the
    table's VC."""
    source = _vc_source(site_source)
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

    result = processor.create_datasource_vc_lineage(tableau_urn("ds-1"))

    events_urn = tableau_urn("vc-2.web.events")
    amount, ts = result.fine_grained_lineages
    assert amount.upstreams == [make_schema_field_urn(events_urn, "AMOUNT")]
    assert amount.transformOperation == "Source: amount from web.events"
    assert ts.upstreams == [make_schema_field_urn(events_urn, "TS")]


def test_malformed_datasource_urn_is_counted_as_a_parsing_error(
    site_source: SiteSourceFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A URN Tableau data cannot have produced is a data problem: it is counted
    and logged as such, not as an unexpected code bug with a stack trace."""
    source = _vc_source(site_source)

    with caplog.at_level(logging.ERROR):
        result = source.vc_processor.create_datasource_vc_lineage("not-a-urn")

    assert result.fine_grained_lineages == []
    assert source.report.num_virtual_connections_lineage_parsing_errors == 1
    assert caplog.records and all(r.exc_info is None for r in caplog.records)
