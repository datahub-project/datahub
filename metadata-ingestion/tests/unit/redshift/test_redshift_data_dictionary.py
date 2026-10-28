"""RedshiftDataDictionary: turning raw cursor rows from the Redshift system
tables into the connector's typed records."""

from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.redshift.query import RedshiftServerlessQuery
from datahub.ingestion.source.redshift.redshift_schema import (
    InboundDatashare,
    PartialInboundDatashare,
    RedshiftColumn,
    RedshiftDatabase,
    RedshiftDataDictionary,
    RedshiftExtraTableMeta,
    RedshiftSchema,
)
from datahub.metadata.schema_classes import RecordTypeClass


def _cursor(
    columns: Sequence[str],
    rows: List[Tuple],
    paged: bool = True,
) -> MagicMock:
    cursor = MagicMock()
    cursor.description = [(c,) for c in columns]
    cursor.fetchall.return_value = rows
    cursor.fetchone.return_value = rows[0] if rows else None
    if paged:
        cursor.fetchmany.side_effect = [rows, []]
    return cursor


def _conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_get_query_result_binds_parameters_separately() -> None:
    cursor = MagicMock()
    conn = _conn(cursor)
    RedshiftDataDictionary.get_query_result(conn, "select %s", ("x",))
    executed_sql, params = cursor.execute.call_args[0]
    assert executed_sql.endswith("select %s")
    assert params == ("x",)


def test_serverless_dictionary_uses_serverless_queries() -> None:
    assert isinstance(
        RedshiftDataDictionary(is_serverless=True).queries, RedshiftServerlessQuery
    )


def test_get_databases_and_database_details() -> None:
    conn = _conn(_cursor(["datname"], [("dev",), ("prod",)]))
    assert RedshiftDataDictionary.get_databases(conn) == ["dev", "prod"]

    conn = _conn(_cursor(["name", "type", "options"], [("dev", "shared", "{}")]))
    db = RedshiftDataDictionary.get_database_details(conn, "dev")
    assert db == RedshiftDatabase(name="dev", type="shared", options="{}")

    conn = _conn(_cursor(["name", "type", "options"], []))
    assert RedshiftDataDictionary.get_database_details(conn, "dev") is None


def test_enrich_tables_is_keyed_by_schema_and_table() -> None:
    columns = [
        "database",
        "schema",
        "table",
        "size",
        "tbl_rows",
        "estimated_visible_rows",
        "skew_rows",
        "last_accessed",
        "is_materialized",
    ]
    rows = [
        ("dev", "public", "orders", 10, 100, 90, 1.0, None, False),
        ("dev", "public", "mv", 1, 5, 5, 1.0, None, True),
    ]
    dictionary = RedshiftDataDictionary(is_serverless=False)
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor(columns, rows)
    ):
        enriched = dictionary.enrich_tables(MagicMock())

    assert set(enriched["public"]) == {"orders", "mv"}
    assert enriched["public"]["orders"].size == 10
    assert enriched["public"]["mv"].is_materialized


def _table_row(
    name: str,
    tabletype: str,
    creation_time: Optional[datetime] = None,
    view_definition: Optional[str] = None,
) -> Tuple:
    return (
        "public",
        name,
        tabletype,
        creation_time,
        view_definition,
        "EVEN",
        None,
        None,
        None,
        None,
        None,
        None,
        "owner1",
    )


TABLE_COLUMNS = [
    "schema",
    "relname",
    "tabletype",
    "creation_time",
    "view_definition",
    "diststyle",
    "location",
    "parameters",
    "input_format",
    "output_format",
    "serde_parameters",
    "table_description",
    "owner_name",
]


def test_get_tables_and_views_merges_enrichment_stats() -> None:
    created = datetime(2024, 1, 1, 12, 0)
    accessed = datetime(2024, 1, 5, 12, 0)
    enriched = {
        "public": {
            "orders": RedshiftExtraTableMeta(
                database="dev",
                schema="public",
                table="orders",
                size=2,
                estimated_visible_rows=42,
                last_accessed=accessed,
            ),
            "mv": RedshiftExtraTableMeta(
                database="dev", schema="public", table="mv", is_materialized=True
            ),
        }
    }
    rows = [
        _table_row("orders", "TABLE", creation_time=created),
        _table_row("empty", "TABLE", creation_time=created),
        _table_row("ext", "EXTERNAL_TABLE"),
        _table_row("mv", "MATERIALIZED VIEW", view_definition="select 1"),
        _table_row("v", "VIEW", view_definition="select 2"),
    ]
    dictionary = RedshiftDataDictionary(is_serverless=False)
    with (
        patch.object(dictionary, "enrich_tables", return_value=enriched),
        patch.object(
            RedshiftDataDictionary,
            "get_query_result",
            return_value=_cursor(TABLE_COLUMNS, rows),
        ),
    ):
        tables, views = dictionary.get_tables_and_views(MagicMock(), "dev")

    by_name = {t.name: t for t in tables["public"]}
    assert by_name["orders"].size_in_bytes == 2 * 1024 * 1024
    assert by_name["orders"].rows_count == 42
    assert by_name["orders"].last_altered == accessed.replace(tzinfo=timezone.utc)
    assert by_name["orders"].created == created.replace(tzinfo=timezone.utc)
    # svv_table_info omits empty tables: report them as empty, not unknown.
    assert by_name["empty"].rows_count == 0
    assert by_name["empty"].size_in_bytes == 0
    # External tables are never in svv_table_info; leave stats unset.
    assert by_name["ext"].rows_count is None
    assert by_name["ext"].owner == "owner1"

    by_view = {v.name: v for v in views["public"]}
    assert by_view["mv"].materialized
    assert not by_view["v"].materialized
    assert by_view["v"].ddl == "select 2"


def test_get_tables_and_views_without_enrichment_leaves_stats_unset() -> None:
    rows = [_table_row("orders", "TABLE", creation_time=datetime(2024, 1, 1))]
    dictionary = RedshiftDataDictionary(is_serverless=False)
    with (
        patch.object(dictionary, "enrich_tables") as enrich,
        patch.object(
            RedshiftDataDictionary,
            "get_query_result",
            return_value=_cursor(TABLE_COLUMNS, rows),
        ),
    ):
        tables, _ = dictionary.get_tables_and_views(MagicMock(), "dev", enrich=False)

    enrich.assert_not_called()
    assert tables["public"][0].rows_count is None
    assert tables["public"][0].last_altered is None


def test_get_table_stats_falls_back_to_creation_time_for_last_altered() -> None:
    created = datetime(2024, 1, 1, 12, 0)
    enriched = {
        "public": {
            "orders": RedshiftExtraTableMeta(
                database="dev", schema="public", table="orders", last_accessed=None
            )
        }
    }
    row = _table_row("orders", "TABLE", creation_time=created)
    creation_time, last_altered, rows_count, size = (
        RedshiftDataDictionary.get_table_stats(enriched, TABLE_COLUMNS, "public", row)
    )
    assert creation_time == last_altered == created.replace(tzinfo=timezone.utc)
    assert rows_count is None
    assert size is None


def test_get_columns_for_schema_groups_columns_by_table() -> None:
    columns = [
        "table_name",
        "name",
        "attnum",
        "type",
        "comment",
        "notnull",
        "default",
        "distkey",
        "sortkey",
        "encode",
    ]
    rows = [
        ("orders", "id", 1, "integer", None, True, None, True, False, "az64"),
        ("orders", "note", 2, "varchar", "c", False, "''", False, False, "lzo"),
        ("users", "id", 1, "integer", None, True, None, False, True, "az64"),
    ]
    schema = RedshiftSchema(name="public", database="dev", type="local")
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor(columns, rows)
    ):
        result = RedshiftDataDictionary.get_columns_for_schema(
            MagicMock(), "dev", schema
        )

    assert set(result) == {"orders", "users"}
    note = result["orders"][1]
    assert note.data_type == "VARCHAR"
    assert note.is_nullable
    assert note.default == "''"
    assert result["orders"][0].dist_key
    assert result["users"][0].sort_key


def test_get_schema_fields_for_column_expands_struct() -> None:
    column = RedshiftColumn(
        name="address",
        ordinal_position=1,
        data_type="STRUCT<CITY:VARCHAR,ZIP:INTEGER>",
        comment=None,
        is_nullable=True,
    )
    fields = RedshiftDataDictionary.get_schema_fields_for_column(column)
    assert isinstance(fields[0].type.type, RecordTypeClass)
    assert {f.fieldPath.split(".")[-1] for f in fields[1:]} == {"city", "zip"}


def test_get_lineage_rows_unescapes_ddl_and_tolerates_missing_columns() -> None:
    columns = ["target_schema", "target_table", "ddl", "session_id"]
    rows = [("public", "t", "insert into t\\nselect 1", 123)]
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor(columns, rows)
    ):
        lineage_rows = list(
            RedshiftDataDictionary.get_lineage_rows(MagicMock(), "select ...")
        )

    assert len(lineage_rows) == 1
    row = lineage_rows[0]
    assert row.ddl == "insert into t\nselect 1"
    assert row.session_id == "123"
    assert row.source_schema is None
    assert row.filename is None
    assert row.timestamp is None


def test_temporary_and_alter_rows_skip_entries_without_session() -> None:
    columns = [
        "transaction_id",
        "session_id",
        "query_text",
        "create_command",
        "start_time",
    ]
    now = datetime.now()
    rows = [
        (1, None, "create table #a as select 1", "create table #a", now),
        (2, 7, "create table #b\\nas select 1", "create table #b", now),
    ]
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor(columns, rows)
    ):
        temp_rows = list(RedshiftDataDictionary.get_temporary_rows(MagicMock(), "q"))
    assert [r.session_id for r in temp_rows] == ["7"]
    assert temp_rows[0].query_text == "create table #b\nas select 1"

    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor(columns, rows)
    ):
        alter_rows = list(
            RedshiftDataDictionary.get_alter_table_commands(MagicMock(), "q")
        )
    assert [r.transaction_id for r in alter_rows] == [2]


def test_datashare_lookups() -> None:
    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        return_value=_cursor(
            ["share_type", "share_name", "producer_namespace", "source_database"],
            [("OUTBOUND", "s1", "ns1", "dev")],
        ),
    ):
        outbound = list(RedshiftDataDictionary.get_outbound_datashares(MagicMock()))
    assert [
        (s.share_name, s.producer_namespace, s.source_database) for s in outbound
    ] == [("s1", "ns1", "dev")]

    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        return_value=_cursor(
            ["share_type", "share_name", "producer_namespace", "consumer_database"],
            [("INBOUND", "s2", "ns2", "dev")],
        ),
    ):
        inbound = RedshiftDataDictionary.get_inbound_datashare(MagicMock(), "dev")
    assert inbound == InboundDatashare(
        share_name="s2", producer_namespace="ns2", consumer_database="dev"
    )

    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        return_value=_cursor(["share_type"], []),
    ):
        assert RedshiftDataDictionary.get_inbound_datashare(MagicMock(), "dev") is None


def test_inbound_share_parsing_handles_truncated_options() -> None:
    # The options column is varchar(128), so the namespace may be cut off or
    # missing entirely; we still want the share name.
    full = RedshiftDatabase(
        name="dev",
        type="shared",
        options='{"datashare_name":"s","datashare_producer_account":"1","datashare_producer_namespace":"abc-def"}',
    )
    assert full.get_inbound_share() == InboundDatashare(
        share_name="s", producer_namespace="abc-def", consumer_database="dev"
    )

    truncated = RedshiftDatabase(
        name="dev",
        type="shared",
        options='{"datashare_name":"s","datashare_producer_namespace":"abc-',
    )
    assert truncated.get_inbound_share() == PartialInboundDatashare(
        share_name="s", producer_namespace_prefix="abc-", consumer_database="dev"
    )

    name_only = RedshiftDatabase(
        name="dev", type="shared", options='{"datashare_name":"s","datashare_prod'
    )
    assert name_only.get_inbound_share() == PartialInboundDatashare(
        share_name="s", producer_namespace_prefix="", consumer_database="dev"
    )
    assert (
        PartialInboundDatashare("s", "abc-", "dev").get_description()
        == "Namespace Prefix abc- Share s"
    )

    assert (
        RedshiftDatabase(name="dev", type="shared", options="{}").get_inbound_share()
        is None
    )
    assert (
        RedshiftDatabase(name="dev", type="local", options="{}").get_inbound_share()
        is None
    )


def test_external_schema_without_schema_option_has_no_upstream_schema() -> None:
    schema = RedshiftSchema(
        name="ext",
        database="dev",
        type="external",
        option='{"IAM_ROLE":"arn:aws:iam::1:role/x"}',
        external_platform="redshift",
    )
    assert schema.get_upstream_schema_name() is None
