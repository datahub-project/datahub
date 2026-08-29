import sqlglot

from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    _ColumnLineageInfo,
    _ColumnRef,
    _DownstreamColumnRef,
    _translate_internal_column_lineage,
)

UPSTREAM_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.upstream,PROD)"
DOWNSTREAM_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.downstream,PROD)"
)


def test_upstream_schema_field_urns_skips_empty_columns() -> None:
    cll_info = ColumnLineageInfo(
        downstream=DownstreamColumnRef(table=DOWNSTREAM_URN, column="my_col"),
        upstreams=[
            ColumnRef(table=UPSTREAM_URN, column=""),
            ColumnRef(table=UPSTREAM_URN, column="resolved_col"),
        ],
    )

    assert cll_info.upstream_schema_field_urns() == [
        f"urn:li:schemaField:({UPSTREAM_URN},resolved_col)"
    ]


def test_downstream_schema_field_urn_none_when_column_empty() -> None:
    cll_info = ColumnLineageInfo(
        downstream=DownstreamColumnRef(table=DOWNSTREAM_URN, column=""),
        upstreams=[ColumnRef(table=UPSTREAM_URN, column="col")],
    )

    assert cll_info.downstream_schema_field_urn() is None


def test_downstream_schema_field_urn_built_when_present() -> None:
    cll_info = ColumnLineageInfo(
        downstream=DownstreamColumnRef(table=DOWNSTREAM_URN, column="my_col"),
        upstreams=[],
    )

    assert (
        cll_info.downstream_schema_field_urn()
        == f"urn:li:schemaField:({DOWNSTREAM_URN},my_col)"
    )


def test_translate_internal_column_lineage_drops_unresolved_upstream_column() -> None:
    upstream_table = _TableName(database="db", db_schema="schema", table="upstream")
    downstream_table = _TableName(database="db", db_schema="schema", table="downstream")
    table_name_urn_mapping = {
        upstream_table: UPSTREAM_URN,
        downstream_table: DOWNSTREAM_URN,
    }

    raw_column_lineage = _ColumnLineageInfo(
        downstream=_DownstreamColumnRef(table=downstream_table, column="my_col"),
        upstreams=[
            _ColumnRef(table=upstream_table, column=""),
            _ColumnRef(table=upstream_table, column="resolved_col"),
        ],
    )

    result = _translate_internal_column_lineage(
        table_name_urn_mapping, raw_column_lineage, dialect=sqlglot.Dialect()
    )

    assert result.upstream_schema_field_urns() == [
        f"urn:li:schemaField:({UPSTREAM_URN},resolved_col)"
    ]
