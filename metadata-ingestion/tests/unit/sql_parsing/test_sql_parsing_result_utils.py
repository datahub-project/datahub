from datahub.sql_parsing.sql_parsing_result_utils import (
    transform_parsing_result_to_in_tables_schemas,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
)


def test_transform_parsing_result_to_in_tables_schemas__empty_parsing_result():
    parsing_result = SqlParsingResult(in_tables=[], out_tables=[], column_lineage=None)

    in_tables_schema = transform_parsing_result_to_in_tables_schemas(parsing_result)
    assert not in_tables_schema


def test_transform_parsing_result_to_in_tables_schemas__in_tables_only():
    parsing_result = SqlParsingResult(
        in_tables=["table_urn1", "table_urn2", "table_urn3"],
        out_tables=[],
        column_lineage=None,
    )

    in_tables_schema = transform_parsing_result_to_in_tables_schemas(parsing_result)
    assert in_tables_schema == {
        "table_urn1": set(),
        "table_urn2": set(),
        "table_urn3": set(),
    }


def test_transform_parsing_result_to_in_tables_schemas__in_tables_and_column_linage():
    parsing_result = SqlParsingResult(
        in_tables=["table_urn1", "table_urn2", "table_urn3"],
        out_tables=[],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="out_col1"),
                upstreams=[
                    ColumnRef(table="table_urn1", column="col11"),
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="out_col2"),
                upstreams=[
                    ColumnRef(table="table_urn2", column="col21"),
                    ColumnRef(table="table_urn2", column="col22"),
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="out_col3"),
                upstreams=[
                    ColumnRef(table="table_urn1", column="col12"),
                    ColumnRef(table="table_urn2", column="col23"),
                ],
            ),
        ],
    )

    in_tables_schema = transform_parsing_result_to_in_tables_schemas(parsing_result)
    assert in_tables_schema == {
        "table_urn1": {"col11", "col12"},
        "table_urn2": {"col21", "col22", "col23"},
        "table_urn3": set(),
    }
