from typing import Dict, Set

from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult, Urn


def transform_parsing_result_to_in_tables_schemas(
    parsing_result: SqlParsingResult,
) -> Dict[Urn, Set[str]]:
    table_urn_to_schema_map: Dict[str, Set[str]] = (
        {it: set() for it in parsing_result.in_tables}
        if parsing_result.in_tables
        else {}
    )

    if parsing_result.column_lineage:
        for cli in parsing_result.column_lineage:
            for upstream in cli.upstreams:
                if upstream.table in table_urn_to_schema_map:
                    table_urn_to_schema_map[upstream.table].add(upstream.column)
                else:
                    table_urn_to_schema_map[upstream.table] = {upstream.column}

    return table_urn_to_schema_map
