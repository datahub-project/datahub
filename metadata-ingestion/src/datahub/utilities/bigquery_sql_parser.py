from typing import List

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigQueryTableRef
from datahub.utilities.sqlglot_lineage import SchemaResolver, sqlglot_lineage


class BigQuerySQLParser:
    def __init__(self, sql_query: str, schema_resolver: SchemaResolver) -> None:
        self.result = sqlglot_lineage(sql_query, schema_resolver)

    def get_tables(self) -> List[str]:
        ans = []
        for urn in self.result.in_tables:
            table_ref = BigQueryTableRef.from_urn(urn)
            ans.append(str(table_ref.table_identifier))
        return ans

    def get_columns(self) -> List[str]:
        ans = []
        for col_info in self.result.column_lineage or []:
            for col_ref in col_info.upstreams:
                ans.append(col_ref.column)
        return ans
