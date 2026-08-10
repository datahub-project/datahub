import itertools
from typing import Any

from datahub.ingestion.agent.sql_query import SqlRows


class BigQueryMetadataProbe:
    """Catalog-query surface for BigQuery (see agent.sql_query).

    BigQuery addresses catalog views as <dataset>.INFORMATION_SCHEMA.<VIEW>, so
    a query must name the dataset; sql_gate understands that shape.
    """

    sql_dialect = "bigquery"

    def __init__(self, client: Any) -> None:
        self._client = client

    def __enter__(self) -> "BigQueryMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    def execute_sql(self, query: str, limit: int) -> SqlRows:
        """Run an already-scope-checked query. NOT a @probe_method: annotating
        it would expose raw SQL through `probe run`, skipping the check."""
        # max_results caps what BigQuery pages back, so a broad catalog query
        # does not stream an entire result set to be thrown away.
        iterator = self._client.query(query).result(max_results=limit)
        columns = [field.name for field in iterator.schema]
        rows = [
            [row[column] for column in columns]
            for row in itertools.islice(iterator, limit)
        ]
        return SqlRows(columns=columns, rows=rows)
