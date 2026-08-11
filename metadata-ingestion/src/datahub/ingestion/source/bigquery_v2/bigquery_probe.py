import itertools
from typing import Any

from datahub.ingestion.agent.sql_passthrough import CatalogRows, SqlCatalogPassthrough
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)


class BigQueryMetadataProbe(SqlCatalogPassthrough):
    """Catalog-query surface for BigQuery.

    BigQuery addresses catalog views as <dataset>.INFORMATION_SCHEMA.<VIEW>, so a
    query must name the dataset; sql_gate understands that shape.
    """

    sql_dialect = "bigquery"

    def __init__(self, client: Any) -> None:
        self._client = client

    @classmethod
    def for_config(cls, config: BigQueryConnectionConfig) -> "BigQueryMetadataProbe":
        """Reuse the connector's own client builder rather than a second SQLAlchemy
        engine, so credentials resolve the way ingestion resolves them."""
        return cls(config.get_bigquery_client())

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        # max_results caps what BigQuery pages back, so a broad catalog query does
        # not stream an entire result set to be thrown away.
        iterator = self._client.query(query).result(max_results=limit)
        columns = [field.name for field in iterator.schema]
        return CatalogRows(
            columns=columns,
            rows=[
                [row[column] for column in columns]
                for row in itertools.islice(iterator, limit)
            ],
        )
