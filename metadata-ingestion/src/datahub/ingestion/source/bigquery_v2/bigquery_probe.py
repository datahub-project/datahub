import itertools
from typing import Any

from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    QueryBudget,
    SqlCatalogPassthrough,
)
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)

# One GiB scanned. BigQuery bills by bytes read regardless of how few rows come
# back, so max_results caps the page and this caps the bill -- generous for the
# INFORMATION_SCHEMA reads a probe is for, and a hard stop on anything that
# wanders into a full table scan.
_MAX_BYTES_BILLED = 1024**3


class BigQueryMetadataProbe(SqlCatalogPassthrough):
    """Catalog-query surface for BigQuery.

    BigQuery addresses catalog views as <dataset>.INFORMATION_SCHEMA.<VIEW>, so a
    query must name the dataset; sql_gate understands that shape.
    """

    sql_dialect = "bigquery"

    # maximum_bytes_billed is the strongest ceiling any dialect here offers: the
    # job is refused before it runs rather than cancelled partway, so it bounds
    # spend rather than just how long we wait for it.
    query_budget = QueryBudget(timeout_seconds=30, max_bytes_billed=_MAX_BYTES_BILLED)

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
        # lazy: the bigquery client library is only needed once a probe runs
        from google.cloud.bigquery import QueryJobConfig

        job_config = QueryJobConfig(
            maximum_bytes_billed=self.query_budget.max_bytes_billed,
            use_query_cache=True,
        )
        # max_results caps what BigQuery pages back, so a broad catalog query does
        # not stream an entire result set to be thrown away. It does NOT cap the
        # bill -- BigQuery charges for bytes scanned whatever the page size -- which
        # is what job_config above is for.
        iterator = self._client.query(query, job_config=job_config).result(
            max_results=limit, timeout=self.query_budget.timeout_seconds
        )
        columns = [field.name for field in iterator.schema]
        return CatalogRows(
            columns=columns,
            rows=[
                [row[column] for column in columns]
                for row in itertools.islice(iterator, limit)
            ],
        )
