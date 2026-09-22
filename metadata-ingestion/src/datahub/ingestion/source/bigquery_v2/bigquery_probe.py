import itertools
from typing import Any, Dict

from datahub.ingestion.agent.sql_gate import INFORMATION_SCHEMA, CatalogScope
from datahub.ingestion.agent.sql_passthrough import (
    PROBE_QUERY_LABEL,
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

    # A named-relation allowlist rather than a schema-level allow of
    # information_schema, which is what the framework default gives and what every
    # other standard dialect can safely use.
    #
    # BigQuery is the exception because it extends INFORMATION_SCHEMA with JOBS,
    # whose `query` column holds the SQL text of every job in the project --
    # WHERE-clause literals, which are row values -- alongside user_email. A
    # schema-level allow permits it, and BigQuery's own lineage extractor reads it
    # (queries_extractor.py), so it is not hypothetical.
    #
    # Excluding JOBS by name would work today and rot tomorrow: the next
    # text-bearing view Google adds arrives permitted. Naming what is allowed keeps
    # the default deny. The list is what BigQuery ingestion itself reads, minus
    # JOBS, so a probe can reproduce anything ingestion does.
    catalog_scope = CatalogScope(
        schemas=frozenset(),
        relations=frozenset(
            f"{INFORMATION_SCHEMA}.{view}"
            for view in (
                "tables",
                "table_options",
                "table_constraints",
                "table_storage",
                "columns",
                "column_field_paths",
                "views",
                "schemata",
                "schemata_options",
                "partitions",
                "key_column_usage",
                "constraint_column_usage",
            )
        ),
    )

    # Both of these are real, server-side and enforced by BigQuery itself --
    # this is the only dialect here where that is true of the time ceiling as
    # well as the cost one. maximum_bytes_billed is the stronger of the two:
    # the job is refused before it runs rather than cancelled partway, so it
    # bounds spend rather than just duration.
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

        timeout = self.query_budget.timeout_seconds
        # A ceiling is passed only when there IS one. QueryJobConfig stringifies
        # whatever it is handed, so `maximum_bytes_billed=None` does not mean
        # "no ceiling": it stores the string 'None', to_api_repr() ships that to
        # BigQuery quite happily, and the getter then raises
        #   ValueError: invalid literal for int() with base 10: 'None'
        # Latent while both ceilings are declared on this class, but
        # QueryBudget.max_bytes_billed defaults to None, so a subclass or a
        # future provider that leaves it unset submits that string. Absent is
        # expressed by omitting the key.
        ceilings: Dict[str, Any] = {}
        if self.query_budget.max_bytes_billed is not None:
            ceilings["maximum_bytes_billed"] = self.query_budget.max_bytes_billed
        if timeout is not None:
            # Server-side, and this is the whole point of it. The budget
            # already declared timeout_seconds=30, but the only thing applying
            # it was `.result(timeout=...)` below -- which bounds how long the
            # CLIENT waits and neither cancels the job nor stops it billing.
            # A ceiling that reads as present and is not is exactly what
            # QueryBudget's docstring warns against, and it is the same defect
            # the Redshift ceiling had: the statement was issued, nothing
            # raised, and nothing was bounded. job_timeout_ms is BigQuery's
            # own cancel-the-job knob, so the declared 30s is now true.
            ceilings["job_timeout_ms"] = timeout * 1000
        job_config = QueryJobConfig(
            use_query_cache=True,
            # Labels are the strongest attribution of the three dialects that
            # offer any: they reach INFORMATION_SCHEMA.JOBS and the billing
            # export, so probe cost is separable from ingestion cost rather
            # than merely tellable apart in a log. BigQuery rejects a label
            # outside [a-z0-9_-], which is why PROBE_QUERY_LABEL is spelled
            # the way it is.
            labels={"application": PROBE_QUERY_LABEL},
            **ceilings,
        )
        # max_results caps what BigQuery pages back, so a broad catalog query does
        # not stream an entire result set to be thrown away. It does NOT cap the
        # bill -- BigQuery charges for bytes scanned whatever the page size -- which
        # is what job_config above is for.
        # Both ceilings, and they are not redundant: job_timeout_ms stops the
        # job, this stops us waiting on a call that is hung for some other
        # reason (a stalled fetch of an already-finished job's pages).
        iterator = self._client.query(query, job_config=job_config).result(
            max_results=limit, timeout=timeout
        )
        columns = [field.name for field in iterator.schema]
        return CatalogRows(
            columns=columns,
            rows=[
                [row[column] for column in columns]
                for row in itertools.islice(iterator, limit)
            ],
        )
