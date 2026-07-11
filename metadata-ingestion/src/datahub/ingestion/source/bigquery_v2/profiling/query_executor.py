import logging
from typing import List, Optional

from google.cloud.bigquery import QueryJobConfig, Row

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    validate_sql_structure,
)

logger = logging.getLogger(__name__)


class QueryExecutor:
    def __init__(self, config: BigQueryV2Config):
        self.config = config

    def _validate_query_security(self, query: str) -> None:
        validate_sql_structure(query)

        # Extra guard against comment-based injection that slips past structural checks.
        dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
        for pattern in dangerous_patterns:
            if pattern in query:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

    def execute_query(self, query: str, context: str = "") -> List[Row]:
        self._validate_query_security(query)

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            logger.debug(
                f"Executing query{f' for {context}' if context else ''} with {timeout}s timeout"
            )

            job_config = QueryJobConfig(
                job_timeout_ms=timeout * 1000,
                use_query_cache=False,
            )

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            results = list(query_job.result())
            logger.debug(
                f"Query returned {len(results)} row(s){f' for {context}' if context else ''}"
            )
            return results
        except Exception as e:
            logger.warning(
                f"Query execution error{f' in {context}' if context else ''}: {e}"
            )
            raise

    def execute_query_with_config(
        self, query: str, job_config: QueryJobConfig, context: str = ""
    ) -> List[Row]:
        self._validate_query_security(query)

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            logger.debug(
                f"Executing query{f' for {context}' if context else ''} with {timeout}s timeout and custom config"
            )

            job_config.job_timeout_ms = timeout * 1000
            job_config.use_query_cache = False

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            results = list(query_job.result())
            logger.debug(
                f"Query returned {len(results)} row(s){f' for {context}' if context else ''}"
            )
            return results
        except Exception as e:
            logger.warning(
                f"Query execution error{f' in {context}' if context else ''}: {e}"
            )
            raise

    def execute_query_safely(
        self, query: str, job_config: Optional[QueryJobConfig] = None, context: str = ""
    ) -> List[Row]:
        # Must re-raise, not swallow: the partition-detection probe relies on the exception.
        logger.debug(f"Executing query{f' for {context}' if context else ''}: {query}")

        if job_config:
            return self.execute_query_with_config(query, job_config, context)
        else:
            return self.execute_query(query, context)

    def build_safe_custom_sql(
        self,
        project: str,
        schema: str,
        table: str,
        where_clause: str = "",
        limit: Optional[int] = None,
    ) -> str:
        from datahub.ingestion.source.bigquery_v2.profiling.security import (
            build_safe_table_reference,
        )

        safe_table_ref = build_safe_table_reference(project, schema, table)

        query_parts = ["SELECT *", f"FROM {safe_table_ref}"]

        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        if limit is not None and limit > 0:
            safe_limit = max(1, min(int(limit), 10_000_000))
            query_parts.append(f"LIMIT {safe_limit}")

        return " ".join(query_parts)

    def get_effective_timeout(self) -> int:
        return self.config.profiling.partition_fetch_timeout
