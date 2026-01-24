"""BigQuery query execution with security validation and error handling."""

import logging
from typing import List, Optional

import google.cloud.bigquery
from google.cloud.bigquery import QueryJobConfig, Row

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    validate_sql_structure,
)

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Handles secure execution of BigQuery queries for profiling operations.

    This class provides a centralized way to execute queries with proper
    security validation, timeout handling, and error management.
    """

    def __init__(self, config: BigQueryV2Config):
        self.config = config

    def _validate_query_security(self, query: str) -> None:
        validate_sql_structure(query)

        dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
        for pattern in dangerous_patterns:
            if pattern in query:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

    def execute_query(self, query: str, context: str = "") -> List[Row]:
        """Execute BigQuery query with timeout and security validation."""
        self._validate_query_security(query)

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            context_info = f" for {context}" if context else ""
            logger.debug(f"Executing query{context_info} with {timeout}s timeout")

            job_config = QueryJobConfig(
                job_timeout_ms=timeout * 1000,
                use_query_cache=False,
            )

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            return list(query_job.result())
        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.warning(f"Query execution error{context_info}: {e}")
            raise

    def execute_query_with_config(
        self, query: str, job_config: QueryJobConfig, context: str = ""
    ) -> List[Row]:
        """Execute query with custom job configuration and parameters."""
        self._validate_query_security(query)

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            context_info = f" for {context}" if context else ""
            logger.debug(
                f"Executing query{context_info} with {timeout}s timeout and custom config"
            )

            job_config.job_timeout_ms = timeout * 1000
            job_config.use_query_cache = False

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            return list(query_job.result())
        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.warning(f"Query execution error{context_info}: {e}")
            raise

    def execute_query_safely(
        self, query: str, job_config: Optional[QueryJobConfig] = None, context: str = ""
    ) -> List[Row]:
        """Execute query with unified error handling."""
        try:
            context_info = f" for {context}" if context else ""
            logger.debug(f"Executing query{context_info}: {query}")

            if job_config:
                return self.execute_query_with_config(query, job_config, context)
            else:
                return self.execute_query(query, context)
        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.warning(f"Query execution failed{context_info}: {e}")
            raise

    def test_query_execution(self, query: str, context: str = "") -> bool:
        """Test query execution without running it."""
        try:
            self._validate_query_security(query)

            job_config = QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            context_info = f" for {context}" if context else ""
            logger.debug(f"Testing query{context_info} with dry run")

            self.config.get_bigquery_client().query(query, job_config=job_config)

            logger.debug(f"Query test successful{context_info}")
            return True

        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.debug(f"Query test failed{context_info}: {e}")
            return False

    def get_query_cost_estimate(self, query: str, context: str = "") -> Optional[int]:
        """Estimate bytes to be processed by query."""
        try:
            self._validate_query_security(query)

            job_config = QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            context_info = f" for {context}" if context else ""
            logger.debug(f"Getting cost estimate{context_info}")

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )

            bytes_processed = query_job.total_bytes_processed
            if bytes_processed:
                logger.debug(
                    f"Query would process {bytes_processed} bytes{context_info}"
                )
                return bytes_processed
            else:
                logger.debug(f"Could not determine bytes to process{context_info}")
                return None

        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.debug(f"Cost estimation failed{context_info}: {e}")
            return None

    def execute_with_retry(
        self,
        query: str,
        job_config: Optional[QueryJobConfig] = None,
        context: str = "",
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> List[Row]:
        """
        Execute a query with retry logic for transient failures.

        Args:
            query: SQL query to execute
            job_config: Optional query job configuration
            context: Optional context for logging
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            Query results as list

        Raises:
            Exception: If all retry attempts fail
        """
        import time

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    context_info = f" in {context}" if context else ""
                    logger.debug(f"Retry attempt {attempt}/{max_retries}{context_info}")
                    time.sleep(retry_delay * attempt)  # Exponential backoff

                return self.execute_query_safely(query, job_config, context)

            except Exception as e:
                last_exception = e

                if not self._is_retryable_error(e):
                    logger.debug(f"Non-retryable error, not retrying: {e}")
                    raise

                if attempt < max_retries:
                    context_info = f" in {context}" if context else ""
                    logger.debug(f"Retryable error{context_info}, will retry: {e}")
                else:
                    context_info = f" in {context}" if context else ""
                    logger.warning(f"All retry attempts failed{context_info}: {e}")

        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"All retry attempts failed{context_info}")

    def _is_retryable_error(self, error: Exception) -> bool:
        error_str = str(error).lower()

        retryable_patterns = [
            "timeout",
            "deadline exceeded",
            "internal error",
            "service unavailable",
            "temporary failure",
            "rate limit",
            "quota exceeded",
            "connection reset",
            "connection refused",
        ]

        return any(pattern in error_str for pattern in retryable_patterns)

    def build_safe_custom_sql(
        self,
        project: str,
        schema: str,
        table: str,
        where_clause: str = "",
        limit: Optional[int] = None,
    ) -> str:
        """
        Build a safe custom SQL query for profiling operations.

        This method constructs SQL queries using validated identifiers
        and optional WHERE clauses and LIMIT clauses.

        Args:
            project: BigQuery project ID (will be validated)
            schema: Dataset name (will be validated)
            table: Table name (will be validated)
            where_clause: Optional WHERE clause (should be pre-validated)
            limit: Optional row limit

        Returns:
            Safe SQL query string

        Raises:
            ValueError: If identifiers are invalid
        """
        from datahub.ingestion.source.bigquery_v2.profiling.security import (
            build_safe_table_reference,
        )

        safe_table_ref = build_safe_table_reference(project, schema, table)

        query_parts = ["SELECT *", f"FROM {safe_table_ref}"]

        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        if limit is not None and limit > 0:
            safe_limit = max(1, min(int(limit), 10_000_000))  # Cap at 10M rows
            query_parts.append(f"LIMIT {safe_limit}")

        return " ".join(query_parts)

    def log_query_stats(
        self, query_job: google.cloud.bigquery.QueryJob, context: str = ""
    ) -> None:
        """
        Log statistics about a completed query job.

        Args:
            query_job: Completed BigQuery job
            context: Optional context for logging
        """
        try:
            context_info = f" for {context}" if context else ""

            if (
                hasattr(query_job, "total_bytes_processed")
                and query_job.total_bytes_processed
            ):
                logger.debug(
                    f"Query processed {query_job.total_bytes_processed} bytes{context_info}"
                )

            if (
                hasattr(query_job, "total_bytes_billed")
                and query_job.total_bytes_billed
            ):
                logger.debug(
                    f"Query billed for {query_job.total_bytes_billed} bytes{context_info}"
                )

            if hasattr(query_job, "slot_millis") and query_job.slot_millis:
                logger.debug(
                    f"Query used {query_job.slot_millis} slot-milliseconds{context_info}"
                )

        except Exception as e:
            logger.debug(f"Could not log query stats: {e}")

    def get_effective_timeout(self) -> int:
        """Get effective timeout for query operations in seconds."""
        return self.config.profiling.partition_fetch_timeout

    def is_query_too_expensive(
        self, query: str, max_bytes: int = 1_000_000_000
    ) -> bool:
        try:
            estimated_bytes = self.get_query_cost_estimate(query)
            if estimated_bytes is None:
                return True

            return estimated_bytes > max_bytes

        except Exception:
            return True
