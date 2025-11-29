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
        """
        Initialize the query executor.

        Args:
            config: BigQuery configuration containing client and profiling settings
        """
        self.config = config

    def execute_query(self, query: str, context: str = "") -> List[Row]:
        """
        Execute a BigQuery query with timeout from configuration and enhanced security validation.

        Args:
            query: SQL query to execute
            context: Optional context for logging and error reporting

        Returns:
            List of row results

        Raises:
            ValueError: If query contains dangerous patterns
            Exception: For query execution errors
        """
        # Enhanced security validation
        validate_sql_structure(query)

        # Additional basic pattern check for immediate threats
        dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
        for pattern in dangerous_patterns:
            if pattern in query:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            context_info = f" for {context}" if context else ""
            logger.debug(f"Executing query{context_info} with {timeout}s timeout")

            job_config = QueryJobConfig(
                job_timeout_ms=timeout * 1000,
                # Additional security: disable query caching for sensitive operations
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
        """
        Execute a BigQuery query with custom job configuration including timeout and enhanced security validation.

        Args:
            query: SQL query to execute
            job_config: QueryJobConfig with custom parameters
            context: Optional context for logging and error reporting

        Returns:
            List of row results

        Raises:
            ValueError: If query contains dangerous patterns
            Exception: For query execution errors
        """
        # Enhanced security validation
        validate_sql_structure(query)

        # Additional basic pattern check
        dangerous_patterns = [";", "--", "/*", "xp_cmdshell", "sp_executesql"]
        for pattern in dangerous_patterns:
            if pattern in query:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

        try:
            timeout = self.config.profiling.partition_fetch_timeout
            context_info = f" for {context}" if context else ""
            logger.debug(
                f"Executing query{context_info} with {timeout}s timeout and custom config"
            )

            # Ensure timeout is set even with custom config
            job_config.job_timeout_ms = timeout * 1000
            # Additional security settings
            job_config.use_query_cache = False

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )
            # Convert to list to ensure consistent return type
            return list(query_job.result())
        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.warning(f"Query execution error{context_info}: {e}")
            raise

    def execute_query_safely(
        self, query: str, job_config: Optional[QueryJobConfig] = None, context: str = ""
    ) -> List[Row]:
        """
        Execute a query with consistent error handling and logging.

        This is a convenience method that provides unified error handling
        for both parameterized and non-parameterized queries.

        Args:
            query: SQL query to execute
            job_config: Optional query job configuration with parameters
            context: Context for logging (optional)

        Returns:
            Query results as list

        Raises:
            Exception: For query execution errors
        """
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
        """
        Test if a query can be executed without actually running it.

        This uses BigQuery's dry run feature to validate queries.

        Args:
            query: SQL query to test
            context: Optional context for logging

        Returns:
            True if query is valid and can be executed, False otherwise
        """
        try:
            # Validate query structure first
            validate_sql_structure(query)

            # Use dry run to test query validity
            job_config = QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            context_info = f" for {context}" if context else ""
            logger.debug(f"Testing query{context_info} with dry run")

            # Execute dry run to validate query syntax (result not needed)
            self.config.get_bigquery_client().query(query, job_config=job_config)

            # If we get here without exception, query is valid
            logger.debug(f"Query test successful{context_info}")
            return True

        except Exception as e:
            context_info = f" in {context}" if context else ""
            logger.debug(f"Query test failed{context_info}: {e}")
            return False

    def get_query_cost_estimate(self, query: str, context: str = "") -> Optional[int]:
        """
        Get an estimate of bytes that will be processed by a query.

        This uses BigQuery's dry run feature to get query statistics.

        Args:
            query: SQL query to analyze
            context: Optional context for logging

        Returns:
            Estimated bytes to be processed, or None if estimation fails
        """
        try:
            # Validate query structure first
            validate_sql_structure(query)

            job_config = QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            context_info = f" for {context}" if context else ""
            logger.debug(f"Getting cost estimate{context_info}")

            query_job = self.config.get_bigquery_client().query(
                query, job_config=job_config
            )

            # Get the bytes that would be processed
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

                # Check if this is a retryable error
                if not self._is_retryable_error(e):
                    logger.debug(f"Non-retryable error, not retrying: {e}")
                    raise

                if attempt < max_retries:
                    context_info = f" in {context}" if context else ""
                    logger.debug(f"Retryable error{context_info}, will retry: {e}")
                else:
                    context_info = f" in {context}" if context else ""
                    logger.warning(f"All retry attempts failed{context_info}: {e}")

        # If we get here, all retries failed
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"All retry attempts failed{context_info}")

    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Determine if an error is retryable.

        Args:
            error: Exception to check

        Returns:
            True if the error is likely transient and retryable
        """
        error_str = str(error).lower()

        # Common retryable error patterns
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

        # Build safe table reference (this validates the identifiers)
        safe_table_ref = build_safe_table_reference(project, schema, table)

        # Build the query
        query_parts = ["SELECT *", f"FROM {safe_table_ref}"]

        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        if limit is not None and limit > 0:
            # Validate limit is reasonable
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
        """
        Get the effective timeout for query operations.

        Returns:
            Timeout in seconds
        """
        return self.config.profiling.partition_fetch_timeout

    def is_query_too_expensive(
        self, query: str, max_bytes: int = 1_000_000_000
    ) -> bool:
        """
        Check if a query would process too much data.

        Args:
            query: SQL query to check
            max_bytes: Maximum allowed bytes to process

        Returns:
            True if query would exceed the byte limit
        """
        try:
            estimated_bytes = self.get_query_cost_estimate(query)
            if estimated_bytes is None:
                # If we can't estimate, err on the side of caution
                return True

            return estimated_bytes > max_bytes

        except Exception:
            # If estimation fails, assume it's too expensive
            return True
