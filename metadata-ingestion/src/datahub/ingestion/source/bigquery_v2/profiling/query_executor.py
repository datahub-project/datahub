import logging
import threading
from typing import List, Optional

from google.cloud.bigquery import Client, QueryJobConfig, Row

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    mask_string_literals,
    validate_sql_structure,
)

logger = logging.getLogger(__name__)


class QueryExecutor:
    def __init__(self, config: BigQueryV2Config):
        self.config = config
        # get_bigquery_client() builds a brand-new bigquery.Client (and HTTP session)
        # on every call. Partition-metadata caching and discovery issue many queries per
        # dataset, so cache one client and reuse it. bigquery.Client is safe to share
        # across the deferred-external ThreadPoolExecutor workers; guard construction with
        # a lock so the first concurrent callers don't each build one.
        self._client: Optional[Client] = None
        self._client_lock = threading.Lock()

    def _get_client(self) -> Client:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = self.config.get_bigquery_client()
        return self._client

    def _validate_query_security(self, query: str) -> None:
        # validate_sql_structure returns False (rather than raising) for an empty or
        # non-string query; treat that as a rejection instead of letting it through.
        if not validate_sql_structure(query):
            raise ValueError("Query failed structural validation (empty or malformed)")

        # Extra guard against comment-/statement-separator injection that slips past
        # structural checks. Scan a quote-aware view (mask_string_literals) so a token
        # inside a STRING/Hive partition value — e.g. `col` = 'a--b' or a 'data:' URI —
        # is not misread as an injection boundary and the valid SELECT is not dropped;
        # only a token outside a literal, which really does broaden the query, is caught.
        # Deliberately BigQuery-relevant only: SQL Server builtins like xp_cmdshell are
        # not removed here because they are valid partition values.
        masked = mask_string_literals(query)
        dangerous_patterns = [";", "--", "/*"]
        for pattern in dangerous_patterns:
            if pattern in masked:
                logger.error(
                    f"Query contains potentially dangerous pattern '{pattern}'. Query rejected."
                )
                raise ValueError(f"Query contains dangerous pattern: {pattern}")

    def execute_query_safely(
        self, query: str, job_config: Optional[QueryJobConfig] = None, context: str = ""
    ) -> List[Row]:
        # Failures are logged at DEBUG and re-raised, never swallowed: the
        # partition-detection probe relies on the exception, and the caller holds the
        # report and decides whether a genuine failure warrants a report warning. The
        # security validation runs inside the try so a rejection is logged with its
        # execution context like any other failure.
        try:
            self._validate_query_security(query)

            timeout = self.config.profiling.partition_fetch_timeout
            logger.debug(
                f"Executing query{f' for {context}' if context else ''} with {timeout}s timeout"
            )

            job_config = job_config or QueryJobConfig()
            job_config.job_timeout_ms = timeout * 1000
            job_config.use_query_cache = False

            query_job = self._get_client().query(query, job_config=job_config)
            results = list(query_job.result())
            logger.debug(
                f"Query returned {len(results)} row(s){f' for {context}' if context else ''}"
            )
            return results
        except Exception as e:
            logger.debug(
                f"Query execution error{f' in {context}' if context else ''}: {e}"
            )
            raise
