import logging
from datetime import datetime
from typing import Callable, Iterable, List, Optional

from google.cloud import bigquery
from google.cloud.logging_v2.client import Client as GCPLoggingClient

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditLogEntry,
    BigQueryAuditMetadata,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryAuditLogApiPerfReport,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
)
from datahub.utilities.ratelimiter import RateLimiter

logger: logging.Logger = logging.getLogger(__name__)


# Api interfaces are separated based on functionality they provide
# rather than the underlying bigquery client that is used to
# provide the functionality.
class BigQueryAuditLogApi:
    def __init__(
        self,
        report: BigQueryAuditLogApiPerfReport,
        rate_limit: bool,
        requests_per_min: int,
    ) -> None:
        self.report = report
        self.rate_limit = rate_limit
        self.requests_per_min = requests_per_min

    def get_exported_bigquery_audit_metadata(
        self,
        bigquery_client: bigquery.Client,
        bigquery_audit_metadata_query_template: Callable[
            [
                str,  # dataset: str
                bool,  # use_date_sharded_tables: bool
                Optional[int],  # limit: Optional[int] = None
            ],
            str,
        ],
        bigquery_audit_metadata_datasets: Optional[List[str]],
        use_date_sharded_audit_log_tables: bool,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
    ) -> Iterable[BigQueryAuditMetadata]:
        if bigquery_audit_metadata_datasets is None:
            return

        audit_start_time = start_time.strftime(BQ_DATETIME_FORMAT)
        audit_start_date = start_time.strftime(BQ_DATE_SHARD_FORMAT)

        audit_end_time = end_time.strftime(BQ_DATETIME_FORMAT)
        audit_end_date = end_time.strftime(BQ_DATE_SHARD_FORMAT)

        rate_limiter: Optional[RateLimiter] = None
        if self.rate_limit:
            rate_limiter = RateLimiter(max_calls=self.requests_per_min, period=60)

        with self.report.get_exported_log_entries as current_timer:
            self.report.num_get_exported_log_entries_api_requests += 1
            for dataset in bigquery_audit_metadata_datasets:
                logger.info(
                    f"Start loading log entries from BigQueryAuditMetadata in {dataset}"
                )

                query = bigquery_audit_metadata_query_template(
                    dataset,
                    use_date_sharded_audit_log_tables,
                    limit,
                ).format(
                    start_time=audit_start_time,
                    end_time=audit_end_time,
                    start_date=audit_start_date,
                    end_date=audit_end_date,
                )

                query_job = bigquery_client.query(query)
                logger.info(
                    f"Finished loading log entries from BigQueryAuditMetadata in {dataset}"
                )

                for entry in query_job:
                    with current_timer.pause():
                        if rate_limiter:
                            with rate_limiter:
                                yield entry
                        else:
                            yield entry

    def get_bigquery_log_entries_via_gcp_logging(
        self,
        client: GCPLoggingClient,
        filter: str,
        log_page_size: int,
        limit: Optional[int] = None,
    ) -> Iterable[AuditLogEntry]:
        logger.debug(filter)

        list_entries: Iterable[AuditLogEntry]
        rate_limiter: Optional[RateLimiter] = None
        if self.rate_limit:
            # client.list_entries is a generator, does api calls to GCP Logging when it runs out of entries and needs to fetch more from GCP Logging
            # to properly ratelimit we multiply the page size by the number of requests per minute
            rate_limiter = RateLimiter(
                max_calls=self.requests_per_min * log_page_size,
                period=60,
            )

        with self.report.list_log_entries as current_timer:
            self.report.num_list_log_entries_api_requests += 1
            list_entries = client.list_entries(
                filter_=filter,
                page_size=log_page_size,
                max_results=limit,
            )

            for i, entry in enumerate(list_entries):
                if i > 0 and i % 1000 == 0:
                    logger.info(
                        f"Loaded {i} log entries from GCP Log for {client.project}"
                    )

                with current_timer.pause():
                    if rate_limiter:
                        with rate_limiter:
                            yield entry
                    else:
                        yield entry

            logger.info(
                f"Finished loading log entries from GCP Log for {client.project}"
            )
