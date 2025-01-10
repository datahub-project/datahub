import datetime
import logging
import time
from typing import Any, Dict, Iterator, Optional

from pydantic import BaseModel, Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

DATAHUB_EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest"
DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME = "dataHubExecutionRequestKey"
DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME = "dataHubExecutionRequestInput"
DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME = "dataHubExecutionRequestResult"


class DatahubExecutionRequestCleanupConfig(ConfigModel):
    keep_history_min_count: int = Field(
        10,
        description="Minimum number of execution requests to keep, per ingestion source",
    )

    keep_history_max_count: int = Field(
        1000,
        description="Maximum number of execution requests to keep, per ingestion source",
    )

    keep_history_max_days: int = Field(
        90,
        description="Maximum number of days to keep execution requests for, per ingestion source",
    )

    batch_read_size: int = Field(
        100,
        description="Number of records per read operation",
    )

    enabled: bool = Field(
        True,
        description="Global switch for this cleanup task",
    )

    runtime_limit_seconds: int = Field(
        default=3600,
        description="Maximum runtime in seconds for the cleanup task",
    )

    limit_entities_delete: Optional[int] = Field(
        10000, description="Max number of execution requests to hard delete."
    )

    max_read_errors: int = Field(
        default=10,
        description="Maximum number of read errors before aborting",
    )

    def keep_history_max_milliseconds(self):
        return self.keep_history_max_days * 24 * 3600 * 1000


class DatahubExecutionRequestCleanupReport(SourceReport):
    ergc_records_read: int = 0
    ergc_records_preserved: int = 0
    ergc_records_deleted: int = 0
    ergc_read_errors: int = 0
    ergc_delete_errors: int = 0
    ergc_start_time: Optional[datetime.datetime] = None
    ergc_end_time: Optional[datetime.datetime] = None
    ergc_delete_limit_reached: bool = False
    ergc_runtime_limit_reached: bool = False


class CleanupRecord(BaseModel):
    urn: str
    request_id: str
    status: str
    ingestion_source: str
    requested_at: int


class DatahubExecutionRequestCleanup:
    def __init__(
        self,
        graph: DataHubGraph,
        report: DatahubExecutionRequestCleanupReport,
        config: Optional[DatahubExecutionRequestCleanupConfig] = None,
    ) -> None:
        self.graph = graph
        self.report = report
        self.instance_id = int(time.time())
        self.last_print_time = 0.0

        if config is not None:
            self.config = config
        else:
            self.config = DatahubExecutionRequestCleanupConfig()

    def _print_report(self) -> None:
        time_taken = round(time.time() - self.last_print_time, 1)
        # Print report every 2 minutes
        if time_taken > 120:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def _to_cleanup_record(self, entry: Dict) -> CleanupRecord:
        input_aspect = (
            entry.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME, {})
            .get("value", {})
        )
        result_aspect = (
            entry.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME, {})
            .get("value", {})
        )
        key_aspect = (
            entry.get("aspects", {})
            .get(DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME, {})
            .get("value", {})
        )
        return CleanupRecord(
            urn=entry.get("urn"),
            request_id=key_aspect.get("id"),
            requested_at=input_aspect.get("requestedAt", 0),
            status=result_aspect.get("status", "PENDING"),
            ingestion_source=input_aspect.get("source", {}).get("ingestionSource", ""),
        )

    def _scroll_execution_requests(
        self, overrides: Dict[str, Any] = {}
    ) -> Iterator[CleanupRecord]:
        headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {
            "aspectNames": [
                DATAHUB_EXECUTION_REQUEST_KEY_ASPECT_NAME,
                DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            ],
            "count": str(self.config.batch_read_size),
            "sort": "requestTimeMs",
            "sortOrder": "DESCENDING",
            "systemMetadata": "false",
            "skipCache": "true",
        }
        params.update(overrides)

        while True:
            if self._reached_runtime_limit():
                break
            if self.report.ergc_read_errors >= self.config.max_read_errors:
                self.report.failure(
                    title="Too many read errors, aborting",
                    message="Too many read errors, aborting",
                    context=str(self.instance_id),
                )
                break
            try:
                url = f"{self.graph.config.server}/openapi/v2/entity/{DATAHUB_EXECUTION_REQUEST_ENTITY_NAME}"
                response = self.graph._session.get(url, headers=headers, params=params)
                response.raise_for_status()
                document = response.json()

                entries = document.get("results", [])
                for entry in entries:
                    yield self._to_cleanup_record(entry)

                if "scrollId" not in document:
                    break
                params["scrollId"] = document["scrollId"]
            except Exception as e:
                self.report.failure(
                    title="Failed to fetch next batch of execution requests",
                    message="Failed to fetch next batch of execution requests",
                    context=str(self.instance_id),
                    exc=e,
                )
                self.report.ergc_read_errors += 1

    def _scroll_garbage_records(self):
        state: Dict[str, Dict] = {}

        now_ms = int(time.time()) * 1000
        running_guard_timeout = now_ms - 30 * 24 * 3600 * 1000

        for entry in self._scroll_execution_requests():
            self._print_report()
            self.report.ergc_records_read += 1
            key = entry.ingestion_source

            # Always delete corrupted records
            if not key:
                logger.warning(
                    f"ergc({self.instance_id}): will delete corrupted entry with missing source key: {entry}"
                )
                yield entry
                continue

            if key not in state:
                state[key] = {}
                state[key]["cutoffTimestamp"] = (
                    entry.requested_at - self.config.keep_history_max_milliseconds()
                )

            state[key]["count"] = state[key].get("count", 0) + 1

            # Do not delete if number of requests is below minimum
            if state[key]["count"] < self.config.keep_history_min_count:
                self.report.ergc_records_preserved += 1
                continue

            # Do not delete if number of requests do not exceed allowed maximum,
            # or the cutoff date.
            if (state[key]["count"] < self.config.keep_history_max_count) and (
                entry.requested_at > state[key]["cutoffTimestamp"]
            ):
                self.report.ergc_records_preserved += 1
                continue

            # Do not delete if status is RUNNING or PENDING and created within last month. If the record is >month old and it did not
            # transition to a final state within that timeframe, it likely has no value.
            if entry.requested_at > running_guard_timeout and entry.status in [
                "RUNNING",
                "PENDING",
            ]:
                self.report.ergc_records_preserved += 1
                continue

            # Otherwise delete current record
            logger.info(
                (
                    f"ergc({self.instance_id}): going to delete {entry.request_id} in source {key}; "
                    f"source count: {state[key]['count']}; "
                    f"source cutoff: {state[key]['cutoffTimestamp']}; "
                    f"record timestamp: {entry.requested_at}."
                )
            )
            yield entry

    def _delete_entry(self, entry: CleanupRecord) -> None:
        try:
            self.graph.delete_entity(entry.urn, True)
            self.report.ergc_records_deleted += 1
        except Exception as e:
            self.report.ergc_delete_errors += 1
            self.report.failure(
                title="Failed to delete ExecutionRequest",
                message="Failed to delete ExecutionRequest",
                context=str(self.instance_id),
                exc=e,
            )

    def _reached_runtime_limit(self) -> bool:
        if (
            self.config.runtime_limit_seconds
            and self.report.ergc_start_time
            and (
                datetime.datetime.now() - self.report.ergc_start_time
                >= datetime.timedelta(seconds=self.config.runtime_limit_seconds)
            )
        ):
            self.report.ergc_runtime_limit_reached = True
            logger.info(f"ergc({self.instance_id}): max runtime reached.")
            return True
        return False

    def _reached_delete_limit(self) -> bool:
        if (
            self.config.limit_entities_delete
            and self.report.ergc_records_deleted >= self.config.limit_entities_delete
        ):
            logger.info(
                f"ergc({self.instance_id}): max delete limit reached: {self.config.limit_entities_delete}."
            )
            self.report.ergc_delete_limit_reached = True
            return True
        return False

    def run(self) -> None:
        if not self.config.enabled:
            logger.info(
                f"ergc({self.instance_id}): ExecutionRequest cleaner is disabled."
            )
            return
        self.report.ergc_start_time = datetime.datetime.now()

        logger.info(
            (
                f"ergc({self.instance_id}): Starting cleanup of ExecutionRequest records; "
                f"max days: {self.config.keep_history_max_days}, "
                f"min records: {self.config.keep_history_min_count}, "
                f"max records: {self.config.keep_history_max_count}."
            )
        )

        for entry in self._scroll_garbage_records():
            if self._reached_runtime_limit() or self._reached_delete_limit():
                break
            self._delete_entry(entry)

        self.report.ergc_end_time = datetime.datetime.now()
        logger.info(
            f"ergc({self.instance_id}): Finished cleanup of ExecutionRequest records."
        )
