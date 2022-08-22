import concurrent.futures
import contextlib
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from threading import BoundedSemaphore
from typing import Union, cast

from tdigest import TDigest

from datahub.cli.cli_utils import set_env_variables_override_config
from datahub.configuration.common import ConfigurationError, OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation
from datahub.utilities.server_config_util import set_gms_config

logger = logging.getLogger(__name__)


class DatahubRestSinkConfig(DatahubClientConfig):
    max_pending_requests: int = 1000
    use_sync_emitter_on_async_failure: bool = False


@dataclass
class DataHubRestSinkReport(SinkReport):
    gms_version: str = ""
    pending_requests: int = 0
    _digest = TDigest()

    def compute_stats(self) -> None:
        super().compute_stats()
        self.ninety_fifth_percentile_write_latency_in_millis = self._digest.percentile(
            95
        )
        self.fiftieth_percentile_write_latency_in_millis = self._digest.percentile(50)

    def report_write_latency(self, delta: timedelta) -> None:
        self._digest.update(round(delta.total_seconds() * 1000.0))


class BoundedExecutor:
    """BoundedExecutor behaves as a ThreadPoolExecutor which will block on
    calls to submit() once the limit given as "bound" work items are queued for
    execution.
    :param bound: Integer - the maximum number of items in the work queue
    :param max_workers: Integer - the size of the thread pool
    """

    def __init__(self, bound, max_workers):
        self.executor = ThreadPoolExecutor(max_workers)
        self.semaphore = BoundedSemaphore(bound + max_workers)

    """See concurrent.futures.Executor#submit"""

    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except Exception:
            self.semaphore.release()
            raise
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future

    """See concurrent.futures.Executor#shutdown"""

    def shutdown(self, wait=True):
        self.executor.shutdown(wait)


@dataclass
class DatahubRestSink(Sink):
    config: DatahubRestSinkConfig
    emitter: DatahubRestEmitter
    report: DataHubRestSinkReport
    treat_errors_as_warnings: bool = False

    def __init__(self, ctx: PipelineContext, config: DatahubRestSinkConfig):
        super().__init__(ctx)
        self.config = config
        self.report = DataHubRestSinkReport()
        self.emitter = DatahubRestEmitter(
            self.config.server,
            self.config.token,
            connect_timeout_sec=self.config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=self.config.timeout_sec,
            retry_status_codes=self.config.retry_status_codes,
            retry_max_times=self.config.retry_max_times,
            extra_headers=self.config.extra_headers,
            ca_certificate_path=self.config.ca_certificate_path,
            disable_ssl_verification=self.config.disable_ssl_verification,
        )
        try:
            gms_config = self.emitter.test_connection()
        except Exception as exc:
            raise ConfigurationError(
                f"💥 Failed to connect to DataHub@{self.config.server} (token:{'XXX-redacted' if self.config.token else 'empty'}) over REST",
                exc,
            )

        self.report.gms_version = (
            gms_config.get("versions", {})
            .get("linkedin/datahub", {})
            .get("version", "")
        )
        logger.debug("Setting env variables to override config")
        set_env_variables_override_config(self.config.server, self.config.token)
        logger.debug("Setting gms config")
        set_gms_config(gms_config)
        self.executor = BoundedExecutor(
            max_workers=self.config.max_threads,
            bound=self.config.max_pending_requests,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DatahubRestSink":
        config = DatahubRestSinkConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        if isinstance(workunit, MetadataWorkUnit):
            mwu: MetadataWorkUnit = cast(MetadataWorkUnit, workunit)
            self.treat_errors_as_warnings = mwu.treat_errors_as_warnings

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    def _write_done_callback(
        self,
        record_envelope: RecordEnvelope,
        write_callback: WriteCallback,
        future: concurrent.futures.Future,
    ) -> None:
        self.report.pending_requests -= 1
        if future.cancelled():
            self.report.report_failure({"error": "future was cancelled"})
            write_callback.on_failure(
                record_envelope, OperationalError("future was cancelled"), {}
            )
        elif future.done():
            e = future.exception()
            if not e:
                start_time, end_time = future.result()
                self.report.report_record_written(record_envelope)
                self.report.report_write_latency(end_time - start_time)
                write_callback.on_success(record_envelope, {})
            elif isinstance(e, OperationalError):
                # only OperationalErrors should be ignored
                # trim exception stacktraces in all cases when reporting
                if "stackTrace" in e.info:
                    with contextlib.suppress(Exception):
                        e.info["stackTrace"] = "\n".join(
                            e.info["stackTrace"].split("\n")[:3]
                        )

                if not self.treat_errors_as_warnings:
                    self.report.report_failure({"error": e.message, "info": e.info})
                else:
                    record = record_envelope.record
                    if isinstance(record, MetadataChangeProposalWrapper):
                        # include information about the entity that failed
                        entity_id = cast(
                            MetadataChangeProposalWrapper, record
                        ).entityUrn
                        e.info["id"] = entity_id
                    else:
                        entity_id = None
                    self.report.report_warning({"warning": e.message, "info": e.info})
                write_callback.on_failure(record_envelope, e, e.info)
            else:
                self.report.report_failure({"e": e})
                write_callback.on_failure(record_envelope, Exception(e), {})

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
                UsageAggregation,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        record = record_envelope.record
        try:
            write_future = self.executor.submit(self.emitter.emit, record)
            write_future.add_done_callback(
                functools.partial(
                    self._write_done_callback, record_envelope, write_callback
                )
            )
            self.report.pending_requests += 1
        except RuntimeError:
            if self.config.use_sync_emitter_on_async_failure:
                try:
                    (start, end) = self.emitter.emit(record)
                    write_callback.on_success(record_envelope, success_metadata={})
                except Exception as e:
                    write_callback.on_failure(record_envelope, e, failure_metadata={})
            else:
                raise

    def get_report(self) -> SinkReport:
        return self.report

    def close(self):
        self.executor.shutdown(wait=True)

    def __repr__(self) -> str:
        return self.emitter.__repr__()

    def configured(self) -> str:
        return self.__repr__()
