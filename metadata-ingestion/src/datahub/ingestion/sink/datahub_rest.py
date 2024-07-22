import concurrent.futures
import contextlib
import dataclasses
import functools
import logging
import os
import threading
import uuid
from enum import auto
from typing import List, Optional, Tuple, Union

from datahub.cli.cli_utils import set_env_variables_override_config
from datahub.configuration.common import (
    ConfigEnum,
    ConfigurationError,
    OperationalError,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import (
    NoopWriteCallback,
    Sink,
    SinkReport,
    WriteCallback,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.utilities.partition_executor import (
    BatchPartitionExecutor,
    PartitionExecutor,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.server_config_util import set_gms_config

logger = logging.getLogger(__name__)

DEFAULT_REST_SINK_MAX_THREADS = int(
    os.getenv("DATAHUB_REST_SINK_DEFAULT_MAX_THREADS", 15)
)


class RestSinkMode(ConfigEnum):
    SYNC = auto()
    ASYNC = auto()

    # Uses the new ingestProposalBatch endpoint. Significantly more efficient than the other modes,
    # but requires a server version that supports it.
    # https://github.com/datahub-project/datahub/pull/10706
    ASYNC_BATCH = auto()


class DatahubRestSinkConfig(DatahubClientConfig):
    mode: RestSinkMode = RestSinkMode.ASYNC

    # These only apply in async modes.
    max_threads: int = DEFAULT_REST_SINK_MAX_THREADS
    max_pending_requests: int = 2000

    # Only applies in async batch mode.
    max_per_batch: int = 100


@dataclasses.dataclass
class DataHubRestSinkReport(SinkReport):
    max_threads: Optional[int] = None
    gms_version: Optional[str] = None
    pending_requests: int = 0

    main_thread_blocking_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)

    def compute_stats(self) -> None:
        super().compute_stats()


def _get_urn(record_envelope: RecordEnvelope) -> Optional[str]:
    metadata = record_envelope.record

    if isinstance(metadata, MetadataChangeEvent):
        return metadata.proposedSnapshot.urn
    elif isinstance(metadata, (MetadataChangeProposalWrapper, MetadataChangeProposal)):
        return metadata.entityUrn

    return None


def _get_partition_key(record_envelope: RecordEnvelope) -> str:
    urn = _get_urn(record_envelope)
    if urn:
        return urn

    # This shouldn't happen super frequently, but just adding a fallback of generating
    # a UUID so that we don't do any partitioning.
    return str(uuid.uuid4())


class DatahubRestSink(Sink[DatahubRestSinkConfig, DataHubRestSinkReport]):
    _emitter_thread_local: threading.local
    treat_errors_as_warnings: bool = False

    def __post_init__(self) -> None:
        self._emitter_thread_local = threading.local()

        try:
            gms_config = self.emitter.get_server_config()
        except Exception as exc:
            raise ConfigurationError(
                f"💥 Failed to connect to DataHub with {repr(self.emitter)}"
            ) from exc

        self.report.gms_version = (
            gms_config.get("versions", {})
            .get("acryldata/datahub", {})
            .get("version", None)
        )
        self.report.max_threads = self.config.max_threads
        logger.debug("Setting env variables to override config")
        set_env_variables_override_config(self.config.server, self.config.token)
        logger.debug("Setting gms config")
        set_gms_config(gms_config)

        self.executor: Union[PartitionExecutor, BatchPartitionExecutor]
        if self.config.mode == RestSinkMode.ASYNC_BATCH:
            self.executor = BatchPartitionExecutor(
                max_workers=self.config.max_threads,
                max_pending=self.config.max_pending_requests,
                process_batch=self._emit_batch_wrapper,
                max_per_batch=self.config.max_per_batch,
            )
        else:
            self.executor = PartitionExecutor(
                max_workers=self.config.max_threads,
                max_pending=self.config.max_pending_requests,
            )

    @classmethod
    def _make_emitter(cls, config: DatahubRestSinkConfig) -> DataHubRestEmitter:
        return DataHubRestEmitter(
            config.server,
            config.token,
            connect_timeout_sec=config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=config.timeout_sec,
            retry_status_codes=config.retry_status_codes,
            retry_max_times=config.retry_max_times,
            extra_headers=config.extra_headers,
            ca_certificate_path=config.ca_certificate_path,
            client_certificate_path=config.client_certificate_path,
            disable_ssl_verification=config.disable_ssl_verification,
        )

    @property
    def emitter(self) -> DataHubRestEmitter:
        # While this is a property, it actually uses one emitter per thread.
        # Since emitter is one-to-one with request sessions, using a separate
        # emitter per thread should improve correctness and performance.
        # https://github.com/psf/requests/issues/1871#issuecomment-32751346
        thread_local = self._emitter_thread_local
        if not hasattr(thread_local, "emitter"):
            thread_local.emitter = DatahubRestSink._make_emitter(self.config)
        return thread_local.emitter

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        if isinstance(workunit, MetadataWorkUnit):
            self.treat_errors_as_warnings = workunit.treat_errors_as_warnings

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
                self.report.report_record_written(record_envelope)
                write_callback.on_success(record_envelope, {})
            elif isinstance(e, OperationalError):
                # only OperationalErrors should be ignored
                # trim exception stacktraces in all cases when reporting
                if "stackTrace" in e.info:
                    with contextlib.suppress(Exception):
                        e.info["stackTrace"] = "\n".join(
                            e.info["stackTrace"].split("\n")[:3]
                        )
                        e.info["message"] = e.info.get("message", "").split("\n")[0][
                            :200
                        ]

                # Include information about the entity that failed.
                record_urn = _get_urn(record_envelope)
                if record_urn:
                    e.info["urn"] = record_urn

                if not self.treat_errors_as_warnings:
                    self.report.report_failure({"error": e.message, "info": e.info})
                else:
                    self.report.report_warning({"warning": e.message, "info": e.info})
                write_callback.on_failure(record_envelope, e, e.info)
            else:
                logger.exception(f"Failure: {e}", exc_info=e)
                self.report.report_failure({"e": e})
                write_callback.on_failure(record_envelope, Exception(e), {})

    def _emit_wrapper(
        self,
        record: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
    ) -> None:
        # TODO: Add timing metrics
        self.emitter.emit(record)

    def _emit_batch_wrapper(
        self,
        records: List[
            Tuple[
                Union[
                    MetadataChangeEvent,
                    MetadataChangeProposal,
                    MetadataChangeProposalWrapper,
                ],
            ]
        ],
    ) -> None:
        events: List[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]] = []
        for record in records:
            event = record[0]
            if isinstance(event, MetadataChangeEvent):
                # Unpack MCEs into MCPs.
                mcps = mcps_from_mce(event)
                events.extend(mcps)
            else:
                events.append(event)

        self.emitter.emit_mcps(events)

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        # Because the default is async mode and most sources are slower than the sink, this
        # should only have a high value if the sink is actually a bottleneck.
        with self.report.main_thread_blocking_timer:
            record = record_envelope.record
            if self.config.mode == RestSinkMode.ASYNC:
                assert isinstance(self.executor, PartitionExecutor)
                partition_key = _get_partition_key(record_envelope)
                self.executor.submit(
                    partition_key,
                    self._emit_wrapper,
                    record,
                    done_callback=functools.partial(
                        self._write_done_callback, record_envelope, write_callback
                    ),
                )
                self.report.pending_requests += 1
            elif self.config.mode == RestSinkMode.ASYNC_BATCH:
                assert isinstance(self.executor, BatchPartitionExecutor)
                partition_key = _get_partition_key(record_envelope)
                self.executor.submit(
                    partition_key,
                    record,
                    done_callback=functools.partial(
                        self._write_done_callback, record_envelope, write_callback
                    ),
                )
                self.report.pending_requests += 1
            else:
                # execute synchronously
                try:
                    self._emit_wrapper(record)
                    write_callback.on_success(record_envelope, success_metadata={})
                except Exception as e:
                    write_callback.on_failure(record_envelope, e, failure_metadata={})

    def emit_async(
        self,
        item: Union[
            MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
        ],
    ) -> None:
        return self.write_record_async(
            RecordEnvelope(item, metadata={}),
            NoopWriteCallback(),
        )

    def close(self):
        with self.report.main_thread_blocking_timer:
            self.executor.shutdown()

    def __repr__(self) -> str:
        return self.emitter.__repr__()

    def configured(self) -> str:
        return repr(self)
