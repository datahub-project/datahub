import concurrent.futures
import contextlib
import functools
import logging
import uuid
from dataclasses import dataclass
from enum import auto
from typing import Optional, Union

from datahub.cli.cli_utils import set_env_variables_override_config
from datahub.configuration.common import (
    ConfigEnum,
    ConfigurationError,
    OperationalError,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
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
from datahub.utilities.advanced_thread_executor import PartitionExecutor
from datahub.utilities.server_config_util import set_gms_config

logger = logging.getLogger(__name__)


class SyncOrAsync(ConfigEnum):
    SYNC = auto()
    ASYNC = auto()


class DatahubRestSinkConfig(DatahubClientConfig):
    mode: SyncOrAsync = SyncOrAsync.ASYNC

    # These only apply in async mode.
    max_threads: int = 15
    max_pending_requests: int = 500


@dataclass
class DataHubRestSinkReport(SinkReport):
    max_threads: int = -1
    gms_version: str = ""
    pending_requests: int = 0

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
    emitter: DatahubRestEmitter
    treat_errors_as_warnings: bool = False

    def __post_init__(self) -> None:
        self.emitter = DatahubRestEmitter(
            self.config.server,
            self.config.token,
            connect_timeout_sec=self.config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=self.config.timeout_sec,
            retry_status_codes=self.config.retry_status_codes,
            retry_max_times=self.config.retry_max_times,
            extra_headers=self.config.extra_headers,
            ca_certificate_path=self.config.ca_certificate_path,
            client_certificate_path=self.config.client_certificate_path,
            disable_ssl_verification=self.config.disable_ssl_verification,
        )
        try:
            gms_config = self.emitter.get_server_config()
        except Exception as exc:
            raise ConfigurationError(
                f"💥 Failed to connect to DataHub with {repr(self.emitter)}"
            ) from exc

        self.report.gms_version = (
            gms_config.get("versions", {})
            .get("linkedin/datahub", {})
            .get("version", "")
        )
        self.report.max_threads = self.config.max_threads
        logger.debug("Setting env variables to override config")
        set_env_variables_override_config(self.config.server, self.config.token)
        logger.debug("Setting gms config")
        set_gms_config(gms_config)
        self.executor = PartitionExecutor(
            max_workers=self.config.max_threads,
            max_pending=self.config.max_pending_requests,
        )

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
        record = record_envelope.record
        if self.config.mode == SyncOrAsync.ASYNC:
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
        self.executor.shutdown()

    def __repr__(self) -> str:
        return self.emitter.__repr__()

    def configured(self) -> str:
        return repr(self)
