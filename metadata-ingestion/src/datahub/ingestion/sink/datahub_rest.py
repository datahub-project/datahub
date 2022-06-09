import concurrent.futures
import contextlib
import functools
import logging
from dataclasses import dataclass
from typing import Union, cast

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
    pass


@dataclass
class DataHubRestSinkReport(SinkReport):
    gms_version: str = ""


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
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.max_threads
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
        if future.cancelled():
            self.report.report_failure({"error": "future was cancelled"})
            write_callback.on_failure(
                record_envelope, OperationalError("future was cancelled"), {}
            )
        elif future.done():
            e = future.exception()
            if not e:
                self.report.report_record_written(record_envelope)
                start_time, end_time = future.result()
                self.report.report_downstream_latency(start_time, end_time)
                write_callback.on_success(record_envelope, {})
            elif isinstance(e, OperationalError):
                # only OperationalErrors should be ignored
                if not self.treat_errors_as_warnings:
                    self.report.report_failure({"error": e.message, "info": e.info})
                else:
                    # trim exception stacktraces when reporting warnings
                    if "stackTrace" in e.info:
                        with contextlib.suppress(Exception):
                            e.info["stackTrace"] = "\n".join(
                                e.info["stackTrace"].split("\n")[:2]
                            )
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

        write_future = self.executor.submit(self.emitter.emit, record)
        write_future.add_done_callback(
            functools.partial(
                self._write_done_callback, record_envelope, write_callback
            )
        )

    def get_report(self) -> SinkReport:
        return self.report

    def close(self):
        self.executor.shutdown(wait=True)
