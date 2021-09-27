import logging
from dataclasses import dataclass
from typing import Dict, Optional, Union

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation

logger = logging.getLogger(__name__)


class DatahubRestSinkConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"
    token: Optional[str]
    timeout_sec: Optional[int]
    extra_headers: Optional[Dict[str, str]]


@dataclass
class DatahubRestSink(Sink):
    config: DatahubRestSinkConfig
    emitter: DatahubRestEmitter
    report: SinkReport

    def __init__(self, ctx: PipelineContext, config: DatahubRestSinkConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SinkReport()
        self.emitter = DatahubRestEmitter(
            self.config.server,
            self.config.token,
            connect_timeout_sec=self.config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=self.config.timeout_sec,
            extra_headers=self.config.extra_headers,
        )
        self.emitter.test_connection()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DatahubRestSink":
        config = DatahubRestSinkConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

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
            self.emitter.emit(record)
            self.report.report_record_written(record_envelope)
            write_callback.on_success(record_envelope, {})
        except OperationalError as e:
            self.report.report_failure({"error": e.message, "info": e.info})
            write_callback.on_failure(record_envelope, e, e.info)
        except Exception as e:
            self.report.report_failure({"e": e})
            write_callback.on_failure(record_envelope, e, {})

    def get_report(self) -> SinkReport:
        return self.report

    def close(self):
        pass
