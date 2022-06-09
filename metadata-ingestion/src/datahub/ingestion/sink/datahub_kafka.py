from dataclasses import dataclass
from typing import Optional, Union

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import MetadataChangeProposalClass


class KafkaSinkConfig(KafkaEmitterConfig):
    # This extra layer of indirection exists in case we need to add extra
    # config options to the sink config.
    pass


@dataclass
class _KafkaCallback:
    reporter: SinkReport
    record_envelope: RecordEnvelope
    write_callback: WriteCallback

    def kafka_callback(self, err: Optional[Exception], msg: str) -> None:
        if err is not None:
            self.reporter.report_failure(err)
            self.write_callback.on_failure(
                self.record_envelope, err, {"error": err, "msg": msg}
            )
        else:
            self.reporter.report_record_written(self.record_envelope)
            self.write_callback.on_success(self.record_envelope, {"msg": msg})


@dataclass
class DatahubKafkaSink(Sink):
    config: KafkaSinkConfig
    report: SinkReport
    emitter: DatahubKafkaEmitter

    def __init__(self, config: KafkaSinkConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SinkReport()
        self.emitter = DatahubKafkaEmitter(self.config)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DatahubKafkaSink":
        config = KafkaSinkConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        self.emitter.flush()

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
        if isinstance(record, MetadataChangeEvent):
            self.emitter.emit_mce_async(
                record,
                callback=_KafkaCallback(
                    self.report, record_envelope, write_callback
                ).kafka_callback,
            )
        elif isinstance(
            record, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        ):
            self.emitter.emit_mcp_async(
                record,
                callback=_KafkaCallback(
                    self.report, record_envelope, write_callback
                ).kafka_callback,
            )
        else:
            raise ValueError(
                f"The datahub-kafka sink only supports MetadataChangeEvent/MetadataChangeProposal[Wrapper] classes, not {type(record)}"
            )

    def get_report(self):
        return self.report

    def close(self) -> None:
        self.emitter.flush()
