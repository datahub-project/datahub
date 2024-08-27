from dataclasses import dataclass
from typing import Optional, Union

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


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


class DatahubKafkaSink(Sink[KafkaSinkConfig, SinkReport]):
    emitter: DatahubKafkaEmitter

    def __post_init__(self):
        self.emitter = DatahubKafkaEmitter(self.config)

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
        callback = _KafkaCallback(
            self.report, record_envelope, write_callback
        ).kafka_callback
        try:
            record = record_envelope.record
            self.emitter.emit(
                record,
                callback=callback,
            )
        except Exception as err:
            # In case we throw an exception while trying to emit the record,
            # catch it and report the failure. This might happen if the schema
            # registry is down or otherwise misconfigured, in which case we'd
            # fail when serializing the record.
            callback(err, f"Failed to write record: {err}")

    def close(self) -> None:
        self.emitter.flush()
