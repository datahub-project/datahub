import logging
from dataclasses import dataclass
from typing import Optional, Union

from confluent_kafka import KafkaError, Message

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)


class KafkaSinkConfig(KafkaEmitterConfig):
    # This extra layer of indirection exists in case we need to add extra
    # config options to the sink config.
    pass


def _enhance_schema_registry_error(error_str: str) -> str:
    """Enhance schema registry 404 errors with actionable guidance."""
    if "Schema Registry" in error_str and "404" in error_str:
        return (
            f"{error_str}\n"
            "HINT: Topic names may not match schema registry. "
            "Use 'topic_routes' in sink config to set correct topic names."
        )
    return error_str


def _log_enhanced_error(error_str: str) -> None:
    """Log enhanced error message if applicable."""
    enhanced_error = _enhance_schema_registry_error(error_str)
    if enhanced_error != error_str:
        logger.error(enhanced_error)


@dataclass
class _KafkaCallback:
    reporter: SinkReport
    record_envelope: RecordEnvelope
    write_callback: WriteCallback

    def kafka_callback(self, err: Optional[KafkaError], msg: Optional[Message]) -> None:
        """
        Kafka delivery callback invoked by confluent-kafka producer.

        Args:
            err: KafkaError object if delivery failed, None on success
            msg: Message object with delivery details
        """
        if err is not None:
            error_str = str(err)
            _log_enhanced_error(error_str)

            error_exception = Exception(error_str)
            self.reporter.report_failure(error_exception)
            self.write_callback.on_failure(
                self.record_envelope,
                error_exception,
                {"error": error_str, "msg": str(msg) if msg else "No message"},
            )
        else:
            self.reporter.report_record_written(self.record_envelope)
            self.write_callback.on_success(
                self.record_envelope, {"msg": str(msg) if msg else "Success"}
            )

    def report_emit_failure(self, err: Exception) -> None:
        """
        Report a failure that occurred during emit (before Kafka delivery).

        This is separate from kafka_callback which handles delivery failures.
        """
        error_str = str(err)
        _log_enhanced_error(error_str)

        self.reporter.report_failure(err)
        self.write_callback.on_failure(
            self.record_envelope,
            err,
            {"error": error_str, "msg": f"Failed to write record: {err}"},
        )


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
        kafka_callback = _KafkaCallback(self.report, record_envelope, write_callback)
        try:
            record = record_envelope.record
            self.emitter.emit(
                record,
                callback=kafka_callback.kafka_callback,
            )
        except Exception as err:
            # In case we throw an exception while trying to emit the record,
            # catch it and report the failure. This might happen if the schema
            # registry is down or otherwise misconfigured, in which case we'd
            # fail when serializing the record.
            kafka_callback.report_emit_failure(err)

    def close(self) -> None:
        super().close()
        self.emitter.flush()
