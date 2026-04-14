import logging
import threading
from dataclasses import dataclass, field
from typing import Optional, Union

from confluent_kafka import KafkaError, Message

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
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


@dataclass
class _AggregatingKafkaCallback:
    """Aggregates N Kafka delivery callbacks into a single success/failure signal.

    When an MCE is unpacked into N MCPs, each MCP gets its own Kafka delivery
    callback. This class ensures that the upstream WriteCallback fires exactly
    once: on_success after all N deliveries succeed, or on_failure on the first
    failure. This preserves the 1-record-in = 1-callback-out contract that the
    pipeline expects from write_record_async.

    Thread-safe because Kafka delivery callbacks are invoked from librdkafka's
    background thread. Without the lock, concurrent callbacks could both read
    the same _remaining value, causing the inner callback to fire twice or
    never -- unlikely under CPython's GIL, but possible (and reproducible) on
    free-threaded builds (PEP 703) or alternative runtimes.
    """

    total: int
    inner: _KafkaCallback
    _remaining: int = field(init=False)
    _failed: bool = field(init=False, default=False)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)

    def __post_init__(self) -> None:
        self._remaining = self.total

    def kafka_callback(self, err: Optional[KafkaError], msg: Optional[Message]) -> None:
        with self._lock:
            self._remaining -= 1
            if err is not None and not self._failed:
                self._failed = True
                self.inner.kafka_callback(err, msg)
            elif self._remaining == 0 and not self._failed:
                self.inner.kafka_callback(None, msg)


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

            # If the record is an MCE, unpack it into individual MCPs (one per
            # aspect) and emit each to the MCP topic. This matches the REST sink
            # behavior and ensures all aspects are processed by the always-enabled
            # MCP consumer (the MCE consumer is disabled by default).
            if isinstance(record, MetadataChangeEvent):
                logger.debug(
                    f"Unpacking MCE for {record.proposedSnapshot.urn} into individual MCPs"
                )
                # Materialize to a list so we know the count for the
                # aggregating callback and can detect empty-aspect MCEs.
                mcps = list(mcps_from_mce(record))
                if not mcps:
                    logger.warning(
                        f"MCE for {record.proposedSnapshot.urn} produced zero MCPs"
                    )
                    write_callback.on_success(
                        record_envelope, {"msg": "MCE had zero aspects"}
                    )
                    return

                agg_callback = _AggregatingKafkaCallback(
                    total=len(mcps), inner=kafka_callback
                )
                for mcp in mcps:
                    self.emitter.emit(
                        mcp,
                        callback=agg_callback.kafka_callback,
                    )
            else:
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
