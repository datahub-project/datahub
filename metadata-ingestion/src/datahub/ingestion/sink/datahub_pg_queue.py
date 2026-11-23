"""Sink that writes MCP/MCE metadata to PostgreSQL pgQueue (Kafka-compatible payloads)."""

from __future__ import annotations

import logging
from typing import Union

from datahub.emitter.kafka_emitter import _AggregatingKafkaCallback
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.sink.datahub_kafka import _KafkaCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.pgqueue.config import PgQueueEmitterConfig
from datahub.pgqueue.emitter import DatahubPgQueueEmitter
from datahub.utilities.deprecation import called_from_datahub, warn_deprecated

logger = logging.getLogger(__name__)

_MCE_SINK_DEPRECATION_MESSAGE = (
    "Writing a MetadataChangeEvent to the datahub-pg-queue sink "
    "(DatahubPgQueueSink.write_record_async) "
    "is deprecated and will be removed in a future release. "
    "Use MetadataChangeProposalWrapper instead."
)


class PgQueueSinkConfig(PgQueueEmitterConfig):
    """Recipe configuration mirroring ``KafkaEmitterConfig`` fields plus pgQueue connectivity."""

    pass


class DatahubPgQueueSink(Sink[PgQueueSinkConfig, SinkReport]):
    emitter: DatahubPgQueueEmitter

    def __post_init__(self) -> None:
        self.emitter = DatahubPgQueueEmitter(self.config)

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
        if (
            isinstance(record_envelope.record, MetadataChangeEvent)
            and not called_from_datahub()
        ):
            warn_deprecated(_MCE_SINK_DEPRECATION_MESSAGE)
        queue_callback = _KafkaCallback(self.report, record_envelope, write_callback)
        try:
            record = record_envelope.record

            if isinstance(record, MetadataChangeEvent):
                logger.debug(
                    "Unpacking MCE for %s into individual MCPs",
                    record.proposedSnapshot.urn,
                )
                mcps = list(mcps_from_mce(record))
                if not mcps:
                    logger.warning(
                        "MCE for %s produced zero MCPs",
                        record.proposedSnapshot.urn,
                    )
                    write_callback.on_success(
                        record_envelope, {"msg": "MCE had zero aspects"}
                    )
                    return

                agg_callback = _AggregatingKafkaCallback(
                    total=len(mcps), inner=queue_callback.kafka_callback
                )
                self.emitter.emit_batch(mcps, callback=agg_callback.kafka_callback)
            else:
                self.emitter.emit(record, callback=queue_callback.kafka_callback)
        except Exception as err:
            queue_callback.report_emit_failure(err)

    def close(self) -> None:
        try:
            self.emitter.close()
        finally:
            super().close()
