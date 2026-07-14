import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Optional, Union, cast

from confluent_kafka import KafkaError, Message
from pydantic import Field

from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.kafka_emitter import (
    DatahubKafkaEmitter,
    KafkaEmitterConfig,
    MessageTooLargeError,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.emitter.rest_emitter import DataHubRestEmitter, EmitMode
from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.sink.datahub_rest import (
    DatahubRestSink,
    DatahubRestSinkConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import ChangeTypeClass

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# Change types GMS accepts over async Kafka ingestion for non-timeseries
# aspects: MCPItem.CHANGE_TYPES (UPSERT/UPDATE/CREATE/CREATE_ENTITY) plus PATCH.
# Timeseries aspects (UPSERT-only) are handled in _needs_rest_fallback.
_KAFKA_SUPPORTED_CHANGE_TYPES = frozenset(
    {
        ChangeTypeClass.UPSERT,
        ChangeTypeClass.UPDATE,
        ChangeTypeClass.CREATE,
        ChangeTypeClass.CREATE_ENTITY,
        ChangeTypeClass.PATCH,
    }
)


class KafkaSinkConfig(KafkaEmitterConfig):
    rest_fallback: Optional[DatahubRestSinkConfig] = Field(
        default=None,
        description=(
            "Optional REST sink configuration used as a fallback for change types "
            "that are not supported over async Kafka ingestion (DELETE, RESTATE) -- "
            "GMS rejects these on the Kafka path (see MCPItem.CHANGE_TYPES). When set, "
            "such MCPs are emitted synchronously via this REST endpoint while all other "
            "change types are produced to Kafka. When unset, the sink is pure Kafka and "
            "DELETE/RESTATE MCPs will fail downstream in GMS."
        ),
    )


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
    # Set on any delivery/emit failure. Shared across a sink's callbacks so the
    # sink can refuse to apply a synchronous DELETE/RESTATE after an earlier
    # async write failed in the same run. Invoked from librdkafka's background
    # thread, so a threading.Event (thread-safe) is used.
    failure_signal: Optional[threading.Event] = None

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

            if self.failure_signal is not None:
                self.failure_signal.set()
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

        if self.failure_signal is not None:
            self.failure_signal.set()
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


@dataclass
class KafkaSinkReport(SinkReport):
    # Backpressure is the sink throttling the source to Kafka's sustainable
    # rate; without these it's invisible in the run report (only warns after
    # ~30 polls). undelivered = messages the producer never delivered (dropped).
    kafka_backpressure_engagements: int = 0
    kafka_backpressure_blocked_seconds: float = 0.0
    kafka_undelivered: int = 0


class DatahubKafkaSink(Sink[KafkaSinkConfig, KafkaSinkReport]):
    emitter: DatahubKafkaEmitter

    def __post_init__(self):
        self.emitter = DatahubKafkaEmitter(self.config)
        # Built lazily on first fallback use so pure-Kafka deployments never
        # construct a REST emitter (and never require REST connectivity). Guarded
        # by a lock since write_record_async may run on multiple threads.
        self._rest_fallback_emitter: Optional[DataHubRestEmitter] = None
        self._rest_fallback_lock = threading.Lock()
        self._delivery_failed = threading.Event()
        # Whether flush() has run (pipeline calls it pre-commit). close() skips a
        # second flush when so, to avoid double-reporting the same undelivered
        # messages. close() still flushes on paths that skip the pre-commit flush
        # (e.g. an exception before process_commits).
        self._flushed = False

    def _get_rest_fallback_emitter(self) -> DataHubRestEmitter:
        if self._rest_fallback_emitter is None:
            with self._rest_fallback_lock:
                if self._rest_fallback_emitter is None:
                    assert self.config.rest_fallback is not None
                    self._rest_fallback_emitter = DatahubRestSink.make_emitter(
                        self.config.rest_fallback
                    )
        return self._rest_fallback_emitter

    def _emit_mcp_via_rest_fallback(
        self,
        record: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        delivery_callback: Callable[[Optional[KafkaError], Optional[Message]], None],
    ) -> None:
        """Degrade an oversize MCP (MessageTooLargeError) to the REST fallback.

        REST accepts payloads up to ~16 MiB vs the Kafka topic's ~5 MiB cap, so an
        aspect REST would accept must not fail the run purely because it was routed
        to the async sink. Synchronous SYNC_PRIMARY emit (mirrors DELETE/RESTATE),
        then ticks the original delivery callback so the aggregating-callback
        invariant (N callbacks = 1 write_callback) holds for MCE-unpacked MCPs and
        the single-MCP path alike. Caller guarantees rest_fallback is configured.
        """
        try:
            self._get_rest_fallback_emitter().emit_mcp(
                record, emit_mode=EmitMode.SYNC_PRIMARY
            )
        except Exception as rest_err:
            # The callback wraps err in Exception(str(err)) and fires on_failure;
            # cast is a type-only narrowing for the KafkaError-typed parameter.
            delivery_callback(cast(Optional[KafkaError], rest_err), None)
            return
        delivery_callback(None, None)

    def to_graph(self) -> Optional["DataHubGraph"]:
        """DataHubGraph derived from the REST fallback, for features that need a
        GMS client (e.g. stateful ingestion) when this is the default sink.

        Returns None when no REST fallback is configured (pure Kafka), matching
        the pipeline's expectation that a missing graph disables such features.
        """
        if self.config.rest_fallback is None:
            return None
        return self._get_rest_fallback_emitter().to_graph()

    @staticmethod
    def _needs_rest_fallback(
        record: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
    ) -> bool:
        """Whether an MCP must be sent over REST instead of async Kafka.

        Allow-list based: only change types GMS accepts on the async path stay
        on Kafka; anything else (DELETE, RESTATE, any future/unknown change type)
        goes via REST. Additionally, timeseries aspects accept only UPSERT, so
        any non-UPSERT timeseries change (e.g. a PATCH) also goes via REST.
        """
        if record.aspectName in TIMESERIES_ASPECT_MAP:
            return record.changeType != ChangeTypeClass.UPSERT
        return record.changeType not in _KAFKA_SUPPORTED_CHANGE_TYPES

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        # Intentionally do NOT flush per work unit. A work unit is ~one record,
        # so flushing here forces a synchronous broker round-trip per record --
        # destroying async batching and making linger.ms a no-op. Delivery is
        # driven by poll(0) before each produce and guaranteed by the flush in
        # close() (which also surfaces any undelivered messages as a failure);
        # the bounded queue + block-on-full backpressure cap memory in between.
        pass

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
        kafka_callback = _KafkaCallback(
            self.report,
            record_envelope,
            write_callback,
            failure_signal=self._delivery_failed,
        )
        try:
            record = record_envelope.record

            # Change types not supported over async Kafka ingestion (DELETE,
            # RESTATE, ...) are rejected by GMS. Route them synchronously through
            # the configured REST fallback; otherwise they fall through to Kafka
            # and would fail downstream. If the REST emit raises, the outer except
            # reports the failure via kafka_callback.
            if isinstance(
                record, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            ) and self._needs_rest_fallback(record):
                if self.config.rest_fallback is None:
                    # Not supported over async Kafka and no REST fallback to
                    # degrade to -> fail the record loudly instead of producing a
                    # message GMS would silently reject on the consumer side.
                    kafka_callback.report_emit_failure(
                        Exception(
                            f"{record.changeType} on {record.entityUrn} is not "
                            "supported over async Kafka ingestion and no "
                            "rest_fallback is configured; set a rest_fallback to "
                            "emit these (DELETE/RESTATE/non-UPSERT timeseries)."
                        )
                    )
                    return
                # Drain in-flight async Kafka messages before the synchronous
                # REST delete, so earlier writes for the same entity reach the
                # broker first. NOTE: this is best-effort ordering -- it flushes
                # the local producer queue, but does not wait for the MCP
                # consumer to apply those events, so a REST DELETE can still
                # reach GMS before an earlier Kafka UPSERT is replayed.
                # DELETE-heavy recipes should be aware of this.
                try:
                    undelivered = self.emitter.flush_with_undelivered_count()
                except Exception as flush_err:
                    kafka_callback.report_emit_failure(
                        Exception(
                            f"Kafka flush failed before {record.changeType} on "
                            f"{record.entityUrn}: {flush_err}"
                        )
                    )
                    return
                # Do NOT apply the DELETE/RESTATE if a preceding write failed or
                # is unconfirmed -- that would delete/restate an entity whose
                # write never landed. Fail this record instead; the run already
                # fails via the earlier failure. Conservative (any prior failure
                # blocks any fallback), safe since such MCPs are rare and the run
                # is aborting regardless.
                if undelivered or self._delivery_failed.is_set():
                    kafka_callback.report_emit_failure(
                        Exception(
                            f"Skipping REST fallback for {record.changeType} on "
                            f"{record.entityUrn}: prior Kafka writes not confirmed "
                            f"(undelivered={undelivered}, "
                            f"delivery_failed={self._delivery_failed.is_set()})."
                        )
                    )
                    return
                # Emit synchronously (SYNC_PRIMARY) rather than the emitter's
                # env-driven default: a DELETE/RESTATE must be durably applied by
                # GMS, not left async where DATAHUB_EMIT_MODE=async could void the
                # ordering intent entirely.
                self._get_rest_fallback_emitter().emit_mcp(
                    record, emit_mode=EmitMode.SYNC_PRIMARY
                )
                self.report.report_record_written(record_envelope)
                write_callback.on_success(
                    record_envelope,
                    {"msg": f"Emitted {record.changeType} via REST fallback"},
                )
                return

            # If the record is an MCE, unpack it into individual MCPs (one per
            # aspect) and emit each to the MCP topic. This matches the REST sink
            # behavior and ensures all aspects are processed by the always-enabled
            # MCP consumer (the MCE consumer is disabled by default).
            if isinstance(record, MetadataChangeEvent):
                logger.debug(
                    f"Unpacking MCE for {record.proposedSnapshot.urn} into individual MCPs"
                )
                # Unpacked MCPs go straight to Kafka without _needs_rest_fallback:
                # an MCE is an entity snapshot, whose aspects are always UPSERTs
                # and never timeseries aspects, so it structurally cannot carry a
                # change type that needs the REST fallback.
                #
                # Materialize to a list so we know the count for the
                # aggregating callback and can detect empty-aspect MCEs. Bounded
                # by a single entity's aspect count (small; large only for very
                # lineage/schema-heavy datasets), not the whole stream.
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
                    try:
                        self.emitter.emit(
                            mcp,
                            callback=agg_callback.kafka_callback,
                        )
                    except MessageTooLargeError:
                        # Oversize aspect: degrade to REST fallback for parity with
                        # the REST sink (~16 MiB cap vs Kafka's ~5 MiB). Tick the
                        # aggregating callback as if this MCP delivered, so the
                        # N-callbacks = 1-write_callback invariant still holds.
                        if self.config.rest_fallback is None:
                            # Tick the agg as a failure rather than raise: raise
                            # would hit the outer except and fire on_failure now,
                            # while earlier-produced MCPs' async delivery callbacks
                            # tick the agg later -> double write_callback. Ticking
                            # marks the agg failed so later real deliveries are
                            # ignored and on_failure fires exactly once.
                            agg_callback.kafka_callback(
                                Exception(
                                    f"Oversize MCP for {mcp.entityUrn} exceeds "
                                    "Kafka message.max.bytes and no rest_fallback "
                                    "is configured."
                                ),
                                None,
                            )
                            return
                        self._emit_mcp_via_rest_fallback(
                            mcp, agg_callback.kafka_callback
                        )
            else:
                try:
                    self.emitter.emit(
                        record,
                        callback=kafka_callback.kafka_callback,
                    )
                except MessageTooLargeError:
                    # Oversize aspect: degrade to REST fallback for parity with the
                    # REST sink (~16 MiB cap vs Kafka's ~5 MiB).
                    if self.config.rest_fallback is None:
                        raise
                    self._emit_mcp_via_rest_fallback(
                        record, kafka_callback.kafka_callback
                    )
        except Exception as err:
            # emit() can raise before any async delivery -- e.g. Avro
            # serialization hitting an unreachable/misconfigured schema registry.
            kafka_callback.report_emit_failure(err)

    def flush(self) -> None:
        # Drain the producer and surface any undelivered messages as a failure.
        # Called by the pipeline before process_commits (so checkpoints are not
        # committed for writes that never landed) and again from close(). Any
        # messages still undelivered are lost on process exit, so reporting them
        # is what prevents a silent success-with-data-loss.
        undelivered = self.emitter.flush_with_undelivered_count()
        # Surface backpressure + drops in the run report, not just logs.
        self.report.kafka_backpressure_engagements = (
            self.emitter.backpressure_engagements
        )
        self.report.kafka_backpressure_blocked_seconds = (
            self.emitter.backpressure_blocked_seconds
        )
        if undelivered:
            self.report.kafka_undelivered += undelivered
            self.report.report_failure(
                f"{undelivered} message(s) not delivered to Kafka "
                "(broker unreachable?); this metadata was dropped."
            )
        # Mark flushed only after the drain completes, so a raising flush does
        # not cause close() to skip its safety-net flush.
        self._flushed = True

    def close(self) -> None:
        super().close()
        # Skip if the pipeline already flushed pre-commit, to avoid
        # double-reporting the same undelivered messages.
        if not self._flushed:
            self.flush()
        if self._rest_fallback_emitter is not None:
            self._rest_fallback_emitter.close()
