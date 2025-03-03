import logging
import threading
from typing import Optional

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import WriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import MetadataChangeProposalClass

logger = logging.getLogger(__name__)


class LoggingCallback(WriteCallback):
    def __init__(self, name: str = "") -> None:
        super().__init__()
        self.name = name

    def on_success(
        self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        logger.debug(
            f"{self.name} sink wrote workunit {record_envelope.metadata['workunit_id']}"
        )

    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ) -> None:
        logger.error(
            f"{self.name} failed to write record with workunit {record_envelope.metadata['workunit_id']}",
            extra={"failure_metadata": failure_metadata},
            exc_info=failure_exception,
        )


class DeadLetterQueueCallback(WriteCallback, Closeable):
    def __init__(self, ctx: PipelineContext, config: Optional[FileSinkConfig]) -> None:
        if not config:
            config = FileSinkConfig.parse_obj({"filename": "failed_events.json"})
        self.file_sink: FileSink = FileSink(ctx, config)
        self.file_sink_lock = threading.Lock()
        self.logging_callback = LoggingCallback(name="failure-queue")
        logger.info(f"Failure logging enabled. Will log to {config.filename}.")

    def on_success(
        self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        pass

    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ) -> None:
        if "workunit_id" in record_envelope.metadata and isinstance(
            record_envelope.record, MetadataChangeProposalClass
        ):
            mcp: MetadataChangeProposalClass = record_envelope.record
            if mcp.systemMetadata:
                if not mcp.systemMetadata.properties:
                    mcp.systemMetadata.properties = {}
                if "workunit_id" not in mcp.systemMetadata.properties:
                    # update the workunit id
                    mcp.systemMetadata.properties["workunit_id"] = (
                        record_envelope.metadata["workunit_id"]
                    )
            record_envelope.record = mcp
        with self.file_sink_lock:
            self.file_sink.write_record_async(record_envelope, self.logging_callback)

    def close(self) -> None:
        with self.file_sink_lock:
            self.file_sink.close()
