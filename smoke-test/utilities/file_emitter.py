import time

from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig


class FileEmitter:
    def __init__(
        self, filename: str, run_id: str = f"test_{int(time.time() * 1000.0)}"
    ) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id=run_id),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()
