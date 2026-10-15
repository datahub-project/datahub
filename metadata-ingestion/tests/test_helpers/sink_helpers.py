from dataclasses import dataclass, field
from typing import List

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback


@dataclass
class RecordingSinkReport(SinkReport):
    # A per-instance list: a class-level default would be shared by every
    # pipeline in the test process and leak records between tests.
    received_records: List[RecordEnvelope] = field(default_factory=list)

    def report_record_written(self, record_envelope: RecordEnvelope) -> None:
        super().report_record_written(record_envelope)
        self.received_records.append(record_envelope)


class RecordingSink(Sink[ConfigModel, RecordingSinkReport]):
    def write_record_async(
        self, record_envelope: RecordEnvelope, callback: WriteCallback
    ) -> None:
        self.report.report_record_written(record_envelope)
        callback.on_success(record_envelope, {})
