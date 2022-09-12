from typing import List

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback


class RecordingSinkReport(SinkReport):
    received_records: List[RecordEnvelope] = []

    def report_record_written(self, record_envelope: RecordEnvelope) -> None:
        super().report_record_written(record_envelope)
        self.received_records.append(record_envelope)


class RecordingSink(Sink[ConfigModel, RecordingSinkReport]):
    def write_record_async(
        self, record_envelope: RecordEnvelope, callback: WriteCallback
    ) -> None:
        self.report.report_record_written(record_envelope)
        callback.on_success(record_envelope, {})
