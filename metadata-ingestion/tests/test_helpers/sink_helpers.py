from typing import List

from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.run.pipeline import PipelineContext


class RecordingSinkReport(SinkReport):
    received_records: List[RecordEnvelope] = []

    def report_record_written(self, record_envelope: RecordEnvelope) -> None:
        super().report_record_written(record_envelope)
        self.received_records.append(record_envelope)


class RecordingSink(Sink):
    def __init__(self):
        self.sink_report = RecordingSinkReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Sink":
        return cls()

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    def write_record_async(
        self, record_envelope: RecordEnvelope, callback: WriteCallback
    ) -> None:
        self.sink_report.report_record_written(record_envelope)
        callback.on_success(record_envelope, {})

    def get_report(self) -> SinkReport:
        return self.sink_report

    def close(self) -> None:
        pass
