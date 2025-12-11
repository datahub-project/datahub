# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
