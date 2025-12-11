# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback

logger = logging.getLogger(__name__)


class ConsoleSink(Sink[ConfigModel, SinkReport]):
    def write_record_async(
        self, record_envelope: RecordEnvelope, write_callback: WriteCallback
    ) -> None:
        print(f"{record_envelope}")
        if write_callback:
            self.report.report_record_written(record_envelope)
            write_callback.on_success(record_envelope, {})
