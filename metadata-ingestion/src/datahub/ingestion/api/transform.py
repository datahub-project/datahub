# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from abc import abstractmethod
from typing import Iterable

from datahub.ingestion.api.common import PipelineContext, RecordEnvelope


class Transformer:
    @abstractmethod
    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        """
        Transforms a sequence of records.
        :param records: the records to be transformed
        :return: 0 or more transformed records
        """

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        pass
