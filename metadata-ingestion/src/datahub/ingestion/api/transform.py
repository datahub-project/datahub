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
