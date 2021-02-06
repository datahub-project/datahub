from typing import Iterable
from dataclasses import dataclass
from abc import abstractmethod, ABCMeta
from .closeable import Closeable
from .common import WorkUnit, PipelineContext, RecordEnvelope


class Extractor(Closeable, metaclass=ABCMeta):
    @abstractmethod
    def configure(self, config_dict: dict, ctx: PipelineContext):
        pass

    @abstractmethod
    def get_records(self, workunit: WorkUnit) -> Iterable[RecordEnvelope]:
        pass

# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class Source(Closeable, metaclass = ABCMeta):
    ctx: PipelineContext

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> 'Source':
        pass

    @abstractmethod
    def get_workunits(self) -> Iterable[WorkUnit]:
        pass
