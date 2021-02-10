from dataclasses import dataclass
from typing import TypeVar, Generic, Optional
from abc import abstractmethod, ABCMeta

T = TypeVar('T')

@dataclass
class RecordEnvelope(Generic[T]):
    record: T 
    metadata: dict

@dataclass
class _WorkUnitId(metaclass=ABCMeta):
    id: str

# For information on why the WorkUnit class is structured this way
# and is separating the dataclass portion from the abstract methods, see
# https://github.com/python/mypy/issues/5374#issuecomment-568335302.
class WorkUnit(_WorkUnitId, metaclass=ABCMeta):
    @abstractmethod
    def get_metadata(self) -> dict:
        pass

@dataclass
class PipelineContext:
    run_id: str

