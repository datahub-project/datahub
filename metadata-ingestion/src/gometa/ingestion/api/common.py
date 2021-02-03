from dataclasses import dataclass
from typing import TypeVar, Generic, Optional
from abc import abstractmethod, ABCMeta

T = TypeVar('T')

@dataclass
class RecordEnvelope(Generic[T]):
    record: T 
    metadata: Optional[dict]


@dataclass
class WorkUnit(metaclass=ABCMeta):
    id: str

    @abstractmethod
    def get_metadata(self) -> dict:
        pass

@dataclass
class PipelineContext:
    run_id: str

