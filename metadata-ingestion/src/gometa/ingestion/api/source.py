from abc import abstractmethod, ABCMeta
from .closeable import Closeable
from .common import RecordEnvelope

class WorkUnit(metaclass=ABCMeta):
    @abstractmethod
    def get_metadata(self) -> dict:
        pass


class Extractor(Closeable, metaclass=ABCMeta):
    @abstractmethod
    def configure(self, workunit: WorkUnit):
        pass

    @abstractmethod
    def get_records(self) -> RecordEnvelope:
        pass

class Source(Closeable, metaclass = ABCMeta):

    @abstractmethod
    def configure(self, config_dict: dict):
        pass
    
    @abstractmethod
    def get_workunits(self) -> WorkUnit:
        pass




