from abc import abstractmethod, ABCMeta

from gometa.ingestion.api.common import RecordEnvelope, WorkUnit, PipelineContext


class WriteCallback:

    @abstractmethod
    def on_success(self, record_envelope: RecordEnvelope, success_metadata: dict):
        pass

    @abstractmethod
    def on_failure(self, record_envelope: RecordEnvelope, failure_exception: Exception, failure_metadata: dict):
        pass

class NoopWriteCallback(WriteCallback):
    """Convenience class to support noop"""
    def on_success(self, re, sm):
        pass
    
    def on_failure(self, re, fe, fm):
        pass


class Sink(metaclass=ABCMeta):
    """All Sinks must inherit this base class"""
    @abstractmethod
    def configure(self, config_dict:dict, ctx: PipelineContext, workunit: WorkUnit):
        pass
    
    @abstractmethod
    def write_record_async(self, record_envelope: RecordEnvelope, callback: WriteCallback):
        pass

    @abstractmethod
    def close(self):
        pass
