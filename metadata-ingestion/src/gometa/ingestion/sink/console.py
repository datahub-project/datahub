from gometa.ingestion.api.sink import Sink, WriteCallback
from gometa.ingestion.api.common import RecordEnvelope

class ConsoleSink(Sink):

    def __init__(self):
        self.config = None

    def configure(self, config_dict={}):
        self.config = config_dict
        return self

 
    def write_record_async(self, record_envelope: RecordEnvelope, write_callback: WriteCallback):
        print(record_envelope)
        if write_callback:
            write_callback.on_success(record_envelope, {})
        
    def close(self):
        pass
