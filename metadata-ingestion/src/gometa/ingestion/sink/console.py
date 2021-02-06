from gometa.ingestion.api.sink import Sink, WriteCallback
from gometa.ingestion.api.common import RecordEnvelope
import logging

logger = logging.getLogger(__name__)

class ConsoleSink(Sink):

    @classmethod
    def create(cls, config_dict, ctx):
        return cls(ctx)

 
    def write_record_async(self, record_envelope: RecordEnvelope, write_callback: WriteCallback):
        print(f'{self.ctx.run_id}:{record_envelope}')
        if write_callback:
            write_callback.on_success(record_envelope, {})
        
    def close(self):
        pass
