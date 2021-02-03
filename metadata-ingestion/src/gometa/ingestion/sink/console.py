from gometa.ingestion.api.sink import Sink, WriteCallback
from gometa.ingestion.api.common import RecordEnvelope
import logging

logger = logging.getLogger(__name__)

class ConsoleSink(Sink):

    def __init__(self):
        self.config = None


    def configure(self, config_dict, ctx, workunit):
        self.config = config_dict
        self.id = workunit.id
        self.run_id = ctx.run_id
        return self

 
    def write_record_async(self, record_envelope: RecordEnvelope, write_callback: WriteCallback):
        logger.info(f'{self.run_id}:{self.id}:{record_envelope}')
        if write_callback:
            write_callback.on_success(record_envelope, {})
        
    def close(self):
        pass
