from gometa.ingestion.api.sink import Sink, WriteCallback
from gometa.ingestion.api.common import RecordEnvelope, PipelineContext, WorkUnit
from pydantic import BaseModel
import os
import pathlib

class FileSinkConfig(BaseModel):
    output_dir:str = "output"
    file_name:str = "file.out"

class FileSink(Sink):

    def __init__(self):
        self.config = None

    def configure(self, config_dict, ctx: PipelineContext, workunit: WorkUnit):
        self.config = FileSinkConfig.parse_obj(config_dict)
        self.id = workunit.id
        p = pathlib.Path(f'{self.config.output_dir}/{ctx.run_id}/{self.id}')
        p.mkdir(parents=True)
        fpath = p / self.config.file_name
        self.file = fpath.open('w')
        return self


    def write_record_async(self, record_envelope: RecordEnvelope, write_callback: WriteCallback):
        record_string = str(record_envelope.record)
        metadata = record_envelope.metadata
        out_line=f'{{"record": {record_string}, "metadata": {metadata}}}\n'
        self.file.write(out_line)
        if write_callback:
            write_callback.on_success(record_envelope, {})
        
    def close(self):
        if self.file:
            self.file.close()
