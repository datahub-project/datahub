from gometa.ingestion.api.sink import Sink, WriteCallback, SinkReport
from gometa.ingestion.api.common import RecordEnvelope, PipelineContext, WorkUnit
from pydantic import BaseModel
import os
import pathlib
import logging

logger = logging.getLogger(__name__)

class FileSinkConfig(BaseModel):
    output_dir:str = "output"
    file_name:str = "file.out"

class FileSink(Sink):

    def __init__(self, config: FileSinkConfig, ctx):
        super().__init__(ctx)
        self.config = config
        self.report = SinkReport()
        p = pathlib.Path(f'{self.config.output_dir}/{ctx.run_id}/')
        p.mkdir(parents=True)
        fpath = p / self.config.file_name
        logger.info(f'Will write to {fpath}')
        self.file = fpath.open('w')


    @classmethod
    def create(cls, config_dict, ctx: PipelineContext):
        config = FileSinkConfig.parse_obj(config_dict)
        return cls(config, ctx)


    def handle_work_unit_start(self, wu):
        self.id = wu.id

    def handle_work_unit_end(self, wu):
        pass


    def write_record_async(self, record_envelope: RecordEnvelope, write_callback: WriteCallback):
        record_string = str(record_envelope.record)
        metadata = record_envelope.metadata
        metadata["workunit-id"] = self.id
        out_line=f'{{"record": {record_string}, "metadata": {metadata}}}\n'
        self.file.write(out_line)
        self.report.report_record_written(record_envelope)
        write_callback.on_success(record_envelope, {})
    
    def get_report(self):
        return self.report
        
    def close(self):
        if self.file:
            self.file.close()
