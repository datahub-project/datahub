from gometa.ingestion.api.sink import Sink, WriteCallback, SinkReport
from gometa.ingestion.api.common import RecordEnvelope, PipelineContext
from pydantic import BaseModel
import pathlib
import logging
import json
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

logger = logging.getLogger(__name__)


class FileSinkConfig(BaseModel):
    filename: str


class FileSink(Sink):
    config: FileSinkConfig
    report: SinkReport

    def __init__(self, ctx: PipelineContext, config: FileSinkConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SinkReport()

        fpath = pathlib.Path(self.config.filename)
        logger.info(f'Will write to {fpath}')
        self.file = fpath.open('w')
        self.file.write('[\n')
        self.wrote_something = False

    @classmethod
    def create(cls, config_dict, ctx: PipelineContext):
        config = FileSinkConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def handle_work_unit_start(self, wu):
        self.id = wu.id

    def handle_work_unit_end(self, wu):
        pass

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[MetadataChangeEvent],
        write_callback: WriteCallback,
    ):
        mce = record_envelope.record
        obj = mce.to_obj()

        if self.wrote_something:
            self.file.write(',\n')

        json.dump(obj, self.file, indent=4)
        self.wrote_something = True

        # record_string = str(record_envelope.record)
        # metadata = record_envelope.metadata
        # metadata["workunit-id"] = self.id
        # out_line=f'{{"record": {record_string}, "metadata": {metadata}}}\n'
        self.report.report_record_written(record_envelope)
        write_callback.on_success(record_envelope, {})

    def get_report(self):
        return self.report

    def close(self):
        self.file.write('\n]')
        self.file.close()
