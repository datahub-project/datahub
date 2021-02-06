from typing import Dict
from pydantic import BaseModel
from dataclasses import dataclass, field
from gometa.configuration.common import DynamicTypedConfig, DynamicFactory
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.source import source_class_mapping
from gometa.ingestion.api.common import PipelineContext
from gometa.ingestion.api.sink import Sink, NoopWriteCallback, WriteCallback
from gometa.ingestion.sink import sink_class_mapping
from typing import Optional
import importlib
import time
import logging

logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: Optional[str] = "gometa.ingestion.extractor.generic.WorkUnitMCEExtractor"


class PipelineConfig(BaseModel):
    source: SourceConfig
    sink: DynamicTypedConfig
    run_id: str = str(int(time.time()) * 1000)


class LoggingCallback(WriteCallback):
    def on_success(self, record_envelope, success_meta):
        logger.debug('sink called success callback')

    def on_failure(self, record_envelope, exception, failure_meta):
        logger.exception(f'failed to write {record_envelope.record} with {failure_meta}')


class Pipeline:
    config: PipelineConfig
    ctx: PipelineContext
    source: Optional[Source] = None
    # TODO: make this listing exhaustive

    def get_class_from_name(self, class_string):
        module_name, class_name = class_string.rsplit(".",1)
        MyClass = getattr(importlib.import_module(module_name), class_name)
        return MyClass
    
    def __init__(self, config_dict):
        self.config = PipelineConfig.parse_obj(config_dict)
        self.ctx = PipelineContext(run_id=self.config.run_id)

        source_type = self.config.source.type
        try:
            source_class = source_class_mapping[source_type]
        except KeyError:
            logger.exception(f'Did not find a registered source class for {source_type}')
            raise ValueError("Failed to configure source")
        self.source: Source = source_class.create(self.config.source.dict().get(source_type, {}), self.ctx)
        sink_type = self.config.sink.type
        self.sink_class = sink_class_mapping[sink_type]
        self.sink_config = self.config.dict().get("sink", {"type": "datahub"}).get(sink_type, {})

        # Ensure that sink and extractor can be constructed, even though we use them later
        self.extractor_class = self.get_class_from_name(self.config.source.extractor)

    def run(self):
        callback = LoggingCallback()
        extractor = self.extractor_class()
        SinkClass: Type[Sink] = self.sink_class
        sink = SinkClass.create(self.sink_config, self.ctx)
        for wu in self.source.get_workunits():
            # TODO: change extractor interface
            extractor.configure({}, self.ctx)


            sink.handle_workunit_start(wu)
            logger.warn(f"Configuring sink with workunit {wu.id}")
            for record_envelope in extractor.get_records(wu):
                sink.write_record_async(record_envelope, callback) 
            extractor.close()
            sink.handle_workunit_end(wu)
        sink.close()

        # # TODO: remove this
        # source = Source(...)
        # work_stream = source.get_workunits()

        # extractor = Extractor(...)
        # extracted_stream: Iterable[Tuple[WorkUnit, Iterable[RecordEnvelope]]] = extractor.get_records(work) for work in work_stream

        # sink = Sink(...)
        # for workunit, record_stream in extracted_stream:
        #     associated_sink = sink.with_work_unit(workunit)
        #         for record_envelope in record_stream:
        #             associated_sink.write_record_async(record_envelope)
        #     associated_sink.close()
        # sink.close()
        pass
