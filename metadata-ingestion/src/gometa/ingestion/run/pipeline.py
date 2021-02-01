from pydantic import BaseModel
from dataclasses import dataclass, field
from gometa.configuration.common import DynamicTypedConfig, DynamicFactory
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.api.sink import Sink, NoopWriteCallback, WriteCallback
from typing import Optional
import importlib
import logging
logging.basicConfig(format='%(name)s [%(levelname)s] %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: str
    

class PipelineConfig(BaseModel):
    source: SourceConfig
    sink: DynamicTypedConfig


class LoggingCallback(WriteCallback):
    def on_success(self, record_envelope, success_meta):
        logger.debug(f'successfully wrote {record_envelope.record}')

    def on_failure(self, record_envelope, exception, failure_meta):
        logger.exception(f'failed to write {record_envelope.record} with {failure_meta}')
    
@dataclass
class Pipeline:
    source: Optional[Source] = None
    extractor: Optional[Extractor] = None
    sink: Optional[Sink] = None
    source_class_mapping: Optional[dict] = field(default_factory = lambda: {
        "kafka": "gometa.ingestion.source.kafka.KafkaSource",
        "ldap" : "gometa.ingestion.source.ldap.LdapSource",
    })
    sink_class_mapping: Optional[dict] = field(default_factory = lambda: {
        "kafka": "gometa.ingestion.sink.kafka.KafkaSink",
        "datahub": "gometa.ingestion.sink.datahub.DataHubSink",
        "console": "gometa.ingestion.sink.console.ConsoleSink",
    })

    def get_class_from_name(self, class_string):
        module_name, class_name = class_string.rsplit(".",1)
        MyClass = getattr(importlib.import_module(module_name), class_name)
        return MyClass()
         
    def configure(self, config_dict):
        self.source_factory = DynamicFactory()
        self.config = PipelineConfig.parse_obj(config_dict)
        source_type = self.config.source.type
        source_class = self.source_class_mapping[source_type] 
        self.source = self.get_class_from_name(source_class)
        self.source.configure(self.config.dict().get("source", {}).get(source_type, {}))
        sink_type = self.config.sink.type
        sink_class = self.sink_class_mapping[sink_type]
        self.sink = self.get_class_from_name(sink_class)
        self.sink.configure(self.config.dict().get("sink", {"type": "datahub"}).get(sink_type, {}))
        return self

    def run(self):
        callback = LoggingCallback()
        for w in self.source.get_workunits():
            extractor = self.get_class_from_name(self.config.source.extractor).configure(w)
            for record_envelope in extractor.get_records():
                self.sink.write_record_async(record_envelope, callback) 
            extractor.close()
            self.sink.close()
        
