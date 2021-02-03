from pydantic import BaseModel
from dataclasses import dataclass, field
from gometa.configuration.common import DynamicTypedConfig, DynamicFactory
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.api.common import PipelineContext
from gometa.ingestion.api.sink import Sink, NoopWriteCallback, WriteCallback
from typing import Optional
import importlib
import time
import logging
logging.basicConfig(format='%(name)s [%(levelname)s] %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: Optional[str] = "gometa.ingestion.extractor.generic.WorkUnitMCEExtractor"


class PipelineConfig(BaseModel):
    source: SourceConfig
    sink: DynamicTypedConfig
    run_id: str = str(int(time.time()) * 1000)


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
        "mssql": "gometa.ingestion.source.mssql.SQLServerSource",
        "mysql": "gometa.ingestion.source.mysql.MySQLSource",
        "kafka": "gometa.ingestion.source.kafka.KafkaSource",
        "ldap" : "gometa.ingestion.source.ldap.LdapSource",
    })
    sink_class_mapping: Optional[dict] = field(default_factory = lambda: {
        "kafka": "gometa.ingestion.sink.kafka.KafkaSink",
        "datahub": "gometa.ingestion.sink.datahub.DataHubSink",
        "console": "gometa.ingestion.sink.console.ConsoleSink",
        "file": "gometa.ingestion.sink.file.FileSink",
    })

    def get_class_from_name(self, class_string):
        module_name, class_name = class_string.rsplit(".",1)
        MyClass = getattr(importlib.import_module(module_name), class_name)
        return MyClass()


    def configure(self, config_dict):
        self.source_factory = DynamicFactory()
        self.config = PipelineConfig.parse_obj(config_dict)
        source_type = self.config.source.type
        try:
            source_class  = self.source_class_mapping[source_type]
        except KeyError:
            logger.exception(f'Did not find a registered source class for {source_type}')
            raise ValueError("Failed to configure source")
        self.source = self.get_class_from_name(source_class)
        self.source.configure(self.config.dict().get("source", {}).get(source_type, {}))
        sink_type = self.config.sink.type
        self.sink_class = self.sink_class_mapping[sink_type]
        self.sink_config = self.config.dict().get("sink", {"type": "datahub"}).get(sink_type, {})

        # Ensure that sink and extractor can be constructed, even though we use them later
        extractor = self.get_class_from_name(self.config.source.extractor)
        sink = self.get_class_from_name(self.sink_class)

        self.ctx = PipelineContext(run_id=self.config.run_id)
        return self


    def run(self):
        callback = LoggingCallback()
        for wu in self.source.get_workunits():
            extractor = self.get_class_from_name(self.config.source.extractor)
            extractor.configure(wu)
            sink = self.get_class_from_name(self.sink_class)
            logger.warn(f"Configuring sink with workunit {wu.id}")
            sink.configure(self.sink_config, self.ctx, wu)
            for record_envelope in extractor.get_records():
                sink.write_record_async(record_envelope, callback) 
            extractor.close()
            sink.close()
        
