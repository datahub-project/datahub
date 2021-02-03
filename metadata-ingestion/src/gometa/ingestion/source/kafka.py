from gometa.configuration import ConfigModel, KafkaConnectionConfig
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.api.source import WorkUnit
from typing import Optional
from dataclasses import dataclass
import confluent_kafka
import re
from gometa.ingestion.api.closeable import Closeable


class KafkaSourceConfig(ConfigModel):
    connection: Optional[KafkaConnectionConfig] = KafkaConnectionConfig()
    topic: Optional[str] = ".*" # default is wildcard subscription


@dataclass
class KafkaWorkUnit(WorkUnit):
    config: KafkaSourceConfig

    def get_metadata(self):
        return self.config.dict() 

class KafkaSource(Source):
    def __init__(self):
        pass

    def configure(self, config_dict: dict):
        self.source_config = KafkaSourceConfig.parse_obj(config_dict)
        self.topic_pattern = re.compile(self.source_config.topic)
        self.consumer = confluent_kafka.Consumer({'group.id':'test', 'bootstrap.servers':self.source_config.connection.bootstrap})
        return self
        
    def get_workunits(self):
        topics = self.consumer.list_topics().topics
        for t in topics:
            if re.fullmatch(self.topic_pattern, t): 
                #TODO: topics config should support allow and deny patterns
                if not t.startswith("_"):
                    yield KafkaWorkUnit(id=f'kafka-{t}', config=KafkaSourceConfig(connection=self.source_config.connection, topic=t))

        
    def close(self):
        if self.consumer:
            self.consumer.close()

