from gometa.configuration import ConfigModel, KafkaConnectionConfig
from gometa.ingestion.api.source import Source, Extractor
from gometa.ingestion.api.source import WorkUnit
from typing import Optional, Iterable
from dataclasses import dataclass
import confluent_kafka
import re
from gometa.ingestion.api.closeable import Closeable


class KafkaSourceConfig(ConfigModel):
    connection: KafkaConnectionConfig = KafkaConnectionConfig()
    topic: str = ".*" # default is wildcard subscription


@dataclass
class KafkaWorkUnit(WorkUnit):
    config: KafkaSourceConfig

    def get_metadata(self):
        return self.config.dict() 

@dataclass
class KafkaSource(Source):
    source_config: KafkaSourceConfig
    topic_pattern: re.Pattern
    consumer: confluent_kafka.Consumer

    def __init__(self, config, ctx):
        super().__init__(ctx)
        self.source_config = config
        self.topic_pattern = re.compile(self.source_config.topic)
        self.consumer = confluent_kafka.Consumer({'group.id':'test', 'bootstrap.servers':self.source_config.connection.bootstrap})

    @classmethod
    def create(cls, config_dict, ctx):
        config = KafkaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[KafkaWorkUnit]:
        topics = self.consumer.list_topics().topics
        for t in topics:
            if re.fullmatch(self.topic_pattern, t): 
                # TODO: topics config should support allow and deny patterns
                if not t.startswith("_"):
                    yield KafkaWorkUnit(id=f'kafka-{t}', config=KafkaSourceConfig(connection=self.source_config.connection, topic=t))

        
    def close(self):
        if self.consumer:
            self.consumer.close()

