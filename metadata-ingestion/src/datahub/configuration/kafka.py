from pydantic import Field, validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.validate_host_port import validate_host_port


class _KafkaConnectionConfig(ConfigModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8081"

    # Extra schema registry config.
    # These options will be passed into Kafka's SchemaRegistryClient.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient
    schema_registry_config: dict = Field(default_factory=dict)

    @validator("bootstrap")
    def bootstrap_host_colon_port_comma(cls, val: str) -> str:
        for entry in val.split(","):
            validate_host_port(entry)
        return val


class KafkaConsumerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka consumers"""

    # Extra consumer config.
    # These options will be passed into Kafka's DeserializingConsumer.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer
    # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    consumer_config: dict = Field(default_factory=dict)


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    # Extra producer config.
    # These options will be passed into Kafka's SerializingProducer.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer
    # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    producer_config: dict = Field(default_factory=dict)
