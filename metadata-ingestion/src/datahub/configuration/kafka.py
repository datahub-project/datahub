from pydantic import validator

from datahub.configuration.common import ConfigModel


class _KafkaConnectionConfig(ConfigModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8081"

    # Extra schema registry config.
    # These options will be passed into Kafka's SchemaRegistryClient.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html?highlight=schema%20registry#schemaregistryclient.
    schema_registry_config: dict = {}

    @validator("bootstrap")
    def bootstrap_host_colon_port_comma(cls, val):
        for entry in val.split(","):
            assert ":" in entry, f"entry must be of the form host:port, found {entry}"
            (host, port) = entry.split(":")
            assert host.isalnum(), f"host must be alphanumeric, found {host}"
            assert port.isdigit(), f"port must be all digits, found {port}"


class KafkaConsumerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka consumers"""

    # Extra consumer config.
    # These options will be passed into Kafka's DeserializingConsumer.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#deserializingconsumer
    # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    consumer_config: dict = {}


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    # Extra producer config.
    # These options will be passed into Kafka's SerializingProducer.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#serializingproducer
    # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    producer_config: dict = {}
