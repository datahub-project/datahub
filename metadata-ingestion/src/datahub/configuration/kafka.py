import re

from pydantic import Field, validator

from datahub.configuration.common import ConfigModel


class _KafkaConnectionConfig(ConfigModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8081"

    # Extra schema registry config.
    # These options will be passed into Kafka's SchemaRegistryClient.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html?highlight=schema%20registry#schemaregistryclient.
    schema_registry_config: dict = Field(default_factory=dict)

    @validator("bootstrap")
    def bootstrap_host_colon_port_comma(cls, val: str) -> str:
        for entry in val.split(","):
            # The port can be provided but is not required.
            port = None
            if ":" in entry:
                (host, port) = entry.rsplit(":", 1)
            else:
                host = entry
            assert re.match(
                # This regex is quite loose. Many invalid hostnames or IPs will slip through,
                # but it serves as a good first line of validation. We defer to Kafka for the
                # remaining validation.
                r"^[\w\-\.\:]+$",
                host,
            ), f"host contains bad characters, found {host}"
            if port is not None:
                assert port.isdigit(), f"port must be all digits, found {port}"
        return val


class KafkaConsumerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka consumers"""

    # Extra consumer config.
    # These options will be passed into Kafka's DeserializingConsumer.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#deserializingconsumer
    # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    consumer_config: dict = Field(default_factory=dict)


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    # Extra producer config.
    # These options will be passed into Kafka's SerializingProducer.
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/index.html#serializingproducer
    # and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    producer_config: dict = Field(default_factory=dict)
