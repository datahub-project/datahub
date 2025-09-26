import os

from pydantic import Field, validator

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.configuration.kafka_consumer_config import CallableConsumerConfig
from datahub.configuration.validate_host_port import validate_host_port


def _get_schema_registry_url() -> str:
    """Get schema registry URL with proper base path handling."""
    explicit_url = os.getenv("KAFKA_SCHEMAREGISTRY_URL")
    if explicit_url:
        return explicit_url

    base_path = os.getenv("DATAHUB_GMS_BASE_PATH", "")
    if base_path in ("/", ""):
        base_path = ""

    return f"http://localhost:8080{base_path}/schema-registry/api/"


class _KafkaConnectionConfig(ConfigModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = Field(
        default_factory=_get_schema_registry_url,
        description="Schema registry URL. Can be overridden with KAFKA_SCHEMAREGISTRY_URL environment variable, or will use DATAHUB_GMS_BASE_PATH if not set.",
    )

    schema_registry_config: dict = Field(
        default_factory=dict,
        description="Extra schema registry config serialized as JSON. These options will be passed into Kafka's SchemaRegistryClient. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient",
    )

    client_timeout_seconds: int = Field(
        default=60,
        description="The request timeout used when interacting with the Kafka APIs.",
    )

    @validator("bootstrap")
    def bootstrap_host_colon_port_comma(cls, val: str) -> str:
        for entry in val.split(","):
            validate_host_port(entry)
        return val


class KafkaConsumerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka consumers"""

    consumer_config: dict = Field(
        default_factory=dict,
        description="Extra consumer config serialized as JSON. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .",
    )

    @validator("consumer_config")
    @classmethod
    def resolve_callback(cls, value: dict) -> dict:
        if CallableConsumerConfig.is_callable_config(value):
            try:
                value = CallableConsumerConfig(value).callable_config()
            except Exception as e:
                raise ConfigurationError(e) from e
        return value


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    producer_config: dict = Field(
        default_factory=dict,
        description="Extra producer config serialized as JSON. These options will be passed into Kafka's SerializingProducer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .",
    )
