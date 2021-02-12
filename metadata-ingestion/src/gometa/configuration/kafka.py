from typing import Optional
from pydantic import BaseModel, Field, ValidationError, validator


class _KafkaConnectionConfig(BaseModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8081"

    # extra schema registry config
    schema_registry_config: dict = {}

    @validator('bootstrap')
    def bootstrap_host_colon_port_comma(cls, val):
        for entry in val.split(","):
            assert ":" in entry, f'entry must be of the form host:port, found {entry}'
            (host, port) = entry.split(":")
            assert host.isalnum(), f'host must be alphanumeric, found {host}'
            assert port.isdigit(), f'port must be all digits, found {port}'


class KafkaConsumerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka consumers"""

    # extra consumer config
    consumer_config: dict = {}


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    # extra producer config
    producer_config: dict = {}
