import re

from pydantic import validator

from datahub.configuration.common import ConfigModel


class _KafkaConnectionConfig(ConfigModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8081"

    # extra schema registry config
    schema_registry_config: dict = {}

    @validator("bootstrap")
    def bootstrap_host_colon_port_comma(cls, val: str):
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


class KafkaConsumerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka consumers"""

    # extra consumer config
    consumer_config: dict = {}


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    # extra producer config
    producer_config: dict = {}
