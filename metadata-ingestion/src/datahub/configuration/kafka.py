import logging
from typing import Any, Dict, Optional

from pydantic import Field, root_validator, validator

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.configuration.kafka_consumer_config import CallableConsumerConfig
from datahub.configuration.validate_host_port import validate_host_port

logger = logging.getLogger(__name__)


class _KafkaConnectionConfig(ConfigModel):
    # bootstrap servers
    bootstrap: str = "localhost:9092"

    # schema registry location
    schema_registry_url: str = "http://localhost:8080/schema-registry/api/"

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

    # MSK IAM Authentication Configuration
    sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism for authentication. Supported values: OAUTHBEARER, AWS_MSK_IAM",
    )
    aws_region: Optional[str] = Field(
        default=None,
        description="AWS region for MSK IAM authentication. Required when using OAUTHBEARER with aws_msk_iam_sasl_signer.",
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

    @root_validator
    @classmethod
    def setup_sasl_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Root validator to set up SASL configuration after all fields are validated"""
        sasl_mechanism = values.get("sasl_mechanism")
        aws_region = values.get("aws_region")
        consumer_config = values.get("consumer_config", {})

        # Validate aws_region for OAUTHBEARER
        if sasl_mechanism == "OAUTHBEARER" and not aws_region:
            raise ValueError("aws_region must be set when using OAUTHBEARER (SASL/IAM)")

        if sasl_mechanism:
            consumer_config = consumer_config.copy()  # Don't modify the original
            consumer_config["security.protocol"] = "SASL_SSL"
            consumer_config["sasl.mechanism"] = sasl_mechanism

            if sasl_mechanism == "OAUTHBEARER":
                # Delayed import to avoid making this a hard dependency if not used
                try:
                    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
                except ImportError as e:
                    raise ImportError(
                        "The 'aws_msk_iam_sasl_signer' library is required for SASL/IAM OAUTHBEARER. "
                        "Please install it by adding 'aws-msk-iam-sasl-signer' to your project dependencies, "
                        "or ensure your datahub installation includes kafka-iam extras."
                    ) from e

                def oauth_cb(oauth_config_str: str) -> tuple[str, float]:
                    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
                        aws_region  # type: ignore
                    )
                    return auth_token, expiry_ms / 1000

                consumer_config["oauth_cb"] = oauth_cb
            elif sasl_mechanism == "AWS_MSK_IAM":
                # For librdkafka >= 1.5.0 with AWS_MSK_IAM mechanism,
                # sasl.client.callback.handler.class and sasl.jaas.config
                # are not needed and may cause errors if set, as librdkafka handles this internally.
                # If "Unsupported SASL mechanism: AWS_MSK_IAM" occurs, it means this mechanism
                # is not supported by the specific librdkafka build, despite its version.
                # In that case, OAUTHBEARER with aws_msk_iam_sasl_signer is the fallback.
                pass  # No extra config needed beyond sasl.mechanism itself
            else:
                logger.warning(
                    f"SASL mechanism '{sasl_mechanism}' is configured but not explicitly handled "
                    f"by OAUTHBEARER or AWS_MSK_IAM specific logic. Ensure your librdkafka build supports it directly, "
                    f"or that it doesn't require additional client-side configuration beyond 'sasl.mechanism'."
                )

            values["consumer_config"] = consumer_config

        return values


class KafkaProducerConnectionConfig(_KafkaConnectionConfig):
    """Configuration class for holding connectivity information for Kafka producers"""

    producer_config: dict = Field(
        default_factory=dict,
        description="Extra producer config serialized as JSON. These options will be passed into Kafka's SerializingProducer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#serializingproducer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .",
    )
