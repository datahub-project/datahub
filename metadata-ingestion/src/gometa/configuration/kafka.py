from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConnectionConfig:
    """Configuration class for holding connectivity information for Kafka"""

    # bootstrap servers
    bootstrap: Optional[str] = "localhost:9092"

    # schema registry location
    schema_registry_url: Optional[str] = "http://localhost:8081"
