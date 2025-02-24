import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)

KAFKA = "kafka"
SOURCE = "source"
SINK = "sink"
CONNECTOR_CLASS = "connector.class"


class ProvidedConfig(ConfigModel):
    provider: str
    path_key: str
    value: str


class GenericConnectorConfig(ConfigModel):
    connector_name: str
    source_dataset: str
    source_platform: str


class KafkaConnectSourceConfig(
    PlatformInstanceConfigMixin,
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
):
    # See the Connect REST Interface for details
    # https://docs.confluent.io/platform/current/connect/references/restapi.html#
    connect_uri: str = Field(
        default="http://localhost:8083/", description="URI to connect to."
    )
    username: Optional[str] = Field(default=None, description="Kafka Connect username.")
    password: Optional[str] = Field(default=None, description="Kafka Connect password.")
    cluster_name: Optional[str] = Field(
        default="connect-cluster", description="Cluster to ingest from."
    )
    # convert lineage dataset's urns to lowercase
    convert_lineage_urns_to_lowercase: bool = Field(
        default=False,
        description="Whether to convert the urns of ingested lineage dataset to lowercase",
    )
    connector_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for connectors to filter for ingestion.",
    )
    provided_configs: Optional[List[ProvidedConfig]] = Field(
        default=None, description="Provided Configurations"
    )
    connect_to_platform_map: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description='Platform instance mapping when multiple instances for a platform is available. Entry for a platform should be in either `platform_instance_map` or `connect_to_platform_map`. e.g.`connect_to_platform_map: { "postgres-connector-finance-db": "postgres": "core_finance_instance" }`',
    )
    platform_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description='Platform instance mapping to use when constructing URNs. e.g.`platform_instance_map: { "hive": "warehouse" }`',
    )
    generic_connectors: List[GenericConnectorConfig] = Field(
        default=[],
        description="Provide lineage graph for sources connectors other than Confluent JDBC Source Connector, Debezium Source Connector, and Mongo Source Connector",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class KafkaConnectSourceReport(StaleEntityRemovalSourceReport):
    connectors_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_connector_scanned(self, connector: str) -> None:
        self.connectors_scanned += 1

    def report_dropped(self, connector: str) -> None:
        self.filtered.append(connector)


@dataclass
class KafkaConnectLineage:
    """Class to store Kafka Connect lineage mapping, Each instance is potential DataJob"""

    source_platform: str
    target_dataset: str
    target_platform: str
    job_property_bag: Optional[Dict[str, str]] = None
    source_dataset: Optional[str] = None


@dataclass
class ConnectorManifest:
    """Each instance is potential DataFlow"""

    name: str
    type: str
    config: Dict
    tasks: Dict
    url: Optional[str] = None
    flow_property_bag: Optional[Dict[str, str]] = None
    lineages: List[KafkaConnectLineage] = field(default_factory=list)
    topic_names: Iterable[str] = field(default_factory=list)


def remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        index = len(prefix)
        return text[index:]
    return text


def unquote(
    string: str, leading_quote: str = '"', trailing_quote: Optional[str] = None
) -> str:
    """
    If string starts and ends with a quote, unquote it
    """
    trailing_quote = trailing_quote if trailing_quote else leading_quote
    if string.startswith(leading_quote) and string.endswith(trailing_quote):
        string = string[1:-1]
    return string


def get_dataset_name(
    database_name: Optional[str],
    source_table: str,
) -> str:
    if database_name:
        dataset_name = database_name + "." + source_table
    else:
        dataset_name = source_table

    return dataset_name


def get_platform_instance(
    config: KafkaConnectSourceConfig, connector_name: str, platform: str
) -> Optional[str]:
    instance_name = None
    if (
        config.connect_to_platform_map
        and config.connect_to_platform_map.get(connector_name)
        and config.connect_to_platform_map[connector_name].get(platform)
    ):
        instance_name = config.connect_to_platform_map[connector_name][platform]
        if config.platform_instance_map and config.platform_instance_map.get(platform):
            logger.warning(
                f"Same source platform {platform} configured in both platform_instance_map and connect_to_platform_map."
                "Will prefer connector specific platform instance from connect_to_platform_map."
            )
    elif config.platform_instance_map and config.platform_instance_map.get(platform):
        instance_name = config.platform_instance_map[platform]
    logger.info(
        f"Instance name assigned is: {instance_name} for Connector Name {connector_name} and platform {platform}"
    )
    return instance_name


def transform_connector_config(
    connector_config: Dict, provided_configs: List[ProvidedConfig]
) -> None:
    """This method will update provided configs in connector config values, if any"""
    lookupsByProvider = {}
    for pconfig in provided_configs:
        lookupsByProvider[f"${{{pconfig.provider}:{pconfig.path_key}}}"] = pconfig.value
    for k, v in connector_config.items():
        for key, value in lookupsByProvider.items():
            if key in v:
                connector_config[k] = connector_config[k].replace(key, value)


# TODO: Find a more automated way to discover new platforms with 3 level naming hierarchy.
def has_three_level_hierarchy(platform: str) -> bool:
    return platform in ["postgres", "trino", "redshift", "snowflake"]


@dataclass
class BaseConnector:
    connector_manifest: ConnectorManifest
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        return []

    def extract_flow_property_bag(self) -> Optional[Dict[str, str]]:
        return None
