import logging
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import jpype
import jpype.imports
import requests
from pydantic.fields import Field
from sqlalchemy.engine.url import make_url

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import get_platform_from_sqlalchemy_uri

logger = logging.getLogger(__name__)


class ProvidedConfig(ConfigModel):
    provider: str
    path_key: str
    value: str


class GenericConnectorConfig(ConfigModel):
    connector_name: str
    source_dataset: str
    source_platform: str


class KafkaConnectSourceConfig(DatasetLineageProviderConfigBase):
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
    construct_lineage_workunits: bool = Field(
        default=True,
        description="Whether to create the input and output Dataset entities",
    )
    connector_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for connectors to filter for ingestion.",
    )
    provided_configs: Optional[List[ProvidedConfig]] = Field(
        default=None, description="Provided Configurations"
    )
    connect_to_platform_map: Optional[dict] = Field(
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


@dataclass
class KafkaConnectSourceReport(SourceReport):
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
    instance_name: Optional[str],
    source_table: str,
) -> str:
    if database_name and instance_name:
        dataset_name = instance_name + "." + database_name + "." + source_table
    elif database_name:
        dataset_name = database_name + "." + source_table
    else:
        dataset_name = source_table

    return dataset_name


def get_instance_name(
    config: KafkaConnectSourceConfig, kafka_connector_name: str, source_platform: str
) -> Optional[str]:
    instance_name = None
    if config.connect_to_platform_map:
        for connector_name in config.connect_to_platform_map:
            if connector_name == kafka_connector_name:
                instance_name = config.connect_to_platform_map[connector_name][
                    source_platform
                ]
                if config.platform_instance_map and config.platform_instance_map.get(
                    source_platform
                ):
                    logger.error(
                        f"Same source platform {source_platform} configured in both platform_instance_map and connect_to_platform_map"
                    )
                    sys.exit(
                        "Config Error: Same source platform configured in both platform_instance_map and connect_to_platform_map. Fix the config and re-run again."
                    )
                logger.info(
                    f"Instance name assigned is: {instance_name} for Connector Name {connector_name} and source platform {source_platform}"
                )
                break
    return instance_name


@dataclass
class ConfluentJDBCSourceConnector:
    connector_manifest: ConnectorManifest
    report: KafkaConnectSourceReport

    def __init__(
        self,
        connector_manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ) -> None:
        self.connector_manifest = connector_manifest
        self.config = config
        self.report = report
        self._extract_lineages()

    REGEXROUTER = "org.apache.kafka.connect.transforms.RegexRouter"
    KNOWN_TOPICROUTING_TRANSFORMS = [REGEXROUTER]
    # https://kafka.apache.org/documentation/#connect_included_transformation
    KAFKA_NONTOPICROUTING_TRANSFORMS = [
        "InsertField",
        "InsertField$Key",
        "InsertField$Value",
        "ReplaceField",
        "ReplaceField$Key",
        "ReplaceField$Value",
        "MaskField",
        "MaskField$Key",
        "MaskField$Value",
        "ValueToKey",
        "ValueToKey$Key",
        "ValueToKey$Value",
        "HoistField",
        "HoistField$Key",
        "HoistField$Value",
        "ExtractField",
        "ExtractField$Key",
        "ExtractField$Value",
        "SetSchemaMetadata",
        "SetSchemaMetadata$Key",
        "SetSchemaMetadata$Value",
        "Flatten",
        "Flatten$Key",
        "Flatten$Value",
        "Cast",
        "Cast$Key",
        "Cast$Value",
        "HeadersFrom",
        "HeadersFrom$Key",
        "HeadersFrom$Value",
        "TimestampConverter",
        "Filter",
        "InsertHeader",
        "DropHeaders",
    ]
    # https://docs.confluent.io/platform/current/connect/transforms/overview.html
    CONFLUENT_NONTOPICROUTING_TRANSFORMS = [
        "Drop",
        "Drop$Key",
        "Drop$Value",
        "Filter",
        "Filter$Key",
        "Filter$Value",
        "TombstoneHandler",
    ]
    KNOWN_NONTOPICROUTING_TRANSFORMS = (
        KAFKA_NONTOPICROUTING_TRANSFORMS
        + [
            "org.apache.kafka.connect.transforms.{}".format(t)
            for t in KAFKA_NONTOPICROUTING_TRANSFORMS
        ]
        + CONFLUENT_NONTOPICROUTING_TRANSFORMS
        + [
            "io.confluent.connect.transforms.{}".format(t)
            for t in CONFLUENT_NONTOPICROUTING_TRANSFORMS
        ]
    )

    @dataclass
    class JdbcParser:
        db_connection_url: str
        source_platform: str
        database_name: str
        topic_prefix: str
        query: str
        transforms: list

    def report_warning(self, key: str, reason: str) -> None:
        logger.warning(f"{key}: {reason}")
        self.report.report_warning(key, reason)

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> JdbcParser:
        url = remove_prefix(
            str(connector_manifest.config.get("connection.url")), "jdbc:"
        )
        url_instance = make_url(url)
        source_platform = get_platform_from_sqlalchemy_uri(str(url_instance))
        database_name = url_instance.database
        assert database_name
        db_connection_url = f"{url_instance.drivername}://{url_instance.host}:{url_instance.port}/{database_name}"

        topic_prefix = self.connector_manifest.config.get("topic.prefix", None)

        query = self.connector_manifest.config.get("query", None)

        transform_names = (
            self.connector_manifest.config.get("transforms", "").split(",")
            if self.connector_manifest.config.get("transforms")
            else []
        )

        transforms = []
        for name in transform_names:
            transform = {"name": name}
            transforms.append(transform)
            for key in self.connector_manifest.config.keys():
                if key.startswith("transforms.{}.".format(name)):
                    transform[
                        key.replace("transforms.{}.".format(name), "")
                    ] = self.connector_manifest.config[key]

        return self.JdbcParser(
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
            query,
            transforms,
        )

    def default_get_lineages(
        self,
        topic_prefix: str,
        database_name: str,
        source_platform: str,
        topic_names: Optional[Iterable[str]] = None,
        include_source_dataset: bool = True,
        instance_name: Optional[str] = None,
    ) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = []
        if not topic_names:
            topic_names = self.connector_manifest.topic_names
        table_name_tuples: List[Tuple] = self.get_table_names()
        for topic in topic_names:
            # All good for NO_TRANSFORM or (SINGLE_TRANSFORM and KNOWN_NONTOPICROUTING_TRANSFORM) or (not SINGLE_TRANSFORM and all(KNOWN_NONTOPICROUTING_TRANSFORM))
            source_table: str = (
                remove_prefix(topic, topic_prefix) if topic_prefix else topic
            )
            # include schema name for three-level hierarchies
            if has_three_level_hierarchy(source_platform):
                table_name_tuple: Tuple = next(
                    iter([t for t in table_name_tuples if t and t[-1] == source_table]),
                    (),
                )
                if len(table_name_tuple) > 1:
                    source_table = f"{table_name_tuple[-2]}.{source_table}"
                else:
                    include_source_dataset = False
                    self.report_warning(
                        self.connector_manifest.name,
                        f"could not find schema for table {source_table}",
                    )
            dataset_name: str = get_dataset_name(
                database_name, instance_name, source_table
            )
            lineage = KafkaConnectLineage(
                source_dataset=dataset_name if include_source_dataset else None,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform="kafka",
            )
            lineages.append(lineage)
        return lineages

    def get_table_names(self) -> List[Tuple]:
        sep: str = "."
        leading_quote_char: str = '"'
        trailing_quote_char: str = leading_quote_char

        table_ids: List[str] = []
        if self.connector_manifest.tasks:
            table_ids = (
                ",".join(
                    [
                        task["config"].get("tables")
                        for task in self.connector_manifest.tasks
                    ]
                )
            ).split(",")
            quote_method = self.connector_manifest.config.get(
                "quote.sql.identifiers", "always"
            )
            if (
                quote_method == "always"
                and table_ids
                and table_ids[0]
                and table_ids[-1]
            ):
                leading_quote_char = table_ids[0][0]
                trailing_quote_char = table_ids[-1][-1]
                # This will only work for single character quotes
        elif self.connector_manifest.config.get("table.whitelist"):
            table_ids = self.connector_manifest.config.get("table.whitelist").split(",")  # type: ignore

        # List of Tuple containing (schema, table)
        tables: List[Tuple] = [
            (
                unquote(
                    table_id.split(sep)[-2], leading_quote_char, trailing_quote_char
                )
                if len(table_id.split(sep)) > 1
                else "",
                unquote(
                    table_id.split(sep)[-1], leading_quote_char, trailing_quote_char
                ),
            )
            for table_id in table_ids
        ]
        return tables

    def _extract_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        database_name = parser.database_name
        query = parser.query
        topic_prefix = parser.topic_prefix
        transforms = parser.transforms
        self.connector_manifest.flow_property_bag = self.connector_manifest.config
        instance_name = get_instance_name(
            self.config, self.connector_manifest.name, source_platform
        )

        # Mask/Remove properties that may reveal credentials
        self.connector_manifest.flow_property_bag[
            "connection.url"
        ] = parser.db_connection_url
        if "connection.password" in self.connector_manifest.flow_property_bag:
            del self.connector_manifest.flow_property_bag["connection.password"]
        if "connection.user" in self.connector_manifest.flow_property_bag:
            del self.connector_manifest.flow_property_bag["connection.user"]

        logging.debug(
            f"Extracting source platform: {source_platform} and database name: {database_name} from connection url "
        )

        if not self.connector_manifest.topic_names:
            self.connector_manifest.lineages = lineages
            return

        if query:
            # Lineage source_table can be extracted by parsing query
            for topic in self.connector_manifest.topic_names:
                # default method - as per earlier implementation
                dataset_name: str = get_dataset_name(
                    database_name, instance_name, topic
                )

                lineage = KafkaConnectLineage(
                    source_dataset=None,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)
                self.report_warning(
                    self.connector_manifest.name,
                    "could not find input dataset, the connector has query configuration set",
                )
                self.connector_manifest.lineages = lineages
                return

        SINGLE_TRANSFORM = len(transforms) == 1
        NO_TRANSFORM = len(transforms) == 0
        UNKNOWN_TRANSFORM = any(
            [
                transform["type"]
                not in self.KNOWN_TOPICROUTING_TRANSFORMS
                + self.KNOWN_NONTOPICROUTING_TRANSFORMS
                for transform in transforms
            ]
        )
        ALL_TRANSFORMS_NON_TOPICROUTING = all(
            [
                transform["type"] in self.KNOWN_NONTOPICROUTING_TRANSFORMS
                for transform in transforms
            ]
        )

        if NO_TRANSFORM or ALL_TRANSFORMS_NON_TOPICROUTING:
            self.connector_manifest.lineages = self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
                instance_name=instance_name,
            )
            return

        if SINGLE_TRANSFORM and transforms[0]["type"] == self.REGEXROUTER:
            tables = self.get_table_names()
            topic_names = list(self.connector_manifest.topic_names)

            from java.util.regex import Pattern

            for table in tables:
                source_table: str = table[-1]
                topic = topic_prefix + source_table if topic_prefix else source_table

                transform_regex = Pattern.compile(transforms[0]["regex"])
                transform_replacement = transforms[0]["replacement"]

                matcher = transform_regex.matcher(topic)
                if matcher.matches():
                    topic = matcher.replaceFirst(transform_replacement)

                # Additional check to confirm that the topic present
                # in connector topics

                if topic in self.connector_manifest.topic_names:
                    # include schema name for three-level hierarchies
                    if has_three_level_hierarchy(source_platform) and len(table) > 1:
                        source_table = f"{table[-2]}.{table[-1]}"

                    dataset_name = get_dataset_name(
                        database_name, instance_name, source_table
                    )

                    lineage = KafkaConnectLineage(
                        source_dataset=dataset_name,
                        source_platform=source_platform,
                        target_dataset=topic,
                        target_platform="kafka",
                    )
                    topic_names.remove(topic)
                    lineages.append(lineage)

            if topic_names:
                lineages.extend(
                    self.default_get_lineages(
                        database_name=database_name,
                        source_platform=source_platform,
                        topic_prefix=topic_prefix,
                        topic_names=topic_names,
                        include_source_dataset=False,
                    )
                )
                self.report_warning(
                    self.connector_manifest.name,
                    f"could not find input dataset, for connector topics {topic_names}",
                )
            self.connector_manifest.lineages = lineages
            return
        else:
            include_source_dataset = True
            if SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report_warning(
                    self.connector_manifest.name,
                    f"could not find input dataset, connector has unknown transform - {transforms[0]['type']}",
                )
                include_source_dataset = False
            if not SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report_warning(
                    self.connector_manifest.name,
                    "could not find input dataset, connector has one or more unknown transforms",
                )
                include_source_dataset = False
            lineages = self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
                include_source_dataset=include_source_dataset,
                instance_name=instance_name,
            )
            self.connector_manifest.lineages = lineages
            return


@dataclass
class MongoSourceConnector:
    # https://www.mongodb.com/docs/kafka-connector/current/source-connector/

    connector_manifest: ConnectorManifest

    def __init__(
        self, connector_manifest: ConnectorManifest, config: KafkaConnectSourceConfig
    ) -> None:
        self.connector_manifest = connector_manifest
        self.config = config
        self._extract_lineages()

    @dataclass
    class MongoSourceParser:
        db_connection_url: Optional[str]
        source_platform: str
        database_name: Optional[str]
        topic_prefix: Optional[str]
        transforms: List[str]

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> MongoSourceParser:
        parser = self.MongoSourceParser(
            db_connection_url=connector_manifest.config.get("connection.uri"),
            source_platform="mongodb",
            database_name=connector_manifest.config.get("database"),
            topic_prefix=connector_manifest.config.get("topic_prefix"),
            transforms=connector_manifest.config["transforms"].split(",")
            if "transforms" in connector_manifest.config
            else [],
        )

        return parser

    def _extract_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        topic_naming_pattern = r"mongodb\.(\w+)\.(\w+)"

        if not self.connector_manifest.topic_names:
            return lineages

        for topic in self.connector_manifest.topic_names:
            found = re.search(re.compile(topic_naming_pattern), topic)

            if found:
                table_name = get_dataset_name(found.group(1), None, found.group(2))

                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)
        self.connector_manifest.lineages = lineages


@dataclass
class DebeziumSourceConnector:
    connector_manifest: ConnectorManifest

    def __init__(
        self, connector_manifest: ConnectorManifest, config: KafkaConnectSourceConfig
    ) -> None:
        self.connector_manifest = connector_manifest
        self.config = config
        self._extract_lineages()

    @dataclass
    class DebeziumParser:
        source_platform: str
        server_name: Optional[str]
        database_name: Optional[str]

    def get_server_name(self, connector_manifest: ConnectorManifest) -> str:
        if "topic.prefix" in connector_manifest.config:
            return connector_manifest.config["topic.prefix"]
        else:
            return connector_manifest.config.get("database.server.name", "")

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> DebeziumParser:
        connector_class = connector_manifest.config.get("connector.class", "")

        if connector_class == "io.debezium.connector.mysql.MySqlConnector":
            parser = self.DebeziumParser(
                source_platform="mysql",
                server_name=self.get_server_name(connector_manifest),
                database_name=None,
            )
        elif connector_class == "MySqlConnector":
            parser = self.DebeziumParser(
                source_platform="mysql",
                server_name=self.get_server_name(connector_manifest),
                database_name=None,
            )
        elif connector_class == "io.debezium.connector.mongodb.MongoDbConnector":
            parser = self.DebeziumParser(
                source_platform="mongodb",
                server_name=self.get_server_name(connector_manifest),
                database_name=None,
            )
        elif connector_class == "io.debezium.connector.postgresql.PostgresConnector":
            parser = self.DebeziumParser(
                source_platform="postgres",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.oracle.OracleConnector":
            parser = self.DebeziumParser(
                source_platform="oracle",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.sqlserver.SqlServerConnector":
            parser = self.DebeziumParser(
                source_platform="mssql",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.db2.Db2Connector":
            parser = self.DebeziumParser(
                source_platform="db2",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.vitess.VitessConnector":
            parser = self.DebeziumParser(
                source_platform="vitess",
                server_name=self.get_server_name(connector_manifest),
                database_name=connector_manifest.config.get("vitess.keyspace"),
            )
        else:
            raise ValueError(f"Connector class '{connector_class}' is unknown.")

        return parser

    def _extract_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        server_name = parser.server_name
        database_name = parser.database_name
        topic_naming_pattern = r"({0})\.(\w+\.\w+)".format(server_name)
        instance_name = get_instance_name(
            self.config, self.connector_manifest.name, source_platform
        )

        if not self.connector_manifest.topic_names:
            return lineages
        # Get the platform/platform_instance mapping for every database_server from connect_to_platform_map

        for topic in self.connector_manifest.topic_names:
            found = re.search(re.compile(topic_naming_pattern), topic)

            if found:
                table_name = get_dataset_name(
                    database_name, instance_name, found.group(2)
                )

                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)
        self.connector_manifest.lineages = lineages


@dataclass
class BigQuerySinkConnector:
    connector_manifest: ConnectorManifest
    report: KafkaConnectSourceReport

    def __init__(
        self, connector_manifest: ConnectorManifest, report: KafkaConnectSourceReport
    ) -> None:
        self.connector_manifest = connector_manifest
        self.report = report
        self._extract_lineages()

    @dataclass
    class BQParser:
        project: str
        target_platform: str
        sanitizeTopics: str
        topicsToTables: Optional[str] = None
        datasets: Optional[str] = None
        defaultDataset: Optional[str] = None
        version: str = "v1"

    def report_warning(self, key: str, reason: str) -> None:
        logger.warning(f"{key}: {reason}")
        self.report.report_warning(key, reason)

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> BQParser:
        project = connector_manifest.config["project"]
        sanitizeTopics = connector_manifest.config.get("sanitizeTopics", "false")

        if "defaultDataset" in connector_manifest.config:
            defaultDataset = connector_manifest.config["defaultDataset"]
            return self.BQParser(
                project=project,
                defaultDataset=defaultDataset,
                target_platform="bigquery",
                sanitizeTopics=sanitizeTopics.lower() == "true",
                version="v2",
            )
        else:
            # version 1.6.x and similar configs supported
            datasets = connector_manifest.config["datasets"]
            topicsToTables = connector_manifest.config.get("topicsToTables")

            return self.BQParser(
                project=project,
                topicsToTables=topicsToTables,
                datasets=datasets,
                target_platform="bigquery",
                sanitizeTopics=sanitizeTopics.lower() == "true",
            )

    def get_list(self, property: str) -> Iterable[Tuple[str, str]]:
        entries = property.split(",")
        for entry in entries:
            key, val = entry.rsplit("=")
            yield (key.strip(), val.strip())

    def get_dataset_for_topic_v1(self, topic: str, parser: BQParser) -> Optional[str]:
        topicregex_dataset_map: Dict[str, str] = dict(self.get_list(parser.datasets))  # type: ignore
        from java.util.regex import Pattern

        for pattern, dataset in topicregex_dataset_map.items():
            patternMatcher = Pattern.compile(pattern).matcher(topic)
            if patternMatcher.matches():
                return dataset
        return None

    def sanitize_table_name(self, table_name):
        table_name = re.sub("[^a-zA-Z0-9_]", "_", table_name)
        if re.match("^[^a-zA-Z_].*", table_name):
            table_name = "_" + table_name

        return table_name

    def get_dataset_table_for_topic(
        self, topic: str, parser: BQParser
    ) -> Optional[str]:
        if parser.version == "v2":
            dataset = parser.defaultDataset
            parts = topic.split(":")
            if len(parts) == 2:
                dataset = parts[0]
                table = parts[1]
            else:
                table = parts[0]
        else:
            dataset = self.get_dataset_for_topic_v1(topic, parser)
            if dataset is None:
                return None

            table = topic
            if parser.topicsToTables:
                topicregex_table_map: Dict[str, str] = dict(
                    self.get_list(parser.topicsToTables)  # type: ignore
                )
                from java.util.regex import Pattern

                for pattern, tbl in topicregex_table_map.items():
                    patternMatcher = Pattern.compile(pattern).matcher(topic)
                    if patternMatcher.matches():
                        table = tbl
                        break

        if parser.sanitizeTopics:
            table = self.sanitize_table_name(table)
        return f"{dataset}.{table}"

    def _extract_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        if not parser:
            return lineages
        target_platform = parser.target_platform
        project = parser.project

        self.connector_manifest.flow_property_bag = self.connector_manifest.config

        # Mask/Remove properties that may reveal credentials
        if "keyfile" in self.connector_manifest.flow_property_bag:
            del self.connector_manifest.flow_property_bag["keyfile"]

        for topic in self.connector_manifest.topic_names:
            dataset_table = self.get_dataset_table_for_topic(topic, parser)
            if dataset_table is None:
                self.report_warning(
                    self.connector_manifest.name,
                    f"could not find target dataset for topic {topic}, please check your connector configuration",
                )
                continue
            target_dataset = f"{project}.{dataset_table}"

            lineages.append(
                KafkaConnectLineage(
                    source_dataset=topic,
                    source_platform="kafka",
                    target_dataset=target_dataset,
                    target_platform=target_platform,
                )
            )
        self.connector_manifest.lineages = lineages
        return


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
                connector_config[k] = v.replace(key, value)


@platform_name("Kafka Connect")
@config_class(KafkaConnectSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class KafkaConnectSource(Source):

    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def __init__(self, config: KafkaConnectSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = KafkaConnectSourceReport()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Test the connection
        if self.config.username is not None and self.config.password is not None:
            logger.info(
                f"Connecting to {self.config.connect_uri} with Authentication..."
            )
            self.session.auth = (self.config.username, self.config.password)

        test_response = self.session.get(f"{self.config.connect_uri}")
        test_response.raise_for_status()
        logger.info(f"Connection to {self.config.connect_uri} is ok")
        if not jpype.isJVMStarted():
            jpype.startJVM()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = KafkaConnectSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_connectors_manifest(self) -> List[ConnectorManifest]:
        """Get Kafka Connect connectors manifest using REST API.
        Enrich with lineages metadata.
        """
        connectors_manifest = list()

        connector_response = self.session.get(
            f"{self.config.connect_uri}/connectors",
        )

        payload = connector_response.json()

        for c in payload:
            connector_url = f"{self.config.connect_uri}/connectors/{c}"
            connector_response = self.session.get(connector_url)

            manifest = connector_response.json()
            connector_manifest = ConnectorManifest(**manifest)
            if self.config.provided_configs:
                transform_connector_config(
                    connector_manifest.config, self.config.provided_configs
                )
            # Initialize connector lineages
            connector_manifest.lineages = list()
            connector_manifest.url = connector_url

            topics = self.session.get(
                f"{self.config.connect_uri}/connectors/{c}/topics",
            ).json()

            connector_manifest.topic_names = topics[c]["topics"]

            # Populate Source Connector metadata
            if connector_manifest.type == "source":
                tasks = self.session.get(
                    f"{self.config.connect_uri}/connectors/{c}/tasks",
                ).json()

                connector_manifest.tasks = tasks

                # JDBC source connector lineages
                if connector_manifest.config.get("connector.class").__eq__(
                    "io.confluent.connect.jdbc.JdbcSourceConnector"
                ):
                    connector_manifest = ConfluentJDBCSourceConnector(
                        connector_manifest=connector_manifest,
                        config=self.config,
                        report=self.report,
                    ).connector_manifest
                elif connector_manifest.config.get("connector.class", "").startswith(
                    "io.debezium.connector"
                ):
                    connector_manifest = DebeziumSourceConnector(
                        connector_manifest=connector_manifest, config=self.config
                    ).connector_manifest
                elif (
                    connector_manifest.config.get("connector.class", "")
                    == "com.mongodb.kafka.connect.MongoSourceConnector"
                ):
                    connector_manifest = MongoSourceConnector(
                        connector_manifest=connector_manifest, config=self.config
                    ).connector_manifest
                else:
                    # Find the target connector object in the list, or log an error if unknown.
                    target_connector = None
                    for connector in self.config.generic_connectors:
                        if connector.connector_name == connector_manifest.name:
                            target_connector = connector
                            break
                    if not target_connector:
                        logger.warning(
                            f"Detected undefined connector {connector_manifest.name}, which is not in the customized connector list. Please refer to Kafka Connect ingestion recipe to define this customized connector."
                        )
                        continue

                    for topic in topics:
                        lineage = KafkaConnectLineage(
                            source_dataset=target_connector.source_dataset,
                            source_platform=target_connector.source_platform,
                            target_dataset=topic,
                            target_platform="kafka",
                        )

                    connector_manifest.lineages.append(lineage)

            if connector_manifest.type == "sink":
                if connector_manifest.config.get("connector.class").__eq__(
                    "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
                ):
                    connector_manifest = BigQuerySinkConnector(
                        connector_manifest=connector_manifest, report=self.report
                    ).connector_manifest
                else:
                    self.report.report_dropped(connector_manifest.name)
                    logger.warning(
                        f"Skipping connector {connector_manifest.name}. Lineage for  Connector not yet implemented"
                    )
                pass

            connectors_manifest.append(connector_manifest)

        return connectors_manifest

    def construct_flow_workunit(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.name
        connector_type = connector.type
        connector_class = connector.config.get("connector.class")
        flow_property_bag = connector.flow_property_bag
        # connector_url = connector.url  # NOTE: this will expose connector credential when used
        flow_urn = builder.make_data_flow_urn(
            "kafka-connect", connector_name, self.config.env
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=models.DataFlowInfoClass(
                name=connector_name,
                description=f"{connector_type.capitalize()} connector using `{connector_class}` plugin.",
                customProperties=flow_property_bag,
                # externalUrl=connector_url, # NOTE: this will expose connector credential when used
            ),
        )

        for proposal in [mcp]:
            wu = MetadataWorkUnit(
                id=f"kafka-connect.{connector_name}.{proposal.aspectName}", mcp=proposal
            )
            self.report.report_workunit(wu)
            yield wu

    def construct_job_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.name
        flow_urn = builder.make_data_flow_urn(
            "kafka-connect", connector_name, self.config.env
        )

        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                source_platform_instance = (
                    self.config.platform_instance_map.get(source_platform)
                    if self.config.platform_instance_map
                    else None
                )
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform
                target_platform_instance = (
                    self.config.platform_instance_map.get(target_platform)
                    if self.config.platform_instance_map
                    else None
                )
                job_property_bag = lineage.job_property_bag

                job_id = (
                    source_dataset
                    if source_dataset
                    else f"unknown_source.{target_dataset}"
                )
                job_urn = builder.make_data_job_urn_with_flow(flow_urn, job_id)

                inlets = (
                    [
                        builder.make_dataset_urn_with_platform_instance(
                            source_platform,
                            source_dataset,
                            platform_instance=source_platform_instance,
                            env=self.config.env,
                        )
                    ]
                    if source_dataset
                    else []
                )
                outlets = [
                    builder.make_dataset_urn_with_platform_instance(
                        target_platform,
                        target_dataset,
                        platform_instance=target_platform_instance,
                        env=self.config.env,
                    )
                ]

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInfoClass(
                        name=f"{connector_name}:{job_id}",
                        type="COMMAND",
                        description=None,
                        customProperties=job_property_bag
                        # externalUrl=job_url,
                    ),
                )

                wu = MetadataWorkUnit(
                    id=f"kafka-connect.{connector_name}.{job_id}.{mcp.aspectName}",
                    mcp=mcp,
                )
                self.report.report_workunit(wu)
                yield wu

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInputOutputClass(
                        inputDatasets=inlets,
                        outputDatasets=outlets,
                    ),
                )

                wu = MetadataWorkUnit(
                    id=f"kafka-connect.{connector_name}.{job_id}.{mcp.aspectName}",
                    mcp=mcp,
                )
                self.report.report_workunit(wu)
                yield wu

    def construct_lineage_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                source_platform_instance = (
                    self.config.platform_instance_map.get(source_platform)
                    if self.config.platform_instance_map
                    else None
                )
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform
                target_platform_instance = (
                    self.config.platform_instance_map.get(target_platform)
                    if self.config.platform_instance_map
                    else None
                )

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=builder.make_dataset_urn_with_platform_instance(
                        target_platform,
                        target_dataset,
                        platform_instance=target_platform_instance,
                        env=self.config.env,
                    ),
                    aspect=models.DataPlatformInstanceClass(
                        platform=builder.make_data_platform_urn(target_platform),
                        instance=builder.make_dataplatform_instance_urn(
                            target_platform, target_platform_instance
                        )
                        if target_platform_instance
                        else None,
                    ),
                )

                wu = MetadataWorkUnit(id=target_dataset, mcp=mcp)
                self.report.report_workunit(wu)
                yield wu
                if source_dataset:
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=builder.make_dataset_urn_with_platform_instance(
                            source_platform,
                            source_dataset,
                            platform_instance=source_platform_instance,
                            env=self.config.env,
                        ),
                        aspect=models.DataPlatformInstanceClass(
                            platform=builder.make_data_platform_urn(source_platform),
                            instance=builder.make_dataplatform_instance_urn(
                                source_platform, source_platform_instance
                            )
                            if source_platform_instance
                            else None,
                        ),
                    )

                    wu = MetadataWorkUnit(id=source_dataset, mcp=mcp)
                    self.report.report_workunit(wu)
                    yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        connectors_manifest = self.get_connectors_manifest()
        for connector in connectors_manifest:
            name = connector.name
            if self.config.connector_patterns.allowed(name):
                yield from self.construct_flow_workunit(connector)
                yield from self.construct_job_workunits(connector)
                if self.config.construct_lineage_workunits:
                    yield from self.construct_lineage_workunits(connector)

                self.report.report_connector_scanned(name)

            else:
                self.report.report_dropped(name)

    def get_report(self) -> KafkaConnectSourceReport:
        return self.report


# TODO: Find a more automated way to discover new platforms with 3 level naming hierarchy.
def has_three_level_hierarchy(platform: str) -> bool:
    return platform in ["postgres", "trino", "redshift", "snowflake"]
