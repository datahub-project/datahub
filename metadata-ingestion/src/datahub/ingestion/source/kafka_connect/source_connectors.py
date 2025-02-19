import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.kafka_connect.common import (
    CONNECTOR_CLASS,
    KAFKA,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    get_dataset_name,
    has_three_level_hierarchy,
    remove_prefix,
    unquote,
)
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)


@dataclass
class ConfluentJDBCSourceConnector(BaseConnector):
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
            f"org.apache.kafka.connect.transforms.{t}"
            for t in KAFKA_NONTOPICROUTING_TRANSFORMS
        ]
        + CONFLUENT_NONTOPICROUTING_TRANSFORMS
        + [
            f"io.confluent.connect.transforms.{t}"
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

        topic_prefix = self.connector_manifest.config.get("topic.prefix") or ""

        query = self.connector_manifest.config.get("query") or ""

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
                if key.startswith(f"transforms.{name}."):
                    transform[key.replace(f"transforms.{name}.", "")] = (
                        self.connector_manifest.config[key]
                    )

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
                    self.report.warning(
                        "Could not find schema for table"
                        f"{self.connector_manifest.name} : {source_table}",
                    )
            dataset_name: str = get_dataset_name(database_name, source_table)
            lineage = KafkaConnectLineage(
                source_dataset=dataset_name if include_source_dataset else None,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
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
                (
                    unquote(
                        table_id.split(sep)[-2], leading_quote_char, trailing_quote_char
                    )
                    if len(table_id.split(sep)) > 1
                    else ""
                ),
                unquote(
                    table_id.split(sep)[-1], leading_quote_char, trailing_quote_char
                ),
            )
            for table_id in table_ids
        ]
        return tables

    def extract_flow_property_bag(self) -> Dict[str, str]:
        flow_property_bag = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k not in ["connection.password", "connection.user"]
        }

        # Mask/Remove properties that may reveal credentials
        flow_property_bag["connection.url"] = self.get_parser(
            self.connector_manifest
        ).db_connection_url

        return flow_property_bag

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        database_name = parser.database_name
        query = parser.query
        topic_prefix = parser.topic_prefix
        transforms = parser.transforms

        logging.debug(
            f"Extracting source platform: {source_platform} and database name: {database_name} from connection url "
        )

        if not self.connector_manifest.topic_names:
            return lineages

        if query:
            # Lineage source_table can be extracted by parsing query
            for topic in self.connector_manifest.topic_names:
                # default method - as per earlier implementation
                dataset_name: str = get_dataset_name(database_name, topic)

                lineage = KafkaConnectLineage(
                    source_dataset=None,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform=KAFKA,
                )
                lineages.append(lineage)
                self.report.warning(
                    "Could not find input dataset, the connector has query configuration set",
                    self.connector_manifest.name,
                )
                return lineages

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
            return self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
            )

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
                    topic = str(matcher.replaceFirst(transform_replacement))

                # Additional check to confirm that the topic present
                # in connector topics

                if topic in self.connector_manifest.topic_names:
                    # include schema name for three-level hierarchies
                    if has_three_level_hierarchy(source_platform) and len(table) > 1:
                        source_table = f"{table[-2]}.{table[-1]}"

                    dataset_name = get_dataset_name(database_name, source_table)

                    lineage = KafkaConnectLineage(
                        source_dataset=dataset_name,
                        source_platform=source_platform,
                        target_dataset=topic,
                        target_platform=KAFKA,
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
                self.report.warning(
                    "Could not find input dataset for connector topics",
                    f"{self.connector_manifest.name} : {topic_names}",
                )
            return lineages
        else:
            include_source_dataset = True
            if SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report.warning(
                    "Could not find input dataset, connector has unknown transform",
                    f"{self.connector_manifest.name} : {transforms[0]['type']}",
                )
                include_source_dataset = False
            if not SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report.warning(
                    "Could not find input dataset, connector has one or more unknown transforms",
                    self.connector_manifest.name,
                )
                include_source_dataset = False
            lineages = self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
                include_source_dataset=include_source_dataset,
            )
            return lineages


@dataclass
class MongoSourceConnector(BaseConnector):
    # https://www.mongodb.com/docs/kafka-connector/current/source-connector/

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
            transforms=(
                connector_manifest.config["transforms"].split(",")
                if "transforms" in connector_manifest.config
                else []
            ),
        )

        return parser

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        topic_naming_pattern = r"mongodb\.(\w+)\.(\w+)"

        if not self.connector_manifest.topic_names:
            return lineages

        for topic in self.connector_manifest.topic_names:
            found = re.search(re.compile(topic_naming_pattern), topic)

            if found:
                table_name = get_dataset_name(found.group(1), found.group(2))

                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform=KAFKA,
                )
                lineages.append(lineage)
        return lineages


@dataclass
class DebeziumSourceConnector(BaseConnector):
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
        connector_class = connector_manifest.config.get(CONNECTOR_CLASS, "")

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
            database_name = connector_manifest.config.get(
                "database.names"
            ) or connector_manifest.config.get("database.dbname")

            if "," in str(database_name):
                raise Exception(
                    f"Only one database is supported for Debezium's SQL Server connector. Found: {database_name}"
                )

            parser = self.DebeziumParser(
                source_platform="mssql",
                server_name=self.get_server_name(connector_manifest),
                database_name=database_name,
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

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()

        try:
            parser = self.get_parser(self.connector_manifest)
            source_platform = parser.source_platform
            server_name = parser.server_name
            database_name = parser.database_name
            topic_naming_pattern = rf"({server_name})\.(\w+\.\w+)"

            if not self.connector_manifest.topic_names:
                return lineages

            for topic in self.connector_manifest.topic_names:
                found = re.search(re.compile(topic_naming_pattern), topic)

                if found:
                    table_name = get_dataset_name(database_name, found.group(2))

                    lineage = KafkaConnectLineage(
                        source_dataset=table_name,
                        source_platform=source_platform,
                        target_dataset=topic,
                        target_platform=KAFKA,
                    )
                    lineages.append(lineage)
            return lineages
        except Exception as e:
            self.report.warning(
                "Error resolving lineage for connector",
                self.connector_manifest.name,
                exc=e,
            )

        return []


@dataclass
class ConfigDrivenSourceConnector(BaseConnector):
    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages = []
        for connector in self.config.generic_connectors:
            if connector.connector_name == self.connector_manifest.name:
                target_connector = connector
                break
        for topic in self.connector_manifest.topic_names:
            lineage = KafkaConnectLineage(
                source_dataset=target_connector.source_dataset,
                source_platform=target_connector.source_platform,
                target_dataset=topic,
                target_platform=KAFKA,
            )
            lineages.append(lineage)
        return lineages


JDBC_SOURCE_CONNECTOR_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector"
DEBEZIUM_SOURCE_CONNECTOR_PREFIX = "io.debezium.connector"
MONGO_SOURCE_CONNECTOR_CLASS = "com.mongodb.kafka.connect.MongoSourceConnector"
