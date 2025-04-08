import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.kafka_connect.common import (
    KAFKA,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    fix_oracle_tibero_url,
    get_dataset_name,
    remove_prefix,
)
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)


@dataclass
class ConfluentS3SinkConnector(BaseConnector):
    @dataclass
    class S3SinkParser:
        target_platform: str
        bucket: str
        topics_dir: str
        topics: Iterable[str]

    def _get_parser(self, connector_manifest: ConnectorManifest) -> S3SinkParser:
        # https://docs.confluent.io/kafka-connectors/s3-sink/current/configuration_options.html#s3
        bucket = connector_manifest.config.get("s3.bucket.name")
        if not bucket:
            raise ValueError(
                "Could not find 's3.bucket.name' in connector configuration"
            )

        # https://docs.confluent.io/kafka-connectors/s3-sink/current/configuration_options.html#storage
        topics_dir = connector_manifest.config.get("topics.dir", "topics")

        return self.S3SinkParser(
            target_platform="s3",
            bucket=bucket,
            topics_dir=topics_dir,
            topics=connector_manifest.topic_names,
        )

    def extract_flow_property_bag(self) -> Dict[str, str]:
        # Mask/Remove properties that may reveal credentials
        flow_property_bag = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k
            not in [
                "aws.access.key.id",
                "aws.secret.access.key",
                "s3.sse.customer.key",
                "s3.proxy.password",
            ]
        }
        return flow_property_bag

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        try:
            parser = self._get_parser(self.connector_manifest)

            lineages: List[KafkaConnectLineage] = list()
            for topic in parser.topics:
                target_dataset = f"{parser.bucket}/{parser.topics_dir}/{topic}"

                lineages.append(
                    KafkaConnectLineage(
                        source_dataset=topic,
                        source_platform="kafka",
                        target_dataset=target_dataset,
                        target_platform=parser.target_platform,
                    )
                )
            return lineages
        except Exception as e:
            self.report.warning(
                "Error resolving lineage for connector",
                self.connector_manifest.name,
                exc=e,
            )

        return []


@dataclass
class SnowflakeSinkConnector(BaseConnector):
    @dataclass
    class SnowflakeParser:
        database_name: str
        schema_name: str
        topics_to_tables: Dict[str, str]

    def get_table_name_from_topic_name(self, topic_name: str) -> str:
        """
        This function converts the topic name to a valid Snowflake table name using some rules.
        Refer below link for more info
        https://docs.snowflake.com/en/user-guide/kafka-connector-overview#target-tables-for-kafka-topics
        """
        table_name = re.sub("[^a-zA-Z0-9_]", "_", topic_name)
        if re.match("^[^a-zA-Z_].*", table_name):
            table_name = "_" + table_name
        # Connector  may append original topic's hash code as suffix for conflict resolution
        # if generated table names for 2 topics are similar. This corner case is not handled here.
        # Note that Snowflake recommends to choose topic names that follow the rules for
        # Snowflake identifier names so this case is not recommended by snowflake.
        return table_name

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> SnowflakeParser:
        database_name = connector_manifest.config["snowflake.database.name"]
        schema_name = connector_manifest.config["snowflake.schema.name"]

        # Fetch user provided topic to table map
        provided_topics_to_tables: Dict[str, str] = {}
        if connector_manifest.config.get("snowflake.topic2table.map"):
            for each in connector_manifest.config["snowflake.topic2table.map"].split(
                ","
            ):
                topic, table = each.split(":")
                provided_topics_to_tables[topic.strip()] = table.strip()

        topics_to_tables: Dict[str, str] = {}
        # Extract lineage for only those topics whose data ingestion started
        for topic in connector_manifest.topic_names:
            if topic in provided_topics_to_tables:
                # If user provided which table to get mapped with this topic
                topics_to_tables[topic] = provided_topics_to_tables[topic]
            else:
                # Else connector converts topic name to a valid Snowflake table name.
                topics_to_tables[topic] = self.get_table_name_from_topic_name(topic)

        return self.SnowflakeParser(
            database_name=database_name,
            schema_name=schema_name,
            topics_to_tables=topics_to_tables,
        )

    def extract_flow_property_bag(self) -> Dict[str, str]:
        # For all snowflake sink connector properties, refer below link
        # https://docs.snowflake.com/en/user-guide/kafka-connector-install#configuring-the-kafka-connector
        # remove private keys, secrets from properties
        flow_property_bag = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k
            not in [
                "snowflake.private.key",
                "snowflake.private.key.passphrase",
                "value.converter.basic.auth.user.info",
            ]
        }

        return flow_property_bag

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)

        for topic, table in parser.topics_to_tables.items():
            target_dataset = f"{parser.database_name}.{parser.schema_name}.{table}"
            lineages.append(
                KafkaConnectLineage(
                    source_dataset=topic,
                    source_platform=KAFKA,
                    target_dataset=target_dataset,
                    target_platform="snowflake",
                )
            )

        return lineages


@dataclass
class BigQuerySinkConnector(BaseConnector):
    @dataclass
    class BQParser:
        project: str
        target_platform: str
        sanitizeTopics: bool
        transforms: list
        topicsToTables: Optional[str] = None
        datasets: Optional[str] = None
        defaultDataset: Optional[str] = None
        version: str = "v1"

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> BQParser:
        project = connector_manifest.config["project"]
        sanitizeTopics = connector_manifest.config.get("sanitizeTopics") or "false"
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

        if "defaultDataset" in connector_manifest.config:
            defaultDataset = connector_manifest.config["defaultDataset"]
            return self.BQParser(
                project=project,
                defaultDataset=defaultDataset,
                target_platform="bigquery",
                sanitizeTopics=sanitizeTopics.lower() == "true",
                version="v2",
                transforms=transforms,
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
                transforms=transforms,
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

    def apply_transformations(
        self, topic: str, transforms: List[Dict[str, str]]
    ) -> str:
        for transform in transforms:
            if transform["type"] == "org.apache.kafka.connect.transforms.RegexRouter":
                regex = transform["regex"]
                replacement = transform["replacement"]
                pattern = re.compile(regex)
                if pattern.match(topic):
                    topic = pattern.sub(replacement, topic, count=1)
        return topic

    def extract_flow_property_bag(self) -> Dict[str, str]:
        # Mask/Remove properties that may reveal credentials
        flow_property_bag = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k not in ["keyfile"]
        }

        return flow_property_bag

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        if not parser:
            return lineages
        target_platform = parser.target_platform
        project = parser.project
        transforms = parser.transforms

        for topic in self.connector_manifest.topic_names:
            transformed_topic = self.apply_transformations(topic, transforms)
            dataset_table = self.get_dataset_table_for_topic(transformed_topic, parser)
            if dataset_table is None:
                self.report.warning(
                    "Could not find target dataset for topic, please check your connector configuration"
                    f"{self.connector_manifest.name} : {transformed_topic} ",
                )
                continue
            target_dataset = f"{project}.{dataset_table}"

            lineages.append(
                KafkaConnectLineage(
                    source_dataset=transformed_topic,
                    source_platform=KAFKA,
                    target_dataset=target_dataset,
                    target_platform=target_platform,
                )
            )
        return lineages


@dataclass
class JdbcSinkConnector(BaseConnector):
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
        target_platform: str
        database_name: str
        table_name: Optional[str]
        transforms: list

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> JdbcParser:
        url = remove_prefix(
            str(connector_manifest.config.get("connection.url")), "jdbc:"
        )
        db_type = "external"
        if "tibero" in url or "oracle" in url:
            url = fix_oracle_tibero_url(url)
            db_type = "tibero" if "tibero" in url else "oracle"
        url_instance = make_url(url)
        platform = get_platform_from_sqlalchemy_uri(str(url_instance))
        target_platform = db_type if platform == "external" else platform
        database_name = url_instance.database or url_instance.query.get("service_name")
        assert database_name
        db_connection_url = f"{url_instance.drivername}://{url_instance.host}:{url_instance.port}/{database_name}"

        # table 이름은 동적명 또는 고정된 문자열
        # "${topic}"일 경우 topic 이름이 그대로 / RegexRouter로 적재 테이블명이 변경될 수 있음
        table_name = self.connector_manifest.config.get("table.name.format")
        schema_name = url_instance.query.get("currentSchema")
        if schema_name:
            table_name = f"{schema_name}.{table_name}"

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
            target_platform,
            database_name,
            table_name,
            transforms,
        )

    def default_get_lineages(
        self,
        database_name: str,
        table_name: str,
        target_platform: str,
        topic_names: Optional[Iterable[str]] = None,
    ) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = []
        if not topic_names:
            topic_names = self.connector_manifest.topic_names

        for topic in topic_names:
            dataset_name: str = get_dataset_name(database_name, table_name)
            lineage = KafkaConnectLineage(
                source_dataset=topic,
                source_platform=KAFKA,
                target_dataset=dataset_name,
                target_platform=target_platform,
            )
            lineages.append(lineage)
        return lineages

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
        target_platform = parser.target_platform
        database_name = parser.database_name
        table_name = parser.table_name
        transforms = parser.transforms
        topic_names = self.connector_manifest.topic_names

        logging.debug(
            f"Extracting source platform: {target_platform} and database name: {database_name} from connection url "
        )

        if not topic_names:
            return lineages

        SINGLE_TRANSFORM = len(transforms) == 1
        NO_TRANSFORM = len(transforms) == 0
        UNKNOWN_TRANSFORM = any(
            [
                transform.get("type")
                not in self.KNOWN_TOPICROUTING_TRANSFORMS
                + self.KNOWN_NONTOPICROUTING_TRANSFORMS
                for transform in transforms
            ]
        )
        ALL_TRANSFORMS_NON_TOPICROUTING = all(
            [
                transform.get("type") in self.KNOWN_NONTOPICROUTING_TRANSFORMS
                for transform in transforms
            ]
        )

        # Case 1: No transform or only non-topic-routing transforms → default 처리
        if NO_TRANSFORM or ALL_TRANSFORMS_NON_TOPICROUTING:
            return self.default_get_lineages(
                database_name=database_name,
                table_name=table_name,
                target_platform=target_platform,
                topic_names=topic_names,
            )

        if SINGLE_TRANSFORM and transforms[0]["type"] == self.REGEXROUTER:
            from java.util.regex import Pattern

            transform_regex = Pattern.compile(transforms[0]["regex"])
            transform_replacement = transforms[0]["replacement"]

            for topic in topic_names:
                matcher = transform_regex.matcher(topic)
                # RegexRouter를 적용하여 변환된 테이블 이름 계산
                transformed_topic = (
                    str(matcher.replaceFirst(transform_replacement))
                    if matcher.matches()
                    else topic
                )

                # table.name.format 에 "${topic}"이 포함된 경우 대체
                table_name_format = self.connector_manifest.config.get(
                    "table.name.format", "${topic}"
                )
                resolved_table_name = table_name_format.replace(
                    "${topic}", transformed_topic
                )

                dataset_name = get_dataset_name(database_name, resolved_table_name)
                lineage = KafkaConnectLineage(
                    source_dataset=topic,
                    source_platform=KAFKA,
                    target_dataset=dataset_name,
                    target_platform=target_platform,
                )
                lineages.append(lineage)

            return lineages
        else:
            if SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report.warning(
                    "Could not find input dataset, connector has unknown transform",
                    f"{self.connector_manifest.name} : {transforms[0]['type']}",
                )
            if not SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report.warning(
                    "Could not find input dataset, connector has one or more unknown transforms",
                    self.connector_manifest.name,
                )
            lineages = self.default_get_lineages(
                database_name=database_name,
                table_name=table_name,
                target_platform=target_platform,
            )
            return lineages


BIGQUERY_SINK_CONNECTOR_CLASS = "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
S3_SINK_CONNECTOR_CLASS = "io.confluent.connect.s3.S3SinkConnector"
SNOWFLAKE_SINK_CONNECTOR_CLASS = "com.snowflake.kafka.connector.SnowflakeSinkConnector"
JDBC_SINK_CONNECTOR_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector"
