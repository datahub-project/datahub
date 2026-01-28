import logging
import re
from dataclasses import dataclass
from typing import Dict, Final, Iterable, List, Optional, Tuple

from sqlalchemy.engine.url import URL, make_url

from datahub.ingestion.source.kafka_connect.common import (
    KAFKA,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_dataset_name,
    has_three_level_hierarchy,
    remove_prefix,
    validate_jdbc_url,
)
from datahub.ingestion.source.kafka_connect.config_constants import (
    ConnectorConfigKeys,
    parse_comma_separated_list,
)
from datahub.ingestion.source.kafka_connect.transform_plugins import (
    get_transform_pipeline,
)
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)

logger = logging.getLogger(__name__)


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
        bucket: Optional[str] = connector_manifest.config.get("s3.bucket.name")
        if not bucket:
            raise ValueError(
                "Could not find 's3.bucket.name' in connector configuration"
            )

        # https://docs.confluent.io/kafka-connectors/s3-sink/current/configuration_options.html#storage
        topics_dir: str = connector_manifest.config.get("topics.dir", "topics")

        return self.S3SinkParser(
            target_platform="s3",
            bucket=bucket,
            topics_dir=topics_dir,
            topics=connector_manifest.topic_names,
        )

    def get_topics_from_config(self) -> List[str]:
        """
        Extract topics from S3 sink connector configuration.

        Supports both explicit topic lists and regex patterns:
        - topics: Comma-separated list of topic names
        - topics.regex: Java regex pattern to match topics dynamically
        """
        config = self.connector_manifest.config

        # Priority 1: Explicit 'topics' field
        topics = config.get(ConnectorConfigKeys.TOPICS, "")
        if topics:
            return parse_comma_separated_list(topics)

        # Priority 2: 'topics.regex' pattern
        topics_regex = config.get(ConnectorConfigKeys.TOPICS_REGEX, "")
        if topics_regex:
            # Expand pattern using available sources
            return self._expand_topic_regex_patterns(
                topics_regex,
                available_topics=self.connector_manifest.topic_names
                if self.connector_manifest.topic_names
                else None,
            )

        return []

    def extract_flow_property_bag(self) -> Dict[str, str]:
        # Mask/Remove properties that may reveal credentials
        flow_property_bag: Dict[str, str] = {
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
            parser: ConfluentS3SinkConnector.S3SinkParser = self._get_parser(
                self.connector_manifest
            )

            # Apply transforms to all topics
            topic_list = list(parser.topics)
            transform_result = get_transform_pipeline().apply_forward(
                topic_list, self.connector_manifest.config
            )
            transformed_topics = transform_result.topics

            # Log any warnings from transform processing
            for warning in transform_result.warnings:
                self.report.warning(
                    f"Transform warning for {self.connector_manifest.name}: {warning}"
                )

            if transform_result.fallback_used:
                self.report.info(
                    f"Complex transforms detected in {self.connector_manifest.name}. "
                    f"Consider using 'generic_connectors' config for explicit mappings."
                )

            lineages: List[KafkaConnectLineage] = list()
            for original_topic, transformed_topic in zip(
                topic_list, transformed_topics, strict=False
            ):
                target_dataset: str = (
                    f"{parser.bucket}/{parser.topics_dir}/{transformed_topic}"
                )

                lineages.append(
                    KafkaConnectLineage(
                        source_dataset=original_topic,
                        source_platform="kafka",
                        target_dataset=target_dataset,
                        target_platform=parser.target_platform,
                    )
                )
            return lineages
        except ValueError as e:
            self.report.warning(
                f"Configuration error in S3 sink connector {self.connector_manifest.name}",
                self.connector_manifest.name,
                exc=e,
            )
        except Exception as e:
            self.report.warning(
                f"Unexpected error resolving lineage for S3 sink connector {self.connector_manifest.name}",
                self.connector_manifest.name,
                exc=e,
            )

        return []

    def get_platform(self) -> str:
        """Get the platform for S3 Sink connector."""
        return "s3"


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
        table_name: str = re.sub("[^a-zA-Z0-9_]", "_", topic_name)
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
        database_name: str = connector_manifest.config["snowflake.database.name"]
        schema_name: str = connector_manifest.config["snowflake.schema.name"]

        # Fetch user provided topic to table map
        provided_topics_to_tables: Dict[str, str] = {}
        if connector_manifest.config.get("snowflake.topic2table.map"):
            try:
                mappings = parse_comma_separated_list(
                    connector_manifest.config["snowflake.topic2table.map"]
                )
                for mapping in mappings:
                    if ":" not in mapping:
                        logger.warning(
                            f"Invalid topic:table mapping format: '{mapping}'. Expected 'topic:table'."
                        )
                        continue
                    topic, table = mapping.split(":", 1)  # Split only on first colon
                    provided_topics_to_tables[topic.strip()] = table.strip()
            except Exception as e:
                logger.warning(f"Failed to parse snowflake.topic2table.map: {e}")

        # Get available topics (all cluster topics for Cloud, connector topics for OSS)
        available_topics = set(
            self.all_cluster_topics or connector_manifest.topic_names
        )

        # Get topics the connector subscribes to from its configuration
        subscribed_topics = set(self.get_topics_from_config())

        # Filter available topics to only those the connector subscribes to
        if subscribed_topics:
            topic_list = list(available_topics.intersection(subscribed_topics))
            logger.debug(
                f"Filtered to {len(topic_list)} subscribed topics for {connector_manifest.name}: {topic_list}"
            )
        else:
            # If no subscription config, use all available topics (OSS behavior)
            topic_list = list(available_topics)
            logger.debug(
                f"No subscription filter found, using all {len(topic_list)} available topics"
            )
        transform_result = get_transform_pipeline().apply_forward(
            topic_list, connector_manifest.config
        )
        transformed_topics = transform_result.topics

        topics_to_tables: Dict[str, str] = {}
        # Extract lineage for only those topics whose data ingestion started
        for original_topic, transformed_topic in zip(
            topic_list, transformed_topics, strict=False
        ):
            if original_topic in provided_topics_to_tables:
                # If user provided which table to get mapped with this topic
                topics_to_tables[original_topic] = provided_topics_to_tables[
                    original_topic
                ]
            else:
                # Use the transformed topic name to generate table name
                topics_to_tables[original_topic] = self.get_table_name_from_topic_name(
                    transformed_topic
                )

        return self.SnowflakeParser(
            database_name=database_name,
            schema_name=schema_name,
            topics_to_tables=topics_to_tables,
        )

    def get_topics_from_config(self) -> List[str]:
        """
        Extract topics from Snowflake sink connector configuration.

        Supports both explicit topic lists and regex patterns:
        - topics: Comma-separated list of topic names
        - topics.regex: Java regex pattern to match topics dynamically
        """
        config = self.connector_manifest.config

        # Priority 1: Explicit 'topics' field
        topics = config.get(ConnectorConfigKeys.TOPICS, "")
        if topics:
            return parse_comma_separated_list(topics)

        # Priority 2: 'topics.regex' pattern
        topics_regex = config.get(ConnectorConfigKeys.TOPICS_REGEX, "")
        if topics_regex:
            # Expand pattern using available sources
            return self._expand_topic_regex_patterns(
                topics_regex,
                available_topics=self.connector_manifest.topic_names
                if self.connector_manifest.topic_names
                else None,
            )

        return []

    def extract_flow_property_bag(self) -> Dict[str, str]:
        # For all snowflake sink connector properties, refer below link
        # https://docs.snowflake.com/en/user-guide/kafka-connector-install#configuring-the-kafka-connector
        # remove private keys, secrets from properties
        flow_property_bag: Dict[str, str] = {
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
        parser: SnowflakeSinkConnector.SnowflakeParser = self.get_parser(
            self.connector_manifest
        )

        for topic, table in parser.topics_to_tables.items():
            target_dataset: str = f"{parser.database_name}.{parser.schema_name}.{table}"

            # Extract column-level lineage if enabled (uses base class method)
            fine_grained = self._extract_fine_grained_lineage(
                source_dataset=topic,
                source_platform=KAFKA,
                target_dataset=target_dataset,
                target_platform="snowflake",
            )

            lineages.append(
                KafkaConnectLineage(
                    source_dataset=topic,
                    source_platform=KAFKA,
                    target_dataset=target_dataset,
                    target_platform="snowflake",
                    fine_grained_lineages=fine_grained,
                )
            )

        return lineages

    def get_platform(self) -> str:
        """Get the platform for Snowflake Sink connector."""
        return "snowflake"


@dataclass
class BigQuerySinkConnector(BaseConnector):
    @dataclass
    class BQParser:
        project: str
        target_platform: str
        sanitizeTopics: bool
        transforms: List[Dict[str, str]]
        topicsToTables: Optional[str] = None
        datasets: Optional[str] = None
        defaultDataset: Optional[str] = None
        version: str = "v1"

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> BQParser:
        project: str = connector_manifest.config["project"]
        sanitizeTopics: str = connector_manifest.config.get("sanitizeTopics") or "false"

        # Parse ALL transforms (original BigQuery logic)
        transform_names: List[str] = (
            self.connector_manifest.config.get("transforms", "").split(",")
            if self.connector_manifest.config.get("transforms")
            else []
        )
        transforms: List[Dict[str, str]] = []
        for name in transform_names:
            transform: Dict[str, str] = {"name": name}
            transforms.append(transform)
            for key in self.connector_manifest.config:
                if key.startswith(f"transforms.{name}."):
                    transform[key.replace(f"transforms.{name}.", "")] = (
                        self.connector_manifest.config[key]
                    )

        # BigQuery connector supports two configuration versions for backward compatibility:
        # v2 (current): Uses 'defaultDataset' with simpler topic:dataset mapping
        # v1 (legacy): Uses 'datasets' with regex-based topic-to-dataset mapping
        #
        # This dual support is necessary because:
        # 1. Many production deployments still use v1 configuration format
        # 2. Breaking changes would require coordinated upgrades across environments
        # 3. v1 supports more complex topic routing that some users depend on

        if "defaultDataset" in connector_manifest.config:
            # v2 configuration: simpler, recommended approach
            defaultDataset: str = connector_manifest.config["defaultDataset"]
            return self.BQParser(
                project=project,
                defaultDataset=defaultDataset,
                target_platform="bigquery",
                sanitizeTopics=sanitizeTopics.lower() == "true",
                version="v2",
                transforms=transforms,
            )
        else:
            # v1 configuration: legacy format with regex-based dataset mapping
            datasets: str = connector_manifest.config["datasets"]
            topicsToTables: Optional[str] = connector_manifest.config.get(
                "topicsToTables"
            )

            return self.BQParser(
                project=project,
                topicsToTables=topicsToTables,
                datasets=datasets,
                target_platform="bigquery",
                sanitizeTopics=sanitizeTopics.lower() == "true",
                transforms=transforms,
            )

    def get_list(self, property: str) -> Iterable[Tuple[str, str]]:
        entries = parse_comma_separated_list(property)
        for entry in entries:
            if "=" not in entry:
                logger.warning(
                    f"Invalid key=value mapping format: '{entry}'. Expected 'key=value'."
                )
                continue
            try:
                key, val = entry.rsplit("=", 1)  # Split only on last equals sign
                yield (key.strip(), val.strip())
            except ValueError as e:
                logger.warning(f"Failed to parse mapping entry '{entry}': {e}")

    def get_dataset_for_topic_v1(self, topic: str, parser: BQParser) -> Optional[str]:
        from datahub.ingestion.source.kafka_connect.pattern_matchers import (
            JavaRegexMatcher,
        )

        topicregex_dataset_map: Dict[str, str] = dict(self.get_list(parser.datasets))  # type: ignore
        matcher = JavaRegexMatcher()

        for pattern, dataset in topicregex_dataset_map.items():
            if matcher.matches(pattern, topic):
                return dataset
        return None

    def sanitize_table_name(self, table_name: str) -> str:
        """
        Sanitize table name for BigQuery compatibility following Kafka Connect BigQuery connector logic.
        Refer to https://cloud.google.com/bigquery/docs/tables#table_naming
        """
        table_name = re.sub("[^a-zA-Z0-9_]", "_", table_name)
        if re.match("^[^a-zA-Z_].*", table_name):
            table_name = "_" + table_name
        return table_name

    def get_dataset_table_for_topic(
        self, topic: str, parser: BQParser
    ) -> Optional[str]:
        if parser.version == "v2":
            dataset: Optional[str] = parser.defaultDataset
            parts: List[str] = topic.split(":")
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
                from datahub.ingestion.source.kafka_connect.pattern_matchers import (
                    JavaRegexMatcher,
                )

                topicregex_table_map: Dict[str, str] = dict(
                    self.get_list(parser.topicsToTables)  # type: ignore
                )
                matcher = JavaRegexMatcher()

                for pattern, tbl in topicregex_table_map.items():
                    if matcher.matches(pattern, topic):
                        table = tbl
                        break

        if parser.sanitizeTopics:
            table = self.sanitize_table_name(table)
        return f"{dataset}.{table}"

    def get_topics_from_config(self) -> List[str]:
        """
        Extract topics from BigQuery sink connector configuration.

        Supports both explicit topic lists and regex patterns:
        - topics: Comma-separated list of topic names
        - topics.regex: Java regex pattern to match topics dynamically
        """
        config = self.connector_manifest.config

        # Priority 1: Explicit 'topics' field
        topics = config.get(ConnectorConfigKeys.TOPICS, "")
        if topics:
            return parse_comma_separated_list(topics)

        # Priority 2: 'topics.regex' pattern
        topics_regex = config.get(ConnectorConfigKeys.TOPICS_REGEX, "")
        if topics_regex:
            # Expand pattern using available sources
            return self._expand_topic_regex_patterns(
                topics_regex,
                available_topics=self.connector_manifest.topic_names
                if self.connector_manifest.topic_names
                else None,
            )

        return []

    def extract_flow_property_bag(self) -> Dict[str, str]:
        # Mask/Remove properties that may reveal credentials
        flow_property_bag: Dict[str, str] = {
            k: v
            for k, v in self.connector_manifest.config.items()
            if k not in ["keyfile"]
        }

        return flow_property_bag

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser: BigQuerySinkConnector.BQParser = self.get_parser(
            self.connector_manifest
        )
        if not parser:
            return lineages
        target_platform: str = parser.target_platform
        project: str = parser.project

        # Apply transforms to all topics
        topic_list = list(self.connector_manifest.topic_names)
        transform_result = get_transform_pipeline().apply_forward(
            topic_list, self.connector_manifest.config
        )
        transformed_topics = transform_result.topics

        # Log any warnings from transform processing
        for warning in transform_result.warnings:
            self.report.warning(
                f"Transform warning for {self.connector_manifest.name}: {warning}"
            )

        if transform_result.fallback_used:
            self.report.info(
                f"Complex transforms detected in {self.connector_manifest.name}. "
                f"Consider using 'generic_connectors' config for explicit mappings."
            )

        for original_topic, transformed_topic in zip(
            topic_list, transformed_topics, strict=False
        ):
            # Use the transformed topic to determine dataset/table
            dataset_table: Optional[str] = self.get_dataset_table_for_topic(
                transformed_topic, parser
            )
            if dataset_table is None:
                self.report.warning(
                    "Could not find target dataset for topic, please check your connector configuration"
                    f"{self.connector_manifest.name} : {transformed_topic} ",
                )
                continue
            target_dataset: str = f"{project}.{dataset_table}"

            # Extract column-level lineage if enabled (uses base class method)
            fine_grained = self._extract_fine_grained_lineage(
                source_dataset=original_topic,
                source_platform=KAFKA,
                target_dataset=target_dataset,
                target_platform=target_platform,
            )

            lineages.append(
                KafkaConnectLineage(
                    source_dataset=original_topic,  # Keep original topic as source
                    source_platform=KAFKA,
                    target_dataset=target_dataset,
                    target_platform=target_platform,
                    fine_grained_lineages=fine_grained,
                )
            )
        return lineages

    def get_platform(self) -> str:
        """Get the platform for BigQuery Sink connector."""
        return "bigquery"


@dataclass
class JdbcSinkParser:
    """
    Data transfer object for JDBC sink connector configuration.

    Mirrors the pattern used in source connectors for consistency.
    """

    db_connection_url: str
    target_platform: str
    database_name: str
    schema_name: Optional[str]
    table_name_format: str


class JdbcSinkParserFactory:
    """
    Factory for creating JDBC sink parsers based on configuration type.

    Supports two configuration styles:
    1. Self-hosted JDBC connectors: Use 'connection.url' with full JDBC URL
    2. Confluent Cloud managed connectors: Use 'connection.host', 'connection.port', 'db.name'
    """

    def create_parser(
        self, connector_manifest: ConnectorManifest, platform: str
    ) -> JdbcSinkParser:
        """
        Main factory method - creates parser from connector configuration.

        Detects configuration style and delegates to appropriate parser method.

        Args:
            connector_manifest: The connector manifest with configuration
            platform: Target platform ('postgres', 'mysql', etc.)

        Returns:
            JdbcSinkParser with parsed configuration

        Raises:
            ValueError: If required configuration is missing or invalid
        """
        config = connector_manifest.config

        # Check which configuration style is being used
        connection_url = config.get("connection.url", "")

        if connection_url and validate_jdbc_url(connection_url):
            # Self-hosted style: Parse JDBC URL
            return self._create_parser_from_url(
                connector_manifest, platform, connection_url
            )
        else:
            # Confluent Cloud style: Build from separate fields
            return self._create_parser_from_fields(connector_manifest, platform)

    def _create_parser_from_url(
        self,
        connector_manifest: ConnectorManifest,
        platform: str,
        connection_url: str,
    ) -> JdbcSinkParser:
        """
        Create parser from self-hosted JDBC connector configuration.

        Uses connection.url field with full JDBC URL.

        Args:
            connector_manifest: The connector manifest
            platform: Target platform
            connection_url: Full JDBC URL

        Returns:
            JdbcSinkParser with parsed configuration
        """
        # Parse JDBC URL using SQLAlchemy
        jdbc_url = remove_prefix(connection_url, "jdbc:")
        url_instance = make_url(jdbc_url)

        # Extract database name
        database_name = url_instance.database
        if not database_name:
            raise ValueError(
                f"Missing database name in JDBC URL: {jdbc_url}. "
                f"JDBC URLs must include a database name, e.g., 'jdbc:postgresql://host:port/database_name'"
            )

        # Get target platform from SQLAlchemy URL
        target_platform = get_platform_from_sqlalchemy_uri(str(url_instance))

        # Extract schema from URL query parameters or use defaults
        schema_name = self._extract_schema_from_url(
            url_instance, platform, connector_manifest.config
        )

        # Build clean connection URL for property bag
        db_connection_url = f"{url_instance.drivername}://{url_instance.host}:{url_instance.port}/{database_name}"

        # Get table name format (how topics map to tables)
        table_name_format = connector_manifest.config.get(
            "table.name.format", "${topic}"
        )

        return JdbcSinkParser(
            db_connection_url=db_connection_url,
            target_platform=target_platform,
            database_name=database_name,
            schema_name=schema_name,
            table_name_format=table_name_format,
        )

    def _create_parser_from_fields(
        self, connector_manifest: ConnectorManifest, platform: str
    ) -> JdbcSinkParser:
        """
        Create parser from Confluent Cloud managed connector configuration.

        Uses separate fields: connection.host, connection.port, db.name

        Args:
            connector_manifest: The connector manifest
            platform: Target platform ('postgres', 'mysql', etc.)

        Returns:
            JdbcSinkParser with parsed configuration

        Raises:
            ValueError: If required fields are missing
        """
        config = connector_manifest.config

        # Extract connection details from separate fields
        host = config.get("connection.host")
        port = config.get("connection.port", "5432")  # Default Postgres port
        database_name = config.get("db.name")

        if not host:
            raise ValueError(
                f"Missing 'connection.host' in Confluent Cloud connector {connector_manifest.name}"
            )

        if not database_name:
            raise ValueError(
                f"Missing 'db.name' in Confluent Cloud connector {connector_manifest.name}"
            )

        # Build a clean connection URL (without credentials)
        db_connection_url = f"{platform}://{host}:{port}/{database_name}"

        # Use platform parameter as target platform
        target_platform = platform

        # Extract schema from config or use defaults
        schema_name = config.get("db.schema") or config.get("schema.name")

        # Use platform-specific defaults if not specified
        if not schema_name and has_three_level_hierarchy(platform):
            if platform == "postgres":
                schema_name = "public"  # PostgreSQL default schema

        # Get table name format (how topics map to tables)
        table_name_format = config.get("table.name.format", "${topic}")

        return JdbcSinkParser(
            db_connection_url=db_connection_url,
            target_platform=target_platform,
            database_name=database_name,
            schema_name=schema_name,
            table_name_format=table_name_format,
        )

    def _extract_schema_from_url(
        self,
        url_instance: URL,
        platform: str,
        config: Dict[str, str],
    ) -> Optional[str]:
        """
        Extract schema name from JDBC URL query parameters or config.

        Args:
            url_instance: SQLAlchemy URL instance
            platform: Database platform ('postgres', 'mysql', etc.)
            config: Full connector configuration

        Returns:
            Schema name or None if not applicable
        """
        schema = None

        # Try to get schema from query parameters (Postgres-specific)
        if url_instance.query:
            # Check for currentSchema or schema parameters
            # url_instance.query.get() can return str or Sequence[str], so we need to handle both
            if "currentSchema" in url_instance.query:
                schema_value = url_instance.query.get("currentSchema")
                if schema_value:
                    schema = (
                        schema_value[0]
                        if isinstance(schema_value, (list, tuple))
                        else str(schema_value)
                    )
            elif "schema" in url_instance.query:
                schema_value = url_instance.query.get("schema")
                if schema_value:
                    schema = (
                        schema_value[0]
                        if isinstance(schema_value, (list, tuple))
                        else str(schema_value)
                    )

        # Fallback: Check explicit config fields
        if not schema:
            schema = config.get("schema.name") or config.get("db.schema")

        # Use platform-specific defaults
        if not schema and has_three_level_hierarchy(platform):
            if platform == "postgres":
                schema = "public"  # PostgreSQL default schema
            # MySQL doesn't use schemas (database == schema)
            # SQL Server, Oracle use user-specific defaults

        return schema


@dataclass
class JdbcSinkConnector(BaseConnector):
    """
    Generic JDBC sink connector for Confluent Cloud managed JDBC sinks.

    Supports PostgresSink and MySqlSink connectors that write Kafka topics
    to database tables.

    This implementation follows the patterns established in source connectors:
    - Uses dedicated parser classes for configuration
    - Leverages SQLAlchemy for JDBC URL parsing
    - Utilizes common utility functions for consistency
    """

    platform: str = "postgres"  # Default platform, overridden in __init__

    def __init__(
        self,
        manifest: ConnectorManifest,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
        platform: str = "postgres",
    ):
        super().__init__(manifest, config, report)
        self.platform = platform
        self._parser_factory = JdbcSinkParserFactory()

    def get_parser(self) -> JdbcSinkParser:
        """
        Get parser for this connector using the factory.

        Returns:
            JdbcSinkParser with parsed configuration

        Raises:
            ValueError: If configuration is invalid
        """
        return self._parser_factory.create_parser(
            self.connector_manifest, self.platform
        )

    def get_table_name_from_topic(self, topic: str, table_format: str) -> str:
        """
        Extract table name from topic using connector configuration.

        Uses the table.name.format config or defaults to topic name.
        Common format: "${topic}" means table name = topic name

        Args:
            topic: The Kafka topic name
            table_format: Table name format from configuration

        Returns:
            Table name derived from topic
        """
        # Replace ${topic} placeholder with actual topic name
        if "${topic}" in table_format:
            return table_format.replace("${topic}", topic)

        # If no ${topic} placeholder, assume format IS the table name
        return table_format

    def get_topics_from_config(self) -> List[str]:
        """
        Extract topics from JDBC sink connector configuration.

        Supports both explicit topic lists and regex patterns:
        - topics: Comma-separated list of topic names
        - topics.regex: Java regex pattern to match topics dynamically
        """
        config = self.connector_manifest.config

        # Priority 1: Explicit 'topics' field
        topics = config.get(ConnectorConfigKeys.TOPICS, "")
        if topics:
            return parse_comma_separated_list(topics)

        # Priority 2: 'topics.regex' pattern
        topics_regex = config.get(ConnectorConfigKeys.TOPICS_REGEX, "")
        if topics_regex:
            # Expand pattern using available sources
            return self._expand_topic_regex_patterns(
                topics_regex,
                available_topics=self.connector_manifest.topic_names
                if self.connector_manifest.topic_names
                else None,
            )

        return []

    def extract_flow_property_bag(self) -> Dict[str, str]:
        """
        Remove sensitive credentials from property bag.

        Uses the parser to get a sanitized connection URL without credentials.
        """
        try:
            parser = self.get_parser()

            # Remove sensitive fields and use sanitized URL
            flow_property_bag: Dict[str, str] = {
                k: v
                for k, v in self.connector_manifest.config.items()
                if k
                not in [
                    "connection.password",
                    "connection.user",
                    "db.password",
                    "db.user",
                ]
            }

            # Replace connection URL with sanitized version
            flow_property_bag["connection.url"] = parser.db_connection_url

            return flow_property_bag

        except Exception as e:
            logger.warning(
                f"Failed to parse JDBC sink connector config for {self.connector_manifest.name}: {e}"
            )
            # Fallback to basic filtering without URL sanitization
            return {
                k: v
                for k, v in self.connector_manifest.config.items()
                if k
                not in [
                    "connection.password",
                    "connection.user",
                    "db.password",
                    "db.user",
                    "connection.url",  # Remove URL entirely if we can't parse it
                ]
            }

    def extract_lineages(self) -> List[KafkaConnectLineage]:
        """
        Extract lineage from Kafka topics to database tables.

        Creates lineage for each topic: Kafka topic → Database table
        Uses the parser for configuration and helper functions for consistency.

        Returns:
            List of lineage mappings
        """
        lineages: List[KafkaConnectLineage] = []

        try:
            # Parse configuration using factory
            parser = self.get_parser()

            logger.debug(
                f"Extracting lineages for JDBC sink: platform={parser.target_platform}, "
                f"database={parser.database_name}, schema={parser.schema_name}"
            )

            # Get available topics (all cluster topics for Cloud, connector topics for OSS)
            available_topics = set(
                self.all_cluster_topics or self.connector_manifest.topic_names
            )

            # Get topics the connector subscribes to from its configuration
            subscribed_topics = set(self.get_topics_from_config())

            # Filter available topics to only those the connector subscribes to
            if subscribed_topics:
                topic_list = list(available_topics.intersection(subscribed_topics))
                logger.debug(
                    f"Filtered to {len(topic_list)} subscribed topics for {self.connector_manifest.name}: {topic_list}"
                )
            else:
                # If no subscription config, use all available topics (OSS behavior)
                topic_list = list(available_topics)
                logger.debug(
                    f"No subscription filter found, using all {len(topic_list)} available topics"
                )
            transform_result = get_transform_pipeline().apply_forward(
                topic_list, self.connector_manifest.config
            )
            transformed_topics = transform_result.topics

            # Log any warnings from transform processing
            for warning in transform_result.warnings:
                self.report.warning(
                    f"Transform warning for {self.connector_manifest.name}: {warning}"
                )

            if transform_result.fallback_used:
                self.report.info(
                    f"Complex transforms detected in {self.connector_manifest.name}. "
                    f"Consider using 'generic_connectors' config for explicit mappings."
                )

            # Create lineage for each topic
            for original_topic, transformed_topic in zip(
                topic_list, transformed_topics, strict=False
            ):
                # Get table name using format from config
                table_name = self.get_table_name_from_topic(
                    transformed_topic, parser.table_name_format
                )

                # Build fully qualified dataset name using helper function
                if parser.schema_name and has_three_level_hierarchy(
                    parser.target_platform
                ):
                    # Platform supports schema hierarchy: database.schema.table
                    table_with_schema = f"{parser.schema_name}.{table_name}"
                    target_dataset = get_dataset_name(
                        parser.database_name, table_with_schema
                    )
                else:
                    # Platform doesn't use schemas: database.table
                    target_dataset = get_dataset_name(parser.database_name, table_name)

                # Extract column-level lineage if enabled (uses base class method)
                fine_grained = self._extract_fine_grained_lineage(
                    source_dataset=original_topic,
                    source_platform=KAFKA,
                    target_dataset=target_dataset,
                    target_platform=parser.target_platform,
                )

                lineages.append(
                    KafkaConnectLineage(
                        source_dataset=original_topic,
                        source_platform=KAFKA,
                        target_dataset=target_dataset,
                        target_platform=parser.target_platform,
                        fine_grained_lineages=fine_grained,
                    )
                )

            logger.debug(
                f"Extracted {len(lineages)} lineages for JDBC sink connector {self.connector_manifest.name}"
            )

            return lineages

        except ValueError as e:
            self.report.warning(
                f"Configuration error in JDBC sink connector {self.connector_manifest.name}",
                self.connector_manifest.name,
                exc=e,
            )
            return []
        except Exception as e:
            self.report.warning(
                f"Failed to extract lineage for JDBC sink connector {self.connector_manifest.name}",
                self.connector_manifest.name,
                exc=e,
            )
            return []

    def get_platform(self) -> str:
        """Get the platform for JDBC Sink connector."""
        return self.platform


BIGQUERY_SINK_CONNECTOR_CLASS: Final[str] = (
    "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
)
S3_SINK_CONNECTOR_CLASS: Final[str] = "io.confluent.connect.s3.S3SinkConnector"
SNOWFLAKE_SINK_CONNECTOR_CLASS: Final[str] = (
    "com.snowflake.kafka.connector.SnowflakeSinkConnector"
)
