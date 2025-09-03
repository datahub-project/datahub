import logging
import re
from dataclasses import dataclass
from typing import Dict, Final, Iterable, List, Optional, Tuple

from datahub.ingestion.source.kafka_connect.common import (
    KAFKA,
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    parse_comma_separated_list,
)

logger = logging.getLogger(__name__)


class BaseTransform:
    """Base class for Kafka Connect transforms."""

    def __init__(self, config: Dict[str, str]):
        self.config = config

    def apply(self, current_topics: List[str]) -> List[str]:
        """Apply the transform to the current topics."""
        raise NotImplementedError("Subclasses must implement apply method")


class RegexRouterTransform(BaseTransform):
    """Kafka Connect RegexRouter transform."""

    def apply(self, current_topics: List[str]) -> List[str]:
        """Apply RegexRouter transform to rename topics."""
        regex_pattern = self.config.get("regex", "")
        replacement = self.config.get("replacement", "")

        if not regex_pattern or replacement is None:
            logger.warning("RegexRouter missing regex or replacement pattern")
            return current_topics

        transformed_topics = []
        for topic in current_topics:
            try:
                # Use Java regex for exact Kafka Connect compatibility
                from java.util.regex import Pattern

                transform_regex = Pattern.compile(regex_pattern)
                matcher = transform_regex.matcher(topic)

                if matcher.matches():
                    transformed_topic = str(matcher.replaceFirst(replacement))
                    logger.debug(f"RegexRouter: {topic} -> {transformed_topic}")
                    transformed_topics.append(transformed_topic)
                else:
                    logger.debug(f"RegexRouter: {topic} (no match)")
                    transformed_topics.append(topic)

            except ImportError:
                logger.warning(
                    f"Java regex library not available for RegexRouter transform. "
                    f"Cannot apply pattern '{regex_pattern}' to topic '{topic}'. "
                    f"Returning original topic name."
                )
                transformed_topics.append(topic)

            except Exception as e:
                logger.warning(
                    f"RegexRouter failed for topic '{topic}' with pattern '{regex_pattern}' "
                    f"and replacement '{replacement}': {e}"
                )
                transformed_topics.append(topic)

        return transformed_topics


def _apply_transforms_to_topics(topics: List[str], config: Dict[str, str]) -> List[str]:
    """Apply transforms to topic list using proper Transform classes."""
    # Get the transforms parameter
    transforms_param: str = config.get("transforms", "")
    if not transforms_param:
        return topics

    # Parse transform names
    transform_names = parse_comma_separated_list(transforms_param)

    # Known transform types
    TRANSFORM_CLASSES = {
        "org.apache.kafka.connect.transforms.RegexRouter": RegexRouterTransform,
        "io.confluent.connect.cloud.transforms.TopicRegexRouter": RegexRouterTransform,
    }

    # Build transform pipeline
    transforms = []
    for transform_name in transform_names:
        if not transform_name:
            continue

        # Get transform configuration
        transform_config = {"name": transform_name}
        transform_prefix = f"transforms.{transform_name}."

        for key, value in config.items():
            if key.startswith(transform_prefix):
                config_key = key[len(transform_prefix) :]
                transform_config[config_key] = value

        # Create transform instance only for known types
        transform_type = transform_config.get("type", "")
        transform_class = TRANSFORM_CLASSES.get(transform_type)

        if transform_class:
            transform_instance = transform_class(transform_config)
            transforms.append(transform_instance)
        else:
            logger.debug(f"Skipping unsupported transform type: {transform_type}")

    # Apply transforms in sequence
    result_topics = topics[:]
    for transform in transforms:
        result_topics = transform.apply(result_topics)

    return result_topics


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
            transformed_topics = _apply_transforms_to_topics(
                topic_list, self.connector_manifest.config
            )

            lineages: List[KafkaConnectLineage] = list()
            for original_topic, transformed_topic in zip(
                topic_list, transformed_topics
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

        # Apply transforms to get final topic names
        topic_list = list(connector_manifest.topic_names)
        transformed_topics = _apply_transforms_to_topics(
            topic_list, connector_manifest.config
        )

        topics_to_tables: Dict[str, str] = {}
        # Extract lineage for only those topics whose data ingestion started
        for original_topic, transformed_topic in zip(topic_list, transformed_topics):
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
        topicregex_dataset_map: Dict[str, str] = dict(self.get_list(parser.datasets))  # type: ignore
        from java.util.regex import Pattern

        for pattern, dataset in topicregex_dataset_map.items():
            patternMatcher = Pattern.compile(pattern).matcher(topic)
            if patternMatcher.matches():
                return dataset
        return None

    def sanitize_table_name(self, table_name: str) -> str:
        """
        Sanitize table name for BigQuery compatibility following Kafka Connect BigQuery connector logic.

        Implementation follows the official BigQuery Kafka Connect connector sanitization behavior:
        - BigQuery allows only specific characters for dataset and table names
        - All invalid characters are replaced by underscores
        - If the resulting name would start with a digit, an underscore is prepended

        References:
        - Aiven BigQuery Connector: https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka
        - Confluent BigQuery Connector: https://github.com/confluentinc/kafka-connect-bigquery
        - BigQuery Naming Rules: https://cloud.google.com/bigquery/docs/tables#table_naming

        BigQuery table naming rules:
        - Must contain only letters (a-z, A-Z), numbers (0-9), and underscores (_)
        - Must start with a letter or underscore
        - Maximum 1024 characters

        Args:
            table_name: The original table name to sanitize

        Returns:
            Sanitized table name that follows BigQuery naming conventions

        Raises:
            ValueError: If the input table_name is empty or results in an empty string after sanitization
        """
        import re

        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")

        # Follow the exact Confluent BigQuery connector sanitization logic:
        # 1. Replace all invalid characters with underscores
        # 2. If name starts with digit or other invalid character, prepend underscore
        # This matches the actual Confluent connector implementation

        # Step 1: Replace all invalid characters with underscores
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", table_name.strip())

        # Step 2: If name doesn't start with letter or underscore, prepend underscore
        if sanitized and re.match(r"^[^a-zA-Z_].*", sanitized):
            sanitized = f"_{sanitized}"

        # If sanitization resulted in empty string, raise error
        if not sanitized:
            raise ValueError(
                f"Table name '{table_name}' cannot be sanitized to a valid BigQuery table name"
            )

        # Truncate if too long (BigQuery table name limit is 1024 characters)
        if len(sanitized) > 1024:
            sanitized = sanitized[:1024].rstrip("_")

        return sanitized

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
        transformed_topics = _apply_transforms_to_topics(
            topic_list, self.connector_manifest.config
        )

        for original_topic, transformed_topic in zip(topic_list, transformed_topics):
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

            lineages.append(
                KafkaConnectLineage(
                    source_dataset=original_topic,  # Keep original topic as source
                    source_platform=KAFKA,
                    target_dataset=target_dataset,
                    target_platform=target_platform,
                )
            )
        return lineages


BIGQUERY_SINK_CONNECTOR_CLASS: Final[str] = (
    "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
)
S3_SINK_CONNECTOR_CLASS: Final[str] = "io.confluent.connect.s3.S3SinkConnector"
SNOWFLAKE_SINK_CONNECTOR_CLASS: Final[str] = (
    "com.snowflake.kafka.connector.SnowflakeSinkConnector"
)
