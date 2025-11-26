"""
Test for validating Kafka Connect configurations end-to-end.
This test can take any connector config and validate the expected lineage results.
"""

# Initialize JPype with explicit Java 17 path to match command line environment
try:
    import jpype
    import jpype.imports

    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())
except Exception as e:
    print(f"JPype initialization failed: {e}")

from typing import Dict, List
from unittest.mock import Mock

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_dataset_name,
    has_three_level_hierarchy,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    ConfluentJDBCSourceConnector,
)


class TestConnectorConfigValidation:
    """Test full connector configurations with expected lineage results."""

    def validate_lineage_fields(
        self,
        connector_config: Dict[str, str],
        topic_names: List[str],
        expected_lineages: List[Dict[str, str]],
        test_name: str = "test",
    ) -> None:
        """
        Helper method to validate that a connector config produces expected lineage results.

        Args:
            connector_config: Full Kafka Connect connector configuration
            topic_names: List of Kafka topic names the connector produces
            expected_lineages: List of expected lineage mappings with keys:
                - source_dataset: Expected source dataset URN
                - source_platform: Expected source platform name
                - target_dataset: Expected target topic name
                - target_platform: Expected target platform (should be 'kafka')
            test_name: Name of the test for debugging
        """
        manifest = ConnectorManifest(
            name=f"{test_name}-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=topic_names,
        )

        mock_config = Mock(spec=KafkaConnectSourceConfig)
        mock_report = Mock(spec=KafkaConnectSourceReport)

        connector = ConfluentJDBCSourceConnector(manifest, mock_config, mock_report)

        # Test configuration parsing
        parser = connector.get_parser(manifest)
        assert parser is not None, "Parser should be created successfully"

        # Test table name extraction
        table_names = connector.get_table_names()
        assert len(table_names) > 0, "Should extract table names from configuration"

        # Simulate lineage extraction (avoiding Java/JPype dependencies)
        lineages = self._simulate_lineage_extraction(connector, parser)

        # Validate results
        assert len(lineages) == len(expected_lineages), (
            f"Expected {len(expected_lineages)} lineages, got {len(lineages)}"
        )

        for i, (actual, expected) in enumerate(zip(lineages, expected_lineages)):
            assert actual.source_dataset == expected["source_dataset"], (
                f"Lineage {i}: Expected source_dataset '{expected['source_dataset']}', "
                f"got '{actual.source_dataset}'"
            )
            assert actual.source_platform == expected["source_platform"], (
                f"Lineage {i}: Expected source_platform '{expected['source_platform']}', "
                f"got '{actual.source_platform}'"
            )
            assert actual.target_dataset == expected["target_dataset"], (
                f"Lineage {i}: Expected target_dataset '{expected['target_dataset']}', "
                f"got '{actual.target_dataset}'"
            )
            assert actual.target_platform == expected["target_platform"], (
                f"Lineage {i}: Expected target_platform '{expected['target_platform']}', "
                f"got '{actual.target_platform}'"
            )

    def _simulate_lineage_extraction(self, connector, parser):
        """Simulate lineage extraction."""
        source_platform = parser.source_platform
        database_name = parser.database_name
        topic_prefix = parser.topic_prefix
        transforms = parser.transforms
        table_name_tuples = connector.get_table_names()

        # Check if we should use pipeline transforms
        if self._should_use_pipeline(transforms):
            return self._extract_with_pipeline(
                transforms,
                table_name_tuples,
                topic_prefix,
                connector,
                database_name,
                source_platform,
            )

        # Handle single RegexRouter transform (legacy logic)
        if self._is_single_regex_transform(transforms):
            return self._extract_with_single_regex(
                transforms[0],
                table_name_tuples,
                topic_prefix,
                connector,
                database_name,
                source_platform,
            )

        # Default: No transform or non-RegexRouter transform
        return self._extract_without_transforms(
            table_name_tuples, topic_prefix, connector, database_name, source_platform
        )

    def _should_use_pipeline(self, transforms):
        """Check if we should use the transform pipeline."""
        return len(transforms) >= 1 and any(
            t.get("type")
            in [
                "io.debezium.transforms.outbox.EventRouter",
                "org.apache.kafka.connect.transforms.RegexRouter",
                "io.confluent.connect.cloud.transforms.TopicRegexRouter",
            ]
            for t in transforms
        )

    def _is_single_regex_transform(self, transforms):
        """Check if this is a single RegexRouter transform."""
        return (
            len(transforms) == 1
            and transforms[0].get("type")
            == "org.apache.kafka.connect.transforms.RegexRouter"
        )

    def _extract_with_pipeline(
        self,
        transforms,
        table_name_tuples,
        topic_prefix,
        connector,
        database_name,
        source_platform,
    ):
        """Extract lineages using transform pipeline."""
        from datahub.ingestion.source.kafka_connect.transform_plugins import (
            get_transform_pipeline,
        )

        try:
            # Get connector class for topic generation strategy
            connector_class = connector.connector_manifest.config.get(
                "connector.class", ""
            )
            config = connector.connector_manifest.config

            lineages = []
            manifest_topics = list(connector.connector_manifest.topic_names)

            # Check if EventRouter is present (outbox pattern)
            has_event_router = any(
                t.get("type") == "io.debezium.transforms.outbox.EventRouter"
                for t in transforms
            )

            # For outbox pattern with EventRouter, we can't predict topic names
            # since they depend on runtime data (event types in the table).
            # In this case, create lineages for all manifest topics from the single source table.
            if has_event_router and len(table_name_tuples) == 1:
                table_id = table_name_tuples[0]

                # Build source dataset name
                if table_id.schema and has_three_level_hierarchy(source_platform):
                    source_table_name = f"{table_id.schema}.{table_id.table}"
                else:
                    source_table_name = table_id.table

                source_dataset = get_dataset_name(database_name, source_table_name)

                # Create lineages for all manifest topics (since EventRouter generates them from runtime data)
                for topic in manifest_topics:
                    lineage = KafkaConnectLineage(
                        source_dataset=source_dataset,
                        source_platform=source_platform,
                        target_dataset=topic,
                        target_platform="kafka",
                    )
                    lineages.append(lineage)

                return lineages

            # For predictable transforms (RegexRouter), use normal pipeline
            for table_id in table_name_tuples:
                # Generate original topic name using connector's naming logic
                original_topic = connector._generate_original_topic_name(
                    table_id.schema or "",
                    table_id.table,
                    topic_prefix or "",
                    connector_class,
                )

                # Apply transforms to predict final topic
                transform_result = get_transform_pipeline().apply_forward(
                    [original_topic], config
                )

                predicted_topic = (
                    transform_result.topics[0]
                    if transform_result.topics
                    else original_topic
                )

                # Check if predicted topic exists in manifest topics
                if predicted_topic in manifest_topics:
                    # Build source dataset name
                    if table_id.schema and has_three_level_hierarchy(source_platform):
                        source_table_name = f"{table_id.schema}.{table_id.table}"
                    else:
                        source_table_name = table_id.table

                    source_dataset = get_dataset_name(database_name, source_table_name)

                    lineage = KafkaConnectLineage(
                        source_dataset=source_dataset,
                        source_platform=source_platform,
                        target_dataset=predicted_topic,
                        target_platform="kafka",
                    )
                    lineages.append(lineage)

            return lineages

        except Exception as e:
            print(f"Pipeline simulation failed: {e}")
            return []

    def _extract_with_single_regex(
        self,
        transform_config,
        table_name_tuples,
        topic_prefix,
        connector,
        database_name,
        source_platform,
    ):
        """Extract lineages with single RegexRouter transform."""
        transform_regex = transform_config["regex"]
        transform_replacement = transform_config["replacement"]
        lineages = []

        for table_id in table_name_tuples:
            source_table = table_id.table

            # Build original topic name (before transform)
            if topic_prefix:
                if has_three_level_hierarchy(source_platform) and table_id.schema:
                    original_topic = (
                        f"{topic_prefix}.{table_id.schema}.{table_id.table}"
                    )
                else:
                    original_topic = f"{topic_prefix}.{table_id.table}"
            else:
                original_topic = table_id.table

            # Apply regex transformation
            try:
                from java.util.regex import Pattern

                pattern = Pattern.compile(transform_regex)
                matcher = pattern.matcher(original_topic)

                if matcher.matches():
                    transformed_topic = str(matcher.replaceFirst(transform_replacement))
                else:
                    transformed_topic = original_topic
            except Exception:
                transformed_topic = original_topic

            # Create lineage if topic matches
            if transformed_topic in connector.connector_manifest.topic_names:
                if has_three_level_hierarchy(source_platform) and table_id.schema:
                    source_table_name = f"{table_id.schema}.{table_id.table}"
                else:
                    source_table_name = source_table

                dataset_name = get_dataset_name(database_name, source_table_name)
                lineage = KafkaConnectLineage(
                    source_dataset=dataset_name,
                    source_platform=source_platform,
                    target_dataset=transformed_topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)

        return lineages

    def _extract_without_transforms(
        self, table_name_tuples, topic_prefix, connector, database_name, source_platform
    ):
        """Extract lineages without transforms."""
        lineages = []

        for topic in connector.connector_manifest.topic_names:
            # Remove topic prefix to get table name
            if topic_prefix and topic.startswith(topic_prefix):
                remaining = topic[len(topic_prefix) :]
                if remaining.startswith("."):
                    remaining = remaining[1:]
                source_table_suffix = remaining
            else:
                source_table_suffix = topic

            # Find matching table by suffix
            matching_table = self._find_matching_table(
                table_name_tuples, source_table_suffix
            )

            if matching_table:
                if has_three_level_hierarchy(source_platform) and matching_table.schema:
                    source_table_name = (
                        f"{matching_table.schema}.{matching_table.table}"
                    )
                else:
                    source_table_name = matching_table.table

                dataset_name = get_dataset_name(database_name, source_table_name)
                lineage = KafkaConnectLineage(
                    source_dataset=dataset_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)

        return lineages

    def _find_matching_table(self, table_ids, source_table_suffix):
        """Find table that matches the given suffix."""
        for table_id in table_ids:
            table_name = table_id.table
            possible_suffixes = [table_name]
            if table_id.schema:
                possible_suffixes.append(f"{table_id.schema}.{table_id.table}")

            if source_table_suffix in possible_suffixes:
                return table_id
        return None

    def test_user_provided_config(self) -> None:
        """Test Confluent Cloud PostgreSQL CDC configuration with RegexRouter transform."""
        connector_config = {
            "connector.class": "PostgresCdcSource",
            "database.server.name": "test_server",
            "database.hostname": "test-postgres.us-east-1.rds.amazonaws.com",
            "database.port": "5432",
            "database.user": "test_user",
            "database.password": "test_password",
            "database.dbname": "test_database",
            "table.include.list": "public.table_a,schema_b.table_b,schema_b.table_c,schema_b.table_d,schema_b.table_e",
            "transforms": "Transform",
            "transforms.Transform.regex": r"(.*)\.(.*)\.(.*)",
            "transforms.Transform.replacement": r"$2.$3",
            "transforms.Transform.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }

        # Topics as they would appear in Kafka (after RegexRouter transform)
        topic_names = [
            "public.table_a",
            "schema_b.table_b",
            "schema_b.table_c",
            "schema_b.table_d",
            "schema_b.table_e",
        ]

        expected_lineages = [
            {
                "source_dataset": "test_database.public.table_a",
                "source_platform": "postgres",
                "target_dataset": "public.table_a",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.schema_b.table_b",
                "source_platform": "postgres",
                "target_dataset": "schema_b.table_b",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.schema_b.table_c",
                "source_platform": "postgres",
                "target_dataset": "schema_b.table_c",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.schema_b.table_d",
                "source_platform": "postgres",
                "target_dataset": "schema_b.table_d",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.schema_b.table_e",
                "source_platform": "postgres",
                "target_dataset": "schema_b.table_e",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(
            connector_config, topic_names, expected_lineages, "user_config"
        )

    def test_cloud_mysql_source_no_transform(self) -> None:
        """Test Confluent Cloud MySQL source connector without transforms."""
        connector_config = {
            "connector.class": "MySqlSource",
            "database.hostname": "test-mysql.amazonaws.com",
            "database.port": "3306",
            "database.user": "test_user",
            "database.password": "test_password",
            "database.dbname": "test_database",
            "database.server.name": "test_mysql_server",
            "table.include.list": "schema_a.table_x,schema_b.table_y",
        }

        # Topics without transforms (server.schema.table format)
        topic_names = [
            "test_mysql_server.schema_a.table_x",
            "test_mysql_server.schema_b.table_y",
        ]

        expected_lineages = [
            {
                "source_dataset": "test_database.table_x",
                "source_platform": "mysql",
                "target_dataset": "test_mysql_server.schema_a.table_x",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.table_y",
                "source_platform": "mysql",
                "target_dataset": "test_mysql_server.schema_b.table_y",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(
            connector_config, topic_names, expected_lineages, "mysql_cloud"
        )

    def test_platform_jdbc_connector_with_topic_prefix(self) -> None:
        """Test traditional Platform JDBC connector with topic prefix."""
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://localhost:5432/testdb?user=analyst&password=secret",
            "table.whitelist": "public.metrics,public.events",
            "topic.prefix": "db-",
            "mode": "incrementing",
            "incrementing.column.name": "id",
        }

        topic_names = ["db-metrics", "db-events"]

        expected_lineages = [
            {
                "source_dataset": "testdb.public.metrics",
                "source_platform": "postgres",
                "target_dataset": "db-metrics",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "testdb.public.events",
                "source_platform": "postgres",
                "target_dataset": "db-events",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(
            connector_config, topic_names, expected_lineages, "platform_jdbc"
        )

    def test_simplified_cloud_postgres_example(self) -> None:
        """Test a simplified Cloud PostgreSQL example with test data."""
        connector_config = {
            "connector.class": "PostgresCdcSource",
            "database.server.name": "test_server",
            "database.hostname": "test-postgres.amazonaws.com",
            "database.port": "5432",
            "database.user": "testuser",
            "database.password": "testpass",
            "database.dbname": "testdb",
            "table.include.list": "public.users,public.orders",
            "transforms": "Transform",
            "transforms.Transform.regex": r"(.*)\.(.*)\.(.*)",
            "transforms.Transform.replacement": r"$2.$3",
            "transforms.Transform.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }

        topic_names = [
            "public.users",  # After transform: test_server.public.users -> public.users
            "public.orders",  # After transform: test_server.public.orders -> public.orders
        ]

        expected_lineages = [
            {
                "source_dataset": "testdb.public.users",
                "source_platform": "postgres",
                "target_dataset": "public.users",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "testdb.public.orders",
                "source_platform": "postgres",
                "target_dataset": "public.orders",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(
            connector_config, topic_names, expected_lineages, "simplified_postgres"
        )

    def test_mysql_cdc_outbox_with_multiple_transforms(self) -> None:
        """Test MySQL CDC connector with EventRouter + RegexRouter transforms (outbox pattern)."""
        connector_config = {
            "after.state.only": "false",
            "cloud.environment": "test",
            "cloud.provider": "aws",
            "connector.class": "MySqlCdcSource",
            "database.connectionTimeZone": "UTC",
            "database.dbname": "test_database",
            "database.hostname": "test-mysql-host.amazonaws.com",
            "database.password": "test_password",
            "database.port": "3306",
            "database.server.name": "test_server",
            "database.ssl.mode": "required",
            "database.sslmode": "require",
            "database.user": "test_user",
            "hstore.handling.mode": "json",
            "interval.handling.mode": "numeric",
            "kafka.auth.mode": "SERVICE_ACCOUNT",
            "kafka.endpoint": "SASL_SSL://test-kafka.us-west-2.aws.confluent.cloud:9092",
            "kafka.region": "us-west-2",
            "kafka.service.account.id": "test-service-account",
            "max.batch.size": "2000",
            "name": "test-outbox-connector",
            "output.data.format": "JSON",
            "output.key.format": "JSON",
            "poll.interval.ms": "500",
            "provide.transaction.metadata": "true",
            "snapshot.locking.mode": "none",
            "snapshot.mode": "schema_only",
            "table.include.list": "test_database.outbox_table",
            "tasks.max": "1",
            "tombstones.on.delete": "false",
            "transforms": "EventRouter,RegexRouter",
            "transforms.EventRouter.route.by.field": "_route_suffix",
            "transforms.EventRouter.table.expand.json.payload": "true",
            "transforms.EventRouter.table.field.event.id": "event_id",
            "transforms.EventRouter.table.field.event.key": "_key",
            "transforms.EventRouter.table.field.event.payload": "event_data",
            "transforms.EventRouter.table.field.event.type": "event_type",
            "transforms.EventRouter.table.fields.additional.placement": "test_header_1:header,test_header_2:header",
            "transforms.EventRouter.type": "io.debezium.transforms.outbox.EventRouter",
            "transforms.RegexRouter.regex": "outbox.event.(.*)",
            "transforms.RegexRouter.replacement": "test.events.$1",
            "transforms.RegexRouter.type": "io.confluent.connect.cloud.transforms.TopicRegexRouter",
        }

        # For outbox pattern with EventRouter + RegexRouter:
        # 1. Original: test_server.test_database.outbox_table
        # 2. After EventRouter: outbox.event.{event_type} (extracts events from outbox table)
        # 3. After RegexRouter: test.events.{event_type}

        # Expected final topics (after both transforms) - in alphabetical order for consistency
        topic_names = [
            "test.events.entity_created",  # Example event types (sorted)
            "test.events.entity_deleted",
            "test.events.entity_updated",
        ]

        expected_lineages = [
            {
                "source_dataset": "test_database.outbox_table",
                "source_platform": "mysql",
                "target_dataset": "test.events.entity_created",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.outbox_table",
                "source_platform": "mysql",
                "target_dataset": "test.events.entity_deleted",
                "target_platform": "kafka",
            },
            {
                "source_dataset": "test_database.outbox_table",
                "source_platform": "mysql",
                "target_dataset": "test.events.entity_updated",
                "target_platform": "kafka",
            },
        ]

        self.validate_lineage_fields(
            connector_config, topic_names, expected_lineages, "mysql_cdc_outbox"
        )
