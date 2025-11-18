"""Tests for config_constants module."""

from datahub.ingestion.source.kafka_connect.config_constants import (
    ConnectorConfigKeys,
    parse_comma_separated_list,
)


class TestParseCommaSeparatedList:
    """Test parse_comma_separated_list() edge cases."""

    def test_empty_string(self) -> None:
        """Empty string should return empty list."""
        assert parse_comma_separated_list("") == []

    def test_whitespace_only(self) -> None:
        """Whitespace-only string should return empty list."""
        assert parse_comma_separated_list("   ") == []
        assert parse_comma_separated_list("\t\n") == []

    def test_single_item(self) -> None:
        """Single item should return list with one element."""
        assert parse_comma_separated_list("item1") == ["item1"]

    def test_multiple_items(self) -> None:
        """Multiple items should be split correctly."""
        assert parse_comma_separated_list("item1,item2,item3") == [
            "item1",
            "item2",
            "item3",
        ]

    def test_leading_comma(self) -> None:
        """Leading comma should be ignored."""
        assert parse_comma_separated_list(",item1,item2") == ["item1", "item2"]

    def test_trailing_comma(self) -> None:
        """Trailing comma should be ignored."""
        assert parse_comma_separated_list("item1,item2,") == ["item1", "item2"]

    def test_leading_and_trailing_commas(self) -> None:
        """Leading and trailing commas should be ignored."""
        assert parse_comma_separated_list(",item1,item2,") == ["item1", "item2"]

    def test_consecutive_commas(self) -> None:
        """Multiple consecutive commas should be treated as empty items and filtered."""
        assert parse_comma_separated_list("item1,,item2,,,item3") == [
            "item1",
            "item2",
            "item3",
        ]

    def test_whitespace_around_items(self) -> None:
        """Whitespace around items should be stripped."""
        assert parse_comma_separated_list(" item1 , item2 , item3 ") == [
            "item1",
            "item2",
            "item3",
        ]

    def test_whitespace_only_items_filtered(self) -> None:
        """Items that are only whitespace should be filtered out."""
        assert parse_comma_separated_list("item1,  ,item2") == ["item1", "item2"]
        assert parse_comma_separated_list("item1, \t ,item2") == ["item1", "item2"]

    def test_mixed_whitespace_and_empty(self) -> None:
        """Mixed whitespace and empty items should all be filtered."""
        assert parse_comma_separated_list("item1, , ,  ,item2") == ["item1", "item2"]

    def test_complex_real_world_example(self) -> None:
        """Real-world example with various edge cases."""
        input_str = " table1 , , table2,  table3  ,, table4,  "
        expected = ["table1", "table2", "table3", "table4"]
        assert parse_comma_separated_list(input_str) == expected

    def test_items_with_special_characters(self) -> None:
        """Items with special characters should be preserved."""
        assert parse_comma_separated_list("schema.table1,schema.table2") == [
            "schema.table1",
            "schema.table2",
        ]
        assert parse_comma_separated_list("db-name,table_name") == [
            "db-name",
            "table_name",
        ]

    def test_items_with_numbers(self) -> None:
        """Items with numbers should be preserved."""
        assert parse_comma_separated_list("table1,table2,table3") == [
            "table1",
            "table2",
            "table3",
        ]

    def test_very_long_list(self) -> None:
        """Long lists should be handled correctly."""
        items = [f"item{i}" for i in range(100)]
        input_str = ",".join(items)
        assert parse_comma_separated_list(input_str) == items


class TestConnectorConfigKeys:
    """Test ConnectorConfigKeys constants."""

    def test_core_connector_keys(self) -> None:
        """Verify core connector configuration keys."""
        assert ConnectorConfigKeys.CONNECTOR_CLASS == "connector.class"
        assert ConnectorConfigKeys.TRANSFORMS == "transforms"

    def test_topic_keys(self) -> None:
        """Verify topic-related configuration keys."""
        assert ConnectorConfigKeys.TOPICS == "topics"
        assert ConnectorConfigKeys.TOPICS_REGEX == "topics.regex"
        assert ConnectorConfigKeys.KAFKA_TOPIC == "kafka.topic"
        assert ConnectorConfigKeys.TOPIC == "topic"
        assert ConnectorConfigKeys.TOPIC_PREFIX == "topic.prefix"

    def test_jdbc_keys(self) -> None:
        """Verify JDBC configuration keys."""
        assert ConnectorConfigKeys.CONNECTION_URL == "connection.url"
        assert ConnectorConfigKeys.TABLE_INCLUDE_LIST == "table.include.list"
        assert ConnectorConfigKeys.TABLE_WHITELIST == "table.whitelist"
        assert ConnectorConfigKeys.QUERY == "query"
        assert ConnectorConfigKeys.MODE == "mode"

    def test_debezium_keys(self) -> None:
        """Verify Debezium/CDC configuration keys."""
        assert ConnectorConfigKeys.DATABASE_SERVER_NAME == "database.server.name"
        assert ConnectorConfigKeys.DATABASE_HOSTNAME == "database.hostname"
        assert ConnectorConfigKeys.DATABASE_PORT == "database.port"
        assert ConnectorConfigKeys.DATABASE_DBNAME == "database.dbname"
        assert ConnectorConfigKeys.DATABASE_INCLUDE_LIST == "database.include.list"

    def test_kafka_keys(self) -> None:
        """Verify Kafka configuration keys."""
        assert ConnectorConfigKeys.KAFKA_ENDPOINT == "kafka.endpoint"
        assert ConnectorConfigKeys.BOOTSTRAP_SERVERS == "bootstrap.servers"
        assert ConnectorConfigKeys.KAFKA_BOOTSTRAP_SERVERS == "kafka.bootstrap.servers"

    def test_bigquery_keys(self) -> None:
        """Verify BigQuery configuration keys."""
        assert ConnectorConfigKeys.PROJECT == "project"
        assert ConnectorConfigKeys.DEFAULT_DATASET == "defaultDataset"
        assert ConnectorConfigKeys.DATASETS == "datasets"
        assert ConnectorConfigKeys.TOPICS_TO_TABLES == "topicsToTables"
        assert ConnectorConfigKeys.SANITIZE_TOPICS == "sanitizeTopics"
        assert ConnectorConfigKeys.KEYFILE == "keyfile"

    def test_snowflake_keys(self) -> None:
        """Verify Snowflake configuration keys."""
        assert ConnectorConfigKeys.SNOWFLAKE_DATABASE_NAME == "snowflake.database.name"
        assert ConnectorConfigKeys.SNOWFLAKE_SCHEMA_NAME == "snowflake.schema.name"
        assert (
            ConnectorConfigKeys.SNOWFLAKE_TOPIC2TABLE_MAP == "snowflake.topic2table.map"
        )
        assert ConnectorConfigKeys.SNOWFLAKE_PRIVATE_KEY == "snowflake.private.key"
        assert (
            ConnectorConfigKeys.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
            == "snowflake.private.key.passphrase"
        )

    def test_s3_keys(self) -> None:
        """Verify S3 configuration keys."""
        assert ConnectorConfigKeys.S3_BUCKET_NAME == "s3.bucket.name"
        assert ConnectorConfigKeys.TOPICS_DIR == "topics.dir"
        assert ConnectorConfigKeys.AWS_ACCESS_KEY_ID == "aws.access.key.id"
        assert ConnectorConfigKeys.AWS_SECRET_ACCESS_KEY == "aws.secret.access.key"
        assert ConnectorConfigKeys.S3_SSE_CUSTOMER_KEY == "s3.sse.customer.key"
        assert ConnectorConfigKeys.S3_PROXY_PASSWORD == "s3.proxy.password"

    def test_auth_keys(self) -> None:
        """Verify authentication configuration keys."""
        assert (
            ConnectorConfigKeys.VALUE_CONVERTER_BASIC_AUTH_USER_INFO
            == "value.converter.basic.auth.user.info"
        )
