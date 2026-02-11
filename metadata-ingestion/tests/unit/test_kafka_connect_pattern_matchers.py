"""Tests for Kafka Connect pattern matching utilities."""

from unittest.mock import Mock

import jpype
import jpype.imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.pattern_matchers import (
    JavaRegexMatcher,
    WildcardMatcher,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started():
    """Ensure JVM is started for all tests requiring Java regex."""
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())
    yield


class TestJavaRegexMatcher:
    """Tests for JavaRegexMatcher with Java regex syntax."""

    def test_simple_pattern_match(self) -> None:
        matcher = JavaRegexMatcher()

        assert matcher.matches("public\\.users", "public.users")
        assert not matcher.matches("public\\.users", "public.orders")

    def test_wildcard_pattern(self) -> None:
        matcher = JavaRegexMatcher()

        assert matcher.matches("public\\..*", "public.users")
        assert matcher.matches("public\\..*", "public.orders")
        assert not matcher.matches("public\\..*", "private.users")

    def test_alternation_pattern(self) -> None:
        matcher = JavaRegexMatcher()

        assert matcher.matches("public\\.(users|orders)", "public.users")
        assert matcher.matches("public\\.(users|orders)", "public.orders")
        assert not matcher.matches("public\\.(users|orders)", "public.products")

    def test_character_class_pattern(self) -> None:
        matcher = JavaRegexMatcher()

        assert matcher.matches("schema_v[0-9]+\\.orders", "schema_v1.orders")
        assert matcher.matches("schema_v[0-9]+\\.orders", "schema_v2.orders")
        assert not matcher.matches("schema_v[0-9]+\\.orders", "schema_vX.orders")

    def test_filter_matches_basic(self) -> None:
        matcher = JavaRegexMatcher()

        tables = ["public.users", "public.orders", "private.users", "public.products"]
        patterns = ["public\\.users", "public\\.orders"]

        result = matcher.filter_matches(patterns, tables)

        assert "public.users" in result
        assert "public.orders" in result
        assert "private.users" not in result
        assert "public.products" not in result

    def test_filter_matches_with_wildcard(self) -> None:
        matcher = JavaRegexMatcher()

        tables = [
            "public.users",
            "public.orders",
            "private.users",
            "schema_v1.orders",
            "schema_v2.orders",
        ]
        patterns = ["public\\..*", "schema_.*\\.orders"]

        result = matcher.filter_matches(patterns, tables)

        assert "public.users" in result
        assert "public.orders" in result
        assert "schema_v1.orders" in result
        assert "schema_v2.orders" in result
        assert "private.users" not in result

    def test_filter_matches_no_duplicates(self) -> None:
        matcher = JavaRegexMatcher()

        tables = ["public.users", "public.orders"]
        patterns = ["public\\.users", "public\\..*"]

        result = matcher.filter_matches(patterns, tables)

        assert result.count("public.users") == 1

    def test_invalid_pattern_returns_false(self) -> None:
        matcher = JavaRegexMatcher()

        assert not matcher.matches("[invalid(regex", "public.users")

    def test_filter_matches_with_invalid_pattern(self) -> None:
        matcher = JavaRegexMatcher()

        tables = ["public.users", "public.orders"]
        patterns = ["[invalid(regex", "public\\.orders"]

        result = matcher.filter_matches(patterns, tables)

        assert "public.orders" in result
        assert "public.users" not in result


class TestWildcardMatcher:
    """Tests for WildcardMatcher with simple wildcard syntax."""

    def test_exact_match(self) -> None:
        matcher = WildcardMatcher()

        assert matcher.matches("ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.USERS")
        assert not matcher.matches("ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS")

    def test_star_wildcard(self) -> None:
        matcher = WildcardMatcher()

        assert matcher.matches("ANALYTICS.PUBLIC.*", "ANALYTICS.PUBLIC.USERS")
        assert matcher.matches("ANALYTICS.PUBLIC.*", "ANALYTICS.PUBLIC.ORDERS")
        assert not matcher.matches("ANALYTICS.PUBLIC.*", "ANALYTICS.PRIVATE.USERS")

    def test_question_mark_wildcard(self) -> None:
        matcher = WildcardMatcher()

        assert matcher.matches("DB.SCHEMA.USER?", "DB.SCHEMA.USER1")
        assert matcher.matches("DB.SCHEMA.USER?", "DB.SCHEMA.USERS")
        assert not matcher.matches("DB.SCHEMA.USER?", "DB.SCHEMA.USER12")

    def test_mixed_wildcards(self) -> None:
        matcher = WildcardMatcher()

        assert matcher.matches("*.PUBLIC.TABLE?", "DB1.PUBLIC.TABLE1")
        assert matcher.matches("*.PUBLIC.TABLE?", "DB2.PUBLIC.TABLEX")
        assert not matcher.matches("*.PUBLIC.TABLE?", "DB1.PRIVATE.TABLE1")

    def test_filter_matches_basic(self) -> None:
        matcher = WildcardMatcher()

        tables = [
            "ANALYTICS.PUBLIC.USERS",
            "ANALYTICS.PUBLIC.ORDERS",
            "ANALYTICS.PRIVATE.USERS",
        ]
        patterns = ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"]

        result = matcher.filter_matches(patterns, tables)

        assert "ANALYTICS.PUBLIC.USERS" in result
        assert "ANALYTICS.PUBLIC.ORDERS" in result
        assert "ANALYTICS.PRIVATE.USERS" not in result

    def test_filter_matches_with_wildcards(self) -> None:
        matcher = WildcardMatcher()

        tables = [
            "ANALYTICS.PUBLIC.USERS",
            "ANALYTICS.PUBLIC.ORDERS",
            "ANALYTICS.PRIVATE.USERS",
            "SALES.PUBLIC.CUSTOMERS",
        ]
        patterns = ["ANALYTICS.PUBLIC.*", "*.PUBLIC.CUSTOMERS"]

        result = matcher.filter_matches(patterns, tables)

        assert "ANALYTICS.PUBLIC.USERS" in result
        assert "ANALYTICS.PUBLIC.ORDERS" in result
        assert "SALES.PUBLIC.CUSTOMERS" in result
        assert "ANALYTICS.PRIVATE.USERS" not in result

    def test_filter_matches_no_duplicates(self) -> None:
        matcher = WildcardMatcher()

        tables = ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"]
        patterns = ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.*"]

        result = matcher.filter_matches(patterns, tables)

        assert result.count("ANALYTICS.PUBLIC.USERS") == 1

    def test_case_sensitivity(self) -> None:
        matcher = WildcardMatcher()

        assert matcher.matches("analytics.public.users", "analytics.public.users")
        assert not matcher.matches("ANALYTICS.PUBLIC.USERS", "analytics.public.users")


class TestDebeziumSourceConnectorPatternMatching:
    """Tests for DebeziumSourceConnector pattern matcher integration."""

    def test_get_pattern_matcher_returns_java_regex_matcher(self) -> None:
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = Mock(spec=KafkaConnectSourceConfig)
        report = Mock(spec=KafkaConnectSourceReport)
        connector = DebeziumSourceConnector(manifest, config, report)

        matcher = connector.get_pattern_matcher()

        assert isinstance(matcher, JavaRegexMatcher)

    def test_filter_tables_by_patterns_uses_java_regex(self) -> None:
        connector_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "myserver",
            "database.dbname": "testdb",
        }

        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config=connector_config,
            tasks=[],
            topic_names=[],
        )

        config = Mock(spec=KafkaConnectSourceConfig)
        report = Mock(spec=KafkaConnectSourceReport)
        connector = DebeziumSourceConnector(manifest, config, report)

        tables = [
            "public.users",
            "public.orders",
            "schema_v1.products",
            "schema_v2.products",
            "private.data",
        ]
        patterns = ["public\\..*", "schema_v[0-9]+\\.products"]

        result = connector._filter_tables_by_patterns(tables, patterns)

        assert "public.users" in result
        assert "public.orders" in result
        assert "schema_v1.products" in result
        assert "schema_v2.products" in result
        assert "private.data" not in result
