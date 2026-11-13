from unittest import mock

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.consumer_group_analyzer import (
    ConsumerGroupAnalyzer,
)


class TestConsumerGroupAnalyzer:
    @pytest.fixture
    def mock_session(self):
        return mock.Mock()

    @pytest.fixture
    def config(self):
        return KafkaConnectSourceConfig(connect_uri="http://localhost:8083")

    @pytest.fixture
    def report(self):
        return KafkaConnectSourceReport()

    @pytest.fixture
    def analyzer(self, mock_session, config, report):
        return ConsumerGroupAnalyzer(mock_session, config, report)

    def test_is_connector_group_direct_match(self, analyzer):
        assert analyzer._is_connector_group("my-connector", "my-connector")
        assert analyzer._is_connector_group("snowflake-sink", "snowflake-sink")

    def test_is_connector_group_connect_prefix(self, analyzer):
        assert analyzer._is_connector_group("connect-my-connector", "my-connector")
        assert analyzer._is_connector_group("connect-bigquery-sink", "bigquery-sink")

    def test_is_connector_group_connector_prefix(self, analyzer):
        assert analyzer._is_connector_group("connector-my-sink", "my-sink")

    def test_is_connector_group_suffix_match(self, analyzer):
        assert analyzer._is_connector_group("cluster-my-connector", "my-connector")

    def test_is_connector_group_contains_match(self, analyzer):
        assert analyzer._is_connector_group(
            "prefix-my-connector-suffix", "my-connector"
        )

    def test_is_connector_group_sink_type_match(self, analyzer):
        assert analyzer._is_connector_group(
            "my-bigquery-group", "test-connector-bigquery"
        )
        assert analyzer._is_connector_group("snowflake-consumer", "my-snowflake-sink")
        assert analyzer._is_connector_group("s3-writer", "data-s3-connector")

    def test_is_connector_group_case_insensitive(self, analyzer):
        assert analyzer._is_connector_group("MY-CONNECTOR", "my-connector")
        assert analyzer._is_connector_group("Connect-MyConnector", "myconnector")

    def test_is_connector_group_no_match(self, analyzer):
        assert not analyzer._is_connector_group("other-group", "my-connector")
        assert not analyzer._is_connector_group("unrelated", "my-sink")

    def test_get_connector_consumer_groups(self, analyzer, mock_session):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [
                {"consumer_group_id": "connect-my-connector"},
                {"consumer_group_id": "other-group"},
                {"consumer_group_id": "my-connector-1"},
            ],
        }
        mock_session.get.return_value = mock_response

        result = analyzer.get_connector_consumer_groups(
            "my-connector", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert len(result) == 2
        assert "connect-my-connector" in result
        assert "my-connector-1" in result
        assert "other-group" not in result

    def test_get_connector_consumer_groups_api_failure(self, analyzer, mock_session):
        mock_session.get.side_effect = Exception("API Error")

        result = analyzer.get_connector_consumer_groups(
            "my-connector", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert result == []

    def test_get_topics_for_consumer_groups(self, analyzer, mock_session):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [
                {"assignments": [{"topic_name": "topic1"}, {"topic_name": "topic2"}]}
            ],
        }
        mock_session.get.return_value = mock_response

        result = analyzer.get_topics_for_consumer_groups(
            ["group1"], "https://kafka-rest.confluent.cloud", "cluster1"
        )

        # Topics may be in any order due to set usage
        assert "group1" in result
        assert set(result["group1"]) == {"topic1", "topic2"}

    def test_get_topics_for_consumer_groups_multiple_groups(
        self, analyzer, mock_session
    ):
        def mock_get(url, **kwargs):
            response = mock.Mock()
            if "group1" in url:
                response.json.return_value = {
                    "kind": "KafkaConsumerList",
                    "data": [{"assignments": [{"topic_name": "topic1"}]}],
                }
            elif "group2" in url:
                response.json.return_value = {
                    "kind": "KafkaConsumerList",
                    "data": [{"assignments": [{"topic_name": "topic2"}]}],
                }
            return response

        mock_session.get.side_effect = mock_get

        result = analyzer.get_topics_for_consumer_groups(
            ["group1", "group2"], "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert result == {"group1": ["topic1"], "group2": ["topic2"]}

    def test_get_topics_for_consumer_groups_handles_failures(
        self, analyzer, mock_session
    ):
        def mock_get(url, **kwargs):
            response = mock.Mock()
            if "group1" in url:
                response.json.return_value = {
                    "kind": "KafkaConsumerList",
                    "data": [{"assignments": [{"topic_name": "topic1"}]}],
                }
            else:
                raise Exception("API Error")
            return response

        mock_session.get.side_effect = mock_get

        result = analyzer.get_topics_for_consumer_groups(
            ["group1", "group2"], "https://kafka-rest.confluent.cloud", "cluster1"
        )

        # Should have group1 but not group2
        assert "group1" in result
        assert "group2" not in result

    def test_analyze_connector_consumption(self, analyzer, mock_session):
        # Mock get_all_consumer_groups
        groups_response = mock.Mock()
        groups_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "connect-my-connector"}],
        }

        # Mock get_topics_for_group
        topics_response = mock.Mock()
        topics_response.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [
                {"assignments": [{"topic_name": "topic1"}, {"topic_name": "topic2"}]}
            ],
        }

        mock_session.get.side_effect = [groups_response, topics_response]

        result = analyzer.analyze_connector_consumption(
            "my-connector", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert len(result) == 2
        assert "topic1" in result
        assert "topic2" in result

    def test_analyze_connector_consumption_no_groups(self, analyzer, mock_session):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"kind": "KafkaConsumerGroupList", "data": []}
        mock_session.get.return_value = mock_response

        result = analyzer.analyze_connector_consumption(
            "my-connector", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert result == []

    def test_analyze_connector_consumption_deduplicates_topics(
        self, analyzer, mock_session
    ):
        groups_response = mock.Mock()
        groups_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [
                {"consumer_group_id": "connect-my-connector-1"},
                {"consumer_group_id": "connect-my-connector-2"},
            ],
        }

        # Both groups consume same topics
        topics_response = mock.Mock()
        topics_response.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [{"assignments": [{"topic_name": "shared_topic"}]}],
        }

        mock_session.get.side_effect = [
            groups_response,
            topics_response,
            topics_response,
        ]

        result = analyzer.analyze_connector_consumption(
            "my-connector", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        # Should deduplicate - only one "shared_topic"
        assert result == ["shared_topic"]

    def test_filter_sink_relevant_groups(self, analyzer):
        all_groups = {
            "connect-bigquery-sink": ["topic1"],
            "snowflake-writer": ["topic2"],
            "random-consumer": ["topic3"],
            "my-s3-connector": ["topic4"],
        }

        result = analyzer.filter_sink_relevant_groups(all_groups)

        assert len(result) == 3
        assert "connect-bigquery-sink" in result
        assert "snowflake-writer" in result
        assert "my-s3-connector" in result
        assert "random-consumer" not in result

    def test_filter_sink_relevant_groups_custom_patterns(self, analyzer):
        all_groups = {"custom-sink-pattern": ["topic1"], "another-group": ["topic2"]}

        result = analyzer.filter_sink_relevant_groups(
            all_groups, sink_connector_patterns=["custom-sink"]
        )

        assert len(result) == 1
        assert "custom-sink-pattern" in result

    def test_get_comprehensive_consumer_analysis(self, analyzer, mock_session):
        groups_response = mock.Mock()
        groups_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "group1"}, {"consumer_group_id": "group2"}],
        }

        topics_response1 = mock.Mock()
        topics_response1.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [{"assignments": [{"topic_name": "topic1"}]}],
        }

        topics_response2 = mock.Mock()
        topics_response2.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [{"assignments": [{"topic_name": "topic2"}]}],
        }

        mock_session.get.side_effect = [
            groups_response,
            topics_response1,
            topics_response2,
        ]

        result = analyzer.get_comprehensive_consumer_analysis(
            "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert result == {"group1": ["topic1"], "group2": ["topic2"]}

    def test_get_comprehensive_consumer_analysis_no_groups(
        self, analyzer, mock_session
    ):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"kind": "KafkaConsumerGroupList", "data": []}
        mock_session.get.return_value = mock_response

        result = analyzer.get_comprehensive_consumer_analysis(
            "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert result == {}

    def test_get_topics_for_group_handles_empty_assignments(
        self, analyzer, mock_session
    ):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [
                {"assignments": []}  # Empty assignments
            ],
        }
        mock_session.get.return_value = mock_response

        result = analyzer._get_topics_for_group(
            "group1", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        assert result == []

    def test_get_topics_for_group_handles_missing_topic_name(
        self, analyzer, mock_session
    ):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [
                {
                    "assignments": [
                        {"topic_name": "topic1"},
                        {},  # Missing topic_name
                        {"other_field": "value"},  # Wrong field
                    ]
                }
            ],
        }
        mock_session.get.return_value = mock_response

        result = analyzer._get_topics_for_group(
            "group1", "https://kafka-rest.confluent.cloud", "cluster1"
        )

        # Should only include valid topic
        assert result == ["topic1"]

    def test_get_all_consumer_groups_handles_trailing_slash(
        self, analyzer, mock_session
    ):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "group1"}],
        }
        mock_session.get.return_value = mock_response

        analyzer._get_all_consumer_groups(
            "https://kafka-rest.confluent.cloud/",  # Trailing slash
            "cluster1",
        )

        # Verify URL was constructed correctly
        called_url = mock_session.get.call_args[0][0]
        assert "//" not in called_url.replace("https://", "")
