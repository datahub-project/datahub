from unittest import mock

import pytest

from datahub.ingestion.source.kafka_connect.topic_cache import (
    ConfluentCloudTopicRetriever,
    KafkaTopicCache,
)


class TestKafkaTopicCache:
    def test_get_all_topics_cache_miss(self):
        cache = KafkaTopicCache()
        result = cache.get_all_topics("cluster1")
        assert result is None
        assert cache.cache_misses == 1
        assert cache.cache_hits == 0

    def test_set_and_get_all_topics(self):
        cache = KafkaTopicCache()
        topics = ["topic1", "topic2", "topic3"]

        cache.set_all_topics("cluster1", topics)
        result = cache.get_all_topics("cluster1")

        assert result == topics
        assert cache.cache_hits == 1
        assert cache.cache_misses == 0

    def test_get_all_topics_returns_copy(self):
        cache = KafkaTopicCache()
        topics = ["topic1", "topic2"]

        cache.set_all_topics("cluster1", topics)
        result1 = cache.get_all_topics("cluster1")
        result2 = cache.get_all_topics("cluster1")

        # Modify one result
        assert result1 is not None
        result1.append("topic3")

        # Original and second result should be unchanged
        assert result2 == ["topic1", "topic2"]
        assert cache.get_all_topics("cluster1") == ["topic1", "topic2"]

    def test_get_consumer_group_assignments_cache_miss(self):
        cache = KafkaTopicCache()
        result = cache.get_consumer_group_assignments("cluster1")
        assert result is None
        assert cache.cache_misses == 1

    def test_set_and_get_consumer_group_assignments(self):
        cache = KafkaTopicCache()
        assignments = {"group1": ["topic1", "topic2"], "group2": ["topic3"]}

        cache.set_consumer_group_assignments("cluster1", assignments)
        result = cache.get_consumer_group_assignments("cluster1")

        assert result == assignments
        assert cache.cache_hits == 1

    def test_invalidate_cluster(self):
        cache = KafkaTopicCache()
        cache.set_all_topics("cluster1", ["topic1"])
        cache.set_consumer_group_assignments("cluster1", {"group1": ["topic1"]})

        cache.invalidate_cluster("cluster1")

        assert cache.get_all_topics("cluster1") is None
        assert cache.get_consumer_group_assignments("cluster1") is None

    def test_invalidate_nonexistent_cluster(self):
        cache = KafkaTopicCache()
        cache.invalidate_cluster("nonexistent")
        # Should not raise any errors

    def test_clear_all(self):
        cache = KafkaTopicCache()
        cache.set_all_topics("cluster1", ["topic1"])
        cache.set_all_topics("cluster2", ["topic2"])
        cache.set_consumer_group_assignments("cluster1", {"group1": ["topic1"]})

        cache.clear_all()

        assert cache.get_all_topics("cluster1") is None
        assert cache.get_all_topics("cluster2") is None
        assert cache.get_consumer_group_assignments("cluster1") is None
        assert cache.cache_hits == 0
        assert cache.cache_misses == 3

    def test_get_cache_stats(self):
        cache = KafkaTopicCache()
        cache.set_all_topics("cluster1", ["topic1"])
        cache.get_all_topics("cluster1")  # hit
        cache.get_all_topics("cluster2")  # miss
        cache.set_consumer_group_assignments("cluster1", {"group1": ["topic1"]})

        stats = cache.get_cache_stats()

        assert stats["cache_hits"] == 1
        assert stats["cache_misses"] == 1
        assert stats["topics_cached_clusters"] == 1
        assert stats["consumer_groups_cached_clusters"] == 1
        assert stats["hit_rate"] == 50

    def test_multiple_clusters(self):
        cache = KafkaTopicCache()
        cache.set_all_topics("cluster1", ["topic1"])
        cache.set_all_topics("cluster2", ["topic2", "topic3"])

        assert cache.get_all_topics("cluster1") == ["topic1"]
        assert cache.get_all_topics("cluster2") == ["topic2", "topic3"]


class TestConfluentCloudTopicRetriever:
    @pytest.fixture
    def mock_session(self):
        return mock.Mock()

    @pytest.fixture
    def cache(self):
        return KafkaTopicCache()

    @pytest.fixture
    def retriever(self, mock_session, cache):
        return ConfluentCloudTopicRetriever(mock_session, cache)

    def test_get_all_topics_cached_cache_hit(self, retriever, cache):
        topics = ["topic1", "topic2"]
        cache.set_all_topics("cluster1", topics)

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == topics
        assert cache.cache_hits == 1

    def test_get_all_topics_cached_api_fetch(self, retriever, mock_session):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "topic1", "is_internal": False},
                {"topic_name": "topic2", "is_internal": False},
                {"topic_name": "_internal", "is_internal": True},
            ],
        }
        mock_session.get.return_value = mock_response

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == ["topic1", "topic2"]
        mock_session.get.assert_called_once()

    def test_get_all_topics_cached_filters_internal_topics(
        self, retriever, mock_session
    ):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "user_topic", "is_internal": False},
                {"topic_name": "__consumer_offsets", "is_internal": True},
                {"topic_name": "_schemas", "is_internal": True},
            ],
        }
        mock_session.get.return_value = mock_response

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == ["user_topic"]

    def test_get_all_topics_cached_api_failure(self, retriever, mock_session):
        mock_session.get.side_effect = Exception("API Error")

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == []

    def test_get_all_topics_caches_result(self, retriever, mock_session, cache):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [{"topic_name": "topic1", "is_internal": False}],
        }
        mock_session.get.return_value = mock_response

        # First call fetches from API
        result1 = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        # Second call should use cache
        result2 = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result1 == ["topic1"]
        assert result2 == ["topic1"]
        assert mock_session.get.call_count == 1  # API called only once
        assert cache.cache_hits == 1

    def test_get_consumer_group_assignments_cached_cache_hit(self, retriever, cache):
        assignments = {"group1": ["topic1"]}
        cache.set_consumer_group_assignments("cluster1", assignments)

        result = retriever.get_consumer_group_assignments_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == assignments
        assert cache.cache_hits == 1

    def test_get_consumer_group_assignments_cached_api_fetch(
        self, retriever, mock_session
    ):
        # Mock consumer groups list response
        groups_response = mock.Mock()
        groups_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "group1"}],
        }

        # Mock consumers response for group1
        consumers_response = mock.Mock()
        consumers_response.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [
                {"assignments": [{"topic_name": "topic1"}, {"topic_name": "topic2"}]}
            ],
        }

        mock_session.get.side_effect = [groups_response, consumers_response]

        result = retriever.get_consumer_group_assignments_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        # Topics may be in any order due to set usage
        assert "group1" in result
        assert set(result["group1"]) == {"topic1", "topic2"}
        assert mock_session.get.call_count == 2

    def test_get_consumer_group_assignments_handles_empty_response(
        self, retriever, mock_session
    ):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"kind": "KafkaConsumerGroupList", "data": []}
        mock_session.get.return_value = mock_response

        result = retriever.get_consumer_group_assignments_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == {}

    def test_get_consumer_group_assignments_api_failure(self, retriever, mock_session):
        mock_session.get.side_effect = Exception("API Error")

        result = retriever.get_consumer_group_assignments_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == {}

    def test_fetch_topics_handles_trailing_slash(self, retriever, mock_session):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [{"topic_name": "topic1", "is_internal": False}],
        }
        mock_session.get.return_value = mock_response

        retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud/",  # Note trailing slash
            "cluster1",
        )

        # Verify URL was constructed correctly (no double slashes)
        called_url = mock_session.get.call_args[0][0]
        assert "//" not in called_url.replace("https://", "")

    def test_unexpected_response_format(self, retriever, mock_session):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"unexpected": "format"}
        mock_session.get.return_value = mock_response

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == []
