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

    def test_fetch_topics_pagination_multiple_pages(self, retriever, mock_session):
        """Test that pagination correctly fetches topics across multiple pages."""
        # First page with next URL
        page1_response = mock.Mock()
        page1_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "topic1", "is_internal": False},
                {"topic_name": "topic2", "is_internal": False},
            ],
            "metadata": {
                "next": "https://pkc-test.region.provider.confluent.cloud/kafka/v3/clusters/cluster1/topics?page_token=abc123"
            },
        }

        # Second page with next URL
        page2_response = mock.Mock()
        page2_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "topic3", "is_internal": False},
                {"topic_name": "_internal", "is_internal": True},
            ],
            "metadata": {
                "next": "https://pkc-test.region.provider.confluent.cloud/kafka/v3/clusters/cluster1/topics?page_token=def456"
            },
        }

        # Third page with no next URL (last page)
        page3_response = mock.Mock()
        page3_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "topic4", "is_internal": False},
            ],
            "metadata": {"next": None},
        }

        mock_session.get.side_effect = [page1_response, page2_response, page3_response]

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        # Should get all non-internal topics from all pages
        assert result == ["topic1", "topic2", "topic3", "topic4"]
        assert mock_session.get.call_count == 3

    def test_fetch_topics_pagination_single_page(self, retriever, mock_session):
        """Test that single page response (no next URL) works correctly."""
        single_page_response = mock.Mock()
        single_page_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "topic1", "is_internal": False},
                {"topic_name": "topic2", "is_internal": False},
            ],
            "metadata": {"next": None},
        }

        mock_session.get.return_value = single_page_response

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == ["topic1", "topic2"]
        assert mock_session.get.call_count == 1

    def test_fetch_topics_pagination_no_metadata(self, retriever, mock_session):
        """Test handling of response without metadata field."""
        response = mock.Mock()
        response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "topic1", "is_internal": False},
            ],
        }

        mock_session.get.return_value = response

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        # Should still return topics from first page
        assert result == ["topic1"]
        assert mock_session.get.call_count == 1

    def test_fetch_consumer_groups_pagination_multiple_pages(
        self, retriever, mock_session
    ):
        """Test that pagination correctly fetches consumer groups across multiple pages."""
        # First page of consumer groups with next URL
        groups_page1 = mock.Mock()
        groups_page1.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "group1"}],
            "metadata": {
                "next": "https://pkc-test.region.provider.confluent.cloud/kafka/v3/clusters/cluster1/consumer-groups?page_token=abc123"
            },
        }

        # Second page of consumer groups without next URL
        groups_page2 = mock.Mock()
        groups_page2.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "group2"}],
            "metadata": {"next": None},
        }

        # Assignments for group1
        group1_assignments = mock.Mock()
        group1_assignments.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [{"assignments": [{"topic_name": "topic1"}]}],
        }

        # Assignments for group2
        group2_assignments = mock.Mock()
        group2_assignments.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [{"assignments": [{"topic_name": "topic2"}]}],
        }

        mock_session.get.side_effect = [
            groups_page1,
            group1_assignments,
            groups_page2,
            group2_assignments,
        ]

        result = retriever.get_consumer_group_assignments_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert "group1" in result
        assert "group2" in result
        assert result["group1"] == ["topic1"]
        assert result["group2"] == ["topic2"]
        assert mock_session.get.call_count == 4

    def test_fetch_consumer_groups_pagination_single_page(
        self, retriever, mock_session
    ):
        """Test that single page consumer groups response works correctly."""
        groups_response = mock.Mock()
        groups_response.json.return_value = {
            "kind": "KafkaConsumerGroupList",
            "data": [{"consumer_group_id": "group1"}],
            "metadata": {"next": None},
        }

        group_assignments = mock.Mock()
        group_assignments.json.return_value = {
            "kind": "KafkaConsumerList",
            "data": [{"assignments": [{"topic_name": "topic1"}]}],
        }

        mock_session.get.side_effect = [groups_response, group_assignments]

        result = retriever.get_consumer_group_assignments_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == {"group1": ["topic1"]}
        assert mock_session.get.call_count == 2

    def test_fetch_topics_pagination_empty_pages(self, retriever, mock_session):
        """Test handling of empty pages during pagination."""
        page1_response = mock.Mock()
        page1_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [{"topic_name": "topic1", "is_internal": False}],
            "metadata": {
                "next": "https://pkc-test.region.provider.confluent.cloud/kafka/v3/clusters/cluster1/topics?page_token=abc123"
            },
        }

        # Empty page
        page2_response = mock.Mock()
        page2_response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [],
            "metadata": {"next": None},
        }

        mock_session.get.side_effect = [page1_response, page2_response]

        result = retriever.get_all_topics_cached(
            "https://pkc-test.region.provider.confluent.cloud", "cluster1"
        )

        assert result == ["topic1"]
        assert mock_session.get.call_count == 2
