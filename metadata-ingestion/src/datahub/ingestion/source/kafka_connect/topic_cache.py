import logging
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class KafkaTopicCache:
    """
    Simple topic cache shared across connectors within a single ingestion run.

    Provides cross-connector topic sharing and consumer group assignment caching
    for improved performance during DataHub ingestion runs.
    """

    def __init__(self):
        """Initialize the topic cache."""
        self._topics_cache: Dict[str, List[str]] = {}
        self._consumer_groups_cache: Dict[str, Dict[str, List[str]]] = {}
        self.cache_hits = 0
        self.cache_misses = 0

    def get_all_topics(self, cluster_id: str) -> Optional[List[str]]:
        """
        Get all topics for a cluster from cache.

        Args:
            cluster_id: Unique identifier for the Kafka cluster

        Returns:
            List of topic names if cached, None otherwise
        """
        if cluster_id in self._topics_cache:
            topics = self._topics_cache[cluster_id]
            self.cache_hits += 1
            logger.debug(f"Cache hit for cluster {cluster_id}: {len(topics)} topics")
            return topics.copy()

        self.cache_misses += 1
        return None

    def set_all_topics(self, cluster_id: str, topics: List[str]) -> None:
        """
        Cache topics for a cluster.

        Args:
            cluster_id: Unique identifier for the Kafka cluster
            topics: List of topic names to cache
        """
        self._topics_cache[cluster_id] = topics.copy()
        logger.info(f"Cached {len(topics)} topics for cluster {cluster_id}")

    def get_consumer_group_assignments(
        self, cluster_id: str
    ) -> Optional[Dict[str, List[str]]]:
        """
        Get consumer group assignments from cache.

        Args:
            cluster_id: Unique identifier for the Kafka cluster

        Returns:
            Dict mapping consumer group names to their assigned topics, None if not cached
        """
        if cluster_id in self._consumer_groups_cache:
            assignments = self._consumer_groups_cache[cluster_id]
            self.cache_hits += 1
            logger.debug(
                f"Cache hit for consumer groups in cluster {cluster_id}: {len(assignments)} groups"
            )
            return {group: topics.copy() for group, topics in assignments.items()}

        self.cache_misses += 1
        return None

    def set_consumer_group_assignments(
        self, cluster_id: str, assignments: Dict[str, List[str]]
    ) -> None:
        """
        Cache consumer group assignments for a cluster.

        Args:
            cluster_id: Unique identifier for the Kafka cluster
            assignments: Dict mapping consumer group names to their assigned topics
        """
        cached_assignments = {
            group: topics.copy() for group, topics in assignments.items()
        }
        self._consumer_groups_cache[cluster_id] = cached_assignments
        logger.info(
            f"Cached consumer group assignments for {len(assignments)} groups in cluster {cluster_id}"
        )

    def invalidate_cluster(self, cluster_id: str) -> None:
        """
        Invalidate all cache entries for a specific cluster.

        Args:
            cluster_id: Unique identifier for the Kafka cluster
        """
        removed_topics = self._topics_cache.pop(cluster_id, None)
        removed_groups = self._consumer_groups_cache.pop(cluster_id, None)

        logger.info(
            f"Invalidated cache for cluster {cluster_id}: "
            f"topics={'yes' if removed_topics else 'no'}, "
            f"consumer_groups={'yes' if removed_groups else 'no'}"
        )

    def clear_all(self) -> None:
        """Clear all cached data."""
        topics_count = len(self._topics_cache)
        groups_count = len(self._consumer_groups_cache)

        self._topics_cache.clear()
        self._consumer_groups_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0

        logger.info(
            f"Cleared all cache data: {topics_count} topic caches, {groups_count} consumer group caches"
        )

    def get_cache_stats(self) -> Dict[str, int]:
        """
        Get cache statistics for reporting.

        Returns:
            Dict with cache hit/miss statistics and entry counts
        """
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "topics_cached_clusters": len(self._topics_cache),
            "consumer_groups_cached_clusters": len(self._consumer_groups_cache),
            "hit_rate": int(
                self.cache_hits / max(self.cache_hits + self.cache_misses, 1) * 100
            ),
        }


class ConfluentCloudTopicRetriever:
    """
    Specialized topic retriever for Confluent Cloud environments.

    Integrates with KafkaTopicCache for efficient cross-connector topic sharing.
    """

    def __init__(self, session: requests.Session, cache: KafkaTopicCache):
        """
        Initialize the topic retriever.

        Args:
            session: Configured requests session with authentication
            cache: Shared topic cache instance
        """
        self.session = session
        self.cache = cache

    def get_all_topics_cached(
        self,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """
        Get all topics using cache-first strategy.

        Args:
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            List of topic names, empty list if retrieval fails
        """
        # Try cache first
        cached_topics = self.cache.get_all_topics(cluster_id)
        if cached_topics is not None:
            return cached_topics

        # Cache miss - fetch from API
        try:
            topics = self._fetch_topics_from_api(
                kafka_rest_endpoint, cluster_id, auth_headers
            )
            if topics:
                self.cache.set_all_topics(cluster_id, topics)
                return topics
            else:
                logger.warning(f"No topics retrieved from API for cluster {cluster_id}")
                return []

        except Exception as e:
            logger.warning(
                f"Failed to retrieve topics from API for cluster {cluster_id}: {e}"
            )
            return []

    def _fetch_topics_from_api(
        self,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """
        Fetch topics directly from Confluent Cloud Kafka REST API v3.

        Handles pagination by following the metadata.next URL until all topics are retrieved.

        Args:
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            List of topic names
        """
        # Build initial API URL
        if kafka_rest_endpoint.endswith("/"):
            kafka_rest_endpoint = kafka_rest_endpoint.rstrip("/")
        topics_url = f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"

        # Set up headers
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth_headers:
            headers.update(auth_headers)

        all_topics = []
        current_url = topics_url
        page_count = 0

        # Paginate through all results
        while current_url:
            page_count += 1
            response = self.session.get(current_url, headers=headers)
            response.raise_for_status()

            # Parse v3 API response format
            topics_data = response.json()
            if topics_data.get("kind") == "KafkaTopicList" and "data" in topics_data:
                # Extract non-internal topics from current page
                page_topics = [
                    topic["topic_name"]
                    for topic in topics_data["data"]
                    if not topic.get("is_internal", False)
                ]
                all_topics.extend(page_topics)
                logger.debug(
                    f"Retrieved {len(page_topics)} topics from page {page_count} for cluster {cluster_id}"
                )

                # Check for next page
                metadata = topics_data.get("metadata", {})
                current_url = metadata.get("next")
            else:
                logger.warning(
                    f"Unexpected response format from Kafka REST API for cluster {cluster_id}"
                )
                break

        logger.info(
            f"Retrieved {len(all_topics)} topics across {page_count} page(s) from Confluent Cloud Kafka REST API v3 for cluster {cluster_id}"
        )
        return all_topics

    def get_consumer_group_assignments_cached(
        self,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, List[str]]:
        """
        Get consumer group assignments using cache-first strategy.

        Args:
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            Dict mapping consumer group names to their assigned topics
        """
        # Try cache first
        cached_assignments = self.cache.get_consumer_group_assignments(cluster_id)
        if cached_assignments is not None:
            return cached_assignments

        # Cache miss - fetch from API
        try:
            assignments = self._fetch_consumer_groups_from_api(
                kafka_rest_endpoint, cluster_id, auth_headers
            )
            if assignments:
                self.cache.set_consumer_group_assignments(cluster_id, assignments)
                return assignments
            else:
                logger.debug(
                    f"No consumer group assignments retrieved for cluster {cluster_id}"
                )
                return {}

        except Exception as e:
            logger.warning(
                f"Failed to retrieve consumer group assignments for cluster {cluster_id}: {e}"
            )
            return {}

    def _fetch_consumer_groups_from_api(
        self,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, List[str]]:
        """
        Fetch consumer group assignments from Confluent Cloud Kafka REST API v3.

        Handles pagination by following the metadata.next URL until all consumer groups are retrieved.

        Args:
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            Dict mapping consumer group names to their assigned topics
        """
        # Build initial API URL for consumer groups
        if kafka_rest_endpoint.endswith("/"):
            kafka_rest_endpoint = kafka_rest_endpoint.rstrip("/")
        consumer_groups_url = (
            f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/consumer-groups"
        )

        # Set up headers
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth_headers:
            headers.update(auth_headers)

        assignments = {}
        current_url = consumer_groups_url
        page_count = 0

        try:
            # Paginate through all consumer groups
            while current_url:
                page_count += 1
                response = self.session.get(current_url, headers=headers)
                response.raise_for_status()

                groups_data = response.json()
                if (
                    groups_data.get("kind") == "KafkaConsumerGroupList"
                    and "data" in groups_data
                ):
                    for group in groups_data["data"]:
                        group_id = group["consumer_group_id"]

                        # Get assignments for each group
                        assignments_url = f"{consumer_groups_url}/{group_id}/consumers"
                        try:
                            assignments_response = self.session.get(
                                assignments_url, headers=headers
                            )
                            assignments_response.raise_for_status()

                            assignments_data = assignments_response.json()
                            if (
                                assignments_data.get("kind") == "KafkaConsumerList"
                                and "data" in assignments_data
                            ):
                                group_topics = set()
                                for consumer in assignments_data["data"]:
                                    for assignment in consumer.get("assignments", []):
                                        group_topics.add(
                                            assignment.get("topic_name", "")
                                        )

                                if group_topics:
                                    assignments[group_id] = list(group_topics)

                        except Exception as e:
                            logger.debug(
                                f"Failed to get assignments for consumer group {group_id}: {e}"
                            )
                            continue

                    # Check for next page
                    metadata = groups_data.get("metadata", {})
                    current_url = metadata.get("next")
                else:
                    logger.warning(
                        f"Unexpected response format from Kafka REST API for consumer groups in cluster {cluster_id}"
                    )
                    break

            logger.info(
                f"Retrieved assignments for {len(assignments)} consumer groups across {page_count} page(s) in cluster {cluster_id}"
            )

        except Exception as e:
            logger.warning(
                f"Failed to retrieve consumer groups from API for cluster {cluster_id}: {e}"
            )

        return assignments
