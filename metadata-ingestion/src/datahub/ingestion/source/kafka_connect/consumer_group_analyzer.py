import logging
from typing import Dict, List, Optional

import requests

from datahub.ingestion.source.kafka_connect.common import (
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)

logger = logging.getLogger(__name__)


class ConsumerGroupAnalyzer:
    """
    Analyze consumer groups to infer sink connector topic consumption.

    This analyzer provides enhanced lineage extraction for sink connectors by:
    - Discovering consumer groups associated with specific connectors
    - Mapping consumer groups to their assigned topics
    - Inferring sink lineage from actual consumption patterns
    """

    def __init__(
        self,
        session: requests.Session,
        config: KafkaConnectSourceConfig,
        report: KafkaConnectSourceReport,
    ):
        """
        Initialize the consumer group analyzer.

        Args:
            session: Configured requests session with authentication
            config: Kafka Connect source configuration
            report: Reporting instance for warnings and errors
        """
        self.session = session
        self.config = config
        self.report = report

    def get_connector_consumer_groups(
        self,
        connector_name: str,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """
        Find consumer groups associated with a specific connector.

        Args:
            connector_name: Name of the sink connector
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            List of consumer group IDs associated with the connector
        """
        try:
            all_groups = self._get_all_consumer_groups(
                kafka_rest_endpoint, cluster_id, auth_headers
            )
            connector_groups = []

            for group_id in all_groups:
                if self._is_connector_group(group_id, connector_name):
                    connector_groups.append(group_id)

            logger.info(
                f"Found {len(connector_groups)} consumer groups for connector {connector_name}"
            )
            return connector_groups

        except Exception as e:
            logger.warning(
                f"Failed to get consumer groups for connector {connector_name}: {e}"
            )
            return []

    def get_topics_for_consumer_groups(
        self,
        consumer_groups: List[str],
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, List[str]]:
        """
        Map consumer groups to their assigned topics.

        Args:
            consumer_groups: List of consumer group IDs
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            Dict mapping consumer group ID to list of assigned topics
        """
        group_topics = {}

        for group_id in consumer_groups:
            try:
                topics = self._get_topics_for_group(
                    group_id, kafka_rest_endpoint, cluster_id, auth_headers
                )
                if topics:
                    group_topics[group_id] = topics
                    logger.debug(
                        f"Consumer group {group_id} assigned to topics: {topics}"
                    )

            except Exception as e:
                logger.debug(f"Failed to get topics for consumer group {group_id}: {e}")
                continue

        return group_topics

    def analyze_connector_consumption(
        self,
        connector_name: str,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """
        Analyze a connector's topic consumption patterns.

        Args:
            connector_name: Name of the sink connector
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            List of topics consumed by the connector
        """
        try:
            # Get consumer groups for this connector
            consumer_groups = self.get_connector_consumer_groups(
                connector_name, kafka_rest_endpoint, cluster_id, auth_headers
            )

            if not consumer_groups:
                logger.debug(f"No consumer groups found for connector {connector_name}")
                return []

            # Get topics for each consumer group
            group_topics = self.get_topics_for_consumer_groups(
                consumer_groups, kafka_rest_endpoint, cluster_id, auth_headers
            )

            # Combine all topics from all groups
            all_topics = set()
            for topics in group_topics.values():
                all_topics.update(topics)

            result = list(all_topics)
            logger.info(
                f"Connector {connector_name} consumes {len(result)} topics via consumer group analysis"
            )
            return result

        except Exception as e:
            logger.warning(
                f"Failed to analyze consumption for connector {connector_name}: {e}"
            )
            return []

    def _get_all_consumer_groups(
        self,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Get all consumer groups from Kafka cluster."""
        # Build API URL for consumer groups
        if kafka_rest_endpoint.endswith("/"):
            kafka_rest_endpoint = kafka_rest_endpoint.rstrip("/")
        groups_url = (
            f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/consumer-groups"
        )

        # Set up headers
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth_headers:
            headers.update(auth_headers)

        # Make API call
        response = self.session.get(groups_url, headers=headers)
        response.raise_for_status()

        # Parse v3 API response format
        groups_data = response.json()
        if (
            groups_data.get("kind") == "KafkaConsumerGroupList"
            and "data" in groups_data
        ):
            group_ids = [group["consumer_group_id"] for group in groups_data["data"]]
            logger.debug(
                f"Retrieved {len(group_ids)} consumer groups from Kafka REST API v3"
            )
            return group_ids
        else:
            logger.warning(
                "Unexpected response format from Kafka REST API consumer groups endpoint"
            )
            return []

    def _is_connector_group(self, group_id: str, connector_name: str) -> bool:
        """
        Determine if a consumer group belongs to a specific connector.

        Common Kafka Connect consumer group naming patterns:
        - connect-<connector-name>
        - <connector-name>
        - <cluster-name>-<connector-name>
        - connector-<connector-name>
        """
        group_lower = group_id.lower()
        connector_lower = connector_name.lower()

        # Direct patterns
        if (
            connector_lower in group_lower
            or group_lower == connector_lower
            or group_lower.startswith(f"connect-{connector_lower}")
            or group_lower.endswith(f"-{connector_lower}")
            or group_lower.startswith(f"connector-{connector_lower}")
        ):
            return True

        # Additional patterns for sink connectors
        sink_patterns = [
            "bigquery",
            "snowflake",
            "s3",
            "elasticsearch",
            "jdbc",
            "postgres",
            "mysql",
        ]

        # Check if connector name contains sink type indicators
        connector_type = None
        for pattern in sink_patterns:
            if pattern in connector_lower:
                connector_type = pattern
                break

        if connector_type and connector_type in group_lower:
            return True

        return False

    def _get_topics_for_group(
        self,
        group_id: str,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Get topics assigned to a specific consumer group."""
        # Build API URL for consumer group consumers
        if kafka_rest_endpoint.endswith("/"):
            kafka_rest_endpoint = kafka_rest_endpoint.rstrip("/")
        consumers_url = f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/consumer-groups/{group_id}/consumers"

        # Set up headers
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth_headers:
            headers.update(auth_headers)

        try:
            # Make API call
            response = self.session.get(consumers_url, headers=headers)
            response.raise_for_status()

            # Parse v3 API response format
            consumers_data = response.json()
            if (
                consumers_data.get("kind") == "KafkaConsumerList"
                and "data" in consumers_data
            ):
                topics = set()
                for consumer in consumers_data["data"]:
                    for assignment in consumer.get("assignments", []):
                        topic_name = assignment.get("topic_name")
                        if topic_name:
                            topics.add(topic_name)

                return list(topics)
            else:
                logger.debug(
                    f"Unexpected response format from consumer group API for {group_id}"
                )
                return []

        except Exception as e:
            logger.debug(f"Failed to get consumers for group {group_id}: {e}")
            return []

    def get_comprehensive_consumer_analysis(
        self,
        kafka_rest_endpoint: str,
        cluster_id: str,
        auth_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, List[str]]:
        """
        Get comprehensive consumer group to topics mapping for the entire cluster.

        This method is useful for getting a complete picture of topic consumption
        patterns across all consumer groups in the cluster.

        Args:
            kafka_rest_endpoint: Kafka REST API v3 endpoint
            cluster_id: Kafka cluster identifier
            auth_headers: Optional authentication headers

        Returns:
            Dict mapping all consumer group IDs to their assigned topics
        """
        try:
            # Get all consumer groups
            all_groups = self._get_all_consumer_groups(
                kafka_rest_endpoint, cluster_id, auth_headers
            )

            if not all_groups:
                logger.warning("No consumer groups found in cluster")
                return {}

            # Get topics for all groups
            group_topics = self.get_topics_for_consumer_groups(
                all_groups, kafka_rest_endpoint, cluster_id, auth_headers
            )

            logger.info(
                f"Analyzed {len(group_topics)} consumer groups with topic assignments"
            )
            return group_topics

        except Exception as e:
            logger.warning(f"Failed to get comprehensive consumer analysis: {e}")
            return {}

    def filter_sink_relevant_groups(
        self,
        all_group_topics: Dict[str, List[str]],
        sink_connector_patterns: Optional[List[str]] = None,
    ) -> Dict[str, List[str]]:
        """
        Filter consumer groups to find those likely associated with sink connectors.

        Args:
            all_group_topics: Complete mapping of consumer groups to topics
            sink_connector_patterns: Optional list of patterns to identify sink connectors

        Returns:
            Filtered dict of consumer groups likely associated with sink connectors
        """
        sink_groups = {}

        # Default sink connector patterns
        if sink_connector_patterns is None:
            sink_connector_patterns = [
                "bigquery",
                "snowflake",
                "s3",
                "elasticsearch",
                "jdbc",
                "postgres",
                "mysql",
                "connect-",
                "connector-",
                "-sink",
                "sink-",
            ]

        for group_id, topics in all_group_topics.items():
            group_lower = group_id.lower()

            # Check if group matches sink patterns
            if any(
                pattern.lower() in group_lower for pattern in sink_connector_patterns
            ):
                sink_groups[group_id] = topics
                logger.debug(
                    f"Identified sink connector group: {group_id} with {len(topics)} topics"
                )

        logger.info(
            f"Found {len(sink_groups)} consumer groups associated with sink connectors"
        )
        return sink_groups
