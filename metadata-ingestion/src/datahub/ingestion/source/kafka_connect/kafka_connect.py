import base64
import logging
from typing import Dict, Iterable, List, Optional

import jpype
import jpype.imports
import requests

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    CONNECTOR_CLASS,
    SINK,
    SOURCE,
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_platform_instance,
    transform_connector_config,
)
from datahub.ingestion.source.kafka_connect.consumer_group_analyzer import (
    ConsumerGroupAnalyzer,
)
from datahub.ingestion.source.kafka_connect.topic_cache import (
    ConfluentCloudTopicRetriever,
    KafkaTopicCache,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@platform_name("Kafka Connect")
@config_class(KafkaConnectSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class KafkaConnectSource(StatefulIngestionSourceBase):
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport
    platform: str = "kafka-connect"

    def __init__(self, config: KafkaConnectSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = KafkaConnectSourceReport()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Test the connection using appropriate credentials
        connect_username, connect_password = self.config.get_connect_credentials()
        if connect_username is not None and connect_password is not None:
            logger.info(
                f"Connecting to {self.config.connect_uri} with Authentication..."
            )
            # Set up Basic Authentication for Connect API (requests handles the encoding automatically)
            self.session.auth = (connect_username, connect_password)
        else:
            # For Confluent Cloud, authentication is required
            if self._detect_confluent_cloud():
                raise ValueError(
                    "Confluent Cloud detected but no Connect API credentials provided. "
                    "Confluent Cloud requires authentication credentials for API access."
                )

        effective_uri = self.config.connect_uri
        test_response = self.session.get(f"{effective_uri}/connectors")
        test_response.raise_for_status()
        logger.info(f"Connection to {self.config.connect_uri} is ok")

        # Detect environment type for topic retrieval strategy
        self._is_confluent_cloud = self._detect_confluent_cloud()
        if self._is_confluent_cloud:
            logger.info(
                "Detected Confluent Cloud - using comprehensive Kafka REST API topic retrieval"
            )
            # For Confluent Cloud, DataHub graph connection is mandatory for lineage resolution
            if ctx.graph is None:
                raise ValueError(
                    "Confluent Cloud detected but DataHub graph connection not configured. "
                    "For Confluent Cloud, the DataHub graph is required for proper lineage resolution. "
                    "Please configure 'datahub_api' in your pipeline configuration with the server URL."
                )
        else:
            logger.info("Detected self-hosted Kafka Connect - using runtime topics API")

        # Note: Removed topic handler registry - topic resolution moved to connector registry

        # Initialize enhanced Confluent Cloud components
        self._topic_cache = KafkaTopicCache()  # Simple cache for single ingestion run
        self._topic_retriever = ConfluentCloudTopicRetriever(
            self.session, self._topic_cache
        )
        self._consumer_group_analyzer = ConsumerGroupAnalyzer(
            self.session, self.config, self.report
        )

        # Cache for all Kafka topics (single ingestion run)
        self._all_kafka_topics_cache: Optional[List[str]] = None

        if not jpype.isJVMStarted():
            jpype.startJVM()

    def get_connectors_manifest(self) -> Iterable[ConnectorManifest]:
        """Get Kafka Connect connectors manifest using REST API."""
        effective_uri = self.config.connect_uri
        connector_response = self.session.get(
            f"{effective_uri}/connectors",
        )

        payload = connector_response.json()

        for connector_name in payload:
            connector_url = f"{effective_uri}/connectors/{connector_name}"
            connector_manifest = self._get_connector_manifest(
                connector_name, connector_url
            )
            if connector_manifest is None or not self.config.connector_patterns.allowed(
                connector_manifest.name
            ):
                self.report.report_dropped(connector_name)
                continue

            if self.config.provided_configs:
                transform_connector_config(
                    connector_manifest.config, self.config.provided_configs
                )

            connector_manifest.url = connector_url
            connector_manifest.topic_names = self._get_connector_topics(
                connector_manifest
            )

            # Add tasks for source connectors
            # Skip for Confluent Cloud as the /tasks endpoint is not available in Connect v1 API
            if connector_manifest.type == SOURCE and not self._is_confluent_cloud:
                connector_manifest.tasks = self._get_connector_tasks(connector_name)

            # Extract lineages for this connector and check if it should be included
            should_include = self.extract_connector_lineages(connector_manifest)
            if not should_include:
                # Skip unsupported connector (matches master's behavior)
                continue

            yield connector_manifest

    def extract_connector_lineages(self, connector_manifest: ConnectorManifest) -> bool:
        """
        Extract lineages for a connector manifest.

        Returns:
            bool: True if connector should be included in output, False if it should be skipped
        """
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        # Try to get a connector handler from the registry
        connector = ConnectorRegistry.get_connector_for_manifest(
            connector_manifest, self.config, self.report, self.ctx
        )

        # For Confluent Cloud, populate all_cluster_topics for validation purposes
        if connector and self._is_confluent_cloud:
            all_cluster_topics = self._get_all_topics_from_kafka_api()
            if all_cluster_topics:
                connector.all_cluster_topics = all_cluster_topics
                logger.debug(
                    f"Populated {len(all_cluster_topics)} cluster topics for connector '{connector_manifest.name}'"
                )

        if not connector:
            # No handler found for this connector class
            if connector_manifest.type == SOURCE:
                # Source connectors without handlers are skipped (master's behavior with 'continue')
                self._handle_unsupported_connector(connector_manifest)
                return False  # Skip unsupported source connector
            else:
                # Sink connectors without handlers are still included (master's BaseConnector behavior)
                self._handle_unsupported_connector(connector_manifest)
                connector_manifest.lineages = []
                connector_manifest.flow_property_bag = {}
                return True  # Include unsupported sink connector

        # Handler found - extract lineages and flow_property_bag
        # Master always extracts and yields, regardless of whether they're empty
        connector_manifest.lineages = connector.extract_lineages()
        connector_manifest.flow_property_bag = connector.extract_flow_property_bag()
        return True  # Always include connectors with handlers

    def _handle_unsupported_connector(
        self, connector_manifest: ConnectorManifest
    ) -> None:
        """Handle unsupported connectors with appropriate warnings."""
        connector_class_value = connector_manifest.config.get(CONNECTOR_CLASS) or ""

        self.report.report_dropped(connector_manifest.name)

        if connector_manifest.type == SOURCE:
            self.report.warning(
                "Lineage for Source Connector not supported. "
                "Please refer to Kafka Connect docs to use `generic_connectors` config.",
                context=f"{connector_manifest.name} of type {connector_class_value}",
            )
        elif connector_manifest.type == SINK:
            self.report.warning(
                "Lineage for Sink Connector not supported.",
                context=f"{connector_manifest.name} of type {connector_class_value}",
            )

    def _get_connector_manifest(
        self, connector_name: str, connector_url: str
    ) -> Optional[ConnectorManifest]:
        try:
            connector_response = self.session.get(connector_url)
            connector_response.raise_for_status()
        except Exception as e:
            self.report.warning(
                "Failed to get connector details", connector_name, exc=e
            )
            return None
        manifest = connector_response.json()

        # Filter the manifest to only include fields expected by ConnectorManifest
        # This handles API responses that may contain additional fields (e.g., 'extensions' in Confluent Cloud)
        # Some APIs may have nested structure - try to extract the actual connector info

        # Check if this is a nested response structure (e.g., {"info": {...}})
        if "info" in manifest and isinstance(manifest["info"], dict):
            manifest = manifest["info"]

        # Validate required fields exist and are of correct type
        name = manifest.get("name")
        if not name or not isinstance(name, str):
            # Fallback to connector_name if name is missing or invalid
            logger.debug(
                f"Using fallback name '{connector_name}' for connector with invalid/missing name field"
            )
            name = connector_name

        connector_type = manifest.get("type")
        if not connector_type or not isinstance(connector_type, str):
            self.report.warning(
                "Connector manifest missing or has invalid 'type' field",
                context=f"{connector_name} (manifest keys: {list(manifest.keys())})",
            )
            return None

        config = manifest.get("config", {})
        if not isinstance(config, dict):
            logger.warning(
                f"Connector {connector_name} has invalid 'config' field, using empty dict"
            )
            config = {}

        tasks = manifest.get("tasks", [])
        if not isinstance(tasks, list):
            logger.warning(
                f"Connector {connector_name} has invalid 'tasks' field, using empty list"
            )
            tasks = []

        filtered_manifest = {
            "name": name,
            "type": connector_type,
            "config": config,
            "tasks": tasks,
        }

        connector_manifest = ConnectorManifest(**filtered_manifest)
        return connector_manifest

    def _get_connector_tasks(self, connector_name: str) -> List[Dict[str, dict]]:
        try:
            effective_uri = self.config.connect_uri
            response = self.session.get(
                f"{effective_uri}/connectors/{connector_name}/tasks",
            )
            response.raise_for_status()
        except Exception as e:
            self.report.warning(
                "Error getting connector tasks", context=connector_name, exc=e
            )
            return []

        return response.json()

    def _detect_confluent_cloud(self) -> bool:
        """
        Detect if we're running against Confluent Cloud.

        Detection logic:
        1. If environment_id and cluster_id are explicitly configured, assume Confluent Cloud
        2. Otherwise, check if connect_uri follows Confluent Cloud pattern:
           https://api.confluent.cloud/connect/v1/environments/{env-id}/clusters/{cluster-id}
        """
        # Explicit Confluent Cloud configuration takes precedence
        if (
            self.config.confluent_cloud_environment_id
            and self.config.confluent_cloud_cluster_id
        ):
            return True

        # Fallback to URI-based detection
        uri = self.config.connect_uri.lower()
        return "api.confluent.cloud" in uri and "/connect/v1/" in uri

    def _get_connector_topics(self, connector_manifest: ConnectorManifest) -> List[str]:
        """
        Get topics for a connector using environment-specific strategy.

        This method implements a hybrid approach that handles both Confluent Cloud
        and self-hosted Kafka Connect environments with different strategies:

        **Self-hosted Strategy:**
        - Uses the runtime `/connectors/{name}/topics` API endpoint
        - Returns actual topics that the connector is currently reading from/writing to
        - Provides the most accurate topic information as it reflects runtime state

        **Confluent Cloud Strategy:**
        - Uses configuration-based topic derivation from the connector manifest
        - Extracts topics from connector configuration fields (topics, kafka.topic, etc.)
        - No additional API calls needed since we already have the config from manifest
        - Required because Confluent Cloud doesn't expose the `/topics` endpoint

        **Feature Flag Control:**
        The `use_connect_topics_api` configuration flag controls whether this method
        performs any API calls at all. When disabled, returns empty list to skip
        all topic validation for air-gapped environments or performance optimization.

        **Environment Detection:**
        Automatically detects environment based on connect_uri patterns:
        - Confluent Cloud: URIs containing 'confluent.cloud'
        - Self-hosted: All other URI patterns (localhost, internal domains, etc.)

        Args:
            connector_manifest: ConnectorManifest containing name, type, config, etc.

        Returns:
            List of topic names that the connector reads from or writes to.
            Returns empty list if:
            - Feature flag `use_connect_topics_api` is disabled
            - API calls fail (self-hosted only)
            - Connector has no topic configuration (Confluent Cloud)
        """
        connector_name = connector_manifest.name

        # Check feature flag to determine if we should use Connect API
        if not self.config.use_connect_topics_api:
            logger.info(
                f"Connect topics API disabled via config - skipping topic retrieval for {connector_name}"
            )
            return []

        # Environment-specific approach
        if self._is_confluent_cloud:
            # Confluent Cloud: Don't populate topic_names with all cluster topics
            # All cluster topics will be stored separately and passed where needed
            # This prevents accidental parsing of non-connector-specific topics
            logger.debug(
                f"Confluent Cloud environment - not populating topic_names for {connector_name}"
            )
            return []
        else:
            # Self-hosted: Use original runtime topics API
            # These are connector-specific topics, safe to populate in topic_names
            topics = self._get_topics_self_hosted(connector_name)

            # For sink connectors, filter out stale topics
            # The /topics API returns all topics ever used, but connector config specifies current topics
            if connector_manifest.type == SINK and topics:
                sink_filter = SinkTopicFilter()
                if sink_filter.has_topic_config(connector_manifest.config):
                    filtered_topics = sink_filter.filter_stale_topics(
                        topics, connector_manifest.config
                    )
                    logger.debug(
                        f"Filtered {len(topics) - len(filtered_topics)} stale topics for sink connector {connector_name}"
                    )
                    return filtered_topics

            return topics

    def _get_topics_self_hosted(self, connector_name: str) -> List[str]:
        """Get topics using the original runtime /topics API (self-hosted only)."""
        try:
            effective_uri = self.config.connect_uri
            response = self.session.get(
                f"{effective_uri}/connectors/{connector_name}/topics"
            )
            response.raise_for_status()

            processed_topics = response.json()[connector_name]["topics"]
            logger.debug(
                f"Retrieved {len(processed_topics)} runtime topics from self-hosted API for {connector_name}"
            )
            return processed_topics

        except Exception as e:
            self.report.warning(
                "Error getting connector topics from runtime API",
                context=connector_name,
                exc=e,
            )
            return []

    def _get_topics_confluent_cloud_from_manifest(
        self, connector_manifest: ConnectorManifest
    ) -> List[str]:
        """
        Get topics for Confluent Cloud using comprehensive Kafka REST API.

        This method now gets the actual complete list of topics from the Kafka cluster
        via Kafka REST API v3, which enables the reverse transform pipeline strategy
        to work properly with all existing topics.

        Args:
            connector_manifest: ConnectorManifest with config, type, etc.

        Returns:
            List of all topic names from the Kafka cluster.
            Falls back to config-based derivation if Kafka API fails.
        """
        try:
            # First try to get all topics from Kafka REST API for comprehensive coverage
            all_kafka_topics = self._get_all_topics_from_kafka_api()
            if all_kafka_topics:
                logger.debug(
                    f"Retrieved {len(all_kafka_topics)} topics from Kafka REST API for transform pipeline"
                )
                return all_kafka_topics

            # Fallback to config-based derivation if Kafka API fails
            logger.info(
                "Kafka REST API not available, falling back to config-based topic derivation"
            )
            return self._get_topics_from_connector_config(connector_manifest)

        except Exception as e:
            logger.debug(
                f"Failed to get topics for connector {connector_manifest.name}: {e}"
            )
            # Final fallback to config-based approach
            return self._get_topics_from_connector_config(connector_manifest)

    def _get_all_topics_from_kafka_api(self) -> List[str]:
        """
        Get all topics from Confluent Cloud Kafka REST API v3.

        This provides the comprehensive topic list needed for the reverse transform
        pipeline strategy to work effectively.

        Uses instance variable caching to avoid repeated API calls during a single ingestion run.

        Returns:
            List of all topic names from the Kafka cluster.
            Empty list if API is not accessible or fails.
        """
        # Return cached result if available
        if self._all_kafka_topics_cache is not None:
            return self._all_kafka_topics_cache

        try:
            # Extract cluster information from Connect URI
            kafka_rest_endpoint, cluster_id = self._parse_confluent_cloud_info()
            if not kafka_rest_endpoint or not cluster_id:
                logger.debug("Could not extract Kafka REST endpoint from Connect URI")
                return []

            # Build Kafka REST API v3 endpoint for listing topics
            # Format: https://pkc-xxxxx.region.provider.confluent.cloud/kafka/v3/clusters/{cluster-id}/topics
            if kafka_rest_endpoint.endswith("/"):
                kafka_rest_endpoint = kafka_rest_endpoint.rstrip("/")
            topics_url = f"{kafka_rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"

            # Set up authentication for Kafka API
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            auth = None

            # Check if we have Kafka-specific credentials configured
            if self.config.kafka_api_key and self.config.kafka_api_secret:
                # Use Kafka-specific API credentials with Basic auth
                headers["Authorization"] = self._create_basic_auth_header(
                    self.config.kafka_api_key, self.config.kafka_api_secret
                )
                logger.debug("Using dedicated Kafka API credentials for authentication")

            # Fallback to reusing Connect credentials (same API key/secret in Confluent Cloud)
            elif hasattr(self.session, "auth") and self.session.auth:
                auth = self.session.auth
                logger.debug("Reusing Connect credentials for Kafka API authentication")

            else:
                logger.warning(
                    "No authentication credentials available for Kafka API - API call may fail"
                )

            # Make API call to get all topics
            response = self.session.get(topics_url, headers=headers, auth=auth)
            response.raise_for_status()

            # Parse v3 API response format
            topics_data = response.json()
            if topics_data.get("kind") == "KafkaTopicList" and "data" in topics_data:
                all_topics = [
                    topic["topic_name"]
                    for topic in topics_data["data"]
                    if not topic.get("is_internal", False)
                ]
                logger.info(
                    f"Retrieved {len(all_topics)} topics from Confluent Cloud Kafka REST API v3"
                )
                # Cache the result for subsequent calls
                self._all_kafka_topics_cache = all_topics
                return all_topics
            else:
                logger.warning("Unexpected response format from Kafka REST API")
                # Cache empty result to avoid repeated failures
                self._all_kafka_topics_cache = []
                return []

        except Exception as e:
            logger.debug(f"Failed to get topics from Kafka REST API: {e}")
            # Cache empty result to avoid repeated API calls on failure
            self._all_kafka_topics_cache = []
            return []

    def _parse_confluent_cloud_info(self) -> tuple[Optional[str], Optional[str]]:
        """
        Parse Confluent Cloud Connect URI and connector configs to extract Kafka REST endpoint and cluster ID.

        Connect URI format:
        https://api.confluent.cloud/connect/v1/environments/{env-id}/clusters/{cluster-id}

        Returns:
            Tuple of (kafka_rest_endpoint, cluster_id) or (None, None) if parsing fails.
        """
        try:
            # First check if user provided explicit Kafka REST endpoint
            if self.config.kafka_rest_endpoint:
                cluster_id = self._extract_cluster_id_from_connect_uri()
                if cluster_id:
                    logger.info(
                        f"Using configured Kafka REST endpoint: {self.config.kafka_rest_endpoint} with cluster ID: {cluster_id}"
                    )
                    return self.config.kafka_rest_endpoint, cluster_id
                else:
                    logger.warning(
                        "Kafka REST endpoint provided but could not extract cluster ID from Connect URI"
                    )
                    return None, None

            # Try to auto-derive Kafka REST endpoint from connector configurations
            derived_endpoint = self._derive_kafka_rest_endpoint_from_connectors()
            cluster_id = self._extract_cluster_id_from_connect_uri()

            if derived_endpoint and cluster_id:
                logger.info(
                    f"Auto-derived Kafka REST endpoint: {derived_endpoint} with cluster ID: {cluster_id}"
                )
                return derived_endpoint, cluster_id

            # Fallback: extract cluster ID but no REST endpoint
            if cluster_id:
                logger.info(f"Extracted cluster ID: {cluster_id} from Connect URI")
                logger.info(
                    "Could not auto-derive Kafka REST endpoint from connector configs"
                )
                logger.info(
                    "For comprehensive topic retrieval, please configure kafka_rest_endpoint"
                )
                return None, cluster_id

            return None, None

        except Exception as e:
            logger.debug(
                f"Failed to parse Confluent Cloud info from URI {self.config.connect_uri}: {e}"
            )
            return None, None

    def _derive_kafka_rest_endpoint_from_connectors(self) -> Optional[str]:
        """
        Try to derive the Kafka REST endpoint from connector configurations.

        Some connectors (especially Cloud ones) include kafka.endpoint in their config
        which we can use to derive the REST endpoint.

        Returns:
            Kafka REST endpoint URL or None if not found.
        """
        try:
            connector_names = self._get_connector_names_for_endpoint_discovery()
            if not connector_names:
                return None

            return self._find_kafka_endpoint_from_connectors(connector_names)

        except Exception as e:
            logger.debug(f"Failed to derive Kafka endpoint from connector configs: {e}")
            return None

    def _get_connector_names_for_endpoint_discovery(self) -> List[str]:
        """Get list of connector names for endpoint discovery."""
        effective_uri = self.config.connect_uri
        response = self.session.get(f"{effective_uri}/connectors")
        response.raise_for_status()
        return response.json()

    def _find_kafka_endpoint_from_connectors(
        self, connector_names: List[str]
    ) -> Optional[str]:
        """Search through connectors to find a Confluent Cloud Kafka endpoint."""
        # Check first few connectors for Kafka endpoint information
        for connector_name in connector_names[:3]:  # Check max 3 connectors
            rest_endpoint = self._extract_kafka_endpoint_from_connector(connector_name)
            if rest_endpoint:
                return rest_endpoint

        logger.debug("No Kafka endpoint found in connector configurations")
        return None

    def _extract_kafka_endpoint_from_connector(
        self, connector_name: str
    ) -> Optional[str]:
        """Extract Kafka endpoint from a single connector configuration."""
        try:
            effective_uri = self.config.connect_uri
            connector_response = self.session.get(
                f"{effective_uri}/connectors/{connector_name}"
            )
            connector_response.raise_for_status()
            connector_data = connector_response.json()
            config = connector_data.get("config", {})

            # Look for Kafka endpoint in various config fields
            kafka_endpoint = (
                config.get("kafka.endpoint")
                or config.get("bootstrap.servers")
                or config.get("kafka.bootstrap.servers")
            )

            if kafka_endpoint and "confluent.cloud" in kafka_endpoint:
                # Parse the broker endpoint to get the REST endpoint
                # Format: SASL_SSL://pkc-xxxxx.region.provider.confluent.cloud:9092
                # Convert to: https://pkc-xxxxx.region.provider.confluent.cloud
                rest_endpoint = self._convert_broker_to_rest_endpoint(kafka_endpoint)
                if rest_endpoint:
                    logger.info(
                        f"Auto-derived Kafka REST endpoint from connector {connector_name}: {rest_endpoint}"
                    )
                    return rest_endpoint

        except Exception as e:
            logger.debug(
                f"Failed to check connector {connector_name} for Kafka endpoint: {e}"
            )

        return None

    def _convert_broker_to_rest_endpoint(self, broker_endpoint: str) -> Optional[str]:
        """
        Convert Kafka broker endpoint to REST API endpoint.

        Input: SASL_SSL://pkc-xxxxx.region.provider.confluent.cloud:9092
        Output: https://pkc-xxxxx.region.provider.confluent.cloud
        """
        try:
            # Remove protocol prefix and port
            if "://" in broker_endpoint:
                endpoint = broker_endpoint.split("://")[1]
            else:
                endpoint = broker_endpoint

            # Remove port if present
            if ":" in endpoint:
                endpoint = endpoint.split(":")[0]

            # Convert to HTTPS REST endpoint
            if "confluent.cloud" in endpoint:
                rest_endpoint = f"https://{endpoint}"
                logger.debug(
                    f"Converted broker endpoint {broker_endpoint} to REST endpoint {rest_endpoint}"
                )
                return rest_endpoint

            return None

        except Exception as e:
            logger.debug(
                f"Failed to convert broker endpoint {broker_endpoint} to REST endpoint: {e}"
            )
            return None

    def _extract_cluster_id_from_connect_uri(self) -> Optional[str]:
        """Extract cluster ID from Confluent Cloud Connect URI."""
        try:
            uri = self.config.connect_uri

            # Format: https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456
            if "/environments/" in uri and "/clusters/" in uri:
                parts = uri.split("/")
                cluster_index = parts.index("clusters") + 1

                if cluster_index < len(parts):
                    cluster_id = parts[cluster_index]
                    return cluster_id

            return None

        except Exception as e:
            logger.debug(f"Failed to extract cluster ID from Connect URI: {e}")
            return None

    def _get_topics_from_connector_config(
        self, connector_manifest: ConnectorManifest
    ) -> List[str]:
        """
        Get topics from connector configuration using registry approach.

        This delegates topic extraction to the connector registry to avoid circular imports.
        """
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        return ConnectorRegistry.get_topics_from_config(
            connector_manifest, self.config, self.report, self.ctx
        )

    # Note: _get_topic_fields_for_connector and get_platform_from_connector_class removed
    # These are now handled directly by connector implementations via the registry

    def _get_cached_kafka_topics(self) -> List[str]:
        """Get all Kafka topics using enhanced caching for Confluent Cloud."""
        try:
            kafka_rest_endpoint, cluster_id = self._parse_confluent_cloud_info()
            if kafka_rest_endpoint and cluster_id:
                auth_headers = self._get_kafka_auth_headers()
                return self._topic_retriever.get_all_topics_cached(
                    kafka_rest_endpoint, cluster_id, auth_headers
                )
            else:
                # Fallback to original API method if no endpoint/cluster
                return self._get_all_topics_from_kafka_api()
        except Exception as e:
            logger.warning(f"Failed to get cached Kafka topics: {e}")
            return []

    def _get_cached_consumer_groups(
        self, kafka_rest_endpoint: str, cluster_id: str
    ) -> Dict[str, List[str]]:
        """Get consumer group assignments using enhanced caching."""
        try:
            auth_headers = self._get_kafka_auth_headers()
            return self._topic_retriever.get_consumer_group_assignments_cached(
                kafka_rest_endpoint, cluster_id, auth_headers
            )
        except Exception as e:
            logger.warning(f"Failed to get cached consumer groups: {e}")
            return {}

    def _create_basic_auth_header(self, username: str, password: str) -> str:
        """Create a Basic Auth header value."""
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        return f"Basic {credentials}"

    def _get_kafka_auth_headers(self) -> Optional[Dict[str, str]]:
        """Get authentication headers for Kafka REST API."""
        kafka_username, kafka_password = self.config.get_kafka_credentials()
        if kafka_username and kafka_password:
            return {
                "Authorization": self._create_basic_auth_header(
                    kafka_username, kafka_password
                )
            }
        return None

    def construct_flow_workunit(self, connector: ConnectorManifest) -> MetadataWorkUnit:
        connector_name = connector.name
        connector_type = connector.type
        connector_class = connector.config.get(CONNECTOR_CLASS)
        flow_property_bag = connector.flow_property_bag
        # connector_url = connector.url  # NOTE: this will expose connector credential when used
        flow_urn = builder.make_data_flow_urn(
            self.platform,
            connector_name,
            self.config.env,
            self.config.platform_instance,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=models.DataFlowInfoClass(
                name=connector_name,
                description=f"{connector_type.capitalize()} connector using `{connector_class}` plugin.",
                customProperties=flow_property_bag,
                # externalUrl=connector_url, # NOTE: this will expose connector credential when used
            ),
        ).as_workunit()

    def construct_job_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.name
        flow_urn = builder.make_data_flow_urn(
            self.platform,
            connector_name,
            self.config.env,
            self.config.platform_instance,
        )

        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform
                job_property_bag = lineage.job_property_bag

                source_platform_instance = get_platform_instance(
                    self.config, connector_name, source_platform
                )
                target_platform_instance = get_platform_instance(
                    self.config, connector_name, target_platform
                )

                job_id = self.get_job_id(lineage, connector, self.config)
                job_urn = builder.make_data_job_urn_with_flow(flow_urn, job_id)

                inlets = (
                    [
                        self.make_lineage_dataset_urn(
                            source_platform, source_dataset, source_platform_instance
                        )
                    ]
                    if source_dataset
                    else []
                )
                outlets = [
                    self.make_lineage_dataset_urn(
                        target_platform, target_dataset, target_platform_instance
                    )
                ]

                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInfoClass(
                        name=f"{connector_name}:{job_id}",
                        type="COMMAND",
                        customProperties=job_property_bag,
                    ),
                ).as_workunit()

                # Convert fine-grained lineage dictionaries to proper class instances
                fine_grained_lineages_typed = None
                if lineage.fine_grained_lineages:
                    fine_grained_lineages_typed = [
                        models.FineGrainedLineageClass(
                            upstreamType=fg["upstreamType"],
                            downstreamType=fg["downstreamType"],
                            upstreams=fg.get("upstreams"),
                            downstreams=fg.get("downstreams"),
                        )
                        for fg in lineage.fine_grained_lineages
                    ]

                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInputOutputClass(
                        inputDatasets=inlets,
                        outputDatasets=outlets,
                        fineGrainedLineages=fine_grained_lineages_typed,
                    ),
                ).as_workunit()

    def get_job_id(
        self,
        lineage: KafkaConnectLineage,
        connector: ConnectorManifest,
        config: KafkaConnectSourceConfig,
    ) -> str:
        connector_class = connector.config.get(CONNECTOR_CLASS)

        # Note - This block is only to maintain backward compatibility of Job URN
        if (
            connector_class
            and connector.type == SOURCE
            and (
                "JdbcSourceConnector" in connector_class
                or connector_class.startswith("io.debezium.connector")
                or connector_class in CLOUD_JDBC_SOURCE_CLASSES
            )
            and lineage.source_dataset
            and config.connect_to_platform_map
            and config.connect_to_platform_map.get(connector.name)
            and config.connect_to_platform_map[connector.name].get(
                lineage.source_platform
            )
        ):
            return f"{config.connect_to_platform_map[connector.name][lineage.source_platform]}.{lineage.source_dataset}"

        # Default behavior: use source_dataset for backward compatibility
        # Exception: when one source produces multiple targets (detected by checking other lineages)
        # In that case, append target to make job_id unique (similar to unknown_source pattern)
        if lineage.source_dataset:
            # Check if there are other lineages with the same source (1-to-many case)
            if connector.lineages:
                same_source_count = sum(
                    1
                    for other_lineage in connector.lineages
                    if other_lineage.source_dataset == lineage.source_dataset
                )
                if same_source_count > 1:
                    # Multiple outputs from same source - append target for uniqueness
                    return f"{lineage.source_dataset}.{lineage.target_dataset}"

            # Standard case: one source to one target
            return lineage.source_dataset
        else:
            # No source identified - use target with unknown_source prefix
            return f"unknown_source.{lineage.target_dataset}"

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for connector in self.get_connectors_manifest():
            # Always emit flow workunit (connector metadata) for all connectors
            yield self.construct_flow_workunit(connector)

            # Only emit job workunits if connector has lineages
            if connector.lineages:
                yield from self.construct_job_workunits(connector)

            self.report.report_connector_scanned(connector.name)

    def get_report(self) -> KafkaConnectSourceReport:
        return self.report

    def make_lineage_dataset_urn(
        self, platform: str, name: str, platform_instance: Optional[str]
    ) -> str:
        if self.config.convert_lineage_urns_to_lowercase:
            name = name.lower()

        return builder.make_dataset_urn_with_platform_instance(
            platform, name, platform_instance, self.config.env
        )


class SinkTopicFilter:
    """Helper class to filter Kafka Connect topics based on configuration."""

    def filter_stale_topics(
        self,
        processed_topics: List[str],
        sink_config: Dict[str, str],
    ) -> List[str]:
        """
        Kafka-connect's /topics API returns the set of topic names the connector has been using
        since its creation or since the last time its set of active topics was reset. This means-
        if a topic was ever used by a connector, it will be returned, even if it is no longer used.
        To remove these stale topics from the list, we double-check the list returned by the API
        against the sink connector's config.
        Sink connectors configure exactly one of `topics` or `topics.regex`
        https://kafka.apache.org/documentation/#sinkconnectorconfigs_topics

        Args:
            processed_topics: List of topics currently being processed
            sink_config: Configuration dictionary for the sink connector

        Returns:
            List of filtered topics that match the configuration

        Raises:
            ValueError: If sink connector configuration is missing both 'topics' and 'topics.regex' fields

        """
        # Absence of topics config is a defensive NOOP,
        # although this should never happen in real world
        if not self.has_topic_config(sink_config):
            logger.warning(
                f"Found sink without topics config {sink_config.get(CONNECTOR_CLASS)}"
            )
            return processed_topics

        # Handle explicit topic list
        if sink_config.get("topics"):
            return self._filter_by_topic_list(processed_topics, sink_config["topics"])
        else:
            # Handle regex pattern
            return self._filter_by_topic_regex(
                processed_topics, sink_config["topics.regex"]
            )

    def has_topic_config(self, sink_config: Dict[str, str]) -> bool:
        """Check if sink config has either topics or topics.regex."""
        return bool(sink_config.get("topics") or sink_config.get("topics.regex"))

    def _filter_by_topic_list(
        self, processed_topics: List[str], topics_config: str
    ) -> List[str]:
        """Filter topics based on explicit topic list from config."""
        config_topics = [
            topic.strip() for topic in topics_config.split(",") if topic.strip()
        ]
        return [topic for topic in processed_topics if topic in config_topics]

    def _filter_by_topic_regex(
        self, processed_topics: List[str], regex_pattern: str
    ) -> List[str]:
        """Filter topics based on regex pattern from config."""
        from java.util.regex import Pattern

        regex_matcher = Pattern.compile(regex_pattern)

        return [
            topic
            for topic in processed_topics
            if regex_matcher.matcher(topic).matches()
        ]
