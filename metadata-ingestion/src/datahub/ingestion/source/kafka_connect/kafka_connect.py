import base64
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Iterable, List, Optional

import jpype
import jpype.imports
import requests
from requests.adapters import HTTPAdapter
from typing_extensions import LiteralString
from urllib3.util import Retry

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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.confluent.constants import (
    KAFKA_REST_FETCH_FAILED,
    KAFKA_REST_FETCH_FAILED_CATALOG,
    KAFKA_REST_NO_AUTH,
    KAFKA_REST_NO_AUTH_CATALOG,
    KAFKA_REST_NO_ENDPOINT,
    KAFKA_REST_NO_ENDPOINT_CATALOG,
    KAFKA_REST_UNEXPECTED_RESPONSE,
    KAFKA_REST_UNEXPECTED_RESPONSE_CATALOG,
    KAFKA_TOPIC_FETCH_RETRY_STATUS_CODES,
    MAX_KAFKA_TOPIC_FETCH_ATTEMPTS,
    MAX_KAFKA_TOPIC_PAGES,
)
from datahub.ingestion.source.confluent.models import (
    BM_COLLISION_WITH_CONNECTOR_CONFIG,
    non_colliding_business_metadata,
)
from datahub.ingestion.source.kafka_connect.common import (
    CLOUD_JDBC_SOURCE_CLASSES,
    CONNECTOR_CLASS,
    KAFKA,
    SINK,
    SOURCE,
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
    get_platform_instance,
    transform_connector_config,
)
from datahub.ingestion.source.kafka_connect.confluent_catalog import (
    CatalogConnector,
    ConnectorCatalog,
)
from datahub.ingestion.source.kafka_connect.confluent_catalog_constants import (
    LINEAGE_SOURCE_CATALOG,
    LINEAGE_SOURCE_PROPERTY,
)
from datahub.ingestion.source.kafka_connect.consumer_group_analyzer import (
    ConsumerGroupAnalyzer,
)
from datahub.ingestion.source.kafka_connect.topic_cache import (
    ConfluentCloudTopicRetriever,
    KafkaTopicCache,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sql_parsing.schema_resolver_provider import SchemaResolverProvider

logger = logging.getLogger(__name__)


class CatalogLineageOutcome(Enum):
    # No catalog entry, lineage disabled, or not a source: behave as if no catalog.
    NOT_APPLICABLE = auto()
    # Catalog applies but lists no topics: fall back to config-inferred lineage.
    NO_CATALOG_TOPICS = auto()
    # Every catalog topic was absent from the live cluster: emit no lineage.
    FILTERED_OUT = auto()
    RESOLVED = auto()


@dataclass(frozen=True)
class CatalogLineageResult:
    outcome: CatalogLineageOutcome
    lineages: List[KafkaConnectLineage] = field(default_factory=list)


@platform_name("Kafka Connect")
@config_class(KafkaConnectSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.TAGS,
    "Requires Confluent Cloud Stream Governance; enable via `confluent_catalog`",
    supported=True,
)
class KafkaConnectSource(StatefulIngestionSourceBase):
    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport
    platform: str = "kafka-connect"

    def __init__(self, config: KafkaConnectSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = KafkaConnectSourceReport()
        self.session = self._create_json_session()

        # Separate session for Kafka REST API calls — must NOT inherit Connect auth,
        # because Confluent Cloud uses different credentials for Connect vs Kafka APIs.
        # Mount retries for transient REST failures (see MAX_KAFKA_TOPIC_FETCH_ATTEMPTS).
        self.kafka_session = self._create_kafka_session()

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
            if self.config.is_confluent_cloud():
                raise ValueError(
                    "Confluent Cloud detected but no Connect API credentials provided. "
                    "Confluent Cloud requires authentication credentials for API access."
                )

        effective_uri = self.config.connect_uri
        test_response = self.session.get(f"{effective_uri}/connectors")
        test_response.raise_for_status()
        logger.info(f"Connection to {self.config.connect_uri} is ok")

        # Detect environment type for topic retrieval strategy
        self._is_confluent_cloud = self.config.is_confluent_cloud()
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

        # Create shared SchemaResolverProvider if graph is available.
        # The provider's lru_cache deduplicates bulk fetches across connectors.
        self._schema_resolver_provider: Optional[SchemaResolverProvider] = None
        if self.config.use_schema_resolver and self.ctx.graph:
            self._schema_resolver_provider = SchemaResolverProvider(
                graph=self.ctx.graph
            )

        # Note: Removed topic handler registry - topic resolution moved to connector registry

        # Initialize enhanced Confluent Cloud components
        self._topic_cache = KafkaTopicCache()  # Simple cache for single ingestion run
        self._topic_retriever = ConfluentCloudTopicRetriever(
            self.kafka_session, self._topic_cache
        )
        self._consumer_group_analyzer = ConsumerGroupAnalyzer(
            self.kafka_session, self.config, self.report
        )

        # Cache for the live cluster topic list. None = unavailable or not yet fetched;
        # [] = resolved empty cluster. See _get_all_topics_from_kafka_api.
        self._all_kafka_topics_cache: Optional[List[str]] = None
        self._all_kafka_topics_resolved: bool = False

        self._catalog = self._create_catalog()

        if not jpype.isJVMStarted():
            jpype.startJVM()

    def _create_catalog(self) -> Optional[ConnectorCatalog]:
        if not self.config.confluent_catalog.enabled:
            return None

        # Gate on the catalog SR URL, not connect_uri — independently configurable.
        if not self.config.confluent_catalog.check_confluent_cloud_endpoint(
            self.report
        ):
            return None

        logger.info("Confluent Stream Catalog enabled for connector metadata")
        return ConnectorCatalog(self.config.confluent_catalog, self.report)

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
                continue

            yield connector_manifest

    def extract_connector_lineages(self, connector_manifest: ConnectorManifest) -> bool:
        from datahub.ingestion.source.kafka_connect.connector_registry import (
            ConnectorRegistry,
        )

        connector = ConnectorRegistry.get_connector_for_manifest(
            connector_manifest, self.config, self.report, self._schema_resolver_provider
        )

        catalog_connector = self._get_catalog_connector(connector_manifest)

        all_cluster_topics: Optional[List[str]] = None
        if self._is_confluent_cloud:
            all_cluster_topics = self._get_all_topics_from_kafka_api()
            # Catalog cross-check below uses the full list. Only hand it to
            # connectors that need it for inference (sinks, JDBC, EventRouter).
            if (
                connector
                and all_cluster_topics is not None
                and connector.requires_cluster_topics()
            ):
                connector.all_cluster_topics = all_cluster_topics
                logger.debug(
                    f"Populated {len(all_cluster_topics)} cluster topics for connector '{connector_manifest.name}'"
                )

        catalog = self._build_catalog_lineages(
            connector_manifest, catalog_connector, all_cluster_topics
        )

        if catalog.outcome == CatalogLineageOutcome.RESOLVED:
            connector_manifest.lineages = catalog.lineages
        elif catalog.outcome == CatalogLineageOutcome.FILTERED_OUT and connector:
            # Unsupported connectors fall through to be dropped below instead.
            connector_manifest.lineages = []
            self.report.warning(
                message="Dropping all lineage for a connector because none of its "
                "Stream Catalog topics are present on the live Kafka cluster; its "
                "config-inferred lineage is not used as a fallback",
                context=f"connector={connector_manifest.name}",
            )
        elif connector:
            connector_manifest.lineages = connector.extract_lineages()
            if catalog.outcome == CatalogLineageOutcome.NO_CATALOG_TOPICS:
                self.report.report_catalog_lineage_fallback(connector_manifest.name)
        else:
            self._handle_unsupported_connector(connector_manifest)
            if connector_manifest.type == SOURCE:
                return False
            connector_manifest.lineages = []

        connector_manifest.flow_property_bag = (
            connector.extract_flow_property_bag() if connector else {}
        )
        self._apply_catalog_flow_properties(connector_manifest, catalog_connector)
        return True

    def _get_catalog_connector(
        self, connector_manifest: ConnectorManifest
    ) -> Optional[CatalogConnector]:
        if not self._catalog:
            return None
        return self._catalog.get_connector(connector_manifest.name)

    def _build_catalog_lineages(
        self,
        connector_manifest: ConnectorManifest,
        catalog_connector: Optional[CatalogConnector],
        all_cluster_topics: Optional[List[str]],
    ) -> CatalogLineageResult:
        if (
            not catalog_connector
            or not self.config.confluent_catalog.include_lineage
            or connector_manifest.type != SOURCE
        ):
            return CatalogLineageResult(CatalogLineageOutcome.NOT_APPLICABLE)

        topics = catalog_connector.get_topic_names()
        if not topics:
            return CatalogLineageResult(CatalogLineageOutcome.NO_CATALOG_TOPICS)

        if all_cluster_topics is not None:
            cluster_topic_set = set(all_cluster_topics)
            missing = [topic for topic in topics if topic not in cluster_topic_set]
            if missing:
                self.report.warning(
                    message="Ignoring Stream Catalog lineage topics that are not present on "
                    "the live Kafka cluster",
                    context=f"connector={connector_manifest.name}, topics={missing}",
                )
                topics = [topic for topic in topics if topic in cluster_topic_set]
            if not topics:
                return CatalogLineageResult(CatalogLineageOutcome.FILTERED_OUT)

        self.report.catalog_lineage_connectors += 1
        logger.debug(
            f"Using {len(topics)} Stream Catalog topics for connector '{connector_manifest.name}'"
        )
        return CatalogLineageResult(
            CatalogLineageOutcome.RESOLVED,
            [
                KafkaConnectLineage(
                    source_dataset=None,
                    source_platform=self.platform,
                    target_dataset=topic,
                    target_platform=KAFKA,
                    job_property_bag={LINEAGE_SOURCE_PROPERTY: LINEAGE_SOURCE_CATALOG},
                )
                for topic in topics
            ],
        )

    def _apply_catalog_flow_properties(
        self,
        connector_manifest: ConnectorManifest,
        catalog_connector: Optional[CatalogConnector],
    ) -> None:
        if (
            not catalog_connector
            or not self.config.confluent_catalog.include_business_metadata
        ):
            return

        # Connector config wins on name collision.
        existing = connector_manifest.flow_property_bag or {}
        properties = non_colliding_business_metadata(
            catalog_connector,
            existing,
            self.report,
            collision_message=BM_COLLISION_WITH_CONNECTOR_CONFIG,
            context=f"connector={connector_manifest.name}",
        )
        if properties:
            connector_manifest.flow_property_bag = {**existing, **properties}
            self.report.catalog_connectors_with_business_metadata += 1

    def _handle_unsupported_connector(
        self, connector_manifest: ConnectorManifest
    ) -> None:
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
                message="Failed to get connector details",
                context=connector_name,
                exc=e,
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

        connector_manifest = ConnectorManifest(
            name=name,
            type=connector_type,
            config=config,
            tasks=tasks,
        )
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

    @staticmethod
    def _create_json_session() -> requests.Session:
        """Create a requests session with standard JSON headers."""
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        return session

    @classmethod
    def _create_kafka_session(cls) -> requests.Session:
        session = cls._create_json_session()
        retry_strategy = Retry(
            total=MAX_KAFKA_TOPIC_FETCH_ATTEMPTS - 1,
            backoff_factor=1,
            status_forcelist=list(KAFKA_TOPIC_FETCH_RETRY_STATUS_CODES),
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_connector_topics(self, connector_manifest: ConnectorManifest) -> List[str]:
        """Get topics for a connector using environment-specific strategy.

        Self-hosted: uses runtime ``/connectors/{name}/topics`` API.
        Confluent Cloud: returns empty (topics are populated separately via Kafka REST API).
        Disabled entirely when ``use_connect_topics_api`` is False.
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
            Falls back to config-based derivation if Kafka API is unavailable.
        """
        try:
            all_kafka_topics = self._get_all_topics_from_kafka_api()
            if all_kafka_topics is not None:
                logger.debug(
                    f"Retrieved {len(all_kafka_topics)} topics from Kafka REST API for transform pipeline"
                )
                return all_kafka_topics

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

    def _get_all_topics_from_kafka_api(self) -> Optional[List[str]]:
        # None = unavailable; [] = empty cluster.
        if not self._all_kafka_topics_resolved:
            self._all_kafka_topics_cache = self._fetch_all_topics_from_kafka_api()
            self._all_kafka_topics_resolved = True
        return self._all_kafka_topics_cache

    def _fetch_all_topics_from_kafka_api(self) -> Optional[List[str]]:
        try:
            kafka_rest_endpoint, cluster_id = self._parse_confluent_cloud_info()
            if not kafka_rest_endpoint or not cluster_id:
                self._warn_kafka_topics_unavailable(
                    KAFKA_REST_NO_ENDPOINT_CATALOG
                    if self._catalog is not None
                    else KAFKA_REST_NO_ENDPOINT,
                    context=f"connect_uri={self.config.connect_uri}",
                )
                return None

            topics_url = f"{kafka_rest_endpoint.rstrip('/')}/kafka/v3/clusters/{cluster_id}/topics"

            auth_headers = self._get_kafka_auth_headers()
            if auth_headers is None:
                self._warn_kafka_topics_unavailable(
                    KAFKA_REST_NO_AUTH_CATALOG
                    if self._catalog is not None
                    else KAFKA_REST_NO_AUTH,
                    context=f"topics_url={topics_url}",
                )
                return None

            # v3 list endpoints page via `metadata.next` (an absolute URL, null on the
            # last page). Follow it so a large cluster isn't silently truncated to the
            # first page. The None (list unavailable) vs [] (authoritative empty cluster)
            # distinction the catalog cross-check relies on is preserved: any read or
            # shape failure returns None, a fully-read empty cluster returns [].
            all_topics: List[str] = []
            next_url: Optional[str] = topics_url
            pages = 0
            while next_url is not None:
                if pages >= MAX_KAFKA_TOPIC_PAGES:
                    self._warn_kafka_topics_unavailable(
                        KAFKA_REST_UNEXPECTED_RESPONSE_CATALOG
                        if self._catalog is not None
                        else KAFKA_REST_UNEXPECTED_RESPONSE,
                        context=f"topics_url={topics_url}, pages={pages}",
                    )
                    return None

                response = self.kafka_session.get(next_url, headers=auth_headers)
                response.raise_for_status()
                topics_data = response.json()
                pages += 1

                if (
                    topics_data.get("kind") != "KafkaTopicList"
                    or "data" not in topics_data
                ):
                    self._warn_kafka_topics_unavailable(
                        KAFKA_REST_UNEXPECTED_RESPONSE_CATALOG
                        if self._catalog is not None
                        else KAFKA_REST_UNEXPECTED_RESPONSE,
                        context=f"topics_url={topics_url}",
                    )
                    return None

                all_topics.extend(
                    topic["topic_name"]
                    for topic in topics_data["data"]
                    if not topic.get("is_internal", False)
                )
                next_url = (topics_data.get("metadata") or {}).get("next")

            logger.info(
                f"Retrieved {len(all_topics)} topics from Confluent Cloud "
                "Kafka REST API v3"
            )
            return all_topics

        except Exception as e:
            self._warn_kafka_topics_unavailable(
                KAFKA_REST_FETCH_FAILED_CATALOG
                if self._catalog is not None
                else KAFKA_REST_FETCH_FAILED,
                context=f"connect_uri={self.config.connect_uri}",
                exc=e,
            )
            return None

    def _warn_kafka_topics_unavailable(
        self,
        message: LiteralString,
        *,
        context: str,
        exc: Optional[Exception] = None,
    ) -> None:
        # Always warn: available_topics() consumers need this list with or without
        # Stream Catalog. Callers pick catalog-suffixed wording when the catalog is on.
        self.report.warning(message=message, context=context, exc=exc)

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
            connector_manifest, self.config, self.report, self._schema_resolver_provider
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
                return self._get_all_topics_from_kafka_api() or []
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
                outlets = (
                    [
                        self.make_lineage_dataset_urn(
                            target_platform, target_dataset, target_platform_instance
                        )
                    ]
                    if target_dataset
                    else []
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInfoClass(
                        name=f"{connector_name}:{job_id}",
                        type="COMMAND",
                        customProperties=job_property_bag,
                    ),
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=models.DataJobInputOutputClass(
                        inputDatasets=inlets,
                        outputDatasets=outlets,
                        fineGrainedLineages=lineage.fine_grained_lineages,
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

    def construct_catalog_metadata_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        """
        Only the Kafka Connect pipeline is tagged here. Tags on the topics themselves
        are emitted by the `kafka` source, which owns those datasets.
        """
        catalog_connector = self._get_catalog_connector(connector)
        if not catalog_connector:
            return

        if not self.config.confluent_catalog.include_tags or not catalog_connector.tags:
            return

        flow_urn = builder.make_data_flow_urn(
            self.platform,
            connector.name,
            self.config.env,
            self.config.platform_instance,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=builder.make_global_tag_aspect_with_tag_list(
                sorted(set(catalog_connector.tags))
            ),
        ).as_workunit()
        self.report.catalog_tagged_flows += 1

    def _warn_if_catalog_partial(self) -> None:
        # Mirror the kafka source: a partial catalog read means some connectors may
        # silently miss their catalog metadata this run. Only relevant when it is applied.
        if (
            (
                self.config.confluent_catalog.include_tags
                or self.config.confluent_catalog.include_business_metadata
            )
            and self._catalog is not None
            and not self._catalog.is_complete()
        ):
            self.report.warning(
                message="The Confluent Stream Catalog was only partially read, so some "
                "connectors may be missing catalog tags or business metadata this run. Re-run "
                "once the catalog is fully readable.",
                context="confluent_catalog",
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self._warn_if_catalog_partial()
        for connector in self.get_connectors_manifest():
            # Always emit flow workunit (connector metadata) for all connectors
            yield self.construct_flow_workunit(connector)

            yield from self.construct_catalog_metadata_workunits(connector)

            # Only emit job workunits if connector has lineages
            if connector.lineages:
                yield from self.construct_job_workunits(connector)

            self.report.report_connector_scanned(connector.name)

    def close(self) -> None:
        self.session.close()
        self.kafka_session.close()
        if self._catalog:
            try:
                self._catalog.close()
            except Exception:
                logger.warning("Failed to close Stream Catalog client", exc_info=True)
        if self._schema_resolver_provider:
            try:
                self._schema_resolver_provider.close()
            except Exception:
                logger.warning(
                    "Failed to close schema resolver provider", exc_info=True
                )
        super().close()

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
