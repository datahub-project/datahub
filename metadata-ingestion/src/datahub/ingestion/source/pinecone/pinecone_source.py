"""DataHub source for Pinecone vector database."""

import logging
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
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
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.pinecone.config import PineconeConfig
from datahub.ingestion.source.pinecone.pinecone_client import (
    IndexInfo,
    NamespaceStats,
    PineconeClient,
)
from datahub.ingestion.source.pinecone.report import PineconeSourceReport
from datahub.ingestion.source.pinecone.schema_inference import MetadataSchemaInferrer
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

# Platform constants
PLATFORM_NAME = "pinecone"
PLATFORM_URN = make_data_platform_urn(PLATFORM_NAME)


@platform_name("Pinecone")
@config_class(PineconeConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class PineconeSource(StatefulIngestionSourceBase):
    """
    DataHub source for Pinecone vector database.

    Extracts:
    - Index metadata (dimension, metric, configuration)
    - Namespace information (vector counts)
    - Container hierarchy (Index -> Namespace -> Dataset)
    """

    def __init__(self, config: PineconeConfig, ctx: PipelineContext):
        """Initialize the Pinecone source."""
        super().__init__(config, ctx)
        self.config = config
        self.report = PineconeSourceReport()
        self.client = PineconeClient(config)

        # Initialize schema inferrer if enabled
        self.schema_inferrer = None
        if config.enable_schema_inference:
            self.schema_inferrer = MetadataSchemaInferrer(
                max_fields=config.max_metadata_fields
            )

        # Initialize domain registry if needed
        self.domain_registry = DomainRegistry(
            cached_domains=[
                domain_id
                for domain_id in ([] if not config.domain else [config.domain])
            ],
            graph=ctx.graph,
        )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "PineconeSource":
        """Create a new instance of the source."""
        config = PineconeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Get workunit processors including stale entity removal."""
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Generate metadata workunits for Pinecone indexes and namespaces.

        Flow:
        1. List all indexes
        2. For each index, emit container workunit
        3. List namespaces in each index
        4. For each namespace, emit container + dataset workunits
        """
        try:
            # Phase 1: Discover indexes
            indexes = self.client.list_indexes()
            logger.info(f"Discovered {len(indexes)} indexes")

            for index_info in indexes:
                # Check if index should be processed
                if not self.config.index_pattern.allowed(index_info.name):
                    self.report.report_index_filtered(index_info.name)
                    logger.debug(f"Filtered out index: {index_info.name}")
                    continue

                self.report.report_index_scanned(index_info.name)
                logger.info(f"Processing index: {index_info.name}")

                try:
                    # Emit index container
                    yield from self._generate_index_container(index_info)

                    # Phase 2: Process namespaces
                    yield from self._process_namespaces(index_info)

                except Exception as e:
                    logger.error(
                        f"Failed to process index {index_info.name}: {e}", exc_info=True
                    )
                    self.report.report_index_failed(index_info.name, str(e))
                    continue

        except Exception as e:
            logger.error(f"Failed to list indexes: {e}", exc_info=True)
            raise

    def _generate_index_container(
        self, index_info: IndexInfo
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate container workunit for a Pinecone index.

        Args:
            index_info: Information about the index

        Yields:
            MetadataWorkUnit for the index container
        """
        # Create container key for the index
        container_key = ContainerKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            container_name=index_info.name,
        )

        # Determine index type from spec
        spec = index_info.spec
        index_type = "unknown"
        pod_type = None
        replicas = None

        if "serverless" in spec:
            index_type = "serverless"
        elif "pod" in spec:
            index_type = "pod"
            pod_spec = spec.get("pod", {})
            pod_type = pod_spec.get("pod_type")
            replicas = pod_spec.get("replicas")

        # Build custom properties
        custom_properties = {
            "dimension": str(index_info.dimension),
            "metric": index_info.metric,
            "index_type": index_type,
            "host": index_info.host,
            "status": index_info.status,
        }

        if pod_type:
            custom_properties["pod_type"] = pod_type
        if replicas:
            custom_properties["replicas"] = str(replicas)

        # Add spec details
        if "serverless" in spec:
            serverless_spec = spec.get("serverless", {})
            if "cloud" in serverless_spec:
                custom_properties["cloud"] = serverless_spec["cloud"]
            if "region" in serverless_spec:
                custom_properties["region"] = serverless_spec["region"]

        # Generate container workunits
        yield from gen_containers(
            container_key=container_key,
            name=index_info.name,
            sub_types=[DatasetContainerSubTypes.PINECONE_INDEX],
            description=f"Pinecone {index_type} index with {index_info.dimension} dimensions using {index_info.metric} metric",
            custom_properties=custom_properties,
        )

        logger.debug(f"Generated container for index: {index_info.name}")

    def _process_namespaces(self, index_info: IndexInfo) -> Iterable[MetadataWorkUnit]:
        """
        Process all namespaces within an index.

        Args:
            index_info: Information about the index

        Yields:
            MetadataWorkUnits for namespace containers and datasets
        """
        try:
            # List namespaces
            namespaces = self.client.list_namespaces(index_info.name)
            logger.info(
                f"Found {len(namespaces)} namespaces in index {index_info.name}"
            )

            for namespace_stats in namespaces:
                # Check if namespace should be processed
                namespace_name = namespace_stats.name or "(default)"

                if not self.config.namespace_pattern.allowed(namespace_stats.name):
                    self.report.report_namespace_filtered(
                        index_info.name, namespace_name
                    )
                    logger.debug(
                        f"Filtered out namespace: {index_info.name}/{namespace_name}"
                    )
                    continue

                self.report.report_namespace_scanned(index_info.name, namespace_name)
                logger.info(f"Processing namespace: {index_info.name}/{namespace_name}")

                try:
                    # Emit namespace container
                    yield from self._generate_namespace_container(
                        index_info, namespace_stats
                    )

                    # Emit dataset for the namespace
                    yield from self._generate_namespace_dataset(
                        index_info, namespace_stats
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to process namespace {index_info.name}/{namespace_name}: {e}",
                        exc_info=True,
                    )
                    self.report.report_namespace_failed(
                        index_info.name, namespace_name, str(e)
                    )
                    continue

        except Exception as e:
            logger.error(
                f"Failed to list namespaces for index {index_info.name}: {e}",
                exc_info=True,
            )
            raise

    def _generate_namespace_container(
        self, index_info: IndexInfo, namespace_stats: NamespaceStats
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate container workunit for a namespace.

        Args:
            index_info: Information about the parent index
            namespace_stats: Statistics for the namespace

        Yields:
            MetadataWorkUnit for the namespace container
        """
        # Create container key for the namespace
        namespace_display_name = namespace_stats.name or "(default)"
        namespace_container_name = f"{index_info.name}.{namespace_stats.name}"

        namespace_container_key = ContainerKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            container_name=namespace_container_name,
        )

        # Parent container key (the index)
        parent_container_key = ContainerKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            container_name=index_info.name,
        )

        # Build custom properties
        custom_properties = {
            "vector_count": str(namespace_stats.vector_count),
            "index_name": index_info.name,
        }

        # Generate container workunits with parent relationship
        yield from gen_containers(
            container_key=namespace_container_key,
            name=namespace_display_name,
            sub_types=[DatasetContainerSubTypes.PINECONE_NAMESPACE],
            parent_container_key=parent_container_key,
            description=f"Namespace in Pinecone index {index_info.name} containing {namespace_stats.vector_count} vectors",
            custom_properties=custom_properties,
        )

        logger.debug(f"Generated container for namespace: {namespace_container_name}")

    def _generate_namespace_dataset(
        self, index_info: IndexInfo, namespace_stats: NamespaceStats
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate dataset workunit for a namespace.

        The dataset represents the collection of vectors in the namespace.
        Includes schema inference if enabled.

        Args:
            index_info: Information about the parent index
            namespace_stats: Statistics for the namespace

        Yields:
            MetadataWorkUnit for the dataset
        """
        # Create dataset URN
        dataset_name = f"{index_info.name}.{namespace_stats.name}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=PLATFORM_NAME,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Create namespace container key for parent relationship
        namespace_container_key = ContainerKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            container_name=dataset_name,
        )

        # Dataset properties
        namespace_display_name = namespace_stats.name or "(default)"
        dataset_properties = DatasetPropertiesClass(
            name=namespace_display_name,
            description=f"Vector collection in namespace '{namespace_display_name}' of index '{index_info.name}'",
            customProperties={
                "vector_count": str(namespace_stats.vector_count),
                "dimension": str(index_info.dimension),
                "metric": index_info.metric,
                "index_name": index_info.name,
                "namespace": namespace_stats.name,
            },
        )

        # Emit dataset properties
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # Add schema metadata if inference is enabled
        if self.schema_inferrer and namespace_stats.vector_count > 0:
            try:
                schema_metadata = self._infer_schema(
                    index_info, namespace_stats, dataset_name
                )

                if schema_metadata:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=schema_metadata,
                    ).as_workunit()
                    logger.info(f"Emitted schema metadata for {dataset_name}")

            except Exception as e:
                logger.error(
                    f"Failed to infer schema for {dataset_name}: {e}", exc_info=True
                )
                self.report.report_schema_inference_failed(dataset_name, str(e))

        # Add dataset to container
        yield from add_dataset_to_container(
            container_key=namespace_container_key,
            dataset_urn=dataset_urn,
        )

        # Add platform instance
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=PLATFORM_URN,
                    instance=make_dataplatform_instance_urn(
                        PLATFORM_NAME, self.config.platform_instance
                    ),
                ),
            ).as_workunit()

        # Add status aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Add subtypes
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=["Vector Collection"]),
        ).as_workunit()

        self.report.report_dataset_generated()
        logger.debug(f"Generated dataset for namespace: {dataset_name}")

    def _infer_schema(
        self,
        index_info: IndexInfo,
        namespace_stats: NamespaceStats,
        dataset_name: str,
    ) -> Optional[SchemaMetadataClass]:
        """
        Infer schema from vector metadata.

        Args:
            index_info: Information about the index
            namespace_stats: Statistics for the namespace
            dataset_name: Name of the dataset

        Returns:
            SchemaMetadataClass or None if inference fails
        """
        try:
            # Sample vectors from the namespace
            logger.info(
                f"Sampling {self.config.schema_sampling_size} vectors "
                f"from {dataset_name} for schema inference"
            )

            vectors = self.client.sample_vectors(
                index_name=index_info.name,
                namespace=namespace_stats.name,
                limit=self.config.schema_sampling_size,
            )

            if not vectors:
                logger.info(
                    f"No vectors sampled from {dataset_name}, skipping schema inference"
                )
                return None

            # Check if any vectors have metadata
            vectors_with_metadata = [v for v in vectors if v.metadata]
            if not vectors_with_metadata:
                logger.info(f"No metadata found in sampled vectors from {dataset_name}")
                return None

            logger.info(
                f"Found {len(vectors_with_metadata)} vectors with metadata "
                f"out of {len(vectors)} sampled"
            )

            # Infer schema
            schema_metadata = self.schema_inferrer.infer_schema(
                vectors=vectors_with_metadata,
                dataset_name=dataset_name,
                platform=PLATFORM_NAME,
            )

            return schema_metadata

        except Exception as e:
            logger.error(
                f"Schema inference failed for {dataset_name}: {e}", exc_info=True
            )
            return None

    def get_report(self) -> PineconeSourceReport:
        """Get the ingestion report."""
        return self.report
