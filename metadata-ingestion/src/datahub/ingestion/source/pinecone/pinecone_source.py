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
    DEFAULT_NAMESPACE,
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

logger = logging.getLogger(__name__)

PLATFORM_NAME = "pinecone"
PLATFORM_URN = make_data_platform_urn(PLATFORM_NAME)


class PineconeIndexKey(ContainerKey):
    index_name: str


class PineconeNamespaceKey(ContainerKey):
    index_name: str
    namespace: str


def _namespace_display_name(namespace: str) -> str:
    """Human-readable name for a namespace, handling the default namespace."""
    if not namespace or namespace == DEFAULT_NAMESPACE:
        return "(default)"
    return namespace


def _namespace_for_urn(namespace: str) -> str:
    """Stable namespace identifier for URNs, avoiding empty strings and trailing dots."""
    if not namespace or namespace == DEFAULT_NAMESPACE:
        return DEFAULT_NAMESPACE
    return namespace


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

    Extracts index metadata, namespace information, and container hierarchy
    (Index -> Namespace -> Dataset).
    """

    report: PineconeSourceReport

    def __init__(self, config: PineconeConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = PineconeSourceReport()
        self.client = PineconeClient(config)

        self.schema_inferrer: Optional[MetadataSchemaInferrer] = None
        if config.enable_schema_inference:
            self.schema_inferrer = MetadataSchemaInferrer(
                max_fields=config.max_metadata_fields
            )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "PineconeSource":
        config = PineconeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            indexes = self.client.list_indexes()
            logger.info(f"Discovered {len(indexes)} indexes")

            for index_info in indexes:
                if not self.config.index_pattern.allowed(index_info.name):
                    self.report.report_index_filtered(index_info.name)
                    continue

                self.report.report_index_scanned(index_info.name)

                try:
                    yield from self._generate_index_container(index_info)
                    yield from self._process_namespaces(index_info)
                except Exception as e:
                    logger.error(
                        f"Failed to process index {index_info.name}: {e}",
                        exc_info=True,
                    )
                    self.report.report_index_failed(index_info.name, str(e))

        except Exception as e:
            logger.error(f"Failed to list indexes: {e}", exc_info=True)
            raise

    def _make_index_key(self, index_name: str) -> PineconeIndexKey:
        return PineconeIndexKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            index_name=index_name,
        )

    def _make_namespace_key(
        self, index_name: str, namespace: str
    ) -> PineconeNamespaceKey:
        return PineconeNamespaceKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            index_name=index_name,
            namespace=_namespace_for_urn(namespace),
        )

    def _generate_index_container(
        self, index_info: IndexInfo
    ) -> Iterable[MetadataWorkUnit]:
        container_key = self._make_index_key(index_info.name)

        spec = index_info.spec
        index_type = "unknown"
        extra_properties: Dict[str, str] = {
            "dimension": str(index_info.dimension),
            "metric": index_info.metric,
            "index_type": index_type,
            "host": index_info.host,
            "status": index_info.status,
        }

        if "serverless" in spec:
            extra_properties["index_type"] = "serverless"
            serverless_spec = spec.get("serverless", {})
            if "cloud" in serverless_spec:
                extra_properties["cloud"] = serverless_spec["cloud"]
            if "region" in serverless_spec:
                extra_properties["region"] = serverless_spec["region"]
        elif "pod" in spec:
            extra_properties["index_type"] = "pod"
            pod_spec = spec.get("pod", {})
            if pod_spec.get("pod_type"):
                extra_properties["pod_type"] = pod_spec["pod_type"]
            if pod_spec.get("replicas"):
                extra_properties["replicas"] = str(pod_spec["replicas"])

        yield from gen_containers(
            container_key=container_key,
            name=index_info.name,
            sub_types=[DatasetContainerSubTypes.PINECONE_INDEX],
            description=(
                f"Pinecone {extra_properties['index_type']} index with "
                f"{index_info.dimension} dimensions using {index_info.metric} metric"
            ),
            extra_properties=extra_properties,
        )

    def _process_namespaces(self, index_info: IndexInfo) -> Iterable[MetadataWorkUnit]:
        try:
            namespaces = self.client.list_namespaces(index_info.name)

            for namespace_stats in namespaces:
                display_name = _namespace_display_name(namespace_stats.name)

                if not self.config.namespace_pattern.allowed(namespace_stats.name):
                    self.report.report_namespace_filtered(index_info.name, display_name)
                    continue

                self.report.report_namespace_scanned(index_info.name, display_name)

                try:
                    yield from self._generate_namespace_container(
                        index_info, namespace_stats
                    )
                    yield from self._generate_namespace_dataset(
                        index_info, namespace_stats
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to process namespace {index_info.name}/{display_name}: {e}",
                        exc_info=True,
                    )
                    self.report.report_namespace_failed(
                        index_info.name, display_name, str(e)
                    )

        except Exception as e:
            logger.error(
                f"Failed to list namespaces for index {index_info.name}: {e}",
                exc_info=True,
            )
            raise

    def _generate_namespace_container(
        self, index_info: IndexInfo, namespace_stats: NamespaceStats
    ) -> Iterable[MetadataWorkUnit]:
        namespace_container_key = self._make_namespace_key(
            index_info.name, namespace_stats.name
        )
        parent_container_key = self._make_index_key(index_info.name)

        yield from gen_containers(
            container_key=namespace_container_key,
            name=_namespace_display_name(namespace_stats.name),
            sub_types=[DatasetContainerSubTypes.PINECONE_NAMESPACE],
            parent_container_key=parent_container_key,
            description=(
                f"Namespace in Pinecone index {index_info.name} "
                f"containing {namespace_stats.vector_count} vectors"
            ),
            extra_properties={
                "vector_count": str(namespace_stats.vector_count),
                "index_name": index_info.name,
            },
        )

    def _generate_namespace_dataset(
        self, index_info: IndexInfo, namespace_stats: NamespaceStats
    ) -> Iterable[MetadataWorkUnit]:
        ns_urn_part = _namespace_for_urn(namespace_stats.name)
        dataset_name = f"{index_info.name}.{ns_urn_part}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=PLATFORM_NAME,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        namespace_container_key = self._make_namespace_key(
            index_info.name, namespace_stats.name
        )

        display_name = _namespace_display_name(namespace_stats.name)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=display_name,
                description=(
                    f"Vector collection in namespace '{display_name}' "
                    f"of index '{index_info.name}'"
                ),
                customProperties={
                    "vector_count": str(namespace_stats.vector_count),
                    "dimension": str(index_info.dimension),
                    "metric": index_info.metric,
                    "index_name": index_info.name,
                    "namespace": namespace_stats.name,
                },
            ),
        ).as_workunit()

        if self.schema_inferrer is not None and namespace_stats.vector_count > 0:
            try:
                schema_metadata = self._infer_schema(
                    index_info, namespace_stats, dataset_name
                )
                if schema_metadata:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=schema_metadata,
                    ).as_workunit()
            except Exception as e:
                logger.error(
                    f"Failed to infer schema for {dataset_name}: {e}", exc_info=True
                )
                self.report.report_schema_inference_failed(dataset_name, str(e))

        yield from add_dataset_to_container(
            container_key=namespace_container_key,
            dataset_urn=dataset_urn,
        )

        instance_urn = (
            make_dataplatform_instance_urn(PLATFORM_NAME, self.config.platform_instance)
            if self.config.platform_instance
            else None
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=PLATFORM_URN,
                instance=instance_urn,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=["Vector Collection"]),
        ).as_workunit()

        self.report.report_dataset_generated()

    def _infer_schema(
        self,
        index_info: IndexInfo,
        namespace_stats: NamespaceStats,
        dataset_name: str,
    ) -> Optional[SchemaMetadataClass]:
        assert self.schema_inferrer is not None

        vectors = self.client.sample_vectors(
            index_name=index_info.name,
            namespace=namespace_stats.name,
            limit=self.config.schema_sampling_size,
        )

        if not vectors:
            return None

        vectors_with_metadata = [v for v in vectors if v.metadata]
        if not vectors_with_metadata:
            return None

        return self.schema_inferrer.infer_schema(
            vectors=vectors_with_metadata,
            dataset_name=dataset_name,
            platform=PLATFORM_NAME,
        )

    def get_report(self) -> PineconeSourceReport:
        return self.report
