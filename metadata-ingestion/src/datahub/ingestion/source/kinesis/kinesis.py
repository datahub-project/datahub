from typing import Iterable

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.kinesis.kinesis_client import KinesisClient
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
)


@platform_name("Kinesis")
@config_class(KinesisSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Not supported")
class KinesisSource(StatefulIngestionSourceBase, TestableSource):
    """
    This connector extracts metadata from AWS Kinesis.
    """

    def __init__(self, config: KinesisSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.client = KinesisClient(config.connection, config.region_name)
        self.report = SourceReport()

    @classmethod
    def test_connection(cls, config_dict: dict) -> TestConnectionReport:
        config = KinesisSourceConfig.parse_obj(config_dict)
        report = TestConnectionReport()
        try:
            client = KinesisClient(config.connection, config.region_name)
            # Try listing one stream implies connection works
            # We iterate just once to verify API access
            for _ in client.list_streams():
                break
            report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return report

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "KinesisSource":
        config = KinesisSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        DataHub Source Wrapper
        """
        region = self.client.region_name or "unknown-region"

        # 1. Emit Region Container
        region_container_key = ContainerKey(
            platform="kinesis",
            instance=self.config.platform_instance,
            env=self.config.env,
            container_path=[region],
        )

        yield from gen_containers(
            container_key=region_container_key,
            name=region,
            sub_types=["Region"],
            description=f"AWS Region {region}",
        )

        # 2. Iterate Streams
        for stream_name in self.client.list_streams():
            if not self.config.stream_pattern.allowed(stream_name):
                continue

            # Fetch details
            stream_details = self.client.describe_stream(stream_name)
            if not stream_details:
                continue

            # Fetch tags
            tags = self.client.list_tags_for_stream(stream_name)

            # Emit Dataset
            dataset_urn = make_dataset_urn_with_platform_instance(
                platform="kinesis",
                name=stream_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            # Dataset Properties
            custom_props = {
                "StreamARN": stream_details.get("StreamARN"),
                "StreamStatus": stream_details.get("StreamStatus"),
                "RetentionPeriodHours": str(stream_details.get("RetentionPeriodHours")),
                "EncryptionType": stream_details.get("EncryptionType"),
            }
            # Add shard count
            shards = stream_details.get("Shards", [])
            custom_props["ShardCount"] = str(len(shards))
            # Open/Closed shards logic (implied by SequenceNumberRange)
            # A closed shard has an EndingSequenceNumber.
            open_shards = [
                s
                for s in shards
                if "EndingSequenceNumber" not in s.get("SequenceNumberRange", {})
            ]
            custom_props["OpenShardCount"] = str(len(open_shards))

            dataset_props = DatasetPropertiesClass(
                description=f"Kinesis Stream {stream_name}",
                customProperties={
                    k: str(v) for k, v in custom_props.items() if v is not None
                },
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=dataset_props,
            ).as_workunit()

            # Global Tags
            if tags:
                tag_aspect = GlobalTagsClass(
                    tags=[
                        TagAssociationClass(
                            tag=make_tag_urn(f"{t['Key']}:{t.get('Value', '')}")
                        )
                        for t in tags
                    ]
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=tag_aspect,
                ).as_workunit()

            # SubTypes
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.TOPIC]),
            ).as_workunit()

            # DataPlatformInstance
            if self.config.platform_instance:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=make_data_platform_urn("kinesis"),
                        instance=make_dataplatform_instance_urn(
                            "kinesis", self.config.platform_instance
                        ),
                    ),
                ).as_workunit()

            # Link to Container
            yield from add_dataset_to_container(
                container_key=region_container_key,
                dataset_urn=dataset_urn,
            )

    def get_report(self):
        return self.report
