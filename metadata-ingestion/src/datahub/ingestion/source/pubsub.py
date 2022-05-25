from datetime import datetime
from functools import lru_cache
from typing import Dict, Iterable, Optional
import dateutil.parser as dp
import requests
from pydantic import validator
from pydantic.fields import Field, List
from requests.models import HTTPError
from sqllineage.runner import LineageRunner
import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.source_common import PlatformSourceConfigBase
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    DatasetSnapshot
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    SubTypesClass,
    DataPlatformInstanceClass,
    DataPlatformInfoClass,
    DataPlatformSnapshotClass,
    UpstreamLineageClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    UpstreamClass,
    DatasetLineageTypeClass,
)
from datahub.utilities import config_clean
from google.cloud import pubsub_v1
from google.auth import jwt


class GoogleCloudCredential(ConfigModel):
    project_id: str = Field(description="Project id to set the credentials")
    private_key_id: str = Field(description="Private key id")
    private_key: str = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str =Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(
        default="service_account", description="Authentication type"
    )
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )


class PubSubSourceConfig(ConfigModel):
    projects: List[str] = Field(description="Google Cloud Project id")
    credential: Optional[GoogleCloudCredential] = Field( default=None,
        description="BigQuery credential informations"
    )

@platform_name("Pub/Sub")
@config_class(PubSubSourceConfig)
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class PubSubSource(Source):
    """
    This plugin extracts Charts, dashboards, and associated metadata. This plugin is in beta and has only been tested
    on PostgreSQL and H2 database.
    ### Dashboard

    [/api/dashboard](https://www.metabase.com/docs/latest/api-documentation.html#dashboard) endpoint is used to
    retrieve the following dashboard information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the dashboard in Metabase
    - Associated charts

    ### Chart

    [/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint is used to
    retrieve the following information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the chart in Metabase
    - Datasource and lineage

    The following properties for a chart are ingested in DataHub.

    | Name          | Description                                     |
    | ------------- | ----------------------------------------------- |
    | `Dimensions`  | Column names                                    |
    | `Filters`     | Any filters applied to the chart                |
    | `Metrics`     | All columns that are being used for aggregation |


    """

    config: PubSubSourceConfig
    report: SourceReport
    platform:str = "pubsub"
    audience_susbcriber:str = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
    audience_publisher: str = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"

    def __hash__(self):
        return id(self)

    def __init__(self, config: PubSubSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.publisher_client = pubsub_v1.PublisherClient(credentials=jwt.Credentials.from_service_account_info(
            self.config.credential.dict()
            ,audience=self.audience_publisher
        ))
        self.subscriber_client = pubsub_v1.SubscriberClient(credentials=jwt.Credentials.from_service_account_info(
            self.config.credential.dict()
            ,audience=self.audience_susbcriber
        ))

    def construct_subscription(self, subscription: pubsub_v1.types.Subscription):
        subscription_urn = builder.make_dataset_urn(self.platform, subscription.name)
        project_id = subscription.name.split("/")[1]
        subscription_name = subscription.name.split("/")[-1]
        dataset_snapshot = DatasetSnapshot(
            urn=subscription_urn,
            aspects=[]
        )
        properties = DatasetPropertiesClass(
            customProperties={
                "message_retention_duration": str(subscription.message_retention_duration).split(",")[0],
                "ack_deadline_seconds": str(subscription.ack_deadline_seconds),
                "topic": subscription.topic.split("/")[-1],
                "detached": str(subscription.detached).lower(),
                "filter": subscription.filter.replace("\n"," ").strip(),
                "enable_message_ordering": str(subscription.enable_message_ordering).lower()
            },
            name=subscription_name,
            externalUrl=f"https://console.cloud.google.com/cloudpubsub/subscription/detail/{subscription_name}?project={project_id}"
        )
        dataset_snapshot.aspects.append(properties)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=subscription_urn, mce=mce)
        self.report.report_workunit(wu)
        yield wu

        subtype_wu = MetadataWorkUnit(
            id=f"{subscription_urn}-subtype",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=subscription_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["subscription"]),
            ),
        )
        self.report.report_workunit(subtype_wu)
        yield subtype_wu

        upstream_lineage = UpstreamLineageClass(upstreams=[UpstreamClass(
                dataset=builder.make_dataset_urn(self.platform, subscription.topic),
                type=DatasetLineageTypeClass.COPY,
            )]
        )
        upstream_wu = MetadataWorkUnit(
            id=f"{subscription_urn}-upstream",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=subscription_urn,
                aspectName="upstreamLineage",
                aspect=upstream_lineage,
            ),
        )
        self.report.report_workunit(upstream_wu)
        yield upstream_wu

    def construct_topic(self, topic: pubsub_v1.types.Topic):
        topic_urn = builder.make_dataset_urn(self.platform, topic.name)
        project_id = topic.name.split("/")[1]
        topic_name = topic.name.split("/")[-1]
        dataset_snapshot = DatasetSnapshot(
            urn=topic_urn,
            aspects=[]
        )

        properties = DatasetPropertiesClass(
            customProperties={},
            name=topic_name,
            externalUrl=f"https://console.cloud.google.com/cloudpubsub/topic/detail/{topic_name}?project={project_id}"
        )
        dataset_snapshot.aspects.append(properties)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=topic_urn, mce=mce)
        self.report.report_workunit(wu)
        yield wu

        subtype_wu = MetadataWorkUnit(
            id=f"{topic_urn}-subtype",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=topic_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["topic"]),
            ),
        )
        self.report.report_workunit(subtype_wu)
        yield subtype_wu

    def emit_platform_mce(self):
        pub_sub_platform = DataPlatformInfoClass(
            name="pubsub",
            type="MESSAGE_BROKER",
            datasetNameDelimiter="/",
            displayName="Pub/Sub",
            logoUrl="https://seeklogo.com/images/G/google-cloud-pub-sub-logo-B9E569CDE6-seeklogo.com.png"
        )
        urn = builder.make_data_platform_urn("pubsub")
        pub_sub_platform_snapshot = DataPlatformSnapshotClass(
            urn=urn,
            aspects=[pub_sub_platform]
        )
        mce = MetadataChangeEvent(proposedSnapshot=pub_sub_platform_snapshot)
        wu = MetadataWorkUnit(id=urn, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def emit_subscription_mces(self) -> Iterable[MetadataWorkUnit]:
        for project_id in self.config.projects:
            project_subscriptions = self.subscriber_client.list_subscriptions(project=f"projects/{project_id}")
            for subscription in project_subscriptions:
                yield from self.construct_subscription(subscription)

    def emit_topic_mces(self) -> Iterable[MetadataWorkUnit]:
        for project_id in self.config.projects:
            project_topics = self.publisher_client.list_topics(project=f"projects/{project_id}")
            for topic in project_topics:
                yield from self.construct_topic(topic)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = PubSubSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_platform_mce()
        yield from self.emit_topic_mces()
        yield from self.emit_subscription_mces()

    def get_report(self) -> SourceReport:
        return self.report
