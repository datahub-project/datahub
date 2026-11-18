from typing import Any, ClassVar, Dict, Literal, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws_messaging.constants import (
    EVENT_DATA_STORE_PATTERN,
    AwsService,
)
from datahub.ingestion.source.kinesis.kinesis_config import DestinationPlatformDetail
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

# Platforms of datasets this source links to but doesn't emit.
LinkedPlatform = Literal["s3"]


class LinkedPlatformDetail(DestinationPlatformDetail):
    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "platform_instance of that platform's own recipe. If unset, the dataset "
            "URN has no platform_instance."
        ),
    )
    env: Optional[str] = Field(
        default=None,
        description="env of that platform's own recipe. If unset, inherits this recipe's env.",
    )
    convert_urns_to_lowercase: bool = Field(
        default=False,
        description=(
            "Set to that platform's recipe convert_urns_to_lowercase. Defaults to "
            "False, like the s3 source."
        ),
    )


class GlueSchemaRegistryConfigBase(ConfigModel):
    enabled: bool = Field(
        default=False,
        description=(
            "Attach schemas from AWS Glue Schema Registry. Needs glue:GetSchemaVersion. "
            "AWS stores no link between a queue or topic and a schema, so list them "
            "in the schema map."
        ),
    )
    registry_name: str = Field(
        default="default-registry",
        description="Glue Schema Registry registry to read schemas from.",
    )
    use_naming_convention: bool = Field(
        default=False,
        description=(
            "When the schema map has no entry, look for a schema with the same name "
            "as the queue or topic. Off by default: AWS defines no such convention."
        ),
    )

    def schema_map(self) -> Dict[str, str]:
        raise NotImplementedError

    @pydantic.model_validator(mode="after")
    def reject_configured_but_disabled(self) -> "GlueSchemaRegistryConfigBase":
        # A map or naming convention with enabled=False is silently ignored, which
        # is almost never what was meant.
        if not self.enabled and (self.schema_map() or self.use_naming_convention):
            raise ValueError(
                "glue_schema_registry has a schema map or use_naming_convention set but "
                "enabled=False. Set enabled=True, or remove those fields."
            )
        return self


class QueueGlueSchemaRegistryConfig(GlueSchemaRegistryConfigBase):
    queue_schema_map: Dict[str, str] = Field(
        default_factory=dict,
        description="Queue name to schema name in `registry_name`.",
    )

    def schema_map(self) -> Dict[str, str]:
        return self.queue_schema_map


class TopicGlueSchemaRegistryConfig(GlueSchemaRegistryConfigBase):
    topic_schema_map: Dict[str, str] = Field(
        default_factory=dict,
        description="Topic name to schema name in `registry_name`.",
    )

    def schema_map(self) -> Dict[str, str]:
        return self.topic_schema_map


class AwsMessagingLineageConfig(ConfigModel):
    lambda_functions: bool = Field(
        default=True,
        description=(
            "Emit Lambda functions wired to this source's queues or topics (event "
            "source mappings, SNS subscriptions, failure and async destinations) as a "
            "DataFlow with one DataJob."
        ),
    )
    lambda_async_destinations: bool = Field(
        default=False,
        description=(
            "Read each function's asynchronous invocation destinations. One "
            "lambda:ListFunctionEventInvokeConfigs call per function in the region, "
            "made by both the sqs and sns sources; stops at the first AccessDenied. "
            "Stale Lambda output edges are only removed when this is on, since "
            "without it the run can't tell which outputs still exist."
        ),
    )
    eventbridge_rules: bool = Field(
        default=True,
        description="Emit EventBridge rules that target this source's queues or topics.",
    )
    eventbridge_pipes: bool = Field(
        default=True,
        description="Emit EventBridge Pipes whose source or target is one of this source's queues or topics.",
    )
    s3_notifications: bool = Field(
        default=False,
        description=(
            "Add S3 datasets as upstreams of the queues or topics their bucket's "
            "event notifications target: every S3 dataset already in DataHub under "
            "the notification's bucket and key prefix, up to 100. Reads the "
            "notification configuration of each bucket in the recipe's region (one "
            "call per bucket); a source only links its own queues or topics. Needs "
            "a DataHub graph connection, and s3:ListAllMyBuckets plus "
            "s3:GetBucketNotification. ListAllMyBuckets only returns buckets the "
            "recipe's account owns."
        ),
    )

    @pydantic.model_validator(mode="after")
    def reject_async_destinations_without_functions(
        self,
    ) -> "AwsMessagingLineageConfig":
        # Async destinations are read by the Lambda collector, which isn't built
        # without lambda_functions, so the option would be silently ignored.
        if self.lambda_async_destinations and not self.lambda_functions:
            raise ValueError(
                "lineage.lambda_async_destinations needs lineage.lambda_functions. "
                "Set lambda_functions=True, or remove lambda_async_destinations."
            )
        return self


class CloudTrailProducersConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description=(
            "Find producers from CloudTrail Lake: who called SendMessage or Publish "
            "on this source's queues or topics. Needs data-event logging for SQS/SNS "
            "into an event data store. Lake queries are billed per GB scanned."
        ),
    )
    event_data_store: Optional[str] = Field(
        default=None,
        description=(
            "ARN or id of the CloudTrail Lake event data store to query. With an ARN "
            "the query runs in the store's region; with a bare id, in this recipe's "
            "region. A store in another account must be shared with this role through "
            "its resource policy."
        ),
    )
    lookback_days: int = Field(
        default=7,
        ge=1,
        description="How many days of events the producer query covers.",
    )
    query_timeout_seconds: int = Field(
        default=300,
        ge=10,
        description="How long to wait for the CloudTrail Lake query before giving up.",
    )

    @pydantic.model_validator(mode="after")
    def require_event_data_store(self) -> "CloudTrailProducersConfig":
        if self.enabled and not self.event_data_store:
            raise ValueError("cloudtrail_producers.enabled needs event_data_store.")
        if self.event_data_store and not EVENT_DATA_STORE_PATTERN.match(
            self.event_data_store
        ):
            raise ValueError(
                "cloudtrail_producers.event_data_store must be an event data store id "
                "or an arn:aws:cloudtrail:<region>:<account>:eventdatastore/<id> ARN."
            )
        return self


class AwsMessagingConfigBase(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Settings shared by the sqs and sns sources. Use the same env,
    platform_instance and account_platform_instance_map in both recipes so
    lineage between queues and topics joins."""

    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description=(
            "AWS connection config (credentials, region, role assumption, endpoint "
            "override). When omitted, boto3 resolves everything from the standard "
            "credential chain. One recipe covers one region."
        ),
    )
    extract_tags: bool = Field(
        default=True,
        description=(
            "Extract AWS resource tags as DataHub tags. SNS allows 10 "
            "ListTagsForResource calls per second, so turn this off on the sns "
            "source for accounts with tens of thousands of topics."
        ),
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default_factory=dict,
        description=(
            "Domain urn or id to regex patterns on resource names; a matching queue "
            "or topic is assigned that domain. Domain names are not resolved: use "
            "the domain's urn or id."
        ),
    )
    account_platform_instance_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "AWS account id to platform_instance, for lineage to resources in other "
            "accounts whose own recipes set a platform_instance other than the "
            "account id. Accounts not listed use their account id."
        ),
    )
    platform_map: Dict[LinkedPlatform, LinkedPlatformDetail] = Field(
        default_factory=dict,
        description=(
            "Per platform (`s3`), the platform_instance, env and "
            "convert_urns_to_lowercase of that platform's own recipe, so lineage "
            "finds the dataset URNs it emits. A platform not listed uses no "
            "platform_instance, this recipe's env and no lowercasing."
        ),
    )
    cloudtrail_producers: CloudTrailProducersConfig = Field(
        default_factory=CloudTrailProducersConfig,
        description="Producer lineage from CloudTrail Lake (off by default).",
    )
    lineage: AwsMessagingLineageConfig = Field(
        default_factory=AwsMessagingLineageConfig,
        description="Lineage through Lambda and EventBridge.",
    )
    remove_stale_lineage: bool = Field(
        default=True,
        description=(
            "Remove lineage edges this source wrote on an earlier run that no longer "
            "exist in AWS, such as a removed redrive policy or subscription. Only "
            "edges from resources this run read in full are removed. Needs a "
            "DataHub graph connection (the datahub-rest sink or datahub_api); costs "
            "one DataHub lookup per queue or topic."
        ),
    )
    glue_schema_registry: GlueSchemaRegistryConfigBase = Field(
        description="Message schemas from AWS Glue Schema Registry (off by default).",
    )
    include_column_lineage: Optional[bool] = Field(
        default=None,
        description=(
            "Add field-level lineage on hops that deliver the message unchanged or "
            "with a simple field mapping, for fields present in both schemas. See the "
            "docs for which hops qualify. Schemas come from Glue Schema Registry in "
            "this run or from DataHub, one DataHub lookup per hop for a schema not "
            "read from Glue. Defaults to glue_schema_registry.enabled; set it to true "
            "to use schemas that are already in DataHub without Glue."
        ),
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for stale entity removal.",
    )

    # The service aws_endpoint_url points at. Other clients this source builds
    # (tagging, Lambda, Glue, ...) keep the default endpoint, so a VPC endpoint
    # for one service doesn't redirect calls to the others.
    endpoint_service: ClassVar[AwsService]

    @pydantic.model_validator(mode="after")
    def _default_include_column_lineage(self) -> "AwsMessagingConfigBase":
        if self.include_column_lineage is None:
            self.include_column_lineage = self.glue_schema_registry.enabled
        return self

    def make_client(
        self, session: Any, service: AwsService, region: Optional[str] = None
    ) -> Any:
        # boto3 clients have no common static type; callers annotate the result.
        return session.client(
            service.value,
            region_name=region,
            endpoint_url=(
                self.aws_config.aws_endpoint_url
                if service == self.endpoint_service
                else None
            ),
            config=self.aws_config._aws_config(),
        )


class SqsSourceConfig(AwsMessagingConfigBase):
    endpoint_service: ClassVar[AwsService] = AwsService.SQS

    queue_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns on queue names to include.",
    )
    glue_schema_registry: QueueGlueSchemaRegistryConfig = Field(
        default_factory=QueueGlueSchemaRegistryConfig,
        description="Queue schemas from AWS Glue Schema Registry (off by default).",
    )


class SnsDataProtectionConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description=(
            "Read each topic's message data protection policy "
            "(sns:GetDataProtectionPolicy, one call per topic) and tag the topic with "
            "the data identifiers it covers."
        ),
    )
    tag_prefix: str = Field(
        default="aws_data_identifier",
        description="Tags are named `<tag_prefix>:<identifier>`, e.g. `aws_data_identifier:EmailAddress`.",
    )
    glossary_term_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Data identifier name (e.g. `EmailAddress`) to an existing glossary term "
            "urn or name. Only listed identifiers get a term; none are created."
        ),
    )


class SnsSourceConfig(AwsMessagingConfigBase):
    endpoint_service: ClassVar[AwsService] = AwsService.SNS

    topic_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns on topic names to include.",
    )
    include_subscription_attributes: bool = Field(
        default=False,
        description=(
            "Call sns:GetSubscriptionAttributes for each subscription to read its "
            "dead-letter queue. One call per subscription, for every protocol, on "
            "every run; subscription-to-queue edges need no extra calls."
        ),
    )
    glue_schema_registry: TopicGlueSchemaRegistryConfig = Field(
        default_factory=TopicGlueSchemaRegistryConfig,
        description="Topic schemas from AWS Glue Schema Registry (off by default).",
    )
    data_protection: SnsDataProtectionConfig = Field(
        default_factory=SnsDataProtectionConfig,
        description="SNS message data protection policies as tags or glossary terms.",
    )
