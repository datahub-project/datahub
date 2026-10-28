from typing import Any, Callable, Iterable, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import (
    EventBridgeClient,
    LambdaClient,
    PipesClient,
    SnsClient,
    SqsClient,
)
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    EVENTBRIDGE_PLATFORM,
    SNS_PLATFORM,
    SQS_PLATFORM,
    AwsService,
    QueueAttribute,
)
from datahub.ingestion.source.aws_messaging.eventbridge import EventBridgeLineage
from datahub.ingestion.source.aws_messaging.firehose import FirehoseLineage
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.mapper import AwsRegionKey
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.aws_messaging.sns import SnsExtractor
from datahub.ingestion.source.aws_messaging.sqs import SqsExtractor
from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)


@platform_name("Amazon SQS", id=SQS_PLATFORM)
@config_class(AwsMessagingSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.CONTAINERS, "Region containers")
@capability(SourceCapability.TAGS, "From AWS resource tags")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "SNS subscriptions, dead-letter queues, Lambda, EventBridge rules and Pipes",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Defaults to the AWS account id")
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.SCHEMA_METADATA, "SQS and SNS have no schema", supported=False
)
@capability(
    SourceCapability.DESCRIPTIONS, "SQS and SNS have no description", supported=False
)
class AwsMessagingSource(StatefulIngestionSourceBase, TestableSource):
    """Catalogs Amazon SQS queues and Amazon SNS topics as datasets, one region
    per recipe, under the `sqs` and `sns` platforms, with lineage through SNS
    subscriptions, dead-letter queues, Lambda, EventBridge rules and Pipes."""

    config: AwsMessagingSourceConfig
    report: AwsMessagingSourceReport

    def __init__(self, config: AwsMessagingSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = AwsMessagingSourceReport()
        self._session = config.aws_config.get_session()
        if not config.aws_config.aws_region:
            config.aws_config.aws_region = self._session.region_name
        if not config.aws_config.aws_region:
            raise ValueError(
                "AWS region could not be resolved. Set aws_config.aws_region or "
                "AWS_REGION, or configure a default region in your AWS profile."
            )
        self.region: str = config.aws_config.aws_region
        self.report.region = self.region

        self.sqs = SqsClient(
            config.make_client(self._session, AwsService.SQS), self.report
        )
        self.sns = SnsClient(
            config.make_client(self._session, AwsService.SNS), self.report
        )

        account_id = self._resolve_account_id()
        self.report.account_id = account_id
        if not config.platform_instance and account_id:
            config.platform_instance = account_id
        self.urns = UrnBuilder(
            env=config.env,
            account_id=account_id,
            platform_instance=config.platform_instance,
            account_platform_instance_map=config.account_platform_instance_map,
        )
        self.lineage = DatasetLineageCollector(self.report)
        self.resolver = ResourceResolver(urns=self.urns, region=self.region)
        self.sqs_extractor = SqsExtractor(
            config=config,
            report=self.report,
            client=self.sqs,
            urns=self.urns,
            region_key=self._region_key(SQS_PLATFORM),
        )
        self.sns_extractor = SnsExtractor(
            config=config,
            report=self.report,
            client=self.sns,
            urns=self.urns,
            region_key=self._region_key(SNS_PLATFORM),
        )
        lineage_config = config.lineage
        self.lambdas: Optional[LambdaLineage] = (
            LambdaLineage(
                config=config,
                report=self.report,
                client=LambdaClient(
                    config.make_client(self._session, AwsService.LAMBDA), self.report
                ),
                urns=self.urns,
                resolver=self.resolver,
            )
            if lineage_config.lambda_functions
            else None
        )
        self.eventbridge = EventBridgeLineage(
            config=config,
            report=self.report,
            events=EventBridgeClient(
                config.make_client(self._session, AwsService.EVENTS), self.report
            ),
            pipes=PipesClient(
                config.make_client(self._session, AwsService.PIPES), self.report
            ),
            urns=self.urns,
            resolver=self.resolver,
            lambdas=self.lambdas,
            region_key=self._region_key(EVENTBRIDGE_PLATFORM),
        )
        self.firehose: Optional[FirehoseLineage] = (
            FirehoseLineage(
                env=config.env,
                urns=self.urns,
                kinesis_platform_instance=config.kinesis_platform_instance,
                graph=ctx.graph,
                report=self.report,
            )
            if lineage_config.firehose
            else None
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AwsMessagingSource":
        config = AwsMessagingSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Every queue and topic must be known before any lineage is resolved,
        # since an edge to a local queue or topic is only kept if it was ingested.
        if self.config.include_sqs:
            yield from self._sqs_workunits()
        if self.config.include_sns:
            yield from self._sns_workunits()
        for queue in self.sqs_extractor.queues:
            self.sqs_extractor.add_lineage(queue, self.lineage, self.resolver)
        for topic in self.sns_extractor.topics:
            self.sns_extractor.add_lineage(
                topic,
                lineage=self.lineage,
                resolver=self.resolver,
                lambdas=self.lambdas,
                firehose=self.firehose,
            )
        if self.lambdas is not None:
            self.lambdas.collect()
        if self.config.lineage.eventbridge_rules:
            yield from self._with_region_container(
                EVENTBRIDGE_PLATFORM, self.eventbridge.rule_workunits()
            )
        if self.config.lineage.eventbridge_pipes:
            yield from self.eventbridge.pipe_workunits()
        # Lambda jobs go last: rules and pipes add themselves as upstream jobs.
        if self.lambdas is not None:
            yield from self.lambdas.workunits()
        if self.firehose is not None:
            yield from self.firehose.workunits()
        yield from self.lineage.workunits()

    def _sqs_workunits(self) -> Iterable[MetadataWorkUnit]:
        for queue in self.sqs_extractor.list_queues():
            if not self.sqs_extractor.queues:
                yield from self._region_container(SQS_PLATFORM)
            self.sqs_extractor.queues.append(queue)
            self.resolver.queue_arns.add(str(queue.arn))
            yield from self.sqs_extractor.to_workunits(queue)

    def _sns_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.sns_extractor.load_subscriptions()
        for topic in self.sns_extractor.list_topics():
            if not self.sns_extractor.topics:
                yield from self._region_container(SNS_PLATFORM)
            self.sns_extractor.topics.append(topic)
            self.resolver.topic_arns.add(str(topic.arn))
            yield from self.sns_extractor.to_workunits(topic)

    def _with_region_container(
        self, platform: str, workunits: Iterable[MetadataWorkUnit]
    ) -> Iterable[MetadataWorkUnit]:
        # Only emit the region container once something is placed in it.
        first = True
        for workunit in workunits:
            if first:
                yield from self._region_container(platform)
                first = False
            yield workunit

    def _region_key(self, platform: str) -> AwsRegionKey:
        return AwsRegionKey(
            region=self.region,
            platform=platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _region_container(self, platform: str) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._region_key(platform),
            name=self.region,
            sub_types=[GenericContainerSubTypes.AWS_REGION.value],
            external_url=(
                f"https://{self.region}.console.aws.amazon.com/{platform}/v3/home"
                f"?region={self.region}"
            ),
        )

    def _resolve_account_id(self) -> Optional[str]:
        # Read the account off a resource rather than sts:GetCallerIdentity: under
        # cross-account role assumption the caller's account is not the one that
        # owns the resources, and this needs no extra permission.
        try:
            if self.config.include_sns:
                arn = parse_arn(self.sns.first_topic_arn())
                if arn is not None:
                    return arn.account
            if self.config.include_sqs:
                url = self.sqs.first_queue_url()
                if url is not None:
                    attributes = self.sqs.get_attributes(url) or {}
                    arn = parse_arn(attributes.get(QueueAttribute.QUEUE_ARN))
                    if arn is not None:
                        return arn.account
        except (ClientError, BotoCoreError) as e:
            self.report.warning(
                title="Account id not resolved",
                message=(
                    "Could not read a resource ARN to find the AWS account id. URNs "
                    "will not include it; set platform_instance to tell accounts apart."
                ),
                context=aws_error_code(e),
                exc=e,
            )
        return None

    def get_report(self) -> AwsMessagingSourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            config = AwsMessagingSourceConfig.model_validate(config_dict)
            session = config.aws_config.get_session()

            def probe(
                service: AwsService, call: Callable[[Any], Any], label: str
            ) -> CapabilityReport:
                try:
                    call(config.make_client(session, service))
                    return CapabilityReport(capable=True)
                except (ClientError, BotoCoreError) as e:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=f"{label} failed: {aws_error_code(e)}",
                    )

            checks = []
            if config.include_sqs:
                checks.append(
                    probe(
                        AwsService.SQS,
                        lambda c: c.list_queues(MaxResults=1),
                        "sqs:ListQueues",
                    )
                )
            if config.include_sns:
                checks.append(
                    probe(AwsService.SNS, lambda c: c.list_topics(), "sns:ListTopics")
                )
            failed = [check for check in checks if not check.capable]
            report.basic_connectivity = (
                failed[0] if failed else CapabilityReport(capable=True)
            )
        except (
            Exception
        ) as e:  # test_connection reports every failure, config errors included
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{type(e).__name__}: {e}"
            )
        return report
