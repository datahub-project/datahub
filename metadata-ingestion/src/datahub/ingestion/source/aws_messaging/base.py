from typing import Any, Callable, Iterable, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport, TestableSource
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import (
    EventBridgeClient,
    LambdaClient,
    PipesClient,
    S3NotificationClient,
)
from datahub.ingestion.source.aws_messaging.config import (
    AwsMessagingConfigBase,
    GlueSchemaRegistryConfigBase,
)
from datahub.ingestion.source.aws_messaging.constants import (
    EVENTBRIDGE_PLATFORM,
    AwsService,
)
from datahub.ingestion.source.aws_messaging.eventbridge import EventBridgeLineage
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.lineage import (
    DatasetLineageCollector,
    SchemaLookup,
    identity_field_lineage,
)
from datahub.ingestion.source.aws_messaging.mapper import AwsRegionKey, secondary
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.aws_messaging.s3_notifications import (
    S3NotificationLineage,
)
from datahub.ingestion.source.aws_messaging.schema_registry import (
    MessagingSchemaRegistry,
)
from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)


class AwsMessagingSourceBase(StatefulIngestionSourceBase, TestableSource):
    """Region, account and URN handling shared by the sqs and sns sources."""

    platform: str
    service: AwsService

    def __init__(
        self,
        config: AwsMessagingConfigBase,
        ctx: PipelineContext,
        report: AwsMessagingReportBase,
    ):
        super().__init__(config, ctx)
        self.base_config = config
        self.base_report = report
        self.session = config.aws_config.get_session()
        if not config.aws_config.aws_region:
            config.aws_config.aws_region = self.session.region_name
        if not config.aws_config.aws_region:
            raise ValueError(
                "AWS region could not be resolved. Set aws_config.aws_region or "
                "AWS_REGION, or configure a default region in your AWS profile."
            )
        self.region: str = config.aws_config.aws_region
        report.region = self.region

        account_id = self._resolve_account_id()
        report.account_id = account_id
        if not config.platform_instance and account_id:
            config.platform_instance = account_id
        self.urns = UrnBuilder(
            env=config.env,
            account_id=account_id,
            platform_instance=config.platform_instance,
            account_platform_instance_map=config.account_platform_instance_map,
        )
        self.resolver = ResourceResolver(
            urns=self.urns, region=self.region, service=self.service
        )
        self.lineage = DatasetLineageCollector(report)
        self.schemas = SchemaLookup(ctx.graph)
        self.lambdas: Optional[LambdaLineage] = (
            LambdaLineage(
                config=config,
                report=report,
                client=LambdaClient(
                    config.make_client(self.session, AwsService.LAMBDA), report
                ),
                urns=self.urns,
                resolver=self.resolver,
            )
            if config.lineage.lambda_functions
            else None
        )
        self.eventbridge = EventBridgeLineage(
            config=config,
            report=report,
            events=EventBridgeClient(
                config.make_client(self.session, AwsService.EVENTS), report
            ),
            pipes=PipesClient(
                config.make_client(self.session, AwsService.PIPES), report
            ),
            urns=self.urns,
            resolver=self.resolver,
            schemas=self.schemas,
            lambdas=self.lambdas,
            region_key=self.region_key(EVENTBRIDGE_PLATFORM),
        )

    def collect_s3_notifications(self) -> None:
        if not self.base_config.lineage.s3_notifications:
            return
        S3NotificationLineage(
            client=S3NotificationClient(
                self.base_config.make_client(self.session, AwsService.S3),
                self.base_report,
            ),
            resolver=self.resolver,
            graph=self.ctx.graph,
            env=self.base_config.env,
            s3_platform_instance=self.base_config.s3_platform_instance,
            s3_convert_urns_to_lowercase=self.base_config.s3_convert_urns_to_lowercase,
            report=self.base_report,
        ).collect(self.lineage)

    def service_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Lambda functions, EventBridge rules and Pipes around this source's
        resources. Runs after every queue or topic is known."""
        if self.lambdas is not None:
            self.lambdas.collect()
        if self.base_config.lineage.eventbridge_rules:
            # The bus container may also come from the other source, so it is
            # not this source's to delete either.
            yield from secondary(
                self.with_region_container(
                    self.eventbridge.rule_workunits(), EVENTBRIDGE_PLATFORM
                )
            )
        if self.base_config.lineage.eventbridge_pipes:
            yield from self.eventbridge.pipe_workunits()
        # Lambda jobs go last: rules and pipes add themselves as upstream jobs.
        if self.lambdas is not None:
            yield from self.lambdas.workunits()

    def add_identity_column_lineage(
        self, upstream_urn: str, downstream_urn: str
    ) -> None:
        """Field lineage for a hop that delivers the message unchanged, for fields
        in both schemas. Without a schema on both sides the edge stays table-level."""
        upstream_schema = self.schemas.get(upstream_urn)
        downstream_schema = self.schemas.get(downstream_urn)
        if upstream_schema is None or downstream_schema is None:
            self.base_report.column_lineage_skipped["schema missing on one side"] += 1
            return
        lineages = identity_field_lineage(
            upstream_urn=upstream_urn,
            upstream_schema=upstream_schema,
            downstream_urn=downstream_urn,
            downstream_schema=downstream_schema,
        )
        if not lineages:
            self.base_report.column_lineage_skipped["no fields in common"] += 1
            return
        self.lineage.add_fine_grained(downstream_urn, lineages)

    def schema_registry(
        self, config: GlueSchemaRegistryConfigBase
    ) -> Optional[MessagingSchemaRegistry]:
        if not config.enabled:
            return None
        return MessagingSchemaRegistry(
            config=config,
            glue_client=self.base_config.make_client(self.session, AwsService.GLUE),
            platform=self.platform,
            report=self.base_report,
        )

    def first_resource_arn(self) -> Optional[str]:
        """One ARN of this service, used only to read the account id."""
        raise NotImplementedError

    def _resolve_account_id(self) -> Optional[str]:
        # Read the account off a resource rather than sts:GetCallerIdentity: under
        # cross-account role assumption the caller's account is not the one that
        # owns the resources, and this needs no extra permission.
        try:
            arn = parse_arn(self.first_resource_arn())
        except (ClientError, BotoCoreError) as e:
            self.base_report.warning(
                title="Account id not resolved",
                message=(
                    "Could not read a resource ARN to find the AWS account id. URNs "
                    "will not include it; set platform_instance to tell accounts apart."
                ),
                context=aws_error_code(e),
                exc=e,
            )
            return None
        return arn.account if arn is not None else None

    def region_key(self, platform: Optional[str] = None) -> AwsRegionKey:
        return AwsRegionKey(
            region=self.region,
            platform=platform or self.platform,
            instance=self.base_config.platform_instance,
            env=self.base_config.env,
        )

    def with_region_container(
        self, workunits: Iterable[MetadataWorkUnit], platform: Optional[str] = None
    ) -> Iterable[MetadataWorkUnit]:
        # Only emit the region container once something is placed in it.
        platform = platform or self.platform
        first = True
        for workunit in workunits:
            if first:
                yield from gen_containers(
                    container_key=self.region_key(platform),
                    name=self.region,
                    sub_types=[GenericContainerSubTypes.AWS_REGION.value],
                    external_url=(
                        f"https://{self.region}.console.aws.amazon.com/{platform}/v3/home"
                        f"?region={self.region}"
                    ),
                )
                first = False
            yield workunit


def probe(
    config: AwsMessagingConfigBase,
    session: Any,
    service: AwsService,
    call: Callable[[Any], Any],
    label: str,
) -> CapabilityReport:
    try:
        call(config.make_client(session, service))
        return CapabilityReport(capable=True)
    except (ClientError, BotoCoreError) as e:
        return CapabilityReport(
            capable=False, failure_reason=f"{label} failed: {aws_error_code(e)}"
        )
