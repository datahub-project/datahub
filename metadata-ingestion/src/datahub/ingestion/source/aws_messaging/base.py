from typing import Any, Callable, Iterable, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport, TestableSource
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.config import (
    AwsMessagingConfigBase,
    GlueSchemaRegistryConfigBase,
)
from datahub.ingestion.source.aws_messaging.constants import AwsService
from datahub.ingestion.source.aws_messaging.lineage import (
    DatasetLineageCollector,
    SchemaLookup,
)
from datahub.ingestion.source.aws_messaging.mapper import AwsRegionKey
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
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
