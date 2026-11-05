from typing import Any, Callable, Dict, FrozenSet, Iterable, Optional

from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException

from datahub.configuration.common import GraphError
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport, TestableSource
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import (
    CloudTrailLakeClient,
    EventBridgeClient,
    LambdaClient,
    PipesClient,
    S3NotificationClient,
)
from datahub.ingestion.source.aws_messaging.cloudtrail import ProducerLineage
from datahub.ingestion.source.aws_messaging.config import (
    AwsMessagingConfigBase,
    GlueSchemaRegistryConfigBase,
)
from datahub.ingestion.source.aws_messaging.constants import (
    CONSOLE_PATHS,
    EVENTBRIDGE_PIPES_PLATFORM,
    EVENTBRIDGE_PLATFORM,
    LAMBDA_PLATFORM,
    OBSERVED_PRODUCERS_PROPERTY,
    AwsService,
    JobEdgeKind,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.eventbridge import EventBridgeLineage
from datahub.ingestion.source.aws_messaging.jobs import JobEdgeTracker
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.lineage import (
    DatasetLineageCollector,
    SchemaLookup,
    identity_field_lineage,
)
from datahub.ingestion.source.aws_messaging.mapper import (
    AwsRegionKey,
    secondary,
)
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
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
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
        self.create_clients()

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
        self.lineage = DatasetLineageCollector(
            report=report,
            graph=ctx.graph if config.remove_stale_lineage else None,
            relationships=self.owned_relationships(),
        )
        self.schemas = SchemaLookup(ctx.graph, report)
        self.jobs = JobEdgeTracker(
            report=report,
            graph=ctx.graph if config.remove_stale_lineage else None,
            instance=config.platform_instance,
            region=self.region,
            platform=self.platform,
        )
        if not config.lineage.lambda_functions:
            self.jobs.mark_unknown(
                LAMBDA_PLATFORM, JobEdgeKind.CONSUMES, JobEdgeKind.PRODUCES
            )
        if not config.lineage.eventbridge_rules:
            self.jobs.mark_unknown(EVENTBRIDGE_PLATFORM, JobEdgeKind.PRODUCES)
        if not config.lineage.eventbridge_pipes:
            self.jobs.mark_unknown(
                EVENTBRIDGE_PIPES_PLATFORM, JobEdgeKind.CONSUMES, JobEdgeKind.PRODUCES
            )
        self.lambdas: Optional[LambdaLineage] = (
            LambdaLineage(
                config=config,
                report=report,
                client=LambdaClient(
                    config.make_client(self.session, AwsService.LAMBDA), report
                ),
                urns=self.urns,
                resolver=self.resolver,
                region_key=self.region_key(LAMBDA_PLATFORM),
                jobs=self.jobs,
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
            pipes_region_key=self.region_key(EVENTBRIDGE_PIPES_PLATFORM),
            jobs=self.jobs,
        )
        producers_config = config.cloudtrail_producers
        self.producers: Optional[ProducerLineage] = (
            ProducerLineage(
                config=producers_config,
                report=report,
                client=CloudTrailLakeClient(
                    config.make_client(
                        self.session,
                        AwsService.CLOUDTRAIL,
                        # Lake queries run in the event data store's own region.
                        region=self._event_data_store_region(),
                    ),
                    report,
                ),
                urns=self.urns,
                resolver=self.resolver,
                lambdas=self.lambdas,
            )
            if producers_config.enabled
            else None
        )

    def collect_producers(self) -> None:
        """CloudTrail producers. Runs before `service_lineage_workunits`, which
        emits the Lambda jobs this adds outputs to."""
        if self.producers is None:
            return
        if self.producers.query():
            self.producers.add_lambda_edges()
            return
        # Lambda outputs seen through CloudTrail are unknown this run.
        self.jobs.mark_unknown(LAMBDA_PLATFORM, JobEdgeKind.PRODUCES)
        if self.ctx.graph is None:
            self.base_report.warning(
                title="Observed producers not kept",
                message=(
                    "The CloudTrail query failed and there is no DataHub connection to "
                    "read the previous observed_producers, so they are cleared until "
                    "the next successful run."
                ),
            )

    def dataset_custom_properties(
        self, urn: str, properties: Dict[str, str]
    ) -> Dict[str, str]:
        """Adds observed_producers to this run's datasetProperties.

        The query runs before the first dataset is written, so the full write
        already holds this run's value. When it failed, nothing is known about
        who sends, so the value DataHub already holds is kept."""
        if self.producers is None:
            return properties
        if self.producers.query():
            observed = self.producers.observed_producers(urn)
        elif self.ctx.graph is not None:
            try:
                existing = self.ctx.graph.get_aspect(urn, DatasetPropertiesClass)
            except (GraphError, RequestException) as e:
                self.base_report.warning(
                    title="Previous observed producers not read",
                    message=(
                        "The CloudTrail query failed and the value already in "
                        "DataHub couldn't be read, so observed_producers is not "
                        "written for this dataset this run."
                    ),
                    context=urn,
                    exc=e,
                )
                existing = None
            observed = (
                (existing.customProperties or {}).get(OBSERVED_PRODUCERS_PROPERTY)
                if existing is not None
                else None
            )
        else:
            observed = None
        if not observed:
            return properties
        return {**properties, OBSERVED_PRODUCERS_PROPERTY: observed}

    def _event_data_store_region(self) -> Optional[str]:
        store = parse_arn(self.base_config.cloudtrail_producers.event_data_store)
        return store.region if store is not None and store.region else None

    def collect_s3_notifications(self) -> None:
        if not self.base_config.lineage.s3_notifications:
            return
        s3_lineage = S3NotificationLineage(
            client=S3NotificationClient(
                self.base_config.make_client(self.session, AwsService.S3),
                self.base_report,
            ),
            resolver=self.resolver,
            graph=self.ctx.graph,
            env=self.base_config.env,
            s3=self.base_config.platform_map.get("s3"),
            report=self.base_report,
            target_allowed=self.resource_allowed,
        )
        if s3_lineage.collect(self.lineage):
            # An S3 edge's upstream isn't this source's, so it can only be
            # checked from the queue or topic side.
            self.lineage.downstream_relationships[
                LineageRelationship.S3_NOTIFICATION.value
            ] = s3_lineage.can_retract

    def service_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Lambda functions, EventBridge rules and Pipes around this source's
        resources. Runs after every queue or topic is known."""
        if self.lambdas is not None:
            self.lambdas.collect()
        if self.base_config.lineage.eventbridge_rules:
            # The bus container may also come from the other source, so it is
            # not this source's to delete either.
            yield from self.with_region_container(
                secondary(self.eventbridge.rule_workunits()),
                EVENTBRIDGE_PLATFORM,
            )
        if self.base_config.lineage.eventbridge_pipes:
            yield from self.with_region_container(
                secondary(self.eventbridge.pipe_workunits()),
                EVENTBRIDGE_PIPES_PLATFORM,
            )
        # Lambda jobs go last: rules and pipes add themselves as upstream jobs.
        if self.lambdas is not None:
            yield from self.with_region_container(
                secondary(self.lambdas.workunits()), LAMBDA_PLATFORM
            )
        # Only datasets read in full: a topic whose subscriptions couldn't all be
        # listed may still have the Lambda subscriptions this run didn't see.
        yield from self.jobs.stale_workunits(self.lineage.complete)

    def owned_relationships(self) -> FrozenSet[LineageRelationship]:
        """Lineage relationships this source writes and reads in full each run.
        Stale edges of these types are retracted; edges the companion source
        writes are left alone."""
        raise NotImplementedError

    def create_clients(self) -> None:
        """Builds the AWS clients from self.session, once the region is known and
        before the account id is read through them."""
        raise NotImplementedError

    def add_identity_column_lineage(
        self, upstream_urn: str, downstream_urn: str
    ) -> None:
        """Field lineage for a hop that delivers the message unchanged, for fields
        in both schemas. Without a schema on both sides the edge stays table-level."""
        upstream_schema = self.schemas.get(upstream_urn)
        downstream_schema = self.schemas.get(downstream_urn)
        if upstream_schema is None or downstream_schema is None:
            self.base_report.column_lineage_skipped["schema missing on one side"] += 1
            self.lineage.fine_grained_unknown(
                upstream=upstream_urn, downstream=downstream_urn
            )
            return
        lineages = identity_field_lineage(
            upstream_urn=upstream_urn,
            upstream_schema=upstream_schema,
            downstream_urn=downstream_urn,
            downstream_schema=downstream_schema,
        )
        if not lineages:
            self.base_report.column_lineage_skipped["no fields in common"] += 1
        self.lineage.add_fine_grained(
            upstream=upstream_urn, downstream=downstream_urn, lineages=lineages
        )

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

    def glue_schema(
        self, registry: Optional[MessagingSchemaRegistry], name: str, urn: str
    ) -> Optional[SchemaMetadataClass]:
        """A queue or topic's Glue schema, remembered for column lineage."""
        if registry is None:
            return None
        schema = registry.get_schema_metadata(name)
        if schema is not None:
            self.schemas.fetched[urn] = schema
        elif name in registry.failed:
            self.schemas.unreadable.add(urn)
        return schema

    def resource_allowed(self, name: str) -> bool:
        """Whether the queue or topic pattern lets a resource of this name in."""
        raise NotImplementedError

    def first_resource_arn(self) -> Optional[str]:
        """One resource ARN of this service, used only to read the account id; None
        when there are no resources. Anything that doesn't parse as an ARN (an
        unrecognised SQS queue URL, say) is reported as an unreadable account."""
        raise NotImplementedError

    def _resolve_account_id(self) -> Optional[str]:
        # Read the account off a resource rather than sts:GetCallerIdentity: under
        # cross-account role assumption the caller's account is not the one that
        # owns the resources, and this needs no extra permission.
        try:
            resource = self.first_resource_arn()
        except (ClientError, BotoCoreError) as e:
            self._account_unresolved(aws_error_code(e), e)
            return None
        if resource is None:
            # No resources in this region, so nothing gets a URN either way.
            return None
        arn = parse_arn(resource)
        if arn is None:
            self._account_unresolved(f"unrecognised resource id {resource}")
            return None
        return arn.account

    def _account_unresolved(
        self, reason: str, exc: Optional[BaseException] = None
    ) -> None:
        if not self.base_config.platform_instance:
            # The account id becomes the platform_instance, so carrying on
            # without it would move every resource to a URN with no account
            # and duplicate the catalog. The listing needs the same permission,
            # so a failure here is transient or a broken setup, not a valid one.
            raise RuntimeError(
                "Could not read a resource ARN to find the AWS account id "
                f"({reason}). Re-run, or set platform_instance so URNs don't "
                "depend on it."
            ) from exc
        self.base_report.warning(
            title="Account id not resolved",
            message=(
                "Could not read a resource ARN to find the AWS account id; "
                "the configured platform_instance is used, and the account is "
                "taken from the first resource listed."
            ),
            context=reason,
            exc=exc,
        )

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
                key = self.region_key(platform)
                container = gen_containers(
                    container_key=key,
                    name=self.region,
                    sub_types=[GenericContainerSubTypes.AWS_REGION.value],
                    external_url=(
                        f"https://{self.region}.console.aws.amazon.com/"
                        f"{CONSOLE_PATHS[platform]}?region={self.region}"
                    ),
                )
                if platform == self.platform:
                    yield from container
                else:
                    # Both sources emit the lambda and eventbridge region
                    # containers, so neither may soft-delete them when its own
                    # run stops placing anything there. gen_containers already
                    # writes their status.
                    yield from secondary(container)
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
