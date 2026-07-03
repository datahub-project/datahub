import logging
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
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
from datahub.ingestion.source.common.subtypes import GenericContainerSubTypes
from datahub.ingestion.source.kinesis.kinesis_config import (
    PLATFORM_NAME,
    AwsService,
    KinesisSourceConfig,
)
from datahub.ingestion.source.kinesis.kinesis_firehose import KinesisFirehoseExtractor
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_schema_registry import (
    KinesisGlueSchemaRegistry,
)
from datahub.ingestion.source.kinesis.kinesis_stream import KinesisStreamExtractor
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

logger = logging.getLogger(__name__)


class KinesisRegionKey(ContainerKey):
    """Container key for an AWS region; one Region container per recipe."""

    region: str


@platform_name("Amazon Kinesis Data Streams", id=PLATFORM_NAME)
@config_class(KinesisSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Region containers")
@capability(SourceCapability.LINEAGE_COARSE, "Firehose -> destination lineage")
@capability(SourceCapability.TAGS, "From AWS resource tags")
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Opt-in via `glue_schema_registry.enabled` (AWS Glue Schema Registry)",
)
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
class KinesisSource(StatefulIngestionSourceBase, TestableSource):
    """DataHub ingestion source for AWS Kinesis.

    Catalogs **Kinesis Data Streams (KDS)** as DataHub Datasets (subtype
    ``Stream``) and **Amazon Data Firehose** streams as DataFlows (subtype
    ``Firehose Stream``), each containing a single ``Delivery`` DataJob.
    AWS resource tags become DataHub ``globalTags`` (ownership can be derived
    from those tags with the ``extract_ownership_from_tags`` transformer).
    Optionally,
    schemas registered in AWS Glue Schema Registry are attached to streams
    via the ``schemaMetadata`` aspect (Avro, JSON, and Protobuf are
    supported).

    Firehose lineage is emitted as ``dataJobInputOutput`` edges from the
    source Kinesis stream to the destination platform. Six destinations
    are supported in V1: **S3, Redshift, OpenSearch/Elasticsearch,
    Snowflake, Apache Iceberg, and MongoDB**. Other destinations
    (HTTP, Datadog, Splunk, New Relic, etc.) result in a DataJob without
    an output edge, plus a warning.

    The source is region-scoped: one recipe per AWS region. The region is
    encoded in dataset names and the Firehose DataFlow id, so multiple
    regions of the same account share one ``platform_instance`` (the
    account ID by default) without colliding on URN. Authentication uses
    the standard ``AwsConnectionConfig`` (env vars, shared credentials, IAM
    role, SSO profile). Required IAM read permissions:
    ``kinesis:ListStreams``, ``kinesis:DescribeStream``,
    ``kinesis:ListTagsForStream``, ``firehose:ListDeliveryStreams``,
    ``firehose:DescribeDeliveryStream``,
    ``firehose:ListTagsForDeliveryStream``, and (when GSR is enabled)
    ``glue:ListRegistries`` and ``glue:GetSchemaVersion``. See the
    connector docs for the full IAM policy.

    **Important caveat:** when a Firehose destination platform was
    ingested with a non-default ``platform_instance``, the
    ``destination_platform_map`` config option **must** be populated to
    match — otherwise the emitted lineage edges will reference
    syntactically valid but non-existent URNs (a silent failure mode
    where lineage appears in the JSON output but resolves to nothing in
    the DataHub UI). See the connector's "Known Limitations" section
    for full details.
    """

    config: KinesisSourceConfig
    report: KinesisSourceReport

    def __init__(self, config: KinesisSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = KinesisSourceReport()
        self.report.include_streams = config.include_streams
        self.report.include_firehose = config.include_firehose
        self.report.glue_schema_registry_enabled = config.glue_schema_registry.enabled

        # Stale entity removal (deletion detection) is wired automatically by the base
        # Source.get_workunit_processors() because KinesisSourceReport subclasses
        # StaleEntityRemovalSourceReport and this source exposes a state_provider.

        # Shared boto3 session (KDS / KDF / Glue clients are lazily constructed per extractor).
        self._session = config.aws_config.get_session()

        # Resolve aws_region from the boto3 session when not explicitly configured.
        # boto3's resolution chain (in priority order): explicit aws_region → AWS_REGION
        # → AWS_DEFAULT_REGION → ~/.aws/config profile. We snap the resolved value back
        # onto the config so downstream extractors (which read config.aws_config.aws_region
        # for the region container and external_url) see a concrete region rather than None.
        if not config.aws_config.aws_region:
            config.aws_config.aws_region = self._session.region_name
        if not config.aws_config.aws_region:
            raise ValueError(
                "AWS region could not be resolved. Set aws_config.aws_region in the recipe "
                "or AWS_REGION / AWS_DEFAULT_REGION env var, or configure a default region "
                "in your AWS profile."
            )

        # Resolve platform_instance. When the user has not set one explicitly, fall back to
        # the AWS account_id alone. The catalog hierarchy then reads as:
        #   platform → account_id → region container → stream
        # mirroring AWS's own resource model (account owns resources; region is a property
        # of each resource). Cross-region collision safety is handled by encoding region
        # into the dataset name and DataFlow flow_id, not by stuffing it into the
        # platform_instance (which would duplicate the region in the navigation tree).
        if not config.platform_instance:
            account_id = self._resolve_account_id()
            if account_id:
                config.platform_instance = account_id
                self.report.account_id = account_id
                self.report.platform_instance_resolved = config.platform_instance

        # Glue Schema Registry is opt-in (disabled by default).
        schema_registry: Optional[KinesisGlueSchemaRegistry] = None
        if config.glue_schema_registry.enabled:
            glue_client: "GlueClient" = config.make_client(self._session, "glue")
            schema_registry = KinesisGlueSchemaRegistry(
                config=config.glue_schema_registry,
                glue_client=glue_client,
                platform_urn=make_data_platform_urn(PLATFORM_NAME),
                report=self.report,
            )

        self.stream_extractor = KinesisStreamExtractor(
            config=config,
            report=self.report,
            session=self._session,
            region_key=self._region_key(),
            schema_registry=schema_registry,
        )

        self.firehose_extractor = KinesisFirehoseExtractor(
            config=config,
            report=self.report,
            session=self._session,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "KinesisSource":
        config = KinesisSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Defer the regional Container until at least one KDS stream materialises.
        # Each Firehose stream is its own DataFlow (not a child of the region
        # container), so an account+region with only Firehose streams (no KDS)
        # would otherwise emit an empty Region container that just clutters the
        # catalog navigation.
        region_container_emitted = False
        if self.config.include_streams:
            for wu in self.stream_extractor.get_workunits():
                if not region_container_emitted:
                    yield from self._emit_region_container()
                    region_container_emitted = True
                yield wu
        if self.config.include_firehose:
            yield from self.firehose_extractor.get_workunits()

    def _emit_region_container(self) -> Iterable[MetadataWorkUnit]:
        region = self.config.aws_config.aws_region
        assert region is not None  # validated in __init__
        region_key = self._region_key()
        yield from gen_containers(
            container_key=region_key,
            name=region,
            sub_types=[GenericContainerSubTypes.KINESIS_REGION.value],
            description=f"AWS region {region}",
            external_url=f"https://console.aws.amazon.com/kinesis/home?region={region}",
        )

    def _resolve_account_id(self) -> Optional[str]:
        """Derive the AWS account_id from a resource ARN (no sts:GetCallerIdentity).

        AWS ARNs are ``arn:aws:<svc>:<region>:<account>:<resource>`` — the account is
        field 4. Reading it off a resource's own ARN needs no extra IAM permission
        (it reuses the kinesis/firehose read access the connector already requires)
        and reflects the account the *resources* live in, which stays correct under
        cross-account role assumption — unlike sts:GetCallerIdentity, which returns
        the caller's account. Returns None (with a warning) when no resource is
        available or the lookup fails, so the connector degrades to an unset
        platform_instance rather than crashing __init__.
        """
        # Catch BotoCoreError too: NoCredentialsError, EndpointConnectionError,
        # ProfileNotFound and similar non-API errors would otherwise crash __init__.
        try:
            arn = self._first_resource_arn()
        except (ClientError, BotoCoreError) as e:
            code = aws_error_code(e)
            self.report.warning(
                title="account_id not resolved",
                message=(
                    "Could not read a resource ARN to derive the AWS account_id; "
                    "URNs will not include it. Set platform_instance in the recipe "
                    "to disambiguate across accounts."
                ),
                context=f"resource listing returned {code}",
                exc=e,
            )
            return None

        if arn:
            parts = arn.split(":")
            if len(parts) >= 5 and parts[4]:
                return parts[4]
        return None

    def _first_resource_arn(self) -> Optional[str]:
        """Return one Kinesis stream ARN (preferred) or Firehose delivery-stream ARN,
        used solely to derive the account_id. Raises ClientError/BotoCoreError on
        AWS failure (handled by the caller)."""
        if self.config.include_streams:
            kinesis = self.config.make_client(self._session, "kinesis")
            resp = kinesis.list_streams(Limit=1)
            for summary in resp.get("StreamSummaries") or []:
                if summary.get("StreamARN"):
                    return summary["StreamARN"]
            # Older API shape returns only names — describe one to get its ARN.
            names = resp.get("StreamNames") or []
            if names:
                return kinesis.describe_stream(StreamName=names[0])[
                    "StreamDescription"
                ]["StreamARN"]
        if self.config.include_firehose:
            firehose = self.config.make_client(self._session, "firehose")
            resp = firehose.list_delivery_streams(Limit=1)
            names = resp.get("DeliveryStreamNames") or []
            if names:
                return firehose.describe_delivery_stream(DeliveryStreamName=names[0])[
                    "DeliveryStreamDescription"
                ]["DeliveryStreamARN"]
        return None

    def _region_key(self) -> KinesisRegionKey:
        region = self.config.aws_config.aws_region
        assert region is not None  # validated (raises) in __init__
        return KinesisRegionKey(
            region=region,
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def get_report(self) -> KinesisSourceReport:
        return self.report

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            config = KinesisSourceConfig.model_validate(config_dict)
            session = config.aws_config.get_session()

            def _probe(
                service: AwsService,
                op: Callable[[Any], Any],
                label: str,
            ) -> CapabilityReport:
                """Run a probe call and return a CapabilityReport.

                Catches both ClientError (structured AWS API errors like
                AccessDeniedException) and BotoCoreError (credential / network
                errors like NoCredentialsError, EndpointConnectionError). The
                outer `except Exception` below is reserved for config-parsing
                failures only — without catching BotoCoreError here, a
                BotoCoreError from the Firehose or Glue probe would propagate
                to the outer handler and overwrite `basic_connectivity=False`,
                misattributing a Firehose/Glue capability failure to the basic
                Kinesis check.
                """
                try:
                    op(config.make_client(session, service))
                    return CapabilityReport(capable=True)
                except (ClientError, BotoCoreError) as e:
                    code = aws_error_code(e)
                    return CapabilityReport(
                        capable=False, failure_reason=f"{label} failed: {code}"
                    )

            # Basic connectivity: kinesis:ListStreams
            basic = _probe(
                "kinesis", lambda c: c.list_streams(Limit=1), "kinesis:ListStreams"
            )
            report.basic_connectivity = basic
            if not basic.capable:
                return report
            report.capability_report = {
                SourceCapability.DESCRIPTIONS: CapabilityReport(capable=True),
            }

            # Firehose / lineage
            if config.include_firehose:
                report.capability_report[SourceCapability.LINEAGE_COARSE] = _probe(
                    "firehose",
                    lambda c: c.list_delivery_streams(Limit=1),
                    "firehose:ListDeliveryStreams",
                )

            # Glue Schema Registry
            if config.glue_schema_registry.enabled:
                report.capability_report[SourceCapability.SCHEMA_METADATA] = _probe(
                    "glue",
                    lambda c: c.list_registries(MaxResults=1),
                    "glue:ListRegistries",
                )

        except Exception as e:  # test-connection must surface ANY config/auth failure
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{type(e).__name__}: {e}"
            )
        return report
