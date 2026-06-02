import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kinesis.kinesis_aws_utils import aws_error_code
from datahub.ingestion.source.kinesis.kinesis_config import (
    DestinationPlatform,
    KinesisSourceConfig,
)
from datahub.ingestion.source.kinesis.kinesis_firehose_destinations import (
    DESTINATION_HANDLERS,
    ExtendedS3Destination,
)
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_tagging import (
    build_global_tags_from_aws_tags,
    build_ownership_aspect,
    extract_owner_urns_from_tags,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    SubTypesClass,
)

if TYPE_CHECKING:
    from boto3.session import Session
    from mypy_boto3_firehose import FirehoseClient
    from mypy_boto3_firehose.type_defs import DeliveryStreamDescriptionTypeDef

logger = logging.getLogger(__name__)

# KDS streams (the *upstream* in Firehose lineage) live under platform="kinesis".
PLATFORM_NAME = "kinesis"
# Firehose delivery streams (the DataFlow + DataJobs we emit here) live under their
# own platform so the catalog mirrors AWS's split between Kinesis Data Streams and
# Kinesis Data Firehose. See kinesis.py for the design rationale.
FIREHOSE_PLATFORM_NAME = "kinesis-firehose"


class KinesisFirehoseExtractor:
    """Extract Kinesis Firehose delivery streams as a DataFlow + DataJobs.

    One DataFlow per region; one DataJob per delivery stream. Lineage edges
    are added in a follow-up task (Task 10) using the destination handlers
    from Task 8.
    """

    def __init__(
        self,
        config: KinesisSourceConfig,
        report: KinesisSourceReport,
        session: "Session",
    ):
        self.config = config
        self.report = report
        self._firehose: "FirehoseClient" = config.make_client(session, "firehose")

    def dataflow_urn(self) -> str:
        return make_data_flow_urn(
            orchestrator=FIREHOSE_PLATFORM_NAME,
            flow_id=f"{self.config.aws_config.aws_region}-firehose",
            cluster=self.config.env,
            platform_instance=self.config.platform_instance,
        )

    def datajob_urn(self, delivery_stream_name: str) -> str:
        return make_data_job_urn(
            orchestrator=FIREHOSE_PLATFORM_NAME,
            flow_id=f"{self.config.aws_config.aws_region}-firehose",
            job_id=delivery_stream_name,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance,
        )

    def list_delivery_streams(self) -> Iterator[str]:
        # NOTE: boto3 firehose has no paginator for list_delivery_streams (verified
        # via client.can_paginate). We page manually using the
        # `HasMoreDeliveryStreams` + `ExclusiveStartDeliveryStreamName` pattern
        # documented in the AWS Firehose API reference.
        #
        # Critical: distinguish first-page failure (IAM denied → skip section
        # entirely; valid outcome) from mid-pagination failure (throttling,
        # transient AWS error → DATA LOSS if treated as a clean stop). The
        # latter is escalated to report.failure because pages already yielded
        # are persisted as workunits and stateful ingestion would otherwise
        # soft-delete every entity beyond the failure point on the next run.
        exclusive_start: Optional[str] = None
        pages_fetched = 0
        while True:
            kwargs: Dict[str, Any] = {}
            if exclusive_start is not None:
                kwargs["ExclusiveStartDeliveryStreamName"] = exclusive_start
            try:
                resp = self._firehose.list_delivery_streams(**kwargs)
            except (ClientError, BotoCoreError) as e:
                code = aws_error_code(e)
                if pages_fetched == 0:
                    self.report.warning(
                        title="Permission denied for Firehose",
                        message=(
                            f"firehose:ListDeliveryStreams returned {code}; "
                            "skipping Firehose section entirely."
                        ),
                        exc=e,
                    )
                    return
                # Mid-pagination failure — incomplete data is a real problem.
                self.report.failure(
                    title="Firehose listing aborted mid-pagination",
                    message=(
                        f"firehose:ListDeliveryStreams returned {code} after "
                        f"{pages_fetched} page(s). Delivery streams beyond this "
                        "point were NOT scanned. Stateful ingestion may incorrectly "
                        "soft-delete un-listed streams on the next run; re-run "
                        "ingestion to recover."
                    ),
                    exc=e,
                )
                return
            pages_fetched += 1
            names = resp.get("DeliveryStreamNames", []) or []
            for name in names:
                if self.config.delivery_stream_pattern.allowed(name):
                    yield name
                else:
                    self.report.report_delivery_stream_filtered(name)
            if not resp.get("HasMoreDeliveryStreams") or not names:
                break
            exclusive_start = names[-1]

    def describe_delivery_stream(
        self, name: str
    ) -> Optional["DeliveryStreamDescriptionTypeDef"]:
        # Catch BotoCoreError in addition to ClientError so transient network
        # failures (EndpointConnectionError, ReadTimeoutError, etc.) skip this
        # delivery stream rather than aborting the whole get_workunits loop.
        try:
            resp = self._firehose.describe_delivery_stream(DeliveryStreamName=name)
            return resp["DeliveryStreamDescription"]
        except (ClientError, BotoCoreError) as e:
            code = aws_error_code(e)
            self.report.report_delivery_stream_failed(name, code)
            self.report.warning(
                title="Failed to describe delivery stream",
                message=f"AWS API returned {code}; skipping this delivery stream.",
                context=f"delivery_stream={name}",
                exc=e,
            )
            return None

    def get_dataflow_workunit(self) -> Iterable[MetadataWorkUnit]:
        flow_urn = self.dataflow_urn()
        firehose_platform_urn = make_data_platform_urn(FIREHOSE_PLATFORM_NAME)
        region = self.config.aws_config.aws_region
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name="AWS Kinesis Firehose",
                description=f"Kinesis Data Firehose in region {region}",
                externalUrl=f"https://console.aws.amazon.com/firehose/home?region={region}",
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataPlatformInstanceClass(
                platform=firehose_platform_urn,
                instance=(
                    make_dataplatform_instance_urn(
                        FIREHOSE_PLATFORM_NAME, self.config.platform_instance
                    )
                    if self.config.platform_instance
                    else None
                ),
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=SubTypesClass(typeNames=["Firehose"]),
        ).as_workunit()

    def _destination_urn(
        self, platform: DestinationPlatform, name: str, env: Optional[str] = None
    ) -> str:
        """Build a destination dataset URN, applying destination_platform_map
        overrides for platform_instance + env + URN-case when configured.

        Bound method passed to destination handlers' `build_urns` as the
        `urn_builder` callable. ``platform`` is constrained to the closed
        ``DestinationPlatform`` Literal (matches the keys of
        ``KinesisSourceConfig.destination_platform_map``).

        `convert_urns_to_lowercase` defaults to True so emitted URNs match
        the default Snowflake source's lowercasing behavior. Override to
        False for case-sensitive destinations (Iceberg, MongoDB) or for a
        Snowflake/Redshift source ingested with `convert_urns_to_lowercase=False`.
        """
        detail = self.config.destination_platform_map.get(platform)
        platform_instance = detail.platform_instance if detail else None
        resolved_env = env or (detail.env if detail and detail.env else self.config.env)
        # Case-folding default is True; per-destination override flips it.
        if detail is None or detail.convert_urns_to_lowercase:
            name = name.lower()
        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
            env=resolved_env,
        )

    def _source_stream_urn(
        self, delivery_desc: "DeliveryStreamDescriptionTypeDef"
    ) -> Optional[str]:
        """Return the upstream Kinesis stream URN, or None for non-stream sources.

        Firehose `DirectPut` delivery streams have no upstream — return None.
        """
        if delivery_desc.get("DeliveryStreamType") != "KinesisStreamAsSource":
            return None
        src = (delivery_desc.get("Source") or {}).get(
            "KinesisStreamSourceDescription"
        ) or {}
        arn = src.get("KinesisStreamARN", "")
        # ARN format: arn:aws:kinesis:<region>:<account>:stream/<name>
        if not arn or "/" not in arn:
            return None
        stream_name = arn.rsplit("/", 1)[-1]
        # Mirror KinesisStreamExtractor._emit_dataset: region-qualified URN name so
        # lineage edges resolve to the same dataset URN the stream extractor emits.
        region = self.config.aws_config.aws_region
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM_NAME,
            name=f"{region}.{stream_name}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def build_input_output(
        self, delivery_desc: "DeliveryStreamDescriptionTypeDef"
    ) -> DataJobInputOutputClass:
        """Build the dataJobInputOutput aspect for a Firehose delivery stream.

        Inputs: source Kinesis stream URN (when DeliveryStreamType is
        KinesisStreamAsSource). Outputs: destination URN(s) resolved via the
        handler registry, with destination_platform_map overrides applied.
        """
        inputs: List[str] = []
        outputs: List[str] = []

        upstream = self._source_stream_urn(delivery_desc)
        if upstream:
            inputs.append(upstream)

        # Destinations is a list of polymorphic AWS response shapes (one per
        # destination platform). The boto3 TypedDict union doesn't match what
        # handlers structurally accept, so we widen to Dict[str, Any] here —
        # this is the "external API returns truly dynamic data" carve-out from
        # the standards' Data Structures rule.
        destinations: List[Dict[str, Any]] = [
            dict(d) for d in (delivery_desc.get("Destinations") or [])
        ]
        ds_name = delivery_desc.get("DeliveryStreamName", "<unknown>")
        for dest in destinations:
            self._process_destination(dest, ds_name, inputs, outputs)

        return DataJobInputOutputClass(inputDatasets=inputs, outputDatasets=outputs)

    def _process_destination(
        self,
        dest: Dict[str, Any],
        ds_name: str,
        inputs: List[str],
        outputs: List[str],
    ) -> None:
        """Resolve one destination block to lineage URNs, mutating inputs/outputs.

        Extracted from `build_input_output` so the per-destination dispatch,
        the unsupported-destination warning path, and the Extended-S3
        SchemaConfiguration handling can each be reasoned about in isolation.
        Side-effects: appends to ``inputs``/``outputs`` and writes to
        ``self.report`` (warnings, skip counters, lineage counters).
        """
        handler = next((h for h in DESTINATION_HANDLERS if h.matches(dest)), None)
        if handler is None:
            dest_type = next(
                iter(k for k in dest if k.endswith("DestinationDescription")),
                "Unknown",
            )
            self.report.report_unsupported_destination(dest_type, ds_name)
            self.report.warning(
                title="Unsupported Firehose destination",
                message=(
                    f"Destination type {dest_type} has no handler; "
                    "lineage edge skipped."
                ),
                context=f"delivery_stream={ds_name}",
            )
            return

        # Handlers return a list of URNs (always single-element except for
        # IcebergDestination, which can target multiple tables from one
        # delivery stream). Empty list means the handler matched but couldn't
        # construct a valid URN (parse error, missing fields).
        urns = handler.build_urns(dest, self._destination_urn)
        if not urns:
            # Matched-but-empty is qualitatively different from
            # unsupported-destination: the user wrote a *supported* destination
            # type but the URN construction failed (missing BucketARN,
            # unparseable JDBC URL, missing Snowflake db/schema/table). Without
            # this branch, the DataJob would emit with no lineage edge for
            # this destination and the user would have no signal why.
            handler_name = type(handler).__name__
            self.report.report_destination_parse_failure(ds_name, handler_name)
            self.report.warning(
                title="Firehose destination matched handler but produced no URN",
                message=(
                    f"{handler_name} matched the destination block but build_urns() "
                    "returned []. Likely cause: missing or malformed required "
                    "fields (BucketARN, JDBC URL, Database/Schema/Table, etc.). "
                    "Lineage edge for this destination is missing."
                ),
                context=f"delivery_stream={ds_name} handler={handler_name}",
            )
        outputs.extend(urns)

        # Extended S3 destinations may carry SchemaConfiguration in
        # DataFormatConversionConfiguration — a Glue table reference that
        # governs the Parquet/ORC output schema. Surfaced as an UPSTREAM (the
        # table's schema governs what gets written; the S3 path is still the
        # data destination). The handler reports skip-reasons to `self.report`
        # directly; we only need to handle the emit path here.
        if isinstance(handler, ExtendedS3Destination):
            glue_urn = handler.extract_schema_config_glue_urn(
                dest, self._destination_urn, self.report, ds_name
            )
            if glue_urn:
                inputs.append(glue_urn)
                self.report.report_firehose_glue_schema_emitted()

    def fetch_tags(self, delivery_stream_name: str) -> List[Dict[str, str]]:
        """Fetch AWS resource tags for a Firehose delivery stream.

        Mirrors ``KinesisStreamExtractor.fetch_tags`` — short-circuits when
        both ``extract_tags`` and ``extract_owners`` are disabled; emits a
        warning and returns ``[]`` on AWS API failure.
        """
        if not self.config.extract_tags and not self.config.extract_owners:
            return []
        try:
            resp = self._firehose.list_tags_for_delivery_stream(
                DeliveryStreamName=delivery_stream_name
            )
            # boto3 TagTypeDef declares Key/Value as NotRequired[str] which makes
            # `.values()` typed as `object`. Materialise to Dict[str, str] here for
            # the shared kinesis_tagging.py helpers.
            return [
                {"Key": str(t.get("Key", "")), "Value": str(t.get("Value", ""))}
                for t in (resp.get("Tags") or [])
            ]
        except (ClientError, BotoCoreError) as e:
            code = aws_error_code(e)
            self.report.warning(
                title="Tag fetch failed",
                message=(
                    f"firehose:ListTagsForDeliveryStream returned {code}; "
                    "emitting delivery stream without tags/ownership."
                ),
                context=f"delivery_stream={delivery_stream_name}",
                exc=e,
            )
            return []

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Emit DataJob aspects (info, platform-instance, sub-types) and, when
        `include_table_lineage` is True, dataJobInputOutput edges per delivery
        stream. Also emits globalTags + ownership MCPs against the DataJob URN
        when AWS resource tags are present (Task 11).
        """
        firehose_platform_urn = make_data_platform_urn(FIREHOSE_PLATFORM_NAME)
        for name in self.list_delivery_streams():
            desc = self.describe_delivery_stream(name)
            if desc is None:
                continue
            # Task 2 fix-up: report_delivery_stream_scanned() takes no args.
            self.report.report_delivery_stream_scanned()
            job_urn = self.datajob_urn(name)
            region = self.config.aws_config.aws_region
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=name,
                    type="STREAM_DELIVERY",
                    description=f"Firehose delivery stream {name}",
                    customProperties=self._custom_properties(desc),
                    externalUrl=(
                        f"https://console.aws.amazon.com/firehose/home?region={region}"
                        f"#/details/{name}"
                    ),
                ),
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataPlatformInstanceClass(
                    platform=firehose_platform_urn,
                    instance=(
                        make_dataplatform_instance_urn(
                            FIREHOSE_PLATFORM_NAME, self.config.platform_instance
                        )
                        if self.config.platform_instance
                        else None
                    ),
                ),
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=SubTypesClass(typeNames=["Firehose Delivery Stream"]),
            ).as_workunit()
            if self.config.include_table_lineage:
                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=self.build_input_output(desc),
                ).as_workunit()
            # Tags + ownership (Task 11). DataJob has no SDK V2 wrapper yet —
            # emit as direct MCPs against the DataJob URN. Config flags gate
            # which aspects we build; the shared helpers in kinesis_tagging.py
            # are unconditional.
            tags = self.fetch_tags(name)
            global_tags = (
                build_global_tags_from_aws_tags(tags)
                if self.config.extract_tags
                else None
            )
            if global_tags is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn, aspect=global_tags
                ).as_workunit()
            owner_urns = (
                extract_owner_urns_from_tags(tags, self.config.owner_tag_key)
                if self.config.extract_owners
                else []
            )
            ownership = build_ownership_aspect(owner_urns)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn, aspect=ownership
                ).as_workunit()

    @staticmethod
    def _custom_properties(
        desc: "DeliveryStreamDescriptionTypeDef",
    ) -> Dict[str, str]:
        props = {
            "delivery_stream_arn": str(desc.get("DeliveryStreamARN", "")),
            "delivery_stream_status": str(desc.get("DeliveryStreamStatus", "")),
            "delivery_stream_type": str(desc.get("DeliveryStreamType", "")),
            "version_id": str(desc.get("VersionId", "")),
        }
        return {k: v for k, v in props.items() if v}
