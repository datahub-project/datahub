import logging
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.kinesis.kinesis_aws_utils import aws_error_code
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_schema_registry import (
    KinesisGlueSchemaRegistry,
)
from datahub.ingestion.source.kinesis.kinesis_tagging import (
    build_global_tags_from_aws_tags,
    extract_owner_urns_from_tags,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipTypeClass,
)
from datahub.sdk import Dataset

if TYPE_CHECKING:
    from boto3.session import Session
    from mypy_boto3_kinesis import KinesisClient
    from mypy_boto3_kinesis.type_defs import StreamDescriptionTypeDef

logger = logging.getLogger(__name__)


class KinesisStreamExtractor:
    """Extract Kinesis Data Streams into DataHub Datasets.

    Responsibilities:
      - List streams (with pattern filtering)
      - Describe each stream (status, shards, encryption, retention)
      - List tags (used by Task 11 for globalTags + ownership)
      - Emit Dataset entities via SDK V2
    """

    def __init__(
        self,
        config: KinesisSourceConfig,
        report: KinesisSourceReport,
        session: "Session",
        region_key: ContainerKey,
        schema_registry: Optional[KinesisGlueSchemaRegistry] = None,
    ):
        self.config = config
        self.report = report
        self.region_key = region_key
        self.schema_registry = schema_registry
        self._kinesis: "KinesisClient" = config.make_client(session, "kinesis")

    def list_stream_names(self) -> Iterator[str]:
        # Mirror the Firehose listing pattern: distinguish first-page failure
        # (IAM denied → warning, skip section entirely; valid outcome) from
        # mid-pagination failure (throttling, transient AWS error → DATA LOSS
        # if treated as a clean stop). The latter escalates to report.failure
        # because pages already yielded are persisted as workunits and
        # stateful ingestion would otherwise soft-delete every entity beyond
        # the failure point on the next run.
        #
        # boto3 paginators raise the AWS exception from inside __next__ (the
        # underlying ListStreams call), not from paginator.paginate(). So we
        # wrap the for-loop in try/except; an exception on the first
        # __next__ leaves pages_fetched==0 (warning path), an exception on
        # later iterations leaves pages_fetched>0 (failure path).
        paginator = self._kinesis.get_paginator("list_streams")
        pages_fetched = 0
        try:
            for page in paginator.paginate():
                pages_fetched += 1
                for name in page.get("StreamNames", []):
                    if self.config.stream_pattern.allowed(name):
                        yield name
                    else:
                        self.report.report_stream_filtered(name)
        except (ClientError, BotoCoreError) as e:
            code = aws_error_code(e)
            if pages_fetched == 0:
                self.report.warning(
                    title="Permission denied for Kinesis",
                    message=(
                        f"kinesis:ListStreams returned {code}; "
                        "skipping KDS section entirely."
                    ),
                    exc=e,
                )
                return
            # Mid-pagination failure — incomplete data is a real problem.
            self.report.failure(
                title="Kinesis listing aborted mid-pagination",
                message=(
                    f"kinesis:ListStreams returned {code} after "
                    f"{pages_fetched} page(s). Streams beyond this point "
                    "were NOT scanned. Stateful ingestion may incorrectly "
                    "soft-delete un-listed streams on the next run; re-run "
                    "ingestion to recover."
                ),
                exc=e,
            )
            return

    def describe_stream(self, stream_name: str) -> Optional["StreamDescriptionTypeDef"]:
        # Catch BotoCoreError too so transient network issues skip one stream
        # rather than aborting the whole get_workunits loop.
        try:
            return self._kinesis.describe_stream(StreamName=stream_name)[
                "StreamDescription"
            ]
        except (ClientError, BotoCoreError) as e:
            code = aws_error_code(e)
            self.report.report_stream_failed(stream_name, code)
            self.report.warning(
                title="Failed to describe stream",
                message=f"AWS API returned {code}; skipping this stream.",
                context=f"stream={stream_name}",
                exc=e,
            )
            return None

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for name in self.list_stream_names():
            desc = self.describe_stream(name)
            if desc is None:
                continue
            self.report.report_stream_scanned()
            yield from self._emit_dataset(name, desc)

    def fetch_tags(self, stream_name: str) -> List[Dict[str, str]]:
        """Fetch AWS resource tags for a Kinesis stream.

        Short-circuits to an empty list when neither tag-extraction nor
        owner-extraction is enabled. On AWS API failure (e.g. AccessDenied),
        emits a warning and returns an empty list so dataset emission
        continues without tags / ownership.
        """
        if not self.config.extract_tags and not self.config.extract_owners:
            return []
        try:
            resp = self._kinesis.list_tags_for_stream(StreamName=stream_name)
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
                    f"kinesis:ListTagsForStream returned {code}; "
                    "emitting stream without tags/ownership."
                ),
                context=f"stream={stream_name}",
                exc=e,
            )
            return []

    def _emit_dataset(
        self, stream_name: str, desc: "StreamDescriptionTypeDef"
    ) -> Iterable[MetadataWorkUnit]:
        tags = self.fetch_tags(stream_name)
        # Config flags gate which aspects we build; the shared helpers in
        # kinesis_tagging.py are unconditional, so we apply the flags here.
        owner_urns = (
            extract_owner_urns_from_tags(tags, self.config.owner_tag_key)
            if self.config.extract_owners
            else []
        )
        global_tags = (
            build_global_tags_from_aws_tags(tags) if self.config.extract_tags else None
        )
        owner_aspects = [
            OwnerClass(owner=urn, type=OwnershipTypeClass.DATAOWNER)
            for urn in owner_urns
        ]
        # Glue Schema Registry lookup is opt-in (config.glue_schema_registry.enabled).
        # When disabled, schema_registry is None and we skip the lookup entirely.
        schema_metadata = (
            self.schema_registry.get_schema_metadata(stream_name)
            if self.schema_registry is not None
            else None
        )
        # SDK V2 Dataset accepts `tags=Sequence[TagAssociationClass]` and
        # `owners=Sequence[OwnerClass]` directly (see datahub.sdk._shared
        # TagsInputType / OwnersInputType). Pass through when present.
        # URN includes region (`<region>.<stream>`) so the same stream name in two regions
        # of the same account does not collide. display_name is the bare stream_name so
        # the UI shows "TestStream" rather than "us-west-2.TestStream" — region context
        # is already visible from the parent Region container in the hierarchy.
        region = self.config.aws_config.aws_region
        dataset = Dataset(
            platform="kinesis",
            name=f"{region}.{stream_name}",
            display_name=stream_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            subtype=DatasetSubTypes.KINESIS_STREAM.value,
            parent_container=self.region_key,
            description=f"Kinesis Data Stream {stream_name}",
            external_url=(
                f"https://console.aws.amazon.com/kinesis/home?region={region}"
                f"#/streams/details/{stream_name}"
            ),
            custom_properties=self._custom_properties(desc),
            tags=list(global_tags.tags) if global_tags else None,
            owners=owner_aspects or None,
            schema=schema_metadata,
        )
        yield from dataset.as_workunits()

    @staticmethod
    def _custom_properties(desc: "StreamDescriptionTypeDef") -> Dict[str, str]:
        """Stringify the stream description fields users want to see in the UI."""
        shards = desc.get("Shards", []) or []
        mode_details = desc.get("StreamModeDetails")
        stream_mode = (
            mode_details.get("StreamMode") if mode_details else None
        ) or "PROVISIONED"
        props = {
            "stream_arn": str(desc.get("StreamARN", "")),
            "stream_status": str(desc.get("StreamStatus", "")),
            "stream_mode": stream_mode,
            "shard_count": str(len(shards)),
            "retention_hours": str(desc.get("RetentionPeriodHours", "")),
            "encryption_type": str(desc.get("EncryptionType", "")),
            "key_id": str(desc.get("KeyId") or ""),
        }
        return {k: v for k, v in props.items() if v}
