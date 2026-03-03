import logging
from typing import Any, Dict, Iterable, List, Optional

from botocore.config import Config
from botocore.exceptions import ClientError

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig

logger = logging.getLogger(__name__)


class KinesisClient:
    def __init__(self, config: AwsConnectionConfig, region_name: Optional[str] = None):
        self.config = config
        self.region_name = region_name or config.aws_region

        self.session = config.get_session()

        # Configure boto3 client kwargs
        boto_config_kwargs = {
            "read_timeout": config.read_timeout,
            "retries": {
                "max_attempts": config.aws_retry_num,
                "mode": config.aws_retry_mode,
            },
        }
        if config.aws_advanced_config:
            boto_config_kwargs.update(config.aws_advanced_config)

        boto_config = Config(**boto_config_kwargs)

        self.client = self.session.client(
            "kinesis",
            region_name=self.region_name,
            endpoint_url=config.aws_endpoint_url,
            config=boto_config,
        )

    def list_streams(self) -> Iterable[str]:
        """
        List all Kinesis streams in the account/region, handling pagination.
        """
        paginator = self.client.get_paginator("list_streams")
        try:
            for page in paginator.paginate():
                yield from page.get("StreamNames", [])
        except ClientError as e:
            logger.error(f"Error listing Kinesis streams: {e}")
            raise

    def describe_stream(self, stream_name: str) -> Optional[Dict[str, Any]]:
        """
        Get details for a specific stream.
        Uses describe_stream_summary for metadata and list_shards for accurate shard listing.
        """
        try:
            # Get stream configuration
            summary_response = self.client.describe_stream_summary(
                StreamName=stream_name
            )
            description = summary_response.get("StreamDescriptionSummary", {})

            # Get complete list of shards using list_shards
            shards = []
            paginator = self.client.get_paginator("list_shards")
            # list_shards requires StreamName, but the paginator interface might differ.
            # Boto3 list_shards paginator usually takes StreamName.
            # NOTE: list_shards paginator might not be available in older boto3 versions,
            # let's check basic list_shards call loop instead for safety or use paginator if standard.
            # Boto3 docs say list_shards has a paginator.

            try:
                for page in paginator.paginate(StreamName=stream_name):
                    shards.extend(page.get("Shards", []))
            except ClientError as e:
                # Fallback or specific handling if list_shards fails?
                # If list_shards fails, we might just have summary data.
                logger.warning(f"Failed to list shards for {stream_name}: {e}")

            # Combine
            # We map summary fields to what the source expects (which matched verify_stream output)
            # Source expects: StreamARN, StreamStatus, RetentionPeriodHours, EncryptionType, Shards

            result = {
                "StreamARN": description.get("StreamARN"),
                "StreamStatus": description.get("StreamStatus"),
                "RetentionPeriodHours": description.get("RetentionPeriodHours"),
                "EncryptionType": description.get("EncryptionType"),
                "Shards": shards,
            }

            return result

        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.warning(f"Stream {stream_name} not found")
                return None
            logger.error(f"Error describing stream {stream_name}: {e}")
            raise

    def list_tags_for_stream(self, stream_name: str) -> List[Dict[str, str]]:
        """
        List tags for a stream.
        """
        tags = []
        has_more = True
        exclusive_start_tag_key = None

        try:
            while has_more:
                kwargs = {"StreamName": stream_name}
                if exclusive_start_tag_key:
                    kwargs["ExclusiveStartTagKey"] = exclusive_start_tag_key

                response = self.client.list_tags_for_stream(**kwargs)
                tags.extend(response.get("Tags", []))

                has_more = response.get("HasMoreTags", False)
                if tags and has_more:
                    exclusive_start_tag_key = tags[-1]["Key"]

            return tags
        except ClientError as e:
            logger.error(f"Error listing tags for stream {stream_name}: {e}")
            # If we fail to get tags, we probably shouldn't fail ingestion of the stream itself
            # But logging error is good.
            return []
