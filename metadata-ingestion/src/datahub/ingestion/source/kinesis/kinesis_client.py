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
        """
        try:
            # Simple retry logic for throttling could be added here if needed beyond boto3's retries
            response = self.client.describe_stream(StreamName=stream_name)
            return response.get("StreamDescription")
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
