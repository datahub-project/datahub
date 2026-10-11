import functools

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.gcs.gcs_utils import GCS_ENDPOINT_URL, HMACKey


class GCSConnectionConfig(ConfigModel):
    """GCS connection using HMAC keys, accessed via the S3-compatible XML API."""

    credential: HMACKey = Field(
        description="GCS HMAC credentials. See https://cloud.google.com/storage/docs/authentication/hmackeys",
    )
    endpoint_url: str = Field(
        default=GCS_ENDPOINT_URL,
        description="GCS S3-compatible endpoint URL. "
        "Useful for testing with local S3-compatible servers.",
    )

    @functools.cached_property
    def s3_compatible_connection(self) -> AwsConnectionConfig:
        return AwsConnectionConfig(
            aws_endpoint_url=self.endpoint_url,
            aws_access_key_id=self.credential.hmac_access_id,
            aws_secret_access_key=self.credential.hmac_access_secret,
            aws_region="auto",
        )
