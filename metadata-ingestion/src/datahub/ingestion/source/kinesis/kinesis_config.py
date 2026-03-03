from typing import Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class KinesisSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """
    Configuration for the AWS Kinesis source.
    """

    # Connection
    connection: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description="AWS connection configuration",
    )

    # Filtering
    stream_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Kinesis streams by name",
    )

    # Region selection (optional, overrides connection.aws_region if set)
    region_name: Optional[str] = Field(
        default=None,
        description="AWS Region to ingest from. If not specified, uses AWS profile default.",
    )
