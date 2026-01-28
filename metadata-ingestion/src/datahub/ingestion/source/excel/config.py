from typing import List, Optional, Union

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled


class ExcelSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    path_list: List[str] = Field(
        description="List of paths to Excel files or folders to ingest."
    )

    path_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for file paths to filter in ingestion.",
    )

    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None, description="AWS configuration"
    )

    use_s3_bucket_tags: Optional[bool] = Field(
        default=False,
        description="Whether or not to create tags in datahub from the s3 bucket",
    )

    use_s3_object_tags: Optional[bool] = Field(
        default=False,
        description="Whether or not to create tags in datahub from the s3 object",
    )

    verify_ssl: Union[bool, str] = Field(
        default=True,
        description="Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use.",
    )

    azure_config: Optional[AzureConnectionConfig] = Field(
        default=None, description="Azure configuration"
    )

    use_abs_blob_tags: Optional[bool] = Field(
        default=False,
        description="Whether to create tags in datahub from the abs blob tags",
    )

    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Enable to convert the Excel asset urns to lowercase",
    )

    active_sheet_only: bool = Field(
        default=False,
        description="Enable to only ingest the active sheet of the workbook. If not set, all sheets will be ingested.",
    )

    worksheet_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for worksheets to ingest. Worksheets are specified as 'filename_without_extension.worksheet_name'. "
        "For example to allow the worksheet Sheet1 from file report.xlsx, use the pattern: 'report.Sheet1'.",
    )

    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for worksheets to profile. Worksheets are specified as 'filename_without_extension.worksheet_name'. "
        "For example to allow the worksheet Sheet1 from file report.xlsx, use the pattern: 'report.Sheet1'.",
    )

    profiling: GEProfilingConfig = Field(
        default=GEProfilingConfig(),
        description="Configuration for profiling",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )
