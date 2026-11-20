import re
from functools import cached_property
from typing import List, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import HiddenFromDocs
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig


class SqlQueriesSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, IncrementalLineageConfigMixin
):
    query_file: str = Field(description="Path to file to ingest")

    platform: str = Field(
        description="The platform for which to generate data, e.g. snowflake"
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    use_schema_resolver: HiddenFromDocs[bool] = Field(
        True,
        description="Read SchemaMetadata aspects from DataHub to aid in SQL parsing. Turn off only for testing.",
    )
    default_db: Optional[str] = Field(
        None,
        description="The default database to use for unqualified table names",
    )
    default_schema: Optional[str] = Field(
        None,
        description="The default schema to use for unqualified table names",
    )
    override_dialect: Optional[str] = Field(
        None,
        description="The SQL dialect to use when parsing queries. Overrides automatic dialect detection.",
    )
    temp_table_patterns: List[str] = Field(
        description="Regex patterns for temporary tables to filter in lineage ingestion. "
        "Patterns are start-anchored (re.match, like AllowDenyPattern). "
        "For example, 'temp_' matches any table starting with 'temp_'. "
        "This is useful for platforms like Athena "
        "that don't have native temp tables but use naming patterns for fake temp tables.",
        default=[],
    )

    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS configuration for S3 access. Required when query_file is an S3 URI (s3://).",
    )

    _enable_lazy_schema_loading_removed = pydantic_removed_field(
        "enable_lazy_schema_loading", "August", 2026
    )

    @field_validator("temp_table_patterns")
    @classmethod
    def validate_temp_table_patterns(cls, v: List[str]) -> List[str]:
        for pattern in v:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(
                    f"Invalid regex in temp_table_patterns: '{pattern}': {e}"
                ) from e
        return v

    @model_validator(mode="after")
    def validate_s3_config(self) -> "SqlQueriesSourceConfig":
        if is_s3_uri(self.query_file) and not self.aws_config:
            raise ValueError(
                "aws_config is required when query_file is an S3 URI (s3://)"
            )
        return self

    @cached_property
    def _compiled_temp_table_patterns(self) -> List["re.Pattern[str]"]:
        return [
            re.compile(pattern, re.IGNORECASE) for pattern in self.temp_table_patterns
        ]
