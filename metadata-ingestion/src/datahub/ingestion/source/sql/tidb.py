from typing import Any, Dict

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.sql.mysql import MySQLAuthMode, MySQLConfig, MySQLSource

TIDB_DEFAULT_PORT = 4000


class TiDBConfig(MySQLConfig):
    host_port: str = Field(
        default=f"localhost:{TIDB_DEFAULT_PORT}",
        description=f"TiDB host and port. Default port is {TIDB_DEFAULT_PORT}.",
    )

    auth_mode: HiddenFromDocs[MySQLAuthMode] = Field(
        default=MySQLAuthMode.PASSWORD,
        description="TiDB uses standard username/password authentication.",
    )
    aws_config: HiddenFromDocs[AwsConnectionConfig] = Field(
        default_factory=AwsConnectionConfig,
        description="Not applicable for TiDB.",
    )

    include_stored_procedures: HiddenFromDocs[bool] = Field(
        default=False,
        description="Stored procedures and functions are not supported by TiDB.",
    )

    procedure_pattern: HiddenFromDocs[AllowDenyPattern] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Not applicable for TiDB.",
    )

    @model_validator(mode="after")
    def validate_auth_mode(self) -> "TiDBConfig":
        if self.auth_mode != MySQLAuthMode.PASSWORD:
            raise ValueError("TiDB only supports password authentication.")
        return self


@platform_name("TiDB", id="tidb")
@config_class(TiDBConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class TiDBSource(MySQLSource):
    """
    This plugin extracts the following from TiDB:

    Metadata for databases, schemas, and tables
    Column types and schema associated with each table
    Table, row, and column statistics via optional SQL profiling
    """

    config: TiDBConfig

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "TiDBSource":
        config = TiDBConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return "tidb"
