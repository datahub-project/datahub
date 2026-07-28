import os
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
from datahub.ingestion.source.sql.mysql import (
    MySQLAuthMode,
    MySQLConfig,
    MySQLProfilingConfig,
    MySQLSource,
)

TIDB_DEFAULT_PORT = 4000


class TiDBProfilingConfig(MySQLProfilingConfig):
    # TiDB inherits MySQLConfig's narrowed `profiling: MySQLProfilingConfig` type, but TiDB is a
    # distributed HTAP database, not a single-primary row store. Two of MySQL's overrides don't
    # fit and are reverted to the shared GEProfilingConfig default:
    #   - max_workers=5 is a ~40→5 throughput regression for a reason (single-primary contention)
    #     that doesn't hold for TiDB's distributed execution.
    #   - report_expensive_tables=True recommends setting *MySQL* row limits, which is odd advice
    #     for TiDB.
    # The two limit fields are deliberately NOT redeclared: they inherit MySQLProfilingConfig's
    # `None` default. That is what preserves prior behavior — PR 2 newly implements
    # generate_profile_candidates (the enforcement mechanism) for the MySQL family, so a non-None
    # default here would *activate* a guardrail that never ran before, silently dropping profiles
    # for tables over 5M rows using information_schema.tables.table_rows semantics that TiDB
    # (distributed HTAP) does not share with InnoDB. Same failure mode rejected for MySQL in §4.1.
    # The inherited field keeps its Annotated[Optional[int], SupportedSources(["mysql"])] type.
    #
    # NOTE: TiDB also inherits MySQLSource.generate_profile_candidates (the information_schema
    # query added in PR 2). TiDB's `information_schema.tables.table_rows` depends on ANALYZE TABLE
    # and can be zero or badly stale, so the guardrail is only as good as those stats. With the
    # default None the guardrail stays dormant; the stale-stats concern only surfaces if an
    # operator opts in by setting a limit, which is an acceptable documented caveat.
    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for profiling. Set to 1 to disable.",
    )
    report_expensive_tables: bool = Field(
        default=False,
        description="Emit a post-run report.info entry naming the few tables that took the longest to profile.",
    )


class TiDBConfig(MySQLConfig):
    profiling: TiDBProfilingConfig = Field(
        default_factory=TiDBProfilingConfig,
        description="Configuration for profiling TiDB tables.",
    )

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
