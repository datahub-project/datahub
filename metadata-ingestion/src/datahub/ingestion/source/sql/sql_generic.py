from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, List, Optional, TypeVar

from pydantic.fields import Field

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemyConfig, SQLAlchemySource


@dataclass(frozen=True, eq=True)
class BaseColumn:
    name: str
    ordinal_position: int
    is_nullable: bool
    data_type: str
    comment: Optional[str]


SqlTableColumn = TypeVar("SqlTableColumn", bound="BaseColumn")


@dataclass
class BaseTable(Generic[SqlTableColumn]):
    name: str
    comment: Optional[str]
    created: datetime
    last_altered: Optional[datetime]
    size_in_bytes: Optional[int]
    rows_count: Optional[int]
    columns: List[SqlTableColumn] = field(default_factory=list)
    ddl: Optional[str] = None


@dataclass
class BaseView(Generic[SqlTableColumn]):
    name: str
    comment: Optional[str]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    view_definition: str
    size_in_bytes: Optional[int] = None
    rows_count: Optional[int] = None
    columns: List[SqlTableColumn] = field(default_factory=list)


class SQLAlchemyGenericConfig(SQLAlchemyConfig):
    platform: str = Field(
        description="Name of platform being ingested, used in constructing URNs."
    )
    connect_uri: str = Field(
        description="URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls"
    )

    def get_sql_alchemy_url(self):
        return self.connect_uri


@platform_name("Other SQLAlchemy databases", id="sqlalchemy")
@config_class(SQLAlchemyGenericConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class SQLAlchemyGenericSource(SQLAlchemySource):
    """
    The `sqlalchemy` source is useful if we don't have a pre-built source for your chosen
    database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/)
    defined elsewhere. In order to use this, you must `pip install` the required dialect packages yourself.

    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling.
    """

    def __init__(self, config: SQLAlchemyGenericConfig, ctx: PipelineContext):
        super().__init__(config, ctx, config.platform)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLAlchemyGenericConfig.parse_obj(config_dict)
        return cls(config, ctx)
