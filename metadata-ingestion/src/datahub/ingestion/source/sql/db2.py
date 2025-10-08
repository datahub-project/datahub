from typing import (
    Dict,
    Iterable,
)

import pydantic

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri


class Db2Config(BasicSQLAlchemyConfig):
    # Override defaults
    host_port: str = pydantic.Field(default="localhost:50000")
    scheme: str = pydantic.Field(default="db2+ibm_db")  # TODO: hide this?
    username: str
    password: str
    database: str

    def get_sql_alchemy_url(self):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password,
            self.host_port,
        )


@platform_name("IBM Db2", id="db2")
@config_class(Db2Config)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
class Db2Source(SQLAlchemySource):
    def __init__(self, config: Db2Config, ctx: PipelineContext):
        super().__init__(config, ctx, "db2")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "Db2Source":
        config = Db2Config.parse_obj(config_dict)
        return cls(config, ctx)

    def get_schema_names(self, inspector) -> Iterable[str]:
        for s in inspector.get_schema_names():
            # inspect.get_schema_names() can return schema names with extra space on the end
            yield s.rstrip()
