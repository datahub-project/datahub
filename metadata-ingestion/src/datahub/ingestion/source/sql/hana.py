from typing import Dict

import pydantic

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)


class HanaConfig(BasicSQLAlchemyConfig):
    # Override defaults
    host_port: str = pydantic.Field(default="localhost:39041")
    scheme: str = pydantic.Field(default="hana+hdbcli")


class HanaSource(SQLAlchemySource):
    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "hana")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "HanaSource":
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)
