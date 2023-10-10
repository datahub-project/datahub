import logging
from typing import Optional, Any

# This import verifies that the dependencies are available.
import teradatasqlalchemy  # noqa: F401
import teradatasqlalchemy.types as custom_types

from pydantic import BaseModel
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
from datahub.ingestion.source.sql.sql_common import (
    register_custom_type,
)
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemySource,
    TwoTierSQLAlchemyConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    TimeTypeClass,
)

logger: logging.Logger = logging.getLogger(__name__)

register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.INTERVAL_DAY, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_DAY_TO_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MINUTE_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_MINUTE, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_HOUR_TO_SECOND, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_MONTH, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR, TimeTypeClass)
register_custom_type(custom_types.INTERVAL_YEAR_TO_MONTH, TimeTypeClass)
register_custom_type(custom_types.MBB, BytesTypeClass)
register_custom_type(custom_types.MBR, BytesTypeClass)
register_custom_type(custom_types.GEOMETRY, BytesTypeClass)
register_custom_type(custom_types.TDUDT, BytesTypeClass)
register_custom_type(custom_types.XML, BytesTypeClass)


class BaseTeradataConfig(TwoTierSQLAlchemyConfig):
    scheme = Field(default="teradatasql", description="database scheme")


class TeradataConfig(BaseTeradataConfig):
    include_view_lineage = Field(
        default=False, description="Include table lineage for views"
    )

    include_catalog_name = Field(
        default=True, description="Include catalog name in dataset urns"
    )

    catalog = Field(
        default="dbc",
        description="Catalog name to use for catalog-qualified table names",
    )


@platform_name("Teradata")
@config_class(TeradataConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class TeradataSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    """

    config: TeradataConfig

    def __init__(self, config: TeradataConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "teradata")

    @classmethod
    def create(cls, config_dict, ctx):
        config = TeradataConfig.parse_obj(config_dict)
        return cls(config, ctx)
