from typing import Any, List

from pydantic.fields import Field
from sqlalchemy.dialects.mysql import base
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)

# Register Doris-specific data types
# These types are unique to Apache Doris and not present in standard MySQL

# HyperLogLog - Used for approximate distinct count aggregations
HLL = make_sqlalchemy_type("HLL")
register_custom_type(HLL, BytesTypeClass)  # Treat as binary data in DataHub

# Bitmap - Used for bitmap indexing and set operations
BITMAP = make_sqlalchemy_type("BITMAP")
register_custom_type(BITMAP, BytesTypeClass)  # Treat as binary data in DataHub

# Array - Native array type support
DORIS_ARRAY = make_sqlalchemy_type("ARRAY")
register_custom_type(DORIS_ARRAY, ArrayTypeClass)  # Proper array type in DataHub

# JSONB - Binary JSON format (more efficient than MySQL's JSON)
JSONB = make_sqlalchemy_type("JSONB")
register_custom_type(JSONB, RecordTypeClass)  # Treat as record/struct in DataHub

# QUANTILE_STATE - For approximate percentile calculations
QUANTILE_STATE = make_sqlalchemy_type("QUANTILE_STATE")
register_custom_type(QUANTILE_STATE, BytesTypeClass)

# Register these types with the MySQL dialect so SQLAlchemy recognizes them
base.ischema_names["hll"] = HLL
base.ischema_names["bitmap"] = BITMAP
base.ischema_names["array"] = DORIS_ARRAY
base.ischema_names["jsonb"] = JSONB
base.ischema_names["quantile_state"] = QUANTILE_STATE

# Handle case variations
base.ischema_names["HLL"] = HLL
base.ischema_names["BITMAP"] = BITMAP
base.ischema_names["ARRAY"] = DORIS_ARRAY
base.ischema_names["JSONB"] = JSONB
base.ischema_names["QUANTILE_STATE"] = QUANTILE_STATE


class DorisConfig(MySQLConfig):
    # Override host_port to document Doris's default port
    host_port: str = Field(
        default="localhost:9030",
        description="Doris FE (Frontend) host and port in the format host:port. Default port is 9030 (MySQL protocol), not 3306.",
    )

    # Override to hide stored procedure-related fields from docs since they don't work in Doris
    # information_schema.ROUTINES is always empty per Doris documentation
    # https://doris.apache.org/docs/3.x/admin-manual/system-tables/information_schema/routines
    include_stored_procedures: HiddenFromDocs[bool] = Field(
        default=False,
        description="Stored procedures are not supported in Apache Doris. The information_schema.ROUTINES table is always empty.",
    )

    procedure_pattern: HiddenFromDocs[AllowDenyPattern] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Not applicable for Apache Doris as stored procedures are not supported.",
    )


@platform_name("Apache Doris", id="doris")
@config_class(DorisConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class DorisSource(MySQLSource):
    """
    This plugin extracts metadata from Apache Doris, which is largely MySQL-compatible.

    Apache Doris is a modern MPP analytical database that uses the MySQL protocol
    for client connections. While Doris aims for MySQL compatibility, there are some
    differences to be aware of:

    - Data Types: Doris has unique types (HyperLogLog, Bitmap, Array, JSONB) not in MySQL
    - Stored Procedures: Limited support compared to MySQL (disabled by default)
    - System Tables: Uses virtual system tables that are read-only
    - Default Port: 9030 (query port) instead of MySQL's 3306

    This connector extends the MySQL connector and inherits most of its functionality,
    including table/view metadata extraction and profiling capabilities.
    """

    config: DorisConfig

    def __init__(self, config: DorisConfig, ctx: Any):
        super().__init__(config, ctx)

    def get_platform(self):
        return "doris"

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """
        Override to handle Doris's empty information_schema.ROUTINES table.

        According to Apache Doris documentation:
        https://doris.apache.org/docs/3.x/admin-manual/system-tables/information_schema/routines
        "This table is solely for the purpose of maintaining compatibility with MySQL behavior.
        It is always empty."

        Therefore, stored procedures are always disabled for Doris.
        """
        if not self.config.include_stored_procedures:
            return []

        # Even if user explicitly enables stored procedures, return empty list
        # because information_schema.ROUTINES is documented as always empty in Doris
        self.report.report_warning(
            f"{db_name}.{schema}",
            "Stored procedures are not supported in Apache Doris. "
            "The information_schema.ROUTINES table is always empty per Doris documentation.",
        )
        return []
