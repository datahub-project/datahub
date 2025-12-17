import logging
import warnings
from typing import Any, List

from pydantic.fields import Field
from sqlalchemy import text
from sqlalchemy.dialects.mysql import base, mysqldb
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import SAWarning

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source import ge_data_profiler
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
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

logger = logging.getLogger(__name__)

# Suppress SQLAlchemy warnings about Doris-specific DDL syntax
# SQLAlchemy's MySQL dialect doesn't understand Doris-specific CREATE TABLE syntax like:
# - DUPLICATE KEY(...) / AGGREGATE KEY(...) / UNIQUE KEY(...) - Doris table models
# - DISTRIBUTED BY HASH(...) BUCKETS N - Doris distribution strategy
# - PROPERTIES (...) - Doris table properties
# - REPLACE aggregation type on non-key columns in AGGREGATE KEY tables
warnings.filterwarnings(
    "ignore",
    message=".*Unknown schema content.*",
    category=SAWarning,
)
warnings.filterwarnings(
    "ignore",
    message=".*Incomplete reflection of column definition.*",
    category=SAWarning,
)

# Register Doris-specific data types unique to Apache Doris

# HyperLogLog - Used for approximate distinct count aggregations
HLL = make_sqlalchemy_type("HLL")
register_custom_type(HLL, BytesTypeClass)

# Bitmap - Used for bitmap indexing and set operations
BITMAP = make_sqlalchemy_type("BITMAP")
register_custom_type(BITMAP, BytesTypeClass)

# Array - Native array type support
DORIS_ARRAY = make_sqlalchemy_type("ARRAY")
register_custom_type(DORIS_ARRAY, ArrayTypeClass)

# JSONB - Binary JSON format (more efficient than MySQL's JSON)
JSONB = make_sqlalchemy_type("JSONB")
register_custom_type(JSONB, RecordTypeClass)

# QUANTILE_STATE - For approximate percentile calculations
QUANTILE_STATE = make_sqlalchemy_type("QUANTILE_STATE")
register_custom_type(QUANTILE_STATE, BytesTypeClass)

base.ischema_names["hll"] = HLL
base.ischema_names["bitmap"] = BITMAP
base.ischema_names["array"] = DORIS_ARRAY
base.ischema_names["jsonb"] = JSONB
base.ischema_names["quantile_state"] = QUANTILE_STATE

base.ischema_names["HLL"] = HLL
base.ischema_names["BITMAP"] = BITMAP
base.ischema_names["ARRAY"] = DORIS_ARRAY
base.ischema_names["JSONB"] = JSONB
base.ischema_names["QUANTILE_STATE"] = QUANTILE_STATE


# Patch MySQL dialect to preserve original Doris type names in nativeDataType
# Without this patch, HLL/BITMAP/QUANTILE_STATE show as "BLOB" and JSONB shows as "JSON"
try:
    _original_get_columns = mysqldb.MySQLDialect_mysqldb.get_columns

    def get_columns_with_full_type(self, connection, table_name, schema=None, **kw):
        """
        Override get_columns to preserve the original Doris type string.

        This ensures nativeDataType shows "HLL" instead of "BLOB", "BITMAP" instead of "BLOB", etc.

        Note: We use DESCRIBE instead of INFORMATION_SCHEMA.COLUMNS because:
        - INFORMATION_SCHEMA doesn't recognize Doris-specific types (shows "unknown" for HLL/BITMAP/etc)
        - DESCRIBE returns cleaner type names ("int" vs "int(11)", "decimal(10,2)" vs "decimalv3(10, 2)")
        - Performance impact is acceptable: one DESCRIBE per table during metadata extraction
        """
        columns = _original_get_columns(self, connection, table_name, schema, **kw)

        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return columns

        try:
            quote = self.identifier_preparer.quote_identifier
            full_name = ".".join(
                quote(p) for p in [current_schema, table_name] if p is not None
            )

            result = connection.execute(text(f"DESCRIBE {full_name}"))
            type_map = {}
            for row in result:
                col_name = row[0]
                col_type = row[1]
                type_map[col_name] = col_type

            for col in columns:
                col_name = col["name"]
                if col_name in type_map:
                    col["full_type"] = type_map[col_name]
        except Exception as e:
            logger.debug(
                f"Could not fetch original type names for {current_schema}.{table_name}: {e}. "
                "Falling back to SQLAlchemy type names."
            )

        return columns

    mysqldb.MySQLDialect_mysqldb.get_columns = get_columns_with_full_type  # type: ignore[method-assign]
    logger.debug("Successfully patched MySQL dialect to preserve Doris type names")

except Exception as e:
    logger.warning(f"Failed to patch get_columns for Doris type preservation: {e}")


# Patch profiler to exclude Doris-specific types that don't support COUNT DISTINCT
# These types cannot be profiled using standard SQL aggregations
try:
    _original_get_column_types_to_ignore = ge_data_profiler._get_column_types_to_ignore

    def _get_column_types_to_ignore_with_doris(dialect_name: str) -> list:
        ignored_types = _original_get_column_types_to_ignore(dialect_name)

        # For MySQL dialect (which Doris uses), add Doris-specific types
        # These types throw: "COUNT DISTINCT could not process type"
        if dialect_name.lower() == "mysql":
            ignored_types.extend(
                [
                    "HLL",
                    "BITMAP",
                    "QUANTILE_STATE",
                    "ARRAY",
                    "JSONB",
                ]
            )

        return ignored_types

    ge_data_profiler._get_column_types_to_ignore = (
        _get_column_types_to_ignore_with_doris  # type: ignore[assignment]
    )
    logger.debug(
        "Successfully patched profiler to exclude Doris-specific unprofil types"
    )

except Exception as e:
    logger.warning(f"Failed to patch profiler for Doris type exclusion: {e}")


class DorisConfig(MySQLConfig):
    host_port: str = Field(
        default="localhost:9030",
        description="Doris FE (Frontend) host and port. Default port is 9030.",
    )

    profiling: GEProfilingConfig = Field(
        default_factory=GEProfilingConfig,
        description=(
            "Configuration for profiling Doris tables. "
            "Note: Doris-specific types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB) are automatically "
            "excluded from field-level profiling as they do not support COUNT DISTINCT operations. "
            "Table-level metrics (row counts, column counts) are still collected for all tables."
        ),
    )

    # Hidden from docs because information_schema.ROUTINES is always empty in Doris
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
        """
        if not self.config.include_stored_procedures:
            return []

        self.report.report_warning(
            f"{db_name}.{schema}",
            "Stored procedures are not supported in Apache Doris. "
            "The information_schema.ROUTINES table is always empty per Doris documentation.",
        )
        return []
