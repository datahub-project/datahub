import logging
import warnings
from typing import Any, List

import sqlalchemy
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.mysql import base, mysqldb
from sqlalchemy.engine import Engine
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

DORIS_DEFAULT_PORT = 9030

# Suppress SQLAlchemy warnings about Doris-specific DDL syntax (DUPLICATE KEY, DISTRIBUTED BY, PROPERTIES, etc.)
warnings.filterwarnings(
    "ignore", message=".*Unknown schema content.*", category=SAWarning
)
warnings.filterwarnings(
    "ignore",
    message=".*Incomplete reflection of column definition.*",
    category=SAWarning,
)

# Register Doris-specific data types
HLL = make_sqlalchemy_type("HLL")
register_custom_type(HLL, BytesTypeClass)

BITMAP = make_sqlalchemy_type("BITMAP")
register_custom_type(BITMAP, BytesTypeClass)

DORIS_ARRAY = make_sqlalchemy_type("ARRAY")
register_custom_type(DORIS_ARRAY, ArrayTypeClass)

JSONB = make_sqlalchemy_type("JSONB")
register_custom_type(JSONB, RecordTypeClass)

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


def _patch_doris_dialect(dialect: mysqldb.MySQLDialect_mysqldb) -> None:
    """Patches per-engine rather than globally, ensuring MySQL sources remain unaffected."""
    if not isinstance(dialect, mysqldb.MySQLDialect_mysqldb):
        raise TypeError(
            f"Cannot patch dialect of type {type(dialect)} which does not descend from "
            "mysqldb.MySQLDialect_mysqldb"
        )

    _original_get_columns = dialect.get_columns

    def get_columns_with_full_type(connection, table_name, schema=None, **kw):
        columns = _original_get_columns(connection, table_name, schema, **kw)

        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return columns

        doris_types_preserved = 0
        try:
            quote = dialect.identifier_preparer.quote_identifier
            full_name = ".".join(
                quote(p) for p in [current_schema, table_name] if p is not None
            )

            result = connection.execute(text(f"DESCRIBE {full_name}"))
            type_map = {row[0]: row[1] for row in result}

            for col in columns:
                if col["name"] in type_map:
                    doris_type = type_map[col["name"]]
                    col["full_type"] = doris_type
                    if any(
                        dt in doris_type.upper()
                        for dt in ["HLL", "BITMAP", "QUANTILE_STATE", "ARRAY", "JSONB"]
                    ):
                        doris_types_preserved += 1
                        logger.debug(
                            f"Preserved Doris type for {current_schema}.{table_name}.{col['name']}: {doris_type}"
                        )

            if doris_types_preserved > 0:
                logger.info(
                    f"Type preservation: {doris_types_preserved} Doris-specific columns in {current_schema}.{table_name}"
                )
        except Exception as e:
            logger.warning(
                f"DESCRIBE query failed for {current_schema}.{table_name}: {e}. "
                f"Falling back to MySQL types (Doris types may show as BLOB)."
            )

        return columns

    dialect.get_columns = get_columns_with_full_type  # type: ignore[method-assign]


_original_get_column_types_to_ignore = ge_data_profiler._get_column_types_to_ignore
_profiler_patched = False


def _get_column_types_to_ignore_with_doris(dialect_name: str) -> list:
    """Exclude Doris types (HLL, BITMAP, etc.) that don't support COUNT DISTINCT."""
    ignored_types = _original_get_column_types_to_ignore(dialect_name)

    if dialect_name.lower() == "mysql":
        ignored_types.extend(["HLL", "BITMAP", "QUANTILE_STATE", "ARRAY", "JSONB"])

    return ignored_types


def _patch_profiler() -> None:
    """Global patch, applied once on first DorisSource creation."""
    global _profiler_patched

    if _profiler_patched:
        return

    try:
        ge_data_profiler._get_column_types_to_ignore = (
            _get_column_types_to_ignore_with_doris  # type: ignore[assignment]
        )
        _profiler_patched = True
        logger.debug("Profiler patched to exclude Doris-specific unprofilable types")
    except AttributeError as e:
        raise RuntimeError(
            f"Failed to patch profiler (Great Expectations API may have changed): {e}. "
            f"This is required for correct data profiling. "
            f"Please report this issue: https://github.com/datahub-project/datahub/issues/new"
        ) from e


class DorisConfig(MySQLConfig):
    host_port: str = Field(
        default=f"localhost:{DORIS_DEFAULT_PORT}",
        description=f"Doris FE (Frontend) host and port. Default port is {DORIS_DEFAULT_PORT}.",
    )

    profiling: GEProfilingConfig = Field(
        default_factory=GEProfilingConfig,
        description=(
            "Configuration for profiling Doris tables. "
            "Note: Doris types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB) are automatically "
            "excluded from field-level profiling as they don't support COUNT DISTINCT."
        ),
    )

    include_stored_procedures: HiddenFromDocs[bool] = Field(
        default=False,
        description="Stored procedures not supported (information_schema.ROUTINES is always empty).",
    )

    procedure_pattern: HiddenFromDocs[AllowDenyPattern] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Not applicable for Doris.",
    )


@platform_name("Apache Doris", id="doris")
@config_class(DorisConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class DorisSource(MySQLSource):
    config: DorisConfig

    def __init__(self, config: DorisConfig, ctx: Any):
        super().__init__(config, ctx)
        _patch_profiler()

    def _get_database_list(self, inspector: Inspector) -> List[str]:
        if self.config.database and self.config.database != "":
            return [self.config.database]
        return inspector.get_schema_names()

    def _create_patched_engine(self, database: str) -> Engine:
        url = self.config.get_sql_alchemy_url(current_db=database)
        engine = create_engine(url, **self.config.options)

        try:
            _patch_doris_dialect(engine.dialect)
            logger.info(
                f"Created Doris engine for database '{database}' "
                f"(SQLAlchemy {sqlalchemy.__version__}, dialect: {type(engine.dialect).__name__})"
            )
        except (TypeError, AttributeError) as e:
            raise RuntimeError(
                f"Failed to patch SQLAlchemy (version {sqlalchemy.__version__}) "
                f"dialect for Doris type preservation: {e}. "
                f"This is required for correct metadata extraction. "
                f"Please report this issue: https://github.com/datahub-project/datahub/issues/new"
            ) from e

        return engine

    def get_inspectors(self):
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            databases = self._get_database_list(inspector)

            for db in databases:
                if self.config.database_pattern.allowed(db):
                    db_engine = self._create_patched_engine(db)
                    with db_engine.connect() as db_conn:
                        yield inspect(db_conn)

    def get_platform(self):
        return "doris"

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """Doris information_schema.ROUTINES is always empty."""
        if not self.config.include_stored_procedures:
            return []

        self.report.report_warning(
            f"{db_name}.{schema}",
            "Stored procedures not supported in Doris (information_schema.ROUTINES is always empty).",
        )
        return []
