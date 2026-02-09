import logging
import re
import warnings
from typing import (  # noqa: F401 (used in type comments)
    Any,
    Dict,
    Iterable,
    List,
    Optional,
)

from pydantic import Field, model_validator
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.engine import Connection, reflection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeDecorator, TypeEngine

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
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.sql_common import register_custom_type
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)

logger = logging.getLogger(__name__)

DORIS_DEFAULT_PORT = 9030

# Suppress SQLAlchemy warnings about Doris-specific DDL syntax
warnings.filterwarnings(
    "ignore", message=".*Unknown schema content.*", category=SAWarning
)
warnings.filterwarnings(
    "ignore",
    message=".*Incomplete reflection of column definition.*",
    category=SAWarning,
)


class HLL(sqltypes.LargeBinary):
    __visit_name__ = "HLL"


class BITMAP(sqltypes.LargeBinary):
    __visit_name__ = "BITMAP"


class QUANTILE_STATE(sqltypes.LargeBinary):
    __visit_name__ = "QUANTILE_STATE"


class AGG_STATE(sqltypes.LargeBinary):
    __visit_name__ = "AGG_STATE"


class DORIS_ARRAY(TypeDecorator):
    impl = sqltypes.Text
    cache_ok = True
    __visit_name__ = "ARRAY"


class DORIS_MAP(TypeDecorator):
    impl = sqltypes.Text
    cache_ok = True
    __visit_name__ = "MAP"


class DORIS_STRUCT(TypeDecorator):
    impl = sqltypes.Text
    cache_ok = True
    __visit_name__ = "STRUCT"


class DORIS_JSONB(sqltypes.JSON):
    __visit_name__ = "JSONB"


_doris_type_map = {
    "hll": HLL,
    "bitmap": BITMAP,
    "quantile_state": QUANTILE_STATE,
    "agg_state": AGG_STATE,
    "array": DORIS_ARRAY,
    "map": DORIS_MAP,
    "struct": DORIS_STRUCT,
    "jsonb": DORIS_JSONB,
}


def _parse_doris_type(type_str: str) -> TypeEngine:
    type_str = type_str.strip().lower()
    match = re.match(r"^(?P<type>\w+)", type_str)
    if not match:
        return sqltypes.NULLTYPE

    type_name = match.group("type")
    if type_name in _doris_type_map:
        return _doris_type_map[type_name]()

    return sqltypes.NULLTYPE


class DorisDialect(MySQLDialect_pymysql):
    name = "doris"
    supports_statement_cache = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.ischema_names.update(_doris_type_map)

    @reflection.cache  # type: ignore[call-arg]
    def get_columns(self, connection, table_name, schema=None, **kw):
        # type: (Connection, str, Optional[str], Any) -> List[Dict[str, Any]]
        """Uses DESCRIBE to preserve Doris-specific types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB)."""
        columns = super().get_columns(connection, table_name, schema, **kw)

        current_schema = schema or connection.engine.url.database
        if not current_schema:
            return columns

        try:
            quote = self.identifier_preparer.quote_identifier
            full_name = f"{quote(current_schema)}.{quote(table_name)}"
            result = connection.execute(text(f"DESCRIBE {full_name}"))
            type_map = {row[0]: row[1] for row in result}

            for col in columns:
                if col["name"] in type_map:
                    doris_type_str = type_map[col["name"]]
                    col["full_type"] = doris_type_str

                    parsed_type = _parse_doris_type(doris_type_str)
                    if type(parsed_type) is not type(sqltypes.NULLTYPE):
                        col["type"] = parsed_type

        except Exception as e:
            logger.debug(
                f"DESCRIBE query failed for {current_schema}.{table_name}: {e}. "
                f"Using MySQL type reflection."
            )

        return columns

    def get_schema_names(self, connection: Connection, **kw: Any) -> List[str]:
        result = connection.execute(text("SHOW SCHEMAS"))
        return [row[0] for row in result]


register_custom_type(HLL, BytesTypeClass)
register_custom_type(BITMAP, BytesTypeClass)
register_custom_type(QUANTILE_STATE, BytesTypeClass)
register_custom_type(AGG_STATE, BytesTypeClass)
register_custom_type(DORIS_ARRAY, ArrayTypeClass)
register_custom_type(DORIS_MAP, RecordTypeClass)
register_custom_type(DORIS_STRUCT, RecordTypeClass)
register_custom_type(DORIS_JSONB, RecordTypeClass)


class DorisConfig(MySQLConfig):
    scheme: HiddenFromDocs[str] = Field(default="doris+pymysql")

    @model_validator(mode="after")
    def _ensure_doris_scheme(self) -> "DorisConfig":
        """Ensure scheme is always doris+pymysql, overriding parent MySQL's mysql+pymysql."""
        if self.scheme == "mysql+pymysql":
            object.__setattr__(self, "scheme", "doris+pymysql")
        return self

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

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "DorisSource":
        """Override MySQLSource.create() to use DorisConfig instead of MySQLConfig."""
        config = DorisConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _get_database_list(self, inspector: Inspector) -> List[str]:
        if self.config.database and self.config.database != "":
            return [self.config.database]
        return inspector.get_schema_names()

    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)

        with engine.connect() as conn:
            inspector = inspect(conn)
            databases = self._get_database_list(inspector)

            for db in databases:
                if self.config.database_pattern.allowed(db):
                    try:
                        db_url = self.config.get_sql_alchemy_url(current_db=db)
                        db_engine = create_engine(db_url, **self.config.options)

                        with db_engine.connect() as db_conn:
                            yield inspect(db_conn)
                    except Exception as e:
                        self.report.failure(
                            title="Failed to connect to database",
                            message=f"Skipping database due to connection error: {e}",
                            context=db,
                        )

    def get_platform(self) -> str:
        return "doris"

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """Doris information_schema.ROUTINES is always empty."""
        if not self.config.include_stored_procedures:
            return []

        self.report.report_warning(
            title="Stored procedures not supported",
            message="Doris information_schema.ROUTINES is always empty. Stored procedure extraction is not available.",
            context=db_name + "." + schema,
        )
        return []
