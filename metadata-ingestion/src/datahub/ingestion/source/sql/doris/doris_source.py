import logging
import re
from typing import Any, Dict, Iterable, List

from pydantic import Field, field_validator
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

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
from datahub.ingestion.source.sql.doris.doris_dialect import (
    AGG_STATE,
    BITMAP,
    DORIS_ARRAY,
    DORIS_JSONB,
    DORIS_MAP,
    DORIS_STRUCT,
    HLL,
    QUANTILE_STATE,
)
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

# Strip `internal` catalog prefix from view definitions for correct lineage URN matching.
# Matches after whitespace/punctuation or at start; preserves databases named "internal".
_DORIS_CATALOG_PREFIX_PATTERN = re.compile(r"(?<=[\s(,])`internal`\.|^`internal`\.")

# Register Doris custom types with DataHub type mappings
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

    @field_validator("scheme", mode="before")
    @classmethod
    def _ensure_doris_scheme(cls, v: str) -> str:
        """Ensure scheme is always doris+pymysql, overriding parent MySQL's mysql+pymysql."""
        if v == "mysql+pymysql":
            return "doris+pymysql"
        return v

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
        config = DorisConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _get_database_list(self, inspector: Inspector) -> List[str]:
        if self.config.database:
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
                    db_engine = None
                    try:
                        db_url = self.config.get_sql_alchemy_url(current_db=db)
                        db_engine = create_engine(db_url, **self.config.options)

                        with db_engine.connect() as db_conn:
                            yield inspect(db_conn)
                    except Exception as e:
                        self.report.failure(
                            title="Failed to connect to database",
                            message="Skipping database due to connection error.",
                            context=db,
                            exc=e,
                        )
                    finally:
                        if db_engine is not None:
                            db_engine.dispose()

    def get_platform(self) -> str:
        return "doris"

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """Doris information_schema.ROUTINES is always empty."""
        if not self.config.include_stored_procedures:
            return []

        self.report.warning(
            title="Stored procedures not supported",
            message="Doris information_schema.ROUTINES is always empty. Stored procedure extraction is not available.",
            context=db_name + "." + schema,
        )
        return []

    def _get_view_definition(self, inspector: Inspector, schema: str, view: str) -> str:
        """Strip `internal.` catalog prefix from view definitions for correct lineage."""
        view_definition = super()._get_view_definition(inspector, schema, view)
        if not view_definition:
            return view_definition

        return _DORIS_CATALOG_PREFIX_PATTERN.sub("", view_definition)
