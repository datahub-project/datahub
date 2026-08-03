import logging
import re
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, field_validator, model_validator
from sqlalchemy import create_engine, inspect, text
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
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    RecordTypeClass,
)

logger = logging.getLogger(__name__)

DORIS_DEFAULT_PORT = 9030
DORIS_INTERNAL_CATALOG = "internal"

# Strip `catalog`. prefix from view defs so lineage URNs match short database names.
_DORIS_CATALOG_PREFIX_TEMPLATE = r"(?<=[\s(,])`{catalog}`\.|^`{catalog}`\."

register_custom_type(HLL, BytesTypeClass)
register_custom_type(BITMAP, BytesTypeClass)
register_custom_type(QUANTILE_STATE, BytesTypeClass)
register_custom_type(AGG_STATE, BytesTypeClass)
register_custom_type(DORIS_ARRAY, ArrayTypeClass)
register_custom_type(DORIS_MAP, RecordTypeClass)
register_custom_type(DORIS_STRUCT, RecordTypeClass)
register_custom_type(DORIS_JSONB, RecordTypeClass)


def _catalog_prefix_pattern(catalog: str) -> re.Pattern[str]:
    return re.compile(_DORIS_CATALOG_PREFIX_TEMPLATE.format(catalog=re.escape(catalog)))


_DORIS_CATALOG_PREFIX_PATTERN = _catalog_prefix_pattern(DORIS_INTERNAL_CATALOG)


class DorisConfig(MySQLConfig):
    scheme: HiddenFromDocs[str] = Field(default="doris+pymysql")

    @field_validator("scheme", mode="before")
    @classmethod
    def _ensure_doris_scheme(cls, v: str) -> str:
        if v == "mysql+pymysql":
            return "doris+pymysql"
        return v

    host_port: str = Field(
        default=f"localhost:{DORIS_DEFAULT_PORT}",
        description=f"Doris FE (Frontend) host and port. Default port is {DORIS_DEFAULT_PORT}.",
    )

    catalog: Optional[str] = Field(
        default=None,
        description=(
            "Doris catalog to ingest from (for example `iceberg_catalog`). "
            "Defaults to the session catalog (usually the built-in `internal` catalog). "
            "When set, database listing runs after `SWITCH <catalog>` and per-database "
            "connections use the fully qualified `catalog.database` path Doris expects "
            "over the MySQL protocol. If you also ingest the same database names from "
            "another catalog, set `platform_instance` to the catalog name to avoid URN "
            "collisions. You can also pass `database` as `catalog.database`."
        ),
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

    @model_validator(mode="after")
    def _split_catalog_from_database(self) -> "DorisConfig":
        if self.database and "." in self.database:
            catalog_part, database_part = self.database.split(".", 1)
            if not catalog_part or not database_part or "." in database_part:
                return self
            if self.catalog and self.catalog != catalog_part:
                raise ValueError(
                    f"database '{self.database}' does not match catalog '{self.catalog}'"
                )
            self.catalog = catalog_part
            self.database = database_part
        elif (
            self.catalog
            and self.database
            and self.database.startswith(f"{self.catalog}.")
        ):
            self.database = self.database[len(self.catalog) + 1 :]
        return self


@platform_name("Apache Doris", id="doris")
@config_class(DorisConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class DorisSource(MySQLSource):
    config: DorisConfig

    def __init__(self, config: DorisConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self._session_catalog: Optional[str] = config.catalog

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "DorisSource":
        config = DorisConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _active_catalog(self) -> Optional[str]:
        return self._session_catalog or self.config.catalog

    def _qualified_database(self, database: str) -> str:
        # External catalogs need catalog.database in the MySQL-protocol path.
        catalog = self._active_catalog()
        if catalog and catalog != DORIS_INTERNAL_CATALOG:
            return f"{catalog}.{database}"
        return database

    def _short_database_name(self, database: str) -> str:
        catalog = self._active_catalog()
        if catalog and database.startswith(f"{catalog}."):
            return database[len(catalog) + 1 :]
        return database

    def _detect_current_catalog(self, conn: Any) -> Optional[str]:
        try:
            row = conn.execute(text("SELECT CURRENT_CATALOG()")).fetchone()
        except Exception:
            logger.debug("CURRENT_CATALOG() unavailable", exc_info=True)
            return None
        if not row or row[0] is None:
            return None
        catalog = str(row[0]).strip()
        if not catalog or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
            return None
        return catalog

    def _switch_catalog(self, conn: Any, catalog: str) -> None:
        conn.execute(text(f"SWITCH `{catalog}`"))

    def _get_database_list(self, inspector: Inspector) -> List[str]:
        if self.config.database:
            return [self.config.database]
        return [
            self._short_database_name(name) for name in inspector.get_schema_names()
        ]

    def get_inspectors(self) -> Iterable[Inspector]:
        list_db: Optional[str] = None
        if self.config.database:
            list_db = self._qualified_database(self.config.database)

        url = self.config.get_sql_alchemy_url(current_db=list_db)
        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)

        with engine.connect() as conn:
            if self.config.catalog and not self.config.database:
                self._switch_catalog(conn, self.config.catalog)
                self._session_catalog = self.config.catalog
            else:
                detected = self.config.catalog or self._detect_current_catalog(conn)
                self._session_catalog = detected

            inspector = inspect(conn)
            databases = self._get_database_list(inspector)

            for db in databases:
                short_db = self._short_database_name(db)
                if not self.config.database_pattern.allowed(short_db):
                    continue

                db_engine = None
                try:
                    db_url = self.config.get_sql_alchemy_url(
                        current_db=self._qualified_database(short_db)
                    )
                    db_engine = create_engine(db_url, **self.config.options)

                    with db_engine.connect() as db_conn:
                        yield inspect(db_conn)
                except Exception as e:
                    self.report.failure(
                        title="Failed to connect to database",
                        message="Skipping database due to connection error.",
                        context=self._qualified_database(short_db),
                        exc=e,
                    )
                finally:
                    if db_engine is not None:
                        db_engine.dispose()

    def get_db_name(self, inspector: Inspector) -> str:
        db_name = super().get_db_name(inspector)
        return self._short_database_name(db_name)

    def get_platform(self) -> str:
        return "doris"

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        if not self.config.include_stored_procedures:
            return []

        self.report.warning(
            title="Stored procedures not supported",
            message="Doris information_schema.ROUTINES is always empty. Stored procedure extraction is not available.",
            context=db_name + "." + schema,
        )
        return []

    def _get_view_definition(self, inspector: Inspector, schema: str, view: str) -> str:
        view_definition = super()._get_view_definition(inspector, schema, view)
        if not view_definition:
            return view_definition

        catalogs = {DORIS_INTERNAL_CATALOG}
        if self.config.catalog:
            catalogs.add(self.config.catalog)
        if self._session_catalog:
            catalogs.add(self._session_catalog)

        for catalog in catalogs:
            view_definition = _catalog_prefix_pattern(catalog).sub("", view_definition)
        return view_definition
