import functools
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set

from pydantic import Field, field_validator, model_validator
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
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
    IPV4,
    IPV6,
    LARGEINT,
    QUANTILE_STATE,
    VARIANT,
    DorisDialect,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.sql.sql_common import register_custom_type
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)

DORIS_DEFAULT_PORT = 9030
DORIS_INTERNAL_CATALOG = "internal"

# Strip `catalog`. prefix from view defs so lineage URNs match short database names.
_DORIS_CATALOG_PREFIX_TEMPLATE = r"(?<=[\s(,])`{catalog}`\.|^`{catalog}`\."

# Catalog names reach SQL (`SWITCH`) and the connection URL, so restrict them to
# plain identifiers rather than escaping at each use site.
_CATALOG_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")

register_custom_type(HLL, BytesTypeClass)
register_custom_type(BITMAP, BytesTypeClass)
register_custom_type(QUANTILE_STATE, BytesTypeClass)
register_custom_type(AGG_STATE, BytesTypeClass)
register_custom_type(DORIS_ARRAY, ArrayTypeClass)
register_custom_type(DORIS_MAP, RecordTypeClass)
register_custom_type(DORIS_STRUCT, RecordTypeClass)
register_custom_type(DORIS_JSONB, RecordTypeClass)
register_custom_type(VARIANT, RecordTypeClass)
register_custom_type(LARGEINT, NumberTypeClass)
register_custom_type(IPV4, StringTypeClass)
register_custom_type(IPV6, StringTypeClass)


@functools.lru_cache(maxsize=None)
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
            # Leave anything that isn't a plain catalog.database pair alone and let
            # the server reject it.
            if catalog_part and database_part and "." not in database_part:
                if self.catalog and self.catalog != catalog_part:
                    raise ValueError(
                        f"database '{self.database}' does not match catalog '{self.catalog}'"
                    )
                self.catalog = catalog_part
                self.database = database_part
        if self.catalog and not _CATALOG_NAME_PATTERN.fullmatch(self.catalog):
            raise ValueError(
                f"catalog '{self.catalog}' is not a valid Doris identifier: "
                "it must start with a letter or underscore and contain only "
                "letters, digits, underscores and hyphens"
            )
        return self


@dataclass
class DorisSourceReport(SQLSourceReport):
    # Views whose lineage was dropped because they reference another Doris
    # catalog, which this run does not ingest. Should be 0 on a clean run.
    cross_catalog_views_skipped: int = 0
    # Expected to be nonzero on a healthy Doris instance that has async materialized
    # views, which Doris refuses to return DDL for. Only worth investigating if the
    # count exceeds the number of async MVs, or the unexpected-error warning appears.
    tables_reflected_without_keys: int = 0
    # Nonzero means DESCRIBE failed on a table that reflected fine otherwise, so its
    # Doris-specific column types were downgraded to MySQL equivalents.
    tables_with_unreflected_types: int = 0


@platform_name("Apache Doris", id="doris")
@config_class(DorisConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class DorisSource(MySQLSource):
    config: DorisConfig
    report: DorisSourceReport

    def __init__(self, config: DorisConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.report: DorisSourceReport = DorisSourceReport()
        # The base class wired these against the report it built in
        # super().__init__(), so re-point them at the report we actually use.
        self.classification_handler.report = self.report
        self.report.sql_aggregator = self.aggregator.report

        self._session_catalog: Optional[str] = config.catalog
        self._catalog_detection_failed = False

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

    def _detect_current_catalog(self, conn: Connection) -> Optional[str]:
        try:
            row = conn.execute(text("SELECT CURRENT_CATALOG()")).fetchone()
        except Exception:
            # A session that really is in an external catalog then fails every
            # per-database reconnect with `Unknown database`, so leave a trail
            # that ties those failures back to detection.
            self._catalog_detection_failed = True
            logger.warning("CURRENT_CATALOG() unavailable", exc_info=True)
            return None
        if not row or row[0] is None:
            return None
        catalog = str(row[0]).strip()
        if not _CATALOG_NAME_PATTERN.fullmatch(catalog):
            return None
        return catalog

    def _switch_catalog(self, conn: Connection, catalog: str) -> None:
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
        try:
            with engine.connect() as conn:
                if self.config.catalog and not self.config.database:
                    self._switch_catalog(conn, self.config.catalog)
                    self._session_catalog = self.config.catalog
                else:
                    self._session_catalog = (
                        self.config.catalog or self._detect_current_catalog(conn)
                    )

                databases = self._get_database_list(inspect(conn))
        finally:
            # Only used to list databases; dispose so it does not hold a pooled
            # connection open for the whole reflection/profiling run.
            engine.dispose()

        for db in databases:
            short_db = self._short_database_name(db)
            if not self.config.database_pattern.allowed(short_db):
                continue

            qualified_db = self._qualified_database(short_db)
            db_engine = None
            try:
                db_url = self.config.get_sql_alchemy_url(current_db=qualified_db)
                db_engine = create_engine(db_url, **self.config.options)
                db_conn = db_engine.connect()
            except Exception as e:
                if db_engine is not None:
                    db_engine.dispose()
                context = qualified_db
                if self._catalog_detection_failed:
                    context = f"{context} (catalog detection failed)"
                self.report.failure(
                    title="Failed to connect to database",
                    message="Skipping database due to connection error.",
                    context=context,
                    exc=e,
                )
                continue

            try:
                # Invariant: the caller must finish reflection + profiling for this
                # database before asking for the next inspector — the finally below
                # tears the engine down on resume.
                yield inspect(db_conn)
            finally:
                # In finally, not after the yield: an early close (pipeline abort, a
                # fatal error elsewhere) throws GeneratorExit at the yield, and the
                # degradations this database already recorded would otherwise be lost.
                self._report_reflection_fallbacks(db_conn)
                db_conn.close()
                db_engine.dispose()

    def get_db_name(self, inspector: Inspector) -> str:
        db_name = super().get_db_name(inspector)
        return self._short_database_name(db_name)

    def _report_reflection_fallbacks(self, conn: Connection) -> None:
        """Surface the reflection degradations the dialect recorded.

        Called once the caller has finished with a database's inspector, so the
        dialect has recorded every table it fell back on.
        """
        dialect = conn.dialect
        if not isinstance(dialect, DorisDialect):
            return

        for full_name, fallback in dialect.pop_reflection_fallbacks().items():
            self.report.tables_reflected_without_keys += 1
            if fallback.expected:
                self.report.warning(
                    title="Table reflected without keys or comment",
                    message="SHOW CREATE TABLE failed, so the table was reflected from DESCRIBE: columns are complete but keys, foreign keys and the table comment are missing.",
                    context=f"{full_name}: {fallback.error}",
                )
            else:
                self.report.warning(
                    title="Table reflected without keys or comment after an unexpected error",
                    message="SHOW CREATE TABLE failed for a reason Doris is not known to reject. DESCRIBE succeeded, so columns are complete, but keys, foreign keys and the table comment are missing. Check the account's grants on this table.",
                    context=f"{full_name}: {fallback.error}",
                )

        for full_name, error in dialect.pop_type_overlay_failures().items():
            self.report.tables_with_unreflected_types += 1
            self.report.warning(
                title="Doris column types unavailable",
                message="DESCRIBE failed, so column types fall back to MySQL reflection: Doris-specific types such as HLL, BITMAP and VARIANT are reported as their closest MySQL equivalent instead.",
                context=f"{full_name}: {error}",
            )

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

    def _known_catalogs(self) -> Set[str]:
        catalogs = {DORIS_INTERNAL_CATALOG}
        if self.config.catalog:
            catalogs.add(self.config.catalog)
        if self._session_catalog:
            catalogs.add(self._session_catalog)
        return catalogs

    def _get_view_definition(self, inspector: Inspector, schema: str, view: str) -> str:
        view_definition = super()._get_view_definition(inspector, schema, view)
        if not view_definition:
            return view_definition

        active_catalog = self._active_catalog() or DORIS_INTERNAL_CATALOG
        view_definition = _catalog_prefix_pattern(active_catalog).sub(
            "", view_definition
        )

        foreign_catalogs = sorted(
            catalog
            for catalog in self._known_catalogs() - {active_catalog}
            if _catalog_prefix_pattern(catalog).search(view_definition)
        )
        if foreign_catalogs:
            # Stripping another catalog's prefix would resolve its tables against
            # the active catalog's databases, pointing the edge at whatever table
            # shares that name here. No edge beats a wrong edge.
            self.report.cross_catalog_views_skipped += 1
            self.report.warning(
                title="View lineage skipped for cross-catalog reference",
                message=(
                    "The view reads from another Doris catalog, which this run "
                    "does not ingest, so its lineage is dropped rather than "
                    "pointed at same-named tables in the ingested catalog. "
                    "Ingest that catalog with its own recipe to get these edges"
                ),
                context=(
                    f"{schema}.{view}, catalogs={foreign_catalogs}, "
                    f"active_catalog={active_catalog}"
                ),
            )
            return ""

        return view_definition
