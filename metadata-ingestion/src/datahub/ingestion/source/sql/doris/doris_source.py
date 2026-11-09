import functools
import logging
import re
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Union,
)

from pydantic import Field, field_validator, model_validator
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import SQLAlchemyError

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
from datahub.ingestion.api.source_protocols import MetadataWorkUnitIterable
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.data_reader import DataReader
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
from datahub.ingestion.source.sql.mysql import (
    MySQLConfig,
    MySQLProfilingConfig,
    MySQLSource,
)
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, register_custom_type
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
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

    profiling: MySQLProfilingConfig = Field(
        default_factory=MySQLProfilingConfig,
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

    def probe_sql_alchemy_url(self) -> str:
        """The URL the PROBE should dial, which is not the default one.

        Ingestion always passes `current_db` explicitly, and for an external
        catalog it passes the QUALIFIED `catalog.database` that Doris expects
        over the MySQL protocol (see _qualified_database). A caller that
        passes nothing got the bare database instead and therefore landed in
        the session's default catalog -- the internal one -- reading a
        different `sales` than ingestion reads.

        The probe is such a caller: sqlalchemy_probe builds its engine from
        `config.get_sql_alchemy_url()`. So `containers`, and every verdict
        derived from that connection, described the wrong catalog. That is
        the probe/ingestion divergence this stage exists to remove, arriving
        through the connection rather than through the identifier.

        Scoped to the probe rather than defaulted on get_sql_alchemy_url().
        The first version defaulted it, on the reasoning that any caller
        naming no database wants this recipe's catalog. That over-reached:
        MySQLSource._usage_connection calls get_sql_alchemy_url() bare, and
        DorisSource inherits it with no override while include_usage_statistics
        lives on MySQLConfig -- so a Doris recipe with a catalog and usage
        enabled had its usage connection silently moved to a different
        catalog by a change meant for the probe.

        A hook the probe asks for keeps the blast radius at the probe. Every
        other caller gets exactly what it got before.
        """
        if not self.database:
            return self.get_sql_alchemy_url()
        return self.get_sql_alchemy_url(
            current_db=self._qualified_database_name(self.database)
        )

    def _qualified_database_name(self, database: str) -> str:
        """`catalog.database` for an external catalog, the bare name otherwise.

        The config-side twin of DorisSource._qualified_database, which cannot
        be reused here: it reads the SOURCE's detected session catalog, and
        a config has no session. Same rule, same internal-catalog exception.
        """
        if self.catalog and self.catalog != DORIS_INTERNAL_CATALOG:
            return f"{self.catalog}.{database}"
        return database

    def probe_normalize_container(self, name: str) -> str:
        """A listed database in the spelling ingestion matches on.

        Which spelling the SQLAlchemy Inspector returns on an
        external-catalog connection is not settled: ingestion enumerates
        with SHOW DATABASES after SWITCH, never through the Inspector, so
        the connector does not answer it -- though _short_database_name
        exists to strip a `catalog.` prefix, so some Doris path does return
        qualified names.

        Normalizing rather than accepting both spellings, which is what this
        did first. Accepting both fixed the filtering and broke what came
        after it: `containers` would emit whichever spelling the server
        used, a caller passes that back as --parent, and get_identifier
        builds `catalog.database.table` while ingestion matches
        `database.table`. A listing carrying both spellings would also
        report one database twice.

        Ingestion strips the prefix (_short_database_name, get_db_name), so
        the probe reports the stripped form too and the pin needs only one
        spelling.
        """
        if self.catalog and name.startswith(f"{self.catalog}."):
            return name[len(self.catalog) + 1 :]
        return name

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


def _lost_connection(inspector: Inspector) -> Optional[Connection]:
    connection = inspector.bind
    if isinstance(connection, Connection) and connection.invalidated:
        return connection
    return None


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
    # Nonzero means the information_schema comment lookup failed for a database, so
    # some or all of its table and column descriptions are missing.
    databases_without_comments: int = 0
    # Tables and views skipped because the connection dropped and could not be
    # re-established; the database's failure entry says where it happened.
    tables_skipped_after_connection_loss: int = 0


@platform_name("Apache Doris", id="doris")
@config_class(DorisConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DESCRIPTIONS,
    "Table and column comments, read from information_schema",
)
@capability(
    SourceCapability.PROBE,
    "Check what a recipe would ingest before running it, with `datahub recipe probe`",
)
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
        # The profiling pass reflects every table again. It neither reads comments
        # (see DorisDialect.read_comments) nor re-reports the reflection
        # degradations the metadata pass already reported.
        self._in_metadata_pass = True
        # Connections that dropped and could not reconnect; the rest of their
        # database is skipped rather than timing out table by table.
        self._unreachable_connections: Set[Connection] = set()

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
                if isinstance(db_engine.dialect, DorisDialect):
                    db_engine.dialect.read_comments = self._in_metadata_pass
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
                if self._in_metadata_pass:
                    self._report_reflection_fallbacks(db_conn)
                self._unreachable_connections.discard(db_conn)
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
                    title="Table reflected without keys",
                    message="SHOW CREATE TABLE failed, so the table was reflected from DESCRIBE: its columns are complete but keys and foreign keys are missing.",
                    context=f"{full_name}: {fallback.error}",
                )
            else:
                self.report.warning(
                    title="Table reflected without keys after an unexpected error",
                    message="SHOW CREATE TABLE failed for a reason Doris is not known to reject. DESCRIBE succeeded, so its columns are complete, but keys and foreign keys are missing. Check the account's grants on this table.",
                    context=f"{full_name}: {fallback.error}",
                )

        for full_name, error in dialect.pop_type_overlay_failures().items():
            self.report.tables_with_unreflected_types += 1
            self.report.warning(
                title="Doris column types unavailable",
                message="DESCRIBE failed, so column types fall back to MySQL reflection: Doris-specific types such as HLL, BITMAP and VARIANT are reported as their closest MySQL equivalent instead.",
                context=f"{full_name}: {error}",
            )

        for database, error in dialect.pop_comment_lookup_failures().items():
            self.report.databases_without_comments += 1
            self.report.warning(
                title="Doris table and column comments unavailable",
                message="Reading comments from information_schema failed, so some or all of this database's tables and columns were ingested without descriptions, which clears the descriptions an earlier run ingested from Doris. Check that the account can read information_schema.TABLES and information_schema.COLUMNS, and that the lookup finishes within the connection's read timeout and Doris' query_timeout.",
                context=f"{database}: {error}",
            )

    def get_profiling_internal(self) -> MetadataWorkUnitIterable:
        self._in_metadata_pass = False
        try:
            yield from super().get_profiling_internal()
        finally:
            self._in_metadata_pass = True

    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> Sequence[Mapping[str, Any]]:
        # Unlike the base class, a reflection error is raised rather than turned
        # into an empty column list: the table would then be emitted with a
        # field-less schema that overwrites the good one. loop_tables reports and
        # skips it instead, or _process_table does when the connection dropped.
        columns = inspector.get_columns(table, schema)
        if not columns:
            self.report.warning(
                message="Missing column information", context=dataset_name
            )
        return columns

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        return self._skip_lost_connection_tables(
            dataset_name,
            inspector,
            super()._process_table(
                dataset_name, inspector, schema, table, sql_config, data_reader
            ),
        )

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        return self._skip_lost_connection_tables(
            dataset_name,
            inspector,
            super()._process_view(dataset_name, inspector, schema, view, sql_config),
        )

    def _skip_lost_connection_tables(
        self,
        dataset_name: str,
        inspector: Inspector,
        workunits: Iterable[Union[SqlWorkUnit, MetadataWorkUnit]],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        # `workunits` is the base class's generator, so nothing has been read yet.
        if self._skip_unreachable(inspector):
            return
        try:
            yield from workunits
        except Exception as e:
            if not self._skip_on_lost_connection(dataset_name, inspector, e):
                raise

    def _skip_unreachable(self, inspector: Inspector) -> bool:
        if inspector.bind not in self._unreachable_connections:
            return False
        self.report.tables_skipped_after_connection_loss += 1
        return True

    def _skip_on_lost_connection(
        self, dataset_name: str, inspector: Inspector, error: Exception
    ) -> bool:
        # loop_tables and loop_views skip a table that raises with only a warning,
        # so stale-entity removal would then delete it. A failure stops that, which
        # is right when the connection dropped: the table still exists, it just
        # could not be read. The error is not re-raised, so the loop does not also
        # warn about the same table.
        connection = _lost_connection(inspector)
        if connection is None:
            return False
        self.report.failure(
            title="Lost the Doris connection while reflecting a table",
            message="The connection dropped while the table or view was being read, so it was skipped rather than ingested incomplete.",
            context=dataset_name,
            exc=error,
        )
        # Until it is rolled back, every later statement on the connection raises
        # PendingRollbackError. Rolling back and reconnecting here lets the next
        # table carry on, in the same catalog and database since both are in the
        # engine URL.
        connection.rollback()
        try:
            connection.execute(text("SELECT 1"))
        except SQLAlchemyError as e:
            # Each remaining table would otherwise wait out its own connect
            # timeouts before failing the same way.
            self._unreachable_connections.add(connection)
            self.report.failure(
                title="Could not reconnect to Doris",
                message="Reconnecting after a dropped connection failed, so the rest of this database's tables and views were skipped. See tables_skipped_after_connection_loss for how many.",
                context=dataset_name,
                exc=e,
            )
        return True

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
