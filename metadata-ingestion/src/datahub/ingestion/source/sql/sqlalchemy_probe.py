"""The SQL family's provider: listings and per-object metadata through the
SQLAlchemy Inspector, which is what ingestion enumerates through, plus the
gated `sql` command.

Every caller-supplied schema, table or view is resolved against the server's
own listing before reflection (sql_identifier_resolver), so reflection only
receives a string the server produced. The engine is the recipe's own,
bounded and labelled by the config's probe_engine_settings.
"""

import re
from dataclasses import replace
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
)

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError, DBAPIError, NoSuchModuleError

from datahub.ingestion.agent.error_policy import (
    errno_code,
    foreign_label,
    generic_error_code,
    sqlstate_code,
)
from datahub.ingestion.agent.probe_methods import (
    declared_kind_overrides,
    probe_method,
)
from datahub.ingestion.agent.provider_helpers import echoed
from datahub.ingestion.agent.sql_gate import SESSION_TEXT_RELATIONS
from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    QueryBudget,
    SqlCatalogPassthrough,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.protocol_probe_settings import (
    ProbeEngineSettings,
    probe_url,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_identifier_resolver import resolve_listed_name
from datahub.ingestion.source.sql.sql_probe import config_only_source

# On the MySQL protocol, information_schema.processlist and innodb_trx hold
# other sessions' SQL text. CatalogScope withholds them by default; the SQL
# family also adds them back to a config's own scope (for_config), since a
# config's URL can name a MySQL-protocol server whatever its type and its scope
# may replace the default exclusions.
MYSQL_SESSION_TEXT_RELATIONS: FrozenSet[str] = SESSION_TEXT_RELATIONS

# SQLAlchemy and sqlglot disagree on a handful of dialect names: the family's
# default spelling for these, under any config that declares none of its own
# (SQLCommonConfig.probe_sqlglot_dialect). An unmapped name is passed through
# so the scope check refuses it rather than guessing a grammar (see
# sql_gate._resolve_dialect).
_SQLALCHEMY_TO_SQLGLOT_DIALECT: Dict[str, str] = {
    "postgresql": "postgres",
    # Speaks the Postgres dialect.
    "cockroachdb": "postgres",
    "awsathena": "athena",
    "teradatasql": "teradata",
}


def sqlglot_dialect_for(sqlalchemy_dialect_name: str) -> str:
    return _SQLALCHEMY_TO_SQLGLOT_DIALECT.get(
        sqlalchemy_dialect_name, sqlalchemy_dialect_name
    )


# Which Inspector listing a relation name is resolved against.
_Listing = Literal["tables", "views", "materialized_views"]
# A `table` argument may name any relation ingestion reflects, materialized
# views included; tried in order, each only on a miss in the one before.
_TABLE_LISTINGS: Tuple[_Listing, ...] = ("tables", "views", "materialized_views")
_VIEW_LISTINGS: Tuple[_Listing, ...] = ("views", "materialized_views")
# How a refusal names a fallback listing it could not read.
_FALLBACK_NOUNS: Dict[_Listing, str] = {
    "views": "view",
    "materialized_views": "materialized-view",
}


def _pinned_containers(config: object, container_kind: str) -> FrozenSet[str]:
    """Which containers this recipe reads, if it names them.

    Two-tier only: on a three-tier source `database` is where the connection
    opens, not a filter over its schemas. The singular `database` wins over
    the plural `databases`, which is ingestion's precedence, not a union.
    """
    if str(container_kind) != str(DatasetContainerSubTypes.DATABASE):
        return frozenset()
    single = getattr(config, "database", None)
    if single:
        return frozenset({str(single)})
    several = getattr(config, "databases", None)
    if isinstance(several, (list, tuple, set, frozenset)):
        return frozenset(str(one) for one in several if one)
    return frozenset()


# Drivers whose errors carry their code as args[0]: pyodbc a SQLSTATE, PyMySQL
# and mysqlclient an errno. Read from no other exception: args[0] of a plain
# ValueError or KeyError is free text or caller data.
_SQLSTATE_ARG_DRIVERS = ("pyodbc",)
_ERRNO_ARG_DRIVERS = ("pymysql", "MySQLdb")


def _driver_code(error: BaseException) -> Optional[str]:
    """The code one driver error carries itself, in its driver's spelling."""
    pgcode = sqlstate_code(getattr(error, "pgcode", None))
    if pgcode:
        return pgcode
    driver = str(getattr(type(error), "__module__", "")).split(".")[0]
    args = getattr(error, "args", ())
    first = args[0] if isinstance(args, tuple) and args else None
    if driver in _SQLSTATE_ARG_DRIVERS:
        return sqlstate_code(first)
    if driver in _ERRNO_ARG_DRIVERS:
        return errno_code(first)
    return None


# A URL scheme as SQLAlchemy reads it (`postgresql+psycopg2`): safe to show,
# unlike the rest of the URL.
_URL_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9_.+-]{0,63}")


def _url_refusal(config: SQLCommonConfig, exc: ArgumentError) -> ProbeArgumentError:
    """What to tell a caller whose recipe URL SQLAlchemy refused, naming only
    its scheme."""
    try:
        scheme: Optional[str] = make_url(probe_url(config)).drivername
    except Exception:
        scheme = None
    if isinstance(exc, NoSuchModuleError) and scheme and _URL_SCHEME.fullmatch(scheme):
        return ProbeArgumentError(
            f"the recipe's connection URL scheme '{scheme}' names no SQLAlchemy "
            f"dialect installed here: correct the scheme, or install the package "
            f"that provides it"
        )
    return ProbeArgumentError(
        f"the recipe's connection URL could not be used "
        f"({type(exc).__name__}): check its scheme and form"
    )


def _container_normalizer(config: SQLCommonConfig) -> Callable[[str], str]:
    """How this connector spells a listed container for ingestion
    (probe_normalize_container): callers pass `containers` output back as
    --parent."""
    return lambda name: str(config.probe_normalize_container(name))


def probe_engine_options(
    config: SQLCommonConfig, settings: ProbeEngineSettings
) -> Dict[str, Any]:
    """The create_engine kwargs: the recipe's `options`, which every engine
    ingestion builds is given, with the dialect's connect_args merged over the
    recipe's own."""
    # Engine kwargs are heterogeneous (a connect_args dict, pool ints, bools).
    options: Dict[str, Any] = dict(config.options)
    if settings.connect_args:
        # A copy: the recipe's own dict stays as the recipe wrote it.
        options["connect_args"] = {
            **(options.get("connect_args") or {}),
            **settings.connect_args,
        }
    return options


def enforced_budget(budget: QueryBudget, settings: ProbeEngineSettings) -> QueryBudget:
    """The budget as these settings enforce it: without its timeout unless
    they apply one, since a ceiling that reads as present and is not is the
    failure QueryBudget warns against."""
    return budget if settings.timeout_applies else replace(budget, timeout_seconds=None)


def build_probe_engine(
    config: SQLCommonConfig, url: str, settings: ProbeEngineSettings
) -> Engine:
    """An engine for `url` carrying the config's probe settings, prepared
    before any Inspector exists so a replaced dialect takes effect.

    One function because a provider may open several (one per database, as
    some connectors' ingestion does), and each must be bounded and labelled
    like the first.
    """
    try:
        engine = create_engine(url, **probe_engine_options(config, settings))
    except ArgumentError as exc:
        # Raised before any connection: the recipe's URL names a dialect
        # nothing provides, or is not a URL. The caller's to fix (exit 2).
        raise _url_refusal(config, exc) from None
    if settings.prepare is not None:
        settings.prepare(engine)
    return engine


class SqlAlchemyMetadataProbe(SqlCatalogPassthrough):
    """Metadata-only probe methods backed by the SQLAlchemy Inspector.

    Every SQLAlchemy-based SQL connector inherits these. No method runs
    user-supplied SQL or reads table rows.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._insp = inspect(engine)

    # What `containers` lists (named in refusals), primed in for_config from
    # the config's probe_kind_overrides.
    container_kind: str = str(DatasetContainerSubTypes.SCHEMA)

    # The containers this recipe reads when it names any (see
    # _pinned_containers); empty means every one.
    pinned_containers: FrozenSet[str] = frozenset()

    # How a listed container is spelled for ingestion; see
    # _container_normalizer. Identity unless the connector says otherwise.
    container_normalizer: Callable[[str], str] = staticmethod(lambda name: name)

    # The config's probe_sqlglot_dialect, set in for_config through
    # sql_dialect; None derives it from the engine's dialect.
    _declared_sqlglot_dialect: Optional[str] = None

    # The connector's own Source, set in for_config (see
    # sql_probe.config_only_source): `views` returns its _get_view_names.
    # None lists get_view_names alone.
    _view_source: Optional[SQLAlchemySource] = None

    # Listing caches. Created on first use, not in __init__: tests build this
    # class with __new__ and subclasses may bring their own constructor. One
    # probe instance serves one command, so they never go stale.
    _schema_listing: Optional[List[str]] = None
    _relation_listings: Optional[Dict[Tuple[str, str], List[str]]] = None
    # (schema, listing) -> the label of the error a fallback listing raised.
    _unread_fallbacks: Optional[Dict[Tuple[str, str], str]] = None

    # SECURITY: every caller-supplied schema/table/view passes through the
    # resolvers below before reflection. Several dialects format these
    # arguments into reflection SQL, which the `sql` gate never sees.
    def _listed_schemas(self) -> List[str]:
        if self._schema_listing is None:
            self._schema_listing = list(self._insp.get_schema_names())
        return self._schema_listing

    def _listed(
        self, schema: str, listing: _Listing, *, fallback: bool = False
    ) -> List[str]:
        if self._relation_listings is None:
            self._relation_listings = {}
        key = (schema, listing)
        cached = self._relation_listings.get(key)
        if cached is None:
            cached = self._fetch_listing(schema, listing, fallback=fallback)
            self._relation_listings[key] = cached
        return cached

    def _fetch_listing(
        self, schema: str, listing: _Listing, *, fallback: bool
    ) -> List[str]:
        """One relation listing. A fallback listing can only turn a refusal
        into a match, so one that cannot be read counts as empty (some dialects
        inherit a query their server cannot run, as Redshift's
        materialized-view listing does) and a typo still exits 2; the refusal
        then says which listing went unread. The primary listing's errors are
        a connection problem (exit 3)."""
        try:
            if listing == "tables":
                return list(self._insp.get_table_names(schema=schema))
            if listing == "views":
                return list(self._insp.get_view_names(schema=schema))
            return list(self._insp.get_materialized_view_names(schema=schema))
        except NotImplementedError:
            # The base Dialect's default for materialized views: this dialect
            # lists none separately, so it has none to resolve against.
            if listing == "materialized_views" or fallback:
                return []
            raise
        except DBAPIError as exc:
            if not fallback:
                raise
            if self._unread_fallbacks is None:
                self._unread_fallbacks = {}
            self._unread_fallbacks[(schema, listing)] = foreign_label(exc, type(self))
            return []

    def _container_label(self) -> str:
        return self.container_kind.lower()

    def _resolve_schema(self, schema: str) -> str:
        def accepted() -> Iterator[str]:
            for raw in self._listed_schemas():
                yield raw
                # What `containers` reports, derived from the server's string.
                yield self.container_normalizer(raw)

        return resolve_listed_name(
            schema,
            accepted(),
            what=self._container_label(),
            where="on this connection",
            list_command="containers",
        )

    def _resolve_relation(
        self, schema: str, name: str, listings: Tuple[_Listing, ...], what: str
    ) -> Tuple[str, str]:
        on_schema = self._resolve_schema(schema)
        candidates = (
            listed
            for listing in listings
            for listed in self._listed(
                on_schema, listing, fallback=listing != listings[0]
            )
        )
        try:
            relation = resolve_listed_name(
                name,
                candidates,
                what="table, view or materialized view" if what == "table" else what,
                where=f"in {self._container_label()} {echoed(on_schema)}",
                list_command=(
                    f"tables --schema {on_schema!r}` or `views --schema {on_schema!r}"
                ),
            )
        except ProbeArgumentError as refusal:
            unread = self._unread_fallbacks or {}
            notes = [
                f"the {_FALLBACK_NOUNS[listing]} listing could not be read "
                f"({unread[(on_schema, listing)]}); if it is one, this "
                f"connection cannot resolve it"
                for listing in listings[1:]
                if (on_schema, listing) in unread
            ]
            if not notes:
                raise
            raise ProbeArgumentError("; ".join([str(refusal), *notes])) from None
        return on_schema, relation

    @classmethod
    def for_config(cls, config: SQLCommonConfig) -> "SqlAlchemyMetadataProbe":
        """Build over an engine of this recipe's own making."""
        # On the engine, so the Inspector's listings are bounded as well as
        # `sql`; how is the config's to declare.
        try:
            # Reads the URL's query arguments, so a URL that is not one is
            # refused here, before any engine exists.
            settings = config.probe_engine_settings(cls.query_budget)
        except ArgumentError as exc:
            raise _url_refusal(config, exc) from None
        probe = cls(build_probe_engine(config, probe_url(config), settings))
        probe.prime_from_config(config, settings)
        return probe

    def prime_from_config(
        self, config: SQLCommonConfig, settings: ProbeEngineSettings
    ) -> None:
        """The per-recipe state for_config sets, apart from the engine, so a
        subclass with a constructor of its own primes the same."""
        self.sql_dialect = config.probe_sqlglot_dialect()
        self.query_budget = enforced_budget(type(self).query_budget, settings)
        # Per dialect, so from the config rather than the class; a config's
        # own scope still withholds MYSQL_SESSION_TEXT_RELATIONS.
        scope = config.probe_catalog_scope()
        self.catalog_scope = replace(
            scope,
            excluded_relations=scope.excluded_relations | MYSQL_SESSION_TEXT_RELATIONS,
        )
        self.container_kind = declared_kind_overrides(config).get(
            "containers", self.container_kind
        )
        self.pinned_containers = _pinned_containers(config, self.container_kind)
        self.container_normalizer = staticmethod(_container_normalizer(config))  # type: ignore[assignment]
        self._view_source = config_only_source(config)

    def __exit__(self, *exc: object) -> None:
        self._engine.dispose()
        super().__exit__(*exc)

    @property
    def probe_report(self) -> object:
        """The report the connector's own view listing warns into, as when
        Postgres cannot list materialized views; read back after each
        command."""
        return None if self._view_source is None else self._view_source.report

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The driver's code for a failure in this family ("SQLSTATE 42P01",
        "errno 1146"). Read on the error and on the driver error SQLAlchemy
        keeps as `.orig`, which a wrapper raised without `from` does not
        chain; `.orig` gets the generic codes too."""
        code = _driver_code(exc)
        orig = getattr(exc, "orig", None)
        if code is None and isinstance(orig, BaseException):
            code = _driver_code(orig) or generic_error_code(orig)
        return code

    @property
    def sql_dialect(self) -> Optional[str]:
        """The config's declared sqlglot dialect, else the engine dialect's
        name in sqlglot's spelling."""
        return self._declared_sqlglot_dialect or sqlglot_dialect_for(
            self._engine.dialect.name
        )

    @sql_dialect.setter
    def sql_dialect(self, declared: Optional[str]) -> None:
        self._declared_sqlglot_dialect = declared

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        with self._engine.connect() as conn:
            # exec_driver_sql, not text(): text() reads a colon inside a
            # literal or an array slice as a bind parameter, failing a query
            # the gate cleared. The SQL is passed through as written.
            result = conn.exec_driver_sql(query)
            rows = result.fetchmany(limit)
            return CatalogRows(
                columns=list(result.keys()), rows=[list(row) for row in rows]
            )

    @probe_method(row_limit_param="limit")
    def containers(self, limit: int = 200) -> List[str]:
        """Every schema this connection can see -- or database, on a two-tier source
        like MySQL; the reported `kind` says which, because the pattern that filters
        them differs. Includes ones the recipe's pattern would exclude, so
        `probe filter` can explain them, and comes from the connector's own Inspector
        rather than a catalog query, so it is the list ingestion itself enumerates.

        A two-tier recipe naming its databases gets those back rather than
        every database on the server: ingestion reads the ones the recipe
        names, so listing the rest reports containers it will never read.
        Singular `database` and plural `databases` both count. The name is checked against the server's own listing rather than
        echoed back, so a typo still shows as absent instead of being
        confirmed."""
        # In ingestion's spelling, as callers pass them back as --parent, and
        # deduplicated: two server spellings can normalize to one.
        seen: Set[str] = set()
        names = []
        for raw in self._listed_schemas():
            name = self.container_normalizer(raw)
            if name not in seen:
                seen.add(name)
                names.append(name)
        pinned = self.pinned_containers
        if pinned:
            names = [n for n in names if n in pinned]
        return names[:limit]

    @probe_method(
        kind=DatasetSubTypes.TABLE, row_limit_param="limit", parent_params=("schema",)
    )
    def tables(self, schema: str, limit: int = 200) -> List[str]:
        """Tables in one schema, excluding views -- the split ingestion makes when it
        applies table_pattern rather than view_pattern. A catalog query against
        information_schema.tables returns both kinds together, so judging that listing
        as tables gives views a verdict from the wrong pattern.

        The schema travels with the result, so `probe filter` needs no --parent.
        An unlisted schema is refused (exit 2) rather than passed to the dialect."""
        return self._listed(self._resolve_schema(schema), "tables")[:limit]

    @probe_method(
        kind=DatasetSubTypes.VIEW, row_limit_param="limit", parent_params=("schema",)
    )
    def views(self, schema: str, limit: int = 200) -> List[str]:
        """Views in one schema, judged by view_pattern: what the connector's own
        ingestion lists as views, so Postgres's materialized views are among them.
        Separate from `tables` for the reason given there."""
        on_schema = self._resolve_schema(schema)
        if self._view_source is None:
            return self._listed(on_schema, "views")[:limit]
        return list(self._view_source._get_view_names(self._insp, on_schema))[:limit]

    @probe_method()
    def foreign_keys(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Foreign-key constraints on a table: each entry lists the local
        constrained columns and the referred schema/table/columns. Use to
        understand cross-table relationships. Metadata only — no row data."""
        on_schema, relation = self._resolve_relation(
            schema, table, _TABLE_LISTINGS, "table"
        )
        return [
            dict(fk) for fk in self._insp.get_foreign_keys(relation, schema=on_schema)
        ]

    @probe_method(name="view_definition")
    def view_definition(self, schema: str, view: str) -> Optional[str]:
        """The stored CREATE VIEW SQL text for a view (DDL, not query results).
        Returns null if the engine does not expose it."""
        on_schema, relation = self._resolve_relation(
            schema, view, _VIEW_LISTINGS, "view"
        )
        return self._insp.get_view_definition(relation, schema=on_schema)

    @probe_method()
    def primary_key(self, schema: str, table: str) -> Dict[str, object]:
        """The primary-key constraint on a table: the constrained column names
        and the constraint name."""
        on_schema, relation = self._resolve_relation(
            schema, table, _TABLE_LISTINGS, "table"
        )
        return dict(self._insp.get_pk_constraint(relation, schema=on_schema))

    @probe_method()
    def indexes(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Indexes on a table: name, indexed column names, and uniqueness."""
        on_schema, relation = self._resolve_relation(
            schema, table, _TABLE_LISTINGS, "table"
        )
        return [dict(ix) for ix in self._insp.get_indexes(relation, schema=on_schema)]

    @probe_method()
    def columns(self, schema: str, table: str) -> List[Dict[str, object]]:
        """Columns of a table or view: name, data type, nullability, default.
        Structural metadata only — no cell values are read. (schema is the
        container name: the SQL schema, or the database for two-tier sources.)"""
        on_schema, relation = self._resolve_relation(
            schema, table, _TABLE_LISTINGS, "table"
        )
        return [
            {
                "name": c["name"],
                "type": str(c["type"]),
                "nullable": c.get("nullable"),
                "default": str(c["default"]) if c.get("default") is not None else None,
            }
            for c in self._insp.get_columns(relation, schema=on_schema)
        ]

    @probe_method()
    def table_comment(self, schema: str, table: str) -> Dict[str, object]:
        """The table's stored comment/description, if any."""
        on_schema, relation = self._resolve_relation(
            schema, table, _TABLE_LISTINGS, "table"
        )
        return dict(self._insp.get_table_comment(relation, schema=on_schema))
