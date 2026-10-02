from typing import (
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

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import echoed
from datahub.ingestion.agent.sql_passthrough import CatalogRows, SqlCatalogPassthrough
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_identifier_resolver import resolve_listed_name

# SQLAlchemy and sqlglot disagree on a handful of dialect names. An unmapped
# name is passed through so the scope check refuses it rather than guessing a
# grammar (see sql_gate._resolve_dialect).
_SQLALCHEMY_TO_SQLGLOT_DIALECT: Dict[str, str] = {
    "postgresql": "postgres",
    # CockroachDB implements the Postgres wire protocol and dialect, so this names
    # the grammar it actually speaks rather than guessing a near-enough one.
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
# A `table` argument may name any relation ingestion reflects: `columns`
# documents views, and SQLAlchemy 2's Postgres lists materialized views apart
# from both. Tried in order, each only on a miss in the one before.
_TABLE_LISTINGS: Tuple[_Listing, ...] = ("tables", "views", "materialized_views")
_VIEW_LISTINGS: Tuple[_Listing, ...] = ("views", "materialized_views")


def _pinned_containers(config: object, container_kind: str) -> FrozenSet[str]:
    """Which containers this recipe reads, if it names them.

    Two-tier only: on a three-tier source `database` names the database the
    connection opens on, not a filter over the schemas `containers` returns,
    so narrowing to it there would hide every other schema in the very
    database being probed.

    Singular first, plural as the fallback -- the precedence ingestion uses,
    not a union of the two. TeradataSource.get_inspectors is explicit about
    it:

        if self.config.database and self.config.database != "":
            databases = [self.config.database]
        elif self.config.databases:
            databases = list(self.config.databases)
        else:
            databases = <everything>

    So a recipe that sets both -- a connection default plus an ingest list --
    walks only the singular one, and unioning them reported databases
    ingestion never opens: the same mismatch this pin exists to close, one
    size smaller. Teradata is the only connector offering both, and for the
    ones offering just `database` the two rules agree.
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


def _container_normalizer(config: object) -> Callable[[str], str]:
    """How this connector spells a listed container for ingestion.

    Identity for almost everyone. Doris needs it: on an external-catalog
    connection the server may list `iceberg_catalog.sales` where ingestion
    matches `sales`, and the probe has to report what ingestion matches --
    a caller passes `containers` output straight back as --parent.
    """
    hook = getattr(config, "probe_normalize_container", None)
    if callable(hook):
        return lambda name: str(hook(name))
    return lambda name: name


class SqlAlchemyMetadataProbe(SqlCatalogPassthrough):
    """Metadata-only probe methods backed by the SQLAlchemy Inspector.

    Every SQLAlchemy-based SQL connector inherits these. No method runs
    user-supplied SQL or reads table rows.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._insp = inspect(engine)

    # `containers` returns Schemas on a three-tier source and Databases on a two-tier
    # one, and this class serves both -- so the kind comes from the recipe's config,
    # primed in for_config and read back by run_probe_method.
    kind_overrides: Dict[str, str] = {}

    # The containers this recipe reads, when it names any -- primed in
    # for_config, since only the config knows. Empty on a three-tier source,
    # and on a two-tier one that enumerates every database.
    #
    # A set rather than one name because naming several is a supported shape:
    # Teradata's `databases` is documented as "List of databases to ingest",
    # and reading only the singular `database` left such a recipe unpinned --
    # so `containers` reported every database on the server while ingestion
    # enumerated the configured two.
    pinned_containers: FrozenSet[str] = frozenset()

    # How a listed container is spelled for ingestion; see
    # _container_normalizer. Identity unless the connector says otherwise.
    container_normalizer: Callable[[str], str] = staticmethod(lambda name: name)

    # Listing caches. Created on first use, not in __init__: tests build this
    # class with __new__ and subclasses may bring their own constructor. One
    # probe instance serves one command, so they never go stale.
    _schema_listing: Optional[List[str]] = None
    _relation_listings: Optional[Dict[Tuple[str, str], List[str]]] = None

    # SECURITY: every caller-supplied schema/table/view passes through the
    # resolvers below before any Inspector reflection call. Several dialects
    # (sqlalchemy-redshift, Vertica, Teradata, ClickHouse, Druid, Databricks)
    # format these arguments into their reflection SQL, which the `sql` gate
    # never sees; resolving against the server's own listing means reflection
    # only ever receives a string the server produced.
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
        """One relation listing.

        A fallback listing is consulted only after the primary one missed and
        can only turn a refusal into a match, so when it cannot be read it
        counts as empty and a typo still exits 2. Some dialects inherit a
        listing query their server cannot run (Redshift's materialized-view
        query names a column Redshift lacks). The primary listing keeps its
        errors: those are a connection problem (exit 3), not a bad argument.
        """
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
        except DBAPIError:
            if fallback:
                return []
            raise

    def _container_label(self) -> str:
        return str(self.kind_overrides.get("containers", "schema")).lower()

    def _resolve_schema(self, schema: str) -> str:
        def accepted() -> Iterator[str]:
            for raw in self._listed_schemas():
                yield raw
                # What `containers` reports, which callers pass straight
                # back. Derived from the server's string, so still never the
                # caller's.
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
        relation = resolve_listed_name(
            name,
            candidates,
            what="table, view or materialized view" if what == "table" else what,
            where=f"in {self._container_label()} {echoed(on_schema)}",
            list_command=(
                f"tables --schema {on_schema!r}` or `views --schema {on_schema!r}"
            ),
        )
        return on_schema, relation

    @classmethod
    def for_config(cls, config: SQLCommonConfig) -> "SqlAlchemyMetadataProbe":
        """Build over an engine of this recipe's own making.

        Engine construction lives here rather than on the config because the
        provider is what needs it, and because `probe_provider_class()` is then
        the config's only statement about which provider it has.
        """
        # lazy: keep sqlalchemy engine construction off the config import path
        from sqlalchemy import create_engine

        from datahub.ingestion.source.sql.sql_probe import (
            effective_budget,
            engine_options,
            install_statement_timeout,
        )

        # The budget rides on the engine rather than on each statement, because
        # that is the one construction point the whole SQLAlchemy family shares --
        # wiring it per connector would be fifteen chances to forget. It also means
        # the Inspector below inherits it, so the typed listings are bounded too and
        # not just `sql`.
        # probe_sql_alchemy_url where a connector declares one, so a
        # connector whose probe must dial somewhere other than the default
        # says so without changing what every other caller gets. Doris is
        # the case: an external-catalog recipe has to connect to
        # `catalog.database`, which is what ingestion uses, while
        # get_sql_alchemy_url() stays as it was for usage and profiling.
        probe_url = getattr(config, "probe_sql_alchemy_url", None)
        url = probe_url() if callable(probe_url) else config.get_sql_alchemy_url()
        engine = create_engine(url, **engine_options(config, budget=cls.query_budget))
        # Dialects whose ceiling cannot ride on connect_args get it here instead,
        # applied per connection where a wrong variable name is survivable.
        install_statement_timeout(engine, url, cls.query_budget.timeout_seconds)
        # Whatever the connector does to its own engine that a bare create_engine
        # does not. Called before the Inspector is built, since a replaced dialect
        # has to be in place by then to have any effect.
        config.probe_prepare_engine(engine)
        probe = cls(engine)
        # Report what this dialect actually enforces, not what the class declared:
        # only some dialects have a knob to apply the timeout through.
        probe.query_budget = effective_budget(url, cls.query_budget)
        # One provider class serves ~15 dialects, so the catalog surface cannot be a
        # class attribute here -- it comes from the connector's own config, which is
        # per dialect.
        probe.catalog_scope = config.probe_catalog_scope()
        probe.kind_overrides = cls.probe_kind_overrides(config)
        probe.pinned_containers = _pinned_containers(
            config, str(config.probe_container_kind())
        )
        probe.container_normalizer = staticmethod(_container_normalizer(config))  # type: ignore[assignment]
        return probe

    def __exit__(self, *exc: object) -> None:
        self._engine.dispose()

    @property
    def sql_dialect(self) -> str:
        return sqlglot_dialect_for(self._engine.dialect.name)

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        with self._engine.connect() as conn:
            # exec_driver_sql, not execute(text(...)): text() parses the SQL
            # for `:name` bind parameters, and its regex fires on a colon
            # after any non-word character -- inside a string literal, or an
            # array slice. `WHERE column_default = '{"k":v}'` and
            # `SELECT a[:2] ...` both become queries with an unbound
            # parameter, so a query the gate has already cleared dies at
            # execute with StatementError, which recipe_cli's fallback maps
            # to EXIT_CONNECTION -- telling the agent the source is
            # unreachable when the connection was fine. This SQL is opaque
            # passthrough and must not be reinterpreted.
            result = conn.exec_driver_sql(query)
            rows = result.fetchmany(limit)
            return CatalogRows(
                columns=list(result.keys()), rows=[list(row) for row in rows]
            )

    @classmethod
    def probe_kind_overrides(cls, config: SQLCommonConfig) -> Dict[str, str]:
        """Kinds this provider cannot declare on the class, keyed by command.

        `containers` returns Schemas on a three-tier source and Databases on a
        two-tier one, and one provider class serves both -- so the kind comes
        from the recipe, not the decorator. Connection-free: it reads a
        classmethod on the config, which is why `probe methods` can apply it
        too rather than reporting null and leaving the caller to find out by
        running the command.
        """
        return {"containers": str(config.probe_container_kind())}

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
        # Normalized before anything else looks at them: the caller passes
        # these straight back as --parent, so they have to be the spelling
        # ingestion matches on. Deduplicated because two server spellings
        # can normalize to one database, and reporting it twice would read
        # as two.
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
        """Views in one schema, judged by view_pattern. Separate from `tables` for the
        reason given there."""
        return self._listed(self._resolve_schema(schema), "views")[:limit]

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
