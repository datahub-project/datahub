from types import SimpleNamespace
from typing import Any, Callable, List

from datahub.ingestion.agent.probe import ClassifyContext
from datahub.ingestion.source.sql.sql_probe import (
    _classify_container,
    _identifier_target,
    _shim_inspector,
)


def _ignore_warn(message: str) -> None:
    """Default `warn` sink for tests that don't exercise a degrade path."""


class _WarningCollector:
    """Test-only `warn` sink mirroring ClientProbe.list_children's dedup, so a
    unit test can assert on collected messages without going through a real
    probe call."""

    def __init__(self) -> None:
        self.messages: List[str] = []

    def __call__(self, message: str) -> None:
        if message not in self.messages:
            self.messages.append(message)


def _ctx(
    config: Any,
    schema: str,
    entity: str,
    warn: Callable[[str], None] = _ignore_warn,
) -> ClassifyContext:
    return ClassifyContext(
        config=config,
        name=entity,
        fqn=f"{schema}.{entity}",
        pattern_field="table_pattern",
        parent_path=(schema,),
        warn=warn,
    )


def _container_ctx(config: Any, schema: str) -> ClassifyContext:
    # The Schema level is the top of the (schema-top) SQL_PROBE hierarchy, so
    # it has no parent -- ctx.parent_path is empty and ctx.fqn is just the
    # bare schema name, mirroring how ClientProbe._nodes_for_level builds it.
    return ClassifyContext(
        config=config,
        name=schema,
        fqn=schema,
        pattern_field="schema_pattern",
        parent_path=(),
        warn=_ignore_warn,
    )


def test_shim_matches_get_identifier_for_each_sql_source():
    """The probe's filter target is whatever the source's own get_identifier
    says. trino/druid/mysql override get_identifier at the Config level, so
    the real function is directly reachable and checked here without an
    instantiated Source; postgres overrides it at the Source level instead,
    so it gets its own (stronger) check in
    test_shim_matches_a_real_postgres_source_instance below. Verified values:

        postgres, database from the connection : mydb.public.orders
        postgres, database from config         : explicit_db.public.orders
        trino   (config-level get_identifier)  : hive.public.orders
        druid   (config-level get_identifier)  : orders
        mysql   (config-level get_identifier)  : app.orders
    """
    from datahub.ingestion.source.sql.druid import DruidConfig
    from datahub.ingestion.source.sql.mysql import MySQLConfig
    from datahub.ingestion.source.sql.trino import TrinoConfig

    # Kept as separate, concretely-typed assertions (rather than one loop over
    # a mixed list) so each config's own get_identifier is checked against its
    # own declared signature, not a common supertype that doesn't declare it.
    trino_config = TrinoConfig(host_port="localhost:8080", database="hive")
    assert trino_config.get_identifier(schema="public", table="orders") == (
        "hive.public.orders"
    )
    assert _identifier_target(_ctx(trino_config, "public", "orders")) == (
        "hive.public.orders"
    )

    druid_config = DruidConfig(host_port="localhost:8082")
    assert druid_config.get_identifier(schema="public", table="orders") == "orders"
    assert _identifier_target(_ctx(druid_config, "public", "orders")) == "orders"

    mysql_config = MySQLConfig(host_port="localhost:3306")
    assert mysql_config.get_identifier(schema="app", table="orders") == "app.orders"
    assert _identifier_target(_ctx(mysql_config, "app", "orders")) == "app.orders"


def test_shim_matches_a_real_postgres_source_instance():
    """Postgres overrides get_identifier at the Source level (not the
    Config), so the strongest check is against a fully constructed
    PostgresSource -- not the probe's shim -- fed the exact same pure
    inspector stand-in. Proves __new__(PostgresSource) resolves get_db_name
    identically to a real instance, not just plausibly. Covers both of
    postgres's branches: database taken from the live connection, and
    database pinned explicitly in the recipe."""
    from typing import cast

    from sqlalchemy.engine.reflection import Inspector

    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource

    from_connection = PostgresConfig(
        host_port="localhost:5432",
        sqlalchemy_uri="postgresql://user@localhost:5432/mydb",
    )
    from_config = PostgresConfig(
        host_port="localhost:5432",
        sqlalchemy_uri="postgresql://user@localhost:5432/mydb",
        database="explicit_db",
    )
    for config, expected in [
        (from_connection, "mydb.public.orders"),
        (from_config, "explicit_db.public.orders"),
    ]:
        real_source = PostgresSource(config, PipelineContext(run_id="test"))
        # cast: _shim_inspector's stand-in is deliberately not a real
        # Inspector (see sql_probe.py), only pure-parseable-URL-shaped.
        real_target = real_source.get_identifier(
            schema="public",
            entity="orders",
            inspector=cast(Inspector, _shim_inspector(config)),
        )
        assert real_target == expected
        assert _identifier_target(_ctx(config, "public", "orders")) == real_target


def test_druid_target_stays_the_bare_table_name():
    """Druid is the canary: pydruid already formats table names fully
    qualified, so DruidConfig.get_identifier drops the schema entirely. Any
    structural (schema.table) rule would get this wrong -- if this test ever
    starts failing, the fix has gone structural again."""
    from datahub.ingestion.source.sql.druid import DruidConfig

    config = DruidConfig(host_port="localhost:8082")
    ctx = _ctx(config, "public", "orders")
    assert _identifier_target(ctx) == "orders"


def test_db2_shim_still_applies_the_uppercase_db_name_override():
    """Db2Source overrides get_db_name (uppercasing it), and its
    get_identifier calls self.get_db_name -- so the shim must be a real
    (uninitialized) Db2Source instance, not a generic stand-in carrying only
    the base get_db_name, or this override's super() call would either raise
    or silently skip the uppercasing."""
    from datahub.ingestion.source.sql.db2 import Db2Config

    config = Db2Config(host_port="localhost:50000", database="mydb")
    ctx = _ctx(config, "public", "orders")
    assert _identifier_target(ctx) == "MYDB.public.orders"


def test_starrocks_shim_primes_current_catalog_to_its_init_state():
    """StarRocksSource.get_identifier reads self._current_catalog, which
    real ingestion reassigns before every table (get_inspectors sets it once
    per catalog, ahead of enumerating that catalog's tables) -- so it is
    never actually None while any table is being classified. At __init__,
    though, it IS None, and get_identifier's own fallback for a None catalog
    is the literal "default_catalog" -- StarRocks's name for its built-in
    internal catalog that most tables actually live in. Priming the shim to
    that same __init__ value (mirroring current_database's mssql handling)
    turns what used to be an AttributeError fallback into a real answer, not
    a guess: this is exactly the failure mode a recipe with
    table_pattern.allow: ["default_catalog\\.analytics\\..*"] hit before this
    fix (every table reported excluded_by: table_pattern while ingestion
    ingested them all). No warning: this is a real (if partial -- external
    catalogs still can't be resolved) answer, not a degrade."""
    from datahub.ingestion.source.sql.starrocks import StarRocksConfig

    config = StarRocksConfig()
    warn = _WarningCollector()
    ctx = _ctx(config, "analytics", "orders", warn=warn)
    assert _identifier_target(ctx) == "default_catalog.analytics.orders"
    assert warn.messages == []


def test_attribute_error_fallback_message_excludes_fqn_so_dedupe_works(monkeypatch):
    """Regression guard: ctx.warn dedupes on message identity (see
    ClientProbe.list_children's warn closure), but an earlier version of this
    message embedded ctx.fqn, which is different for every node -- defeating
    the dedupe and, per a whole-plan review, flooding ProbeResult.warnings
    with one near-identical entry per table (measured: 200 for 200 StarRocks
    tables, before StarRocks itself was fixed to no longer hit this path at
    all -- see test_starrocks_shim_primes_current_catalog_to_its_init_state
    above). Faking the AttributeError here (rather than relying on a real
    connector) keeps this test valid regardless of which real connectors do
    or don't exercise the fallback at any given time.
    """
    import datahub.ingestion.source.sql.sql_probe as sql_probe_module

    class _FakeSource:
        def get_identifier(self, *, schema, entity, inspector):
            raise AttributeError("'_FakeSource' object has no attribute '_never_set'")

    monkeypatch.setattr(
        sql_probe_module, "_source_class_for", lambda config: _FakeSource
    )
    config = SimpleNamespace(get_sql_alchemy_url=lambda: "sqlite://")

    warn = _WarningCollector()
    for entity in ("orders", "sessions", "customers"):
        ctx = _ctx(config, "public", entity, warn=warn)
        # Falls back to the plain fqn for every node -- this assertion
        # doesn't change; only how many warnings that produces does.
        assert _identifier_target(ctx) == ctx.fqn
    # Before this fix: 3 distinct messages (one per fqn). After: 1.
    assert len(warn.messages) == 1
    assert "_FakeSource" in warn.messages[0]
    assert "_never_set" in warn.messages[0]


def test_redshift_probe_filter_target_includes_the_database_segment():
    """RedshiftSource doesn't extend SQLAlchemySource, so the generic shim has
    no get_identifier to call -- RedshiftConfig.probe_filter_target supplies
    ingestion's own "database.schema.table" target instead (see
    redshift.py's _process_table / _process_view / cache_tables_and_views,
    which all match table_pattern/view_pattern against that same string)."""
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    config = RedshiftConfig(host_port="localhost:5439", database="analytics")
    assert _identifier_target(_ctx(config, "public", "orders")) == (
        "analytics.public.orders"
    )


def test_unity_catalog_probe_filter_target_includes_the_catalog_segment():
    """UnityCatalogSource also doesn't extend SQLAlchemySource; process_tables
    (source.py) matches table_pattern against table.ref.qualified_table_name,
    i.e. "catalog.schema.table". A recipe pinning exactly one catalog gives
    UnityCatalogSourceConfig.probe_filter_target an unambiguous answer -- the
    normal, non-degraded case, so it must record no warning."""
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    config = UnityCatalogSourceConfig.model_validate(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "catalogs": ["main"],
        }
    )
    warn = _WarningCollector()
    assert _identifier_target(_ctx(config, "public", "orders", warn=warn)) == (
        "main.public.orders"
    )
    assert warn.messages == []


def test_unity_catalog_probe_filter_target_falls_back_without_one_pinned_catalog():
    """Without exactly one catalog pinned (none, or several), there is no
    single catalog to prepend without guessing -- so this falls back to the
    generic shim's plain "schema.entity" rather than fabricating an answer.
    That degrade must be visible to whatever reads ProbeResult.warnings, not
    just explained in a docstring -- a plausible-looking two-part target with
    no accompanying warning is exactly the silent-mismatch defect this whole
    stage exists to remove, so both ambiguous shapes (no catalogs, several
    catalogs) must record one."""
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    no_catalogs = UnityCatalogSourceConfig.model_validate(
        {"token": "token", "workspace_url": "https://workspace_url"}
    )
    warn = _WarningCollector()
    assert _identifier_target(_ctx(no_catalogs, "public", "orders", warn=warn)) == (
        "public.orders"
    )
    assert len(warn.messages) == 1
    assert "unity-catalog" in warn.messages[0]
    assert "catalogs" in warn.messages[0]

    several_catalogs = UnityCatalogSourceConfig.model_validate(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "catalogs": ["main", "other"],
        }
    )
    warn = _WarningCollector()
    assert _identifier_target(
        _ctx(several_catalogs, "public", "orders", warn=warn)
    ) == ("public.orders")
    assert len(warn.messages) == 1
    assert "unity-catalog" in warn.messages[0]


def test_unity_catalog_probe_warning_is_not_duplicated_per_node():
    """A single connector-wide reason must not be appended once per node
    classified in one list_children() call -- see ClientProbe.list_children's
    warn dedup. Simulates classifying two different tables under the same
    ambiguous config within one probe call."""
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    config = UnityCatalogSourceConfig.model_validate(
        {"token": "token", "workspace_url": "https://workspace_url"}
    )
    warn = _WarningCollector()
    _identifier_target(_ctx(config, "public", "orders", warn=warn))
    _identifier_target(_ctx(config, "public", "sessions", warn=warn))
    assert len(warn.messages) == 1


def test_redshift_schema_verdict_matches_fully_qualified_name_when_enabled():
    """redshift.py's cache_tables_and_views (and _process_table/_process_view)
    all gate schema iteration through is_schema_allowed(schema_pattern,
    schema, database, match_fully_qualified_names) -- so once that flag is
    on, ingestion checks "database.schema" against schema_pattern, not the
    bare schema name alone. sql_probe.py's generic _classify_container only
    ever checks the bare name; RedshiftConfig.probe_schema_verdict_override
    must correct that for Redshift specifically, the same way
    probe_filter_target corrects the Table level below it."""
    from datahub.configuration.common import AllowDenyPattern
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    bare_name_deny = RedshiftConfig(
        host_port="localhost:5439",
        database="analytics",
        match_fully_qualified_names=True,
        schema_pattern=AllowDenyPattern(deny=[r"^public$"]),
    )
    # A deny anchored to the bare schema name no longer excludes once
    # match_fully_qualified_names is on: ingestion checks "analytics.public".
    verdict = _classify_container(_container_ctx(bare_name_deny, "public"))
    assert verdict.included is True
    assert verdict.excluded_by is None

    fully_qualified_deny = RedshiftConfig(
        host_port="localhost:5439",
        database="analytics",
        match_fully_qualified_names=True,
        schema_pattern=AllowDenyPattern(deny=[r"^analytics\.public$"]),
    )
    verdict = _classify_container(_container_ctx(fully_qualified_deny, "public"))
    assert verdict.included is False
    assert verdict.excluded_by == "schema_pattern"


def test_redshift_schema_verdict_unchanged_when_flag_is_off():
    """match_fully_qualified_names defaults to False, so
    probe_schema_verdict_override must be a no-op (return None) in that case
    -- the bare-name check stays exactly as every other SQL connector's
    does, matching Redshift's own real behavior when the flag is unset."""
    from datahub.configuration.common import AllowDenyPattern
    from datahub.ingestion.source.redshift.config import RedshiftConfig

    config = RedshiftConfig(
        host_port="localhost:5439",
        database="analytics",
        schema_pattern=AllowDenyPattern(deny=[r"^public$"]),
    )
    verdict = _classify_container(_container_ctx(config, "public"))
    assert verdict.included is False
    assert verdict.excluded_by == "schema_pattern"
