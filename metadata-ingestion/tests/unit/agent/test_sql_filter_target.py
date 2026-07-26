from datahub.ingestion.agent.probe import ClassifyContext
from datahub.ingestion.source.sql.sql_probe import (
    _identifier_fallback_warnings,
    _identifier_target,
    _shim_inspector,
)


def _ctx(config, schema, entity):
    return ClassifyContext(
        config=config,
        name=entity,
        fqn=f"{schema}.{entity}",
        pattern_field="table_pattern",
        parent_path=(schema,),
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
        real_target = real_source.get_identifier(
            schema="public", entity="orders", inspector=_shim_inspector(config)
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


def test_starrocks_missing_current_catalog_falls_back_to_fqn_with_a_warning():
    """StarRocksSource.get_identifier reads self._current_catalog, set only
    while a real ingestion run iterates catalogs -- source state this probe
    shim has no equivalent for. That must degrade to the plain fqn and leave
    a visible trace, not silently produce a wrong target."""
    from datahub.ingestion.source.sql.starrocks import StarRocksConfig

    config = StarRocksConfig()
    ctx = _ctx(config, "public", "orders")
    token = _identifier_fallback_warnings.set([])
    try:
        assert _identifier_target(ctx) == ctx.fqn
        warnings = _identifier_fallback_warnings.get()
        assert warnings is not None and len(warnings) == 1
        assert "StarRocksSource" in warnings[0]
        assert "_current_catalog" in warnings[0]
    finally:
        _identifier_fallback_warnings.reset(token)


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
    token = _identifier_fallback_warnings.set([])
    try:
        assert _identifier_target(_ctx(config, "public", "orders")) == (
            "main.public.orders"
        )
        assert _identifier_fallback_warnings.get() == []
    finally:
        _identifier_fallback_warnings.reset(token)


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
    token = _identifier_fallback_warnings.set([])
    try:
        assert _identifier_target(_ctx(no_catalogs, "public", "orders")) == (
            "public.orders"
        )
        warnings = _identifier_fallback_warnings.get()
        assert warnings is not None and len(warnings) == 1
        assert "unity-catalog" in warnings[0]
        assert "catalogs" in warnings[0]
    finally:
        _identifier_fallback_warnings.reset(token)

    several_catalogs = UnityCatalogSourceConfig.model_validate(
        {
            "token": "token",
            "workspace_url": "https://workspace_url",
            "catalogs": ["main", "other"],
        }
    )
    token = _identifier_fallback_warnings.set([])
    try:
        assert _identifier_target(_ctx(several_catalogs, "public", "orders")) == (
            "public.orders"
        )
        warnings = _identifier_fallback_warnings.get()
        assert warnings is not None and len(warnings) == 1
        assert "unity-catalog" in warnings[0]
    finally:
        _identifier_fallback_warnings.reset(token)


def test_unity_catalog_probe_warning_is_not_duplicated_per_node():
    """A single connector-wide reason must not be appended once per node
    classified in one list_children() call -- see _record_identifier_fallback's
    dedup. Simulates classifying two different tables under the same
    ambiguous config within one probe call."""
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    config = UnityCatalogSourceConfig.model_validate(
        {"token": "token", "workspace_url": "https://workspace_url"}
    )
    token = _identifier_fallback_warnings.set([])
    try:
        _identifier_target(_ctx(config, "public", "orders"))
        _identifier_target(_ctx(config, "public", "sessions"))
        warnings = _identifier_fallback_warnings.get()
        assert warnings is not None and len(warnings) == 1
    finally:
        _identifier_fallback_warnings.reset(token)
