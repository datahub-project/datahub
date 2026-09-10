"""A probe query must carry a cost ceiling, not just a row ceiling.

MAX_PROBE_ITEMS bounds the rows a query *returns*. It does not bound the work the
warehouse does: a query can return three rows and still scan a terabyte, and on a
warehouse that bills by bytes scanned the bill is identical either way. These tests
pin the second ceiling, per connector, because only the connector knows which kind
its driver can ask for.
"""

from typing import List

import pytest
import sqlalchemy

from datahub.ingestion.agent.sql_passthrough import QueryBudget, SqlCatalogPassthrough
from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe
from datahub.ingestion.source.snowflake.snowflake_probe import SnowflakeMetadataProbe
from datahub.ingestion.source.sql.sql_probe import (
    applies_statement_timeout,
    effective_budget,
    engine_options,
    install_statement_timeout,
)


def test_a_provider_that_declares_nothing_still_gets_a_ceiling():
    """The default has to be bounded, or every new connector ships unbounded.

    A connector author who never thinks about cost is the common case; the
    framework's job is to make that case safe rather than to rely on them.
    """
    assert SqlCatalogPassthrough.query_budget.timeout_seconds is not None
    assert SqlCatalogPassthrough.query_budget.timeout_seconds > 0


def test_a_budget_describes_what_it_actually_enforces():
    """The description is the honest bit: it must not imply a ceiling that is absent."""
    both = QueryBudget(timeout_seconds=30, max_bytes_billed=1024)
    assert "30s" in both.describe()
    assert "1024" in both.describe().replace(",", "")

    neither = QueryBudget(timeout_seconds=None, max_bytes_billed=None)
    assert "no server-side ceiling" in neither.describe()


def test_bigquery_refuses_the_job_over_a_byte_ceiling():
    """maximum_bytes_billed is the only ceiling here that bounds spend.

    A timeout bounds how long we wait; BigQuery bills for bytes scanned whether or
    not we are still waiting. maximum_bytes_billed makes BigQuery refuse the job
    up front instead, which is the difference between a cap and a hope.
    """

    captured = {}

    class _FakeIterator:
        schema = []

        def __iter__(self):
            return iter(())

    class _FakeJob:
        # Signature mirrors google.cloud.bigquery.QueryJob.result, which takes
        # timeout -- a narrower fake would pass while the real client raised.
        def result(self, max_results=None, timeout=None, **kwargs):
            captured["max_results"] = max_results
            captured["timeout"] = timeout
            return _FakeIterator()

    class _FakeClient:
        def query(self, query, job_config=None):
            captured["job_config"] = job_config
            return _FakeJob()

    probe = BigQueryMetadataProbe(_FakeClient())
    probe.execute_catalog_query("SELECT 1", 10)

    job_config = captured["job_config"]
    assert job_config is not None, "no job config passed, so no ceiling was applied"
    assert job_config.maximum_bytes_billed == probe.query_budget.max_bytes_billed
    assert job_config.maximum_bytes_billed is not None
    # The page cap and the bill cap are different things; both should be set.
    assert captured["max_results"] == 10
    assert captured["timeout"] == probe.query_budget.timeout_seconds


def test_bigquerys_declared_timeout_bounds_the_job_not_just_our_wait():
    """The budget declared timeout_seconds=30 and only `.result(timeout=...)`
    applied it -- which bounds how long the CLIENT waits, and neither cancels
    the job nor stops it billing. A ceiling that reads as present and is not
    is what QueryBudget's docstring warns against, and it is the same defect
    the Redshift ceiling had. job_timeout_ms is BigQuery's own
    cancel-the-job knob.

    Asserting only `.result(timeout=...)` is what missed it for two reviewers'
    worth of review, so this asserts the job config.
    """

    captured = {}

    class _FakeIterator:
        schema = []

        def __iter__(self):
            return iter(())

    class _FakeJob:
        def result(self, max_results=None, timeout=None, **kwargs):
            captured["timeout"] = timeout
            return _FakeIterator()

    class _FakeClient:
        def query(self, query, job_config=None):
            captured["job_config"] = job_config
            return _FakeJob()

    probe = BigQueryMetadataProbe(_FakeClient())
    probe.execute_catalog_query("SELECT 1", 10)

    seconds = probe.query_budget.timeout_seconds
    assert seconds is not None
    # A string, not an int: the client keeps it in the raw API properties, and
    # the live API echoes back jobTimeoutMs '30000'. Compared as a number so
    # the test pins the value rather than that representation.
    assert int(captured["job_config"].job_timeout_ms) == seconds * 1000
    # Both, and they are not redundant: one stops the job, the other stops us
    # waiting on a call hung for some other reason.
    assert captured["timeout"] == seconds


def test_snowflake_asks_the_server_to_stop_rather_than_stopping_waiting():
    """STATEMENT_TIMEOUT_IN_SECONDS is server-side; abandoning the cursor is not.

    A client that gives up leaves the warehouse running the query and billing for
    it, so the timeout has to be set on the session before the query is sent.
    """

    issued = []

    class _FakeConnection:
        def query(self, sql):
            issued.append(sql)
            return []

    probe = SnowflakeMetadataProbe(_FakeConnection())
    probe.execute_catalog_query("SELECT 1", 10)

    timeout_statements = [
        sql for sql in issued if "STATEMENT_TIMEOUT_IN_SECONDS" in sql.upper()
    ]
    assert timeout_statements, f"no session timeout was set; issued: {issued}"
    assert str(probe.query_budget.timeout_seconds) in timeout_statements[0]
    # And it must be set before the query it bounds, not after.
    assert issued.index(timeout_statements[0]) < issued.index("SELECT 1")


def test_the_mysql_family_gets_no_connect_arg_because_mariadb_shares_its_scheme():
    """A connect_arg here would stop MariaDB connecting at all.

    MySQL 5.7.8+ bounds a statement with max_execution_time (ms); MariaDB uses
    max_statement_time (seconds) and errors on the MySQL name. MariaDB's source is
    declared @config_class(MySQLConfig) and inherits scheme "mysql+pymysql", so the
    URL cannot tell them apart -- and an init_command naming the wrong variable runs
    on every connection, which is a connection failure rather than a slow probe.

    The attempt is still made, after connecting, where the wrong spelling is
    survivable. But it is not *reported* as a ceiling: whether either variable
    exists is unknowable from the URL, and where max_execution_time does work it
    bounds SELECTs only, leaving the Inspector's SHOW-based listings unbounded.
    So this asserts the absence of the connect_arg AND that the budget declines
    to claim a ceiling it cannot show.
    """

    class _Config:
        def get_sql_alchemy_url(self) -> str:
            return "mysql+pymysql://u:p@h/db"

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    assert "init_command" not in str(options.get("connect_args", {}))

    url = "mysql+pymysql://u:p@h/db"
    assert not applies_statement_timeout(url, 30)
    assert (
        effective_budget(url, QueryBudget(timeout_seconds=30)).timeout_seconds is None
    )


def test_the_attempt_is_still_installed_even_though_it_is_not_claimed(monkeypatch):
    """Declining to report a ceiling must not mean declining to try for one --
    the listener is still best-effort defence on a server that has the variable."""

    listened: List[str] = []

    class _FakeEngine:
        pass

    monkeypatch.setattr(
        sqlalchemy.event,
        "listen",
        lambda target, name, fn: listened.append(name),
    )
    install_statement_timeout(_FakeEngine(), "mysql+pymysql://u:p@h/db", 30)
    assert listened == ["connect"]


@pytest.mark.parametrize(
    "url,expected_fragment",
    [
        ("postgresql://u:p@h/db", "statement_timeout"),
        ("cockroachdb://u:p@h/db", "statement_timeout"),
    ],
)
def test_the_sqlalchemy_family_gets_a_timeout_through_its_engine(
    url, expected_fragment
):
    """One place covers ~15 dialects, which is why it is applied at engine build.

    Wiring this per connector would mean fifteen chances to forget.
    """

    class _Config:
        def get_sql_alchemy_url(self) -> str:
            return url

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    rendered = str(options.get("connect_args", {}))
    assert expected_fragment in rendered, f"no server-side timeout for {url}"


def test_redshift_gets_no_connect_arg_because_its_driver_is_not_libpq():
    """This list used to include redshift, and a live cluster refused every
    probe connection: `TypeError: connect() got an unexpected keyword argument
    'options'`.

    The `-c setting` string is libpq's, and Redshift's SQLAlchemy driver is
    redshift+redshift_connector -- pure Python, never links libpq. The old test
    passed because it asked about `redshift+psycopg2://`, a URL no config
    produces; _scheme_of truncates at the `+`, so the fiction was invisible.
    Hence the real scheme here.
    """

    class _Config:
        def get_sql_alchemy_url(self) -> str:
            return "redshift+redshift_connector://u:p@h/db"

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    assert "options" not in options.get("connect_args", {})


def test_redshift_still_gets_a_ceiling_and_still_claims_it():
    """Moved off connect_args, not dropped. Unlike the MySQL family there is no
    ambiguity to survive -- statement_timeout is Redshift's one spelling -- so
    the statement is left to fail loudly and the budget may still report it."""
    url = "redshift+redshift_connector://u:p@h/db"
    assert applies_statement_timeout(url, 30)
    assert effective_budget(url, QueryBudget(timeout_seconds=30)).timeout_seconds == 30


def test_redshift_sets_its_ceiling_outside_a_transaction(monkeypatch):
    """A plain SET is transactional in the Postgres family, so without
    autocommit the rollback SQLAlchemy issues on pool return silently undoes
    it. The first version of this listener did exactly that: the statement was
    sent, nothing raised, and the session sat at statement_timeout = 0.

    Asserting the statement was executed is what missed it. Assert the
    connection was in autocommit while it ran.
    """
    executed: List[str] = []
    autocommit_during: List[bool] = []

    class _Cursor:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql):
            executed.append(sql)
            autocommit_during.append(self._conn.autocommit)

        def close(self):
            pass

    class _Conn:
        autocommit = False

        def cursor(self):
            return _Cursor(self)

    listeners: List = []
    monkeypatch.setattr(
        sqlalchemy.event,
        "listen",
        lambda target, name, fn: listeners.append(fn),
    )
    install_statement_timeout(object(), "redshift+redshift_connector://u:p@h/db", 30)
    connection = _Conn()
    listeners[0](connection, None)

    assert executed == ["SET statement_timeout = 30000"]
    assert autocommit_during == [True], "the SET would be rolled back"
    assert connection.autocommit is False, "autocommit must be handed back"


def test_a_dialect_with_no_known_timeout_knob_is_left_alone():
    """Better to declare no ceiling than to pass a connect arg that breaks connecting.

    A wrong connect_args does not degrade the probe -- it stops the connector
    opening a connection at all, which is a worse failure than an unbounded query.
    """

    class _Config:
        def get_sql_alchemy_url(self) -> str:
            return "exotic+driver://u:p@h/db"

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    assert "connect_args" not in options or not options["connect_args"]


def test_a_probe_reports_the_ceiling_it_actually_got_not_the_one_declared():
    """A declared ceiling nobody applies is worse than no ceiling: it reads as safe.

    The default budget carries timeout_seconds=30, but only the dialects in
    _TIMEOUT_CONNECT_ARGS have a knob to apply it through. On the rest the
    effective budget has to say so, or `describe()` reports a limit that does not
    exist -- and an operator reading it concludes the probe is bounded when it is
    not.
    """

    bounded = effective_budget("postgresql://u:p@h/db", QueryBudget(timeout_seconds=30))
    assert bounded.timeout_seconds == 30
    assert "30s" in bounded.describe()

    unbounded = effective_budget(
        "exotic+driver://u:p@h/db", QueryBudget(timeout_seconds=30)
    )
    assert unbounded.timeout_seconds is None
    assert "no server-side ceiling" in unbounded.describe()


def test_the_engine_keeps_the_connector_s_own_options():
    """The budget is additive. A connector's ssl/connect_args must survive it."""

    class _Config:
        # `options`, like every config in this family -- a fake defining only
        # get_options() modelled the probe's old behaviour rather than any real
        # connector, so it passed while the probe read the wrong dict.
        options = {"connect_args": {"sslmode": "require"}, "pool_size": 3}

        def get_sql_alchemy_url(self) -> str:
            return "postgresql://u:p@h/db"

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    assert options["pool_size"] == 3
    assert options["connect_args"]["sslmode"] == "require"
    assert "statement_timeout" in str(options["connect_args"])


def test_a_non_positive_ceiling_is_refused_at_construction():
    """There were two ways to spell "unbounded" and only one was honest.

    Every applier treats <= 0 as no ceiling, but describe() checked only
    `is not None` -- so QueryBudget(timeout_seconds=0) reported "0s", which
    reads as a ceiling and is not one. That is the exact failure this class's
    own docstring warns against, reachable straight through the constructor.
    """
    for kwargs in ({"timeout_seconds": 0}, {"timeout_seconds": -1}):
        with pytest.raises(ValueError, match="positive or None"):
            QueryBudget(**kwargs)
    with pytest.raises(ValueError, match="positive or None"):
        QueryBudget(max_bytes_billed=0)

    # None stays the one representation of unbounded, and describe() says so.
    assert QueryBudget(timeout_seconds=None).describe() == "no server-side ceiling"


def test_the_probe_reads_the_option_dict_ingestion_reads():
    """engine_options preferred get_options() over `options`.

    Every SQLAlchemy engine ingestion builds passes `**config.options` --
    sql_common.get_inspectors, the profilers, athena, oracle, clickhouse, mysql,
    teradata, and unity's hive_metastore_proxy. Nothing on that path calls
    get_options(). unity-catalog is the one config in the probe's SQL family
    defining both, and its two dicts are genuinely different: get_options()
    returns extra_client_options, while unity/source.py hands self.config.options
    to the metastore proxy. So the probe was connecting with options ingestion
    never uses.
    """
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

    config = UnityCatalogSourceConfig.model_validate(
        {
            "workspace_url": "https://dbc-test.cloud.databricks.com",
            "token": "dapi-fake",
            "options": {"pool_size": 7},
            "extra_client_options": {"pool_size": 99},
        }
    )
    assert config.get_options() == {"pool_size": 99}
    assert engine_options(config) == {"pool_size": 7} == config.options


def test_no_config_in_the_sql_probe_family_diverges_on_its_option_source():
    """The generalisable form of the bug above.

    A future config gaining a get_options() would silently re-open it, so this
    walks the registry rather than naming unity-catalog.
    """
    from datahub.ingestion.source.source_registry import source_registry
    from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe

    checked = 0
    for name in sorted(source_registry.mapping.keys()):
        try:
            # Not on the Source base, so mypy cannot see it; every registered
            # source that reaches the probe has it.
            get_config_class = getattr(source_registry.get(name), "get_config_class")  # noqa: B009
            config_cls = get_config_class()
        except Exception:
            # An uninstalled extra is not this test's business.
            continue
        provider = getattr(config_cls, "probe_provider_class", None)
        if provider is None:
            continue
        try:
            if provider() is not SqlAlchemyMetadataProbe:
                continue
        except Exception:
            continue
        checked += 1
        assert "options" in config_cls.model_fields, (
            f"{name}: in the SQL probe family but has no `options` field, so "
            f"engine_options cannot read what ingestion reads"
        )

    # Guards the walk itself: a registry that stopped resolving would otherwise
    # pass this vacuously.
    assert checked > 10, f"only {checked} configs reached -- the walk is broken"
