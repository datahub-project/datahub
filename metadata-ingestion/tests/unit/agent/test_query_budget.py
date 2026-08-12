"""A probe query must carry a cost ceiling, not just a row ceiling.

MAX_PROBE_ITEMS bounds the rows a query *returns*. It does not bound the work the
warehouse does: a query can return three rows and still scan a terabyte, and on a
warehouse that bills by bytes scanned the bill is identical either way. These tests
pin the second ceiling, per connector, because only the connector knows which kind
its driver can ask for.
"""

import pytest

from datahub.ingestion.agent.sql_passthrough import QueryBudget, SqlCatalogPassthrough


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
    from datahub.ingestion.source.bigquery_v2.bigquery_probe import (
        BigQueryMetadataProbe,
    )

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


def test_snowflake_asks_the_server_to_stop_rather_than_stopping_waiting():
    """STATEMENT_TIMEOUT_IN_SECONDS is server-side; abandoning the cursor is not.

    A client that gives up leaves the warehouse running the query and billing for
    it, so the timeout has to be set on the session before the query is sent.
    """
    from datahub.ingestion.source.snowflake.snowflake_probe import (
        SnowflakeMetadataProbe,
    )

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


@pytest.mark.parametrize(
    "url,expected_fragment",
    [
        ("postgresql://u:p@h/db", "statement_timeout"),
        ("redshift+psycopg2://u:p@h/db", "statement_timeout"),
        ("mysql+pymysql://u:p@h/db", "max_execution_time"),
    ],
)
def test_the_sqlalchemy_family_gets_a_timeout_through_its_engine(
    url, expected_fragment
):
    """One place covers ~15 dialects, which is why it is applied at engine build.

    Wiring this per connector would mean fifteen chances to forget.
    """
    from datahub.ingestion.source.sql.sql_probe import engine_options

    class _Config:
        def get_sql_alchemy_url(self) -> str:
            return url

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    rendered = str(options.get("connect_args", {}))
    assert expected_fragment in rendered, f"no server-side timeout for {url}"


def test_a_dialect_with_no_known_timeout_knob_is_left_alone():
    """Better to declare no ceiling than to pass a connect arg that breaks connecting.

    A wrong connect_args does not degrade the probe -- it stops the connector
    opening a connection at all, which is a worse failure than an unbounded query.
    """
    from datahub.ingestion.source.sql.sql_probe import engine_options

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
    from datahub.ingestion.source.sql.sql_probe import effective_budget

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
    from datahub.ingestion.source.sql.sql_probe import engine_options

    class _Config:
        def get_options(self):
            return {"connect_args": {"sslmode": "require"}, "pool_size": 3}

        def get_sql_alchemy_url(self) -> str:
            return "postgresql://u:p@h/db"

    options = engine_options(_Config(), budget=QueryBudget(timeout_seconds=30))
    assert options["pool_size"] == 3
    assert options["connect_args"]["sslmode"] == "require"
    assert "statement_timeout" in str(options["connect_args"])
