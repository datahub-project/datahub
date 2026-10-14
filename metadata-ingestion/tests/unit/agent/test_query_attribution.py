"""A probe says who it is to the server it queries.

A probe runs with ingestion's credentials, over ingestion's own client, so in
the source system's logs its queries are indistinguishable from a scheduled run
-- and an agent exploring a source can issue a lot of them. Only the client can
supply the difference, and each dialect family offers a different mechanism for
it.
"""

import re
from typing import Any, Dict, List

import pytest

from datahub.ingestion.agent.sql_passthrough import PROBE_QUERY_LABEL, QueryBudget
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.snowflake.snowflake_probe import SnowflakeMetadataProbe
from datahub.ingestion.source.sql.cockroachdb import CockroachDBConfig
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.protocol_probe_settings import probe_url
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig
from datahub.ingestion.source.sql.sqlalchemy_probe import probe_engine_options
from tests.unit.agent._driver_capture import driver_connect_kwargs

_CONFIG_FOR_DIALECT = {
    "postgresql": PostgresConfig,
    "cockroachdb": CockroachDBConfig,
    "redshift": RedshiftConfig,
    "mysql": MySQLConfig,
    # MariaDB's source is declared with MySQLConfig.
    "mariadb": MySQLConfig,
    "doris": DorisConfig,
}


def _options_for(url: str, **config_attrs: Any) -> Dict[str, Any]:
    """The engine options the probe builds for the connector config whose
    recipe connects to `url`; a dialect with no connector of its own goes
    through the generic source."""
    config_cls = _CONFIG_FOR_DIALECT.get(url.split("://", 1)[0].split("+", 1)[0])
    config: SQLCommonConfig = (
        config_cls(host_port="h:1", sqlalchemy_uri=url, **config_attrs)
        if config_cls is not None
        else SQLAlchemyGenericConfig(platform="exotic", connect_uri=url, **config_attrs)
    )
    return probe_engine_options(
        config, config.probe_engine_settings(QueryBudget(timeout_seconds=30))
    )


def test_the_label_stays_inside_the_charset_every_dialect_accepts():
    """The tightest of the three rules is BigQuery's, and it is also the one that
    keeps the string safe to interpolate into Snowflake's ALTER SESSION, where no
    bind parameter is available. Widening the charset would break BigQuery and
    open an injection point in the same edit -- so the constraint is asserted
    rather than left to a comment."""
    assert re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", PROBE_QUERY_LABEL)


def test_the_postgres_family_names_itself_through_application_name():
    for url in (
        "postgresql://u:p@h/db",
        "redshift+redshift_connector://u:p@h/db",
        "cockroachdb://u:p@h/db",
    ):
        connect_args = _options_for(url)["connect_args"]
        assert connect_args["application_name"] == PROBE_QUERY_LABEL, url


def test_mysql_uses_the_connection_attribute_it_has_instead():
    """MySQL has no application_name; PyMySQL sends program_name as a connect
    attribute, which lands in performance_schema.session_connect_attrs."""
    connect_args = _options_for("mysql+pymysql://u:p@h/db")["connect_args"]
    assert connect_args["program_name"] == PROBE_QUERY_LABEL


def test_a_dialect_with_no_mechanism_is_left_unlabelled():
    """An unlabelled connection is honest. A label the driver rejects is a
    connection failure, which is a far worse outcome than an anonymous query."""
    options = _options_for("exotic+driver://u:p@h/db")
    assert "connect_args" not in options or not options["connect_args"]


def test_the_label_does_not_displace_the_statement_ceiling():
    """Both want a connect_arg on Postgres, and the ceiling's rides on the libpq
    `options` string. Emitting the label as a second `-c` setting there would
    have silently overwritten it -- so they use different keys, and this is the
    test that would have caught it."""
    connect_args = _options_for("postgresql://u:p@h/db")["connect_args"]
    assert connect_args["application_name"] == PROBE_QUERY_LABEL
    assert "statement_timeout" in connect_args["options"]


def test_a_recipe_that_names_its_own_connection_keeps_that_name():
    """The two merge differently on purpose: a label defers to the recipe, a
    safety ceiling does not."""
    options = _options_for(
        "postgresql://u:p@h/db",
        options={"connect_args": {"application_name": "my_own_name"}},
    )
    assert options["connect_args"]["application_name"] == "my_own_name"
    assert "statement_timeout" in options["connect_args"]["options"]


@pytest.mark.parametrize(
    "url, kwarg",
    [
        (
            "postgresql+psycopg2://u:p@h:5432/db?application_name=recipe_app",
            "application_name",
        ),
        ("mysql+pymysql://u:p@h:3306/db?program_name=recipe_app", "program_name"),
    ],
)
def test_a_client_name_in_the_recipes_url_reaches_the_driver_unreplaced(
    url: str, kwarg: str
) -> None:
    """connect_args override the URL's query in create_engine, so a label in
    the probe's connect_args would replace the name the recipe chose there."""
    config = _CONFIG_FOR_DIALECT[url.split("+", 1)[0]](
        host_port="h:1", sqlalchemy_uri=url
    )
    options = probe_engine_options(
        config, config.probe_engine_settings(QueryBudget(timeout_seconds=30))
    )
    ingestion = driver_connect_kwargs(config.get_sql_alchemy_url(), config.options)
    probe = driver_connect_kwargs(probe_url(config), options)
    assert ingestion[kwarg] == "recipe_app"
    assert probe[kwarg] == "recipe_app"


class _StubConnection:
    def __init__(self, fail_on_tag: bool = False) -> None:
        self.issued: List[str] = []
        self._fail_on_tag = fail_on_tag

    def query(self, sql: str) -> List[Dict[str, Any]]:
        if self._fail_on_tag and "QUERY_TAG" in sql:
            raise RuntimeError("nope")
        self.issued.append(sql)
        return []


def _snowflake_probe_over(connection: _StubConnection) -> SnowflakeMetadataProbe:
    config = type("_Cfg", (), {"get_connection": lambda self: connection})
    return SnowflakeMetadataProbe.for_config(config())  # type: ignore[arg-type]


def test_snowflake_tags_the_session_so_the_tag_covers_every_later_statement():
    connection = _StubConnection()
    probe = _snowflake_probe_over(connection)
    probe.execute_catalog_query("SELECT 1", limit=1)

    tag = f"ALTER SESSION SET QUERY_TAG = '{PROBE_QUERY_LABEL}'"
    assert connection.issued[0] == tag, "the tag must precede everything it labels"
    assert "SELECT 1" in connection.issued


def test_a_session_that_refuses_the_tag_is_still_probed():
    """A label is not a safety control. Refusing to probe an account because its
    query log would have been harder to read is the wrong trade -- unlike the
    statement ceiling, which is left to fail loudly."""
    connection = _StubConnection(fail_on_tag=True)
    probe = _snowflake_probe_over(connection)
    probe.execute_catalog_query("SELECT 1", limit=1)
    assert "SELECT 1" in connection.issued


def test_bigquery_labels_the_job_so_probe_spend_is_separable():
    """Labels reach INFORMATION_SCHEMA.JOBS and the billing export, so this is
    the only one of the three where probe *cost* can be told from ingestion's,
    not just probe traffic."""
    from datahub.ingestion.source.bigquery_v2.bigquery_probe import (
        BigQueryMetadataProbe,
    )

    captured: Dict[str, Any] = {}

    class _Job:
        def result(self, **_kwargs: Any) -> Any:
            return type("_It", (), {"schema": [], "__iter__": lambda self: iter(())})()

    class _Client:
        def query(self, query: str, job_config: Any) -> Any:
            captured["labels"] = job_config.labels
            return _Job()

    BigQueryMetadataProbe(_Client()).execute_catalog_query("SELECT 1", limit=1)
    assert captured["labels"] == {"application": PROBE_QUERY_LABEL}


def test_program_name_goes_to_pymysql_drivers_and_no_others():
    """`program_name` is a PyMySQL feature, not a MySQL one.

    The lookup was keyed on the dialect with the driver stripped, so every
    `mysql://` URI got it -- including `mysql+mysqlconnector`, whose connect()
    validates its keyword arguments and rejects the unknown one. That is the
    failure the test below names as the reason for leaving dialects
    unlabelled: "a label the driver rejects is a connection failure, which is
    a far worse outcome than an anonymous query."

    Keying on the driver also picks up Doris for free -- it defaults to
    `doris+pymysql`, uses the same driver, and was unlabelled only because
    `doris` was missing from a dialect list.
    """
    for url in (
        "mysql+pymysql://h/db",
        "mariadb+pymysql://h/db",
        "doris+pymysql://h/db",
    ):
        connect_args = _options_for(url)["connect_args"]
        assert connect_args["program_name"] == PROBE_QUERY_LABEL, url

    for url in (
        "mysql+mysqlconnector://h/db",
        "mysql+mysqldb://h/db",
        # Bare `mysql://` is MySQLdb in SQLAlchemy, not PyMySQL. Every
        # DataHub config in this family defaults to an explicit +pymysql, so
        # this shape only arrives from a hand-written sqlalchemy_uri.
        "mysql://h/db",
    ):
        options = _options_for(url)
        connect_args = options.get("connect_args", {})
        assert "program_name" not in connect_args, url
