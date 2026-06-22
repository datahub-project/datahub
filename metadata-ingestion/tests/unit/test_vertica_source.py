from sqlalchemy import create_engine, inspect

from datahub.ingestion.source.sql.vertica import VerticaConfig
from datahub.ingestion.source.sql.vertica_inspector import VerticaInspector


def test_reflection_connection_reuses_bound_connection():
    """Vertica reflection reuses the inspector's bound connection (no per-query checkout).

    get_inspectors() binds the inspector to a live Connection, so the Vertica-specific
    reflection methods must reuse it rather than opening a new connection each call —
    and must not close the borrowed connection.
    """
    engine = create_engine("sqlite://")
    conn = engine.connect()
    inspector = VerticaInspector(inspect(conn))

    with inspector._reflection_connection() as reused:
        assert reused is conn
    assert not conn.closed  # borrowed connection must stay open
    conn.close()


def test_reflection_connection_falls_back_to_short_lived_connection():
    """If bound to an Engine, open and close a short-lived connection instead."""
    engine = create_engine("sqlite://")
    inspector = VerticaInspector(inspect(engine))

    with inspector._reflection_connection() as opened:
        assert not opened.closed
    assert opened.closed  # short-lived connection is closed on exit


def test_vertica_uri_https():
    config = VerticaConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:5433",
            "database": "db",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "vertica+vertica_python://user:password@host:5433/db"
    )
