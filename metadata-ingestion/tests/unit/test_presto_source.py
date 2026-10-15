from unittest.mock import MagicMock

from sqlalchemy.sql.elements import TextClause

from datahub.ingestion.source.sql.presto import PrestoDialect


def test_presto_get_schema_names_uses_text_clause():
    """PrestoDialect.get_schema_names must wrap SHOW SCHEMAS in text() for SA 2.0,
    which rejects raw strings in connection.execute() with ObjectNotExecutableError.
    """
    executed = []

    def fake_execute(statement, *args, **kw):
        executed.append(statement)
        return [("schema_a",), ("schema_b",)]

    conn = MagicMock()
    conn.execute.side_effect = fake_execute

    schemas = PrestoDialect.get_schema_names(MagicMock(), conn)

    assert schemas == ["schema_a", "schema_b"]
    assert len(executed) == 1
    assert isinstance(executed[0], TextClause)
