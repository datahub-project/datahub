import pytest

from datahub.pgqueue.sql import qualified_table, quote_ident, validate_pg_identifier


def test_quote_ident_basic() -> None:
    assert quote_ident("queue") == '"queue"'
    assert quote_ident("metadata_queue") == '"metadata_queue"'


def test_validate_pg_identifier_rejects_bad() -> None:
    with pytest.raises(ValueError):
        validate_pg_identifier("bad-name", "x")
    with pytest.raises(ValueError):
        validate_pg_identifier("", "x")


def test_qualified_table() -> None:
    assert qualified_table("queue", "metadata_queue", "topic") == (
        '"queue"."metadata_queue_topic"'
    )


def test_qualified_table_rejects_injection_attempt() -> None:
    with pytest.raises(ValueError):
        qualified_table("queue;drop", "metadata_queue", "topic")
