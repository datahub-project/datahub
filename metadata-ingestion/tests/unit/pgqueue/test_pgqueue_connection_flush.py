"""Tests for pgQueue connection flush / autocommit coordination."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

psycopg2_extensions = pytest.importorskip("psycopg2.extensions")

from datahub.pgqueue.connection import (  # noqa: E402
    flush_pg_connection,
    restore_pg_connection_autocommit,
)


def test_flush_pg_connection_skips_when_closed() -> None:
    conn = MagicMock()
    conn.closed = 1
    flush_pg_connection(conn)
    conn.get_transaction_status.assert_not_called()


def test_flush_pg_connection_rollbacks_inerror() -> None:
    conn = MagicMock()
    conn.closed = 0
    conn.get_transaction_status.return_value = (
        psycopg2_extensions.TRANSACTION_STATUS_INERROR
    )
    flush_pg_connection(conn)
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_flush_pg_connection_commits_intrans() -> None:
    conn = MagicMock()
    conn.closed = 0
    conn.get_transaction_status.return_value = (
        psycopg2_extensions.TRANSACTION_STATUS_INTRANS
    )
    flush_pg_connection(conn)
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


def test_flush_pg_connection_noop_when_idle() -> None:
    conn = MagicMock()
    conn.closed = 0
    conn.get_transaction_status.return_value = (
        psycopg2_extensions.TRANSACTION_STATUS_IDLE
    )
    flush_pg_connection(conn)
    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()


def test_restore_pg_connection_autocommit_rollbacks_failed_txn_first() -> None:
    conn = MagicMock()
    conn.get_transaction_status.return_value = (
        psycopg2_extensions.TRANSACTION_STATUS_INERROR
    )
    restore_pg_connection_autocommit(conn, True)
    conn.rollback.assert_called_once()


def test_flush_pg_connection_rollback_after_failed_commit() -> None:
    conn = MagicMock()
    conn.closed = 0
    conn.get_transaction_status.return_value = (
        psycopg2_extensions.TRANSACTION_STATUS_INTRANS
    )
    conn.commit.side_effect = RuntimeError("commit failed")

    flush_pg_connection(conn)

    conn.commit.assert_called_once()
    conn.rollback.assert_called_once()
