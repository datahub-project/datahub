"""Unit tests for :mod:`datahub.pgqueue.repository` batch helpers (no database)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from datahub.pgqueue.repository import (
    EnqueueBatchItem,
    PgQueueMessageHandle,
    PgQueueRepository,
)


def test_enqueue_batch_empty_returns_without_db_round_trip() -> None:
    repo = PgQueueRepository("queue", "metadata_queue")
    conn = MagicMock()
    assert (
        repo.enqueue_batch(
            conn,
            [],
            partition_count=2,
            retention_max_age_seconds=0,
            max_rows_per_topic=0,
            max_total_payload_bytes=0,
        )
        == []
    )
    conn.cursor.assert_not_called()


def test_enqueue_batch_commits_once() -> None:
    repo = PgQueueRepository("queue", "metadata_queue")
    conn = MagicMock()
    conn.autocommit = True
    dummy = PgQueueMessageHandle(
        id=1,
        enqueued_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        topic_id=1,
        partition_id=0,
        enqueue_seq=1,
    )
    items = [
        EnqueueBatchItem(
            topic_name="t",
            routing_key="a",
            priority=0,
            payload=b"x",
            content_type=None,
            headers=(),
        ),
        EnqueueBatchItem(
            topic_name="t",
            routing_key="b",
            priority=0,
            payload=b"y",
            content_type=None,
            headers=(),
        ),
    ]
    with patch.object(repo, "_enqueue_message_in_transaction", return_value=dummy):
        repo.enqueue_batch(
            conn,
            items,
            partition_count=4,
            retention_max_age_seconds=86400,
            max_rows_per_topic=1_000_000,
            max_total_payload_bytes=10**12,
        )
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


def test_receive_batch_for_group_flush_pg_connection_before_autocommit_toggle() -> None:
    """Implicit transactions (e.g. after ``fetch_topic_row``) block ``set_session``."""
    repo = PgQueueRepository("queue", "metadata_queue")
    conn = MagicMock()
    conn.autocommit = True
    cursor_ctx = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor_ctx
    cursor_ctx.fetchall.return_value = []
    with (
        patch("datahub.pgqueue.repository.flush_pg_connection") as flush,
        patch.object(repo, "_receive_partition_for_group", return_value=[]),
    ):
        repo.receive_batch_for_group(
            conn,
            consumer_group="cg",
            topic_id=1,
            partition_ids=(0,),
            lock_owner="g:1",
            visibility_timeout=timedelta(seconds=30),
            max_messages=10,
        )
    flush.assert_called_once_with(conn)


def test_receive_batch_for_group_zero_skips_cursor() -> None:
    repo = PgQueueRepository("queue", "metadata_queue")
    conn = MagicMock()
    assert (
        repo.receive_batch_for_group(
            conn,
            consumer_group="cg",
            topic_id=1,
            partition_ids=(0, 1),
            lock_owner="g:1",
            visibility_timeout=timedelta(seconds=30),
            max_messages=0,
        )
        == []
    )
    conn.cursor.assert_not_called()
