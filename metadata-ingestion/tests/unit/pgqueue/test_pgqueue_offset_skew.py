"""Unit tests for pgQueue STUCK_AHEAD detection helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

from datahub.pgqueue.offset_skew import PartitionOffsetSkew, warn_if_ahead
from datahub.pgqueue.repository import PgQueueRepository


def test_detect_offset_ahead_of_log_one_cell() -> None:
    repo = PgQueueRepository("queue", "metadata_queue")
    conn = MagicMock()
    cursor_ctx = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor_ctx

    cursor_ctx.fetchall.return_value = [(0, 3)]
    cursor_ctx.fetchone.side_effect = [(10,), (0,)]

    skews = repo.detect_offset_ahead_of_log(
        conn, "cg", topic_id=1, partition_count=2, topic_name="t"
    )
    assert len(skews) == 1
    assert skews[0].partition_id == 0
    assert skews[0].ahead_by == 7
    assert skews[0].topic_name == "t"


def test_warn_if_ahead_does_not_raise() -> None:
    warn_if_ahead(
        PartitionOffsetSkew(
            consumer_group="cg",
            topic_id=1,
            partition_id=0,
            committed_offset=10,
            max_seq=1,
            ahead_by=9,
            topic_name="topic",
        )
    )
