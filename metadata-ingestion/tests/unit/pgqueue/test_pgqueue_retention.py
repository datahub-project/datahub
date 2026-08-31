from datahub.pgqueue.retention import (
    qualified_apply_retention_function,
    sequence_anchor_exclusion_sql,
)


def test_sequence_anchor_exclusion_sql() -> None:
    clause = sequence_anchor_exclusion_sql(
        message_alias="ms",
        qualified_message_table="queue.metadata_queue_message",
    )
    assert "m_anchor.enqueue_seq" in clause
    assert "ms.topic_id" in clause
    assert "ms.partition_id" in clause


def test_qualified_apply_retention_function() -> None:
    assert (
        qualified_apply_retention_function("queue", "metadata_queue")
        == '"queue".metadata_queue_apply_retention'
    )


def test_apply_retention_invokes_sql_function() -> None:
    from unittest.mock import MagicMock

    from datahub.pgqueue.retention import apply_retention

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    apply_retention(
        conn, qualified_apply_retention='"queue".metadata_queue_apply_retention'
    )

    cur.execute.assert_called_once_with(
        'SELECT "queue".metadata_queue_apply_retention()'
    )
