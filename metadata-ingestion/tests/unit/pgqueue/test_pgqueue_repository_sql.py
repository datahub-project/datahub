"""Repository SQL paths with mocked psycopg2 cursors (no live database)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datahub.pgqueue.repository import PgQueueMessageHandle, PgQueueRepository


def _repo() -> PgQueueRepository:
    return PgQueueRepository("queue", "metadata_queue")


def _cursor_conn(cur: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.autocommit = True
    conn.cursor.return_value.__enter__.return_value = cur
    return conn


def _handle(
    msg_id: int = 1,
    *,
    topic_id: int = 10,
    partition_id: int = 0,
    enqueue_seq: int = 42,
) -> PgQueueMessageHandle:
    return PgQueueMessageHandle(
        id=msg_id,
        enqueued_at=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
        topic_id=topic_id,
        partition_id=partition_id,
        enqueue_seq=enqueue_seq,
    )


class TestFetchTopicRow:
    def test_returns_none_when_missing(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = None
        conn = _cursor_conn(cur)

        assert _repo().fetch_topic_row(conn, "MissingTopic") is None

    def test_parses_row(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (7, 4, 3)
        conn = _cursor_conn(cur)

        row = _repo().fetch_topic_row(conn, "MetadataChangeProposal_v1")
        assert row == (7, 4, 3)

    def test_null_default_content_type(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (1, 2, None)
        conn = _cursor_conn(cur)

        row = _repo().fetch_topic_row(conn, "t")
        assert row == (1, 2, None)

    def test_second_fetch_uses_cache(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (3, 1, None)
        conn = _cursor_conn(cur)
        repo = _repo()

        assert repo.fetch_topic_row(conn, "cached") == (3, 1, None)
        assert repo.fetch_topic_row(conn, "cached") == (3, 1, None)
        assert cur.execute.call_count == 1


class TestEnsureTopic:
    def test_upsert_and_select_id(self) -> None:
        mime_cur = MagicMock()
        mime_cur.fetchone.return_value = (2,)
        topic_cur = MagicMock()
        refresh_cur = MagicMock()
        refresh_cur.fetchone.return_value = (99, 4, 2)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.side_effect = [
            mime_cur,
            topic_cur,
            refresh_cur,
        ]

        topic_id = _repo().ensure_topic(
            conn,
            "MetadataChangeProposal_v1",
            partition_count=4,
            retention_max_age_seconds=86400,
            max_rows_per_topic=1_000_000,
            max_total_payload_bytes=1_000_000_000,
        )

        assert topic_id == 99
        assert mime_cur.execute.call_count == 1
        assert "SELECT id FROM" in mime_cur.execute.call_args[0][0]
        assert "WHERE mime = %s" in mime_cur.execute.call_args[0][0]
        assert topic_cur.execute.call_count == 1
        assert refresh_cur.execute.call_count == 1

    def test_upsert_includes_aggressive_retention(self) -> None:
        mime_cur = MagicMock()
        mime_cur.fetchone.return_value = (2,)
        topic_cur = MagicMock()
        refresh_cur = MagicMock()
        refresh_cur.fetchone.return_value = (1, 2, 2)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.side_effect = [
            mime_cur,
            topic_cur,
            refresh_cur,
        ]

        _repo().ensure_topic(
            conn,
            "t",
            partition_count=2,
            retention_max_age_seconds=0,
            max_rows_per_topic=0,
            max_total_payload_bytes=0,
            aggressive_retention=True,
        )

        insert_sql = topic_cur.execute.call_args_list[0][0][0]
        assert "aggressive_retention" in insert_sql
        assert topic_cur.execute.call_args_list[0][0][1][-1] is True


class TestEnsureMimeRegistered:
    def test_lookup_existing_mime_without_insert(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (7,)
        conn = _cursor_conn(cur)

        assert _repo()._ensure_mime_registered(conn, "application/avro") == 7

        assert cur.execute.call_count == 1
        assert "SELECT id FROM" in cur.execute.call_args[0][0]
        assert "INSERT INTO" not in cur.execute.call_args[0][0]

    def test_insert_when_mime_missing(self) -> None:
        cur = MagicMock()
        cur.fetchone.side_effect = [None, (9,)]
        conn = _cursor_conn(cur)

        assert _repo()._ensure_mime_registered(conn, "application/test") == 9

        assert cur.execute.call_count == 3
        assert "SELECT id FROM" in cur.execute.call_args_list[0][0][0]
        insert_sql = cur.execute.call_args_list[1][0][0]
        assert "INSERT INTO" in insert_sql
        assert "ON CONFLICT (mime) DO NOTHING" in insert_sql
        assert "SELECT id FROM" in cur.execute.call_args_list[2][0][0]

    def test_cache_hit_skips_database(self) -> None:
        repo = _repo()
        repo._content_type_id_by_mime["application/avro"] = 7
        conn = MagicMock()

        assert repo._ensure_mime_registered(conn, "application/avro") == 7
        conn.cursor.assert_not_called()


class TestEnqueueMessageInTransaction:
    def test_priority_out_of_range(self) -> None:
        repo = _repo()
        conn = MagicMock()
        with pytest.raises(ValueError, match="priority"):
            repo._enqueue_message_in_transaction(
                conn,
                topic_row=(1, 4, None),
                routing_key="urn:li:dataset:1",
                priority=10,
                payload=b"x",
                content_type=None,
                headers=(),
            )

    def test_inserts_and_returns_handle(self) -> None:
        repo = _repo()
        enq_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
        cur = MagicMock()
        cur.fetchone.return_value = (55, enq_at, 7)
        conn = _cursor_conn(cur)

        with patch.object(repo, "_compute_stored_content_type_id", return_value=None):
            handle = repo._enqueue_message_in_transaction(
                conn,
                topic_row=(10, 8, 3),
                routing_key="urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)",
                priority=0,
                payload=b"\x00avro",
                content_type=None,
                headers=(),
            )

        assert handle.id == 55
        assert handle.topic_id == 10
        assert handle.enqueue_seq == 7
        assert cur.execute.call_count == 2


class TestComputeStoredContentType:
    def test_none_when_no_content_type(self) -> None:
        assert _repo()._compute_stored_content_type_id(MagicMock(), 1, None) is None

    def test_none_when_matches_topic_default(self) -> None:
        repo = _repo()
        with patch.object(repo, "_ensure_mime_registered", return_value=5) as mock_mime:
            assert (
                repo._compute_stored_content_type_id(MagicMock(), 5, "application/json")
                is None
            )
        mock_mime.assert_called_once()

    def test_returns_id_when_differs_from_default(self) -> None:
        repo = _repo()
        with patch.object(repo, "_ensure_mime_registered", return_value=9):
            assert (
                repo._compute_stored_content_type_id(MagicMock(), 5, "text/plain") == 9
            )


@patch("datahub.pgqueue.repository.restore_pg_connection_autocommit")
@patch("datahub.pgqueue.repository.flush_pg_connection")
class TestTransactionalRepositoryMethods:
    def test_enqueue_commits(self, _flush: MagicMock, _restore: MagicMock) -> None:
        repo = _repo()
        conn = MagicMock()
        conn.autocommit = True
        expected = _handle(msg_id=77)

        with patch.object(
            repo, "_enqueue_message_in_transaction", return_value=expected
        ):
            result = repo.enqueue(
                conn,
                topic_name="t",
                routing_key="k",
                partition_count=4,
                retention_max_age_seconds=1,
                max_rows_per_topic=1,
                max_total_payload_bytes=1,
                priority=0,
                payload=b"p",
                content_type=None,
                headers=(),
            )

        assert result is expected
        conn.commit.assert_called_once()

    def test_commit_for_group_advances_offsets(
        self, _flush: MagicMock, _restore: MagicMock
    ) -> None:
        repo = _repo()
        conn = MagicMock()
        conn.autocommit = True
        cur = MagicMock()
        cur.rowcount = 2
        conn.cursor.return_value.__enter__.return_value = cur
        handles = [
            _handle(1, topic_id=10, partition_id=0, enqueue_seq=5),
            _handle(2, topic_id=10, partition_id=0, enqueue_seq=8),
        ]

        deleted = repo.commit_for_group(conn, "my-group", handles)

        assert deleted == 2
        conn.commit.assert_called_once()
        # mark-acked per handle + one offset UPSERT per (topic_id, partition_id)
        assert cur.execute.call_count >= 2

    def test_commit_for_group_empty_is_noop(
        self, _flush: MagicMock, _restore: MagicMock
    ) -> None:
        assert _repo().commit_for_group(MagicMock(), "g", []) == 0

    def test_extend_visibility_updates_leases(
        self, _flush: MagicMock, _restore: MagicMock
    ) -> None:
        from datetime import timedelta

        repo = _repo()
        conn = MagicMock()
        conn.autocommit = True
        cur = MagicMock()
        cur.rowcount = 1
        conn.cursor.return_value.__enter__.return_value = cur

        updated = repo.extend_visibility_for_group(
            conn,
            "my-group",
            [_handle()],
            lock_owner="my-group:uuid",
            extend_by=timedelta(seconds=60),
        )

        assert updated == 1
        conn.commit.assert_called_once()

    def test_register_consumer_upserts(
        self, _flush: MagicMock, _restore: MagicMock
    ) -> None:
        repo = _repo()
        conn = MagicMock()
        conn.autocommit = True
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        repo.register_consumer(conn, "pipeline-1", topic_id=42)

        cur.execute.assert_called_once()
        conn.commit.assert_called_once()
