"""
PostgreSQL queue persistence matching SqlSetup DDL under ``postgres.pgQueue``.

This module only depends on other ``datahub.pgqueue.*`` helpers and standard/third-party
libraries used for SQL (psycopg2); it does not import ``datahub.ingestion.source.*``, keeping
the pgQueue DB client usable alongside catalog ingestion without coupling to source connectors.

**Java alignment:** Java ``EbeanPostgresMetadataQueueStore`` and this Python repository match
payload and Kafka-style headers (JSONB ``headers``). Keep JSON aligned with
``com.linkedin.metadata.queue.QueueHeadersJson``.

1. ``enqueue_seq`` allocation (per ``topic_id`` + ``partition_id``) — Python uses a
   transaction-scoped advisory lock + ``MAX(enqueue_seq)+1``.
2. Consumer progress is tracked via ``consumer_offset`` (``epoch`` /
   ``offset_value`` semantics) plus per-group rows in ``message_group_lease``.
   Dequeue acquires work only through leases (not row locks on ``message``),
   head-of-line per priority within each WFQ band (no skipping a leased lower
   ``enqueue_seq`` at the same priority). ``consumer_offset.offset_value`` is a
   contiguous ``enqueue_seq`` watermark advanced only on gap-free acks.
3. Visibility / ``lock_owner`` string format expected by peer clients (lease rows).
4. Persisted ``routing_key`` on each message row (Kafka-style enqueue key; aligns with Java).
5. Topic upsert: ``partition_count`` is set with ``GREATEST`` so it never drops below the prior
   catalog value or below ``MAX(partition_id)+1`` over existing message rows (matches Java
   ``EbeanPostgresMetadataQueueStore`` / SqlSetup). ``aggressive_retention`` is upserted like Java.
6. Message retention runs via SqlSetup ``{prefix}_apply_retention`` (see ``datahub.pgqueue.retention``);
   any client-side DELETE must use ``sequence_anchor_exclusion_sql`` so ``MAX(enqueue_seq)+1`` stays valid.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from datahub.pgqueue.connection import (
    flush_pg_connection,
    restore_pg_connection_autocommit,
)
from datahub.pgqueue.contiguous_offset import advance_watermark
from datahub.pgqueue.headers import headers_from_db, headers_to_json
from datahub.pgqueue.lease_markers import ACKED_LOCK_OWNER, ACKED_LOCKED_UNTIL
from datahub.pgqueue.offset_skew import PartitionOffsetSkew, warn_if_ahead
from datahub.pgqueue.priority_bands import (
    DEFAULT_BANDS_JSON,
    PriorityBand,
    PriorityBandConfig,
)
from datahub.pgqueue.retention import (
    apply_retention,
    qualified_apply_retention_function,
)
from datahub.pgqueue.sql import qualified_table

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection

logger = logging.getLogger(__name__)

# (topic_id, partition_count, default_content_type_id)
TopicRow = Tuple[int, int, Optional[int]]


@dataclass(frozen=True)
class PgQueueMessageHandle:
    """Primary key plus routing fields required for acks and visibility heartbeats."""

    id: int
    enqueued_at: datetime
    topic_id: int
    partition_id: int
    enqueue_seq: int

    def to_meta_dict(self) -> Dict[str, Any]:
        """Plain dict for embedding in pipeline/event metadata (e.g. actions ack correlation)."""
        return {
            "id": self.id,
            "enqueued_at": self.enqueued_at.isoformat(),
            "topic_id": self.topic_id,
            "partition_id": self.partition_id,
            "enqueue_seq": self.enqueue_seq,
        }

    @classmethod
    def from_meta_dict(cls, data: Dict[str, Any]) -> PgQueueMessageHandle:
        raw_enq = data["enqueued_at"]
        enqueued_at = (
            datetime.fromisoformat(raw_enq) if isinstance(raw_enq, str) else raw_enq
        )
        return cls(
            id=int(data["id"]),
            enqueued_at=enqueued_at,
            topic_id=int(data["topic_id"]),
            partition_id=int(data["partition_id"]),
            enqueue_seq=int(data["enqueue_seq"]),
        )


@dataclass(frozen=True)
class PgQueueReceivedMessage:
    """A queue message with an acquired per-consumer-group lease."""

    handle: PgQueueMessageHandle
    priority: int
    payload: bytes
    content_type: Optional[str]
    payload_compression: int
    headers: Tuple[Tuple[str, bytes], ...]
    routing_key: str
    lock_owner: str


@dataclass(frozen=True)
class EnqueueBatchItem:
    """One logical message in a bulk enqueue call (see :meth:`PgQueueRepository.enqueue_batch`)."""

    topic_name: str
    routing_key: str
    priority: int
    payload: bytes
    content_type: Optional[str]
    headers: Sequence[Tuple[str, bytes]]
    payload_compression: int = 0


def advisory_lock_key(topic_id: int, partition_id: int) -> int:
    """Deterministic positive bigint for ``pg_advisory_xact_lock``."""
    digest = hashlib.sha256(f"{topic_id}:{partition_id}".encode("utf-8")).digest()
    n = int.from_bytes(digest[:8], byteorder="big", signed=False)
    # Keep in signed 63-bit positive range for broad server compatibility.
    return n % (2**62)


def stable_partition_id(routing_key: str, partition_count: int) -> int:
    """CRC32 partition assignment (stable across Python runs)."""
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    import zlib

    crc = zlib.crc32(routing_key.encode("utf-8")) & 0xFFFFFFFF
    return crc % partition_count


class PgQueueRepository:
    def __init__(self, schema: str, table_prefix: str) -> None:
        self._topic = qualified_table(schema, table_prefix, "topic")
        self._message = qualified_table(schema, table_prefix, "message")
        self._consumer_offset = qualified_table(schema, table_prefix, "consumer_offset")
        self._content_type = qualified_table(schema, table_prefix, "content_type")
        self._consumer_registration = qualified_table(
            schema, table_prefix, "consumer_registration"
        )
        self._apply_retention = qualified_apply_retention_function(schema, table_prefix)
        self._lease = qualified_table(schema, table_prefix, "message_group_lease")
        # Per-process caches for immutable catalog rows (aligned with Java EbeanPostgresMetadataQueueStore).
        self._content_type_id_by_mime: Dict[str, int] = {}
        self._topic_row_by_name: Dict[str, TopicRow] = {}

    def fetch_topic_row(
        self, conn: PGConnection, topic_name: str
    ) -> Optional[TopicRow]:
        """Return ``(topic_id, partition_count, default_content_type_id)`` or None."""
        cached = self._topic_row_by_name.get(topic_name)
        if cached is not None:
            return cached
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, partition_count, default_content_type_id FROM {self._topic} WHERE topic_name = %s",
                (topic_name,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            parsed = self._parse_topic_row(row)
            self._topic_row_by_name[topic_name] = parsed
            return parsed

    @staticmethod
    def _parse_topic_row(row: Tuple[Any, ...]) -> TopicRow:
        dct = int(row[2]) if row[2] is not None else None
        return int(row[0]), int(row[1]), dct

    def _get_topic_row(self, conn: PGConnection, topic_name: str) -> TopicRow:
        row = self.fetch_topic_row(conn, topic_name)
        assert row is not None
        return row

    def _refresh_topic_row_cache(self, conn: PGConnection, topic_name: str) -> TopicRow:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, partition_count, default_content_type_id FROM {self._topic} WHERE topic_name = %s",
                (topic_name,),
            )
            row = cur.fetchone()
            assert row is not None
            parsed = self._parse_topic_row(row)
            self._topic_row_by_name[topic_name] = parsed
            return parsed

    def _ensure_mime_registered(self, conn: PGConnection, mime: str) -> int:
        """Resolve MIME to catalog id without burning smallint identity on existing MIME rows."""
        cached = self._content_type_id_by_mime.get(mime)
        if cached is not None:
            return cached
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id FROM {self._content_type} WHERE mime = %s",
                (mime,),
            )
            row = cur.fetchone()
            if row is not None:
                cid = int(row[0])
                self._content_type_id_by_mime[mime] = cid
                return cid
            cur.execute(
                f"INSERT INTO {self._content_type} (mime) VALUES (%s) "
                f"ON CONFLICT (mime) DO NOTHING",
                (mime,),
            )
            cur.execute(
                f"SELECT id FROM {self._content_type} WHERE mime = %s",
                (mime,),
            )
            row = cur.fetchone()
            assert row is not None
            cid = int(row[0])
            self._content_type_id_by_mime[mime] = cid
            return cid

    def ensure_topic(
        self,
        conn: PGConnection,
        topic_name: str,
        partition_count: int,
        retention_max_age_seconds: int,
        max_rows_per_topic: int,
        max_total_payload_bytes: int,
        default_content_type_mime: Optional[str] = None,
        aggressive_retention: bool = False,
    ) -> int:
        """Upsert topic catalog row and return ``topic_id``."""
        mime = default_content_type_mime or "application/avro"
        default_ct_id = self._ensure_mime_registered(conn, mime)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._topic} AS ptopic
                  (topic_name, partition_count,
                   retention_max_age_seconds, max_rows_per_topic, max_total_payload_bytes,
                   default_content_type_id, aggressive_retention)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (topic_name) DO UPDATE SET
                  partition_count = GREATEST(
                    1,
                    EXCLUDED.partition_count,
                    ptopic.partition_count,
                    COALESCE(
                      (
                        SELECT MAX(m.partition_id)
                        FROM {self._message} m
                        WHERE m.topic_id = ptopic.id
                      ),
                      -1
                    ) + 1
                  ),
                  retention_max_age_seconds = EXCLUDED.retention_max_age_seconds,
                  max_rows_per_topic = EXCLUDED.max_rows_per_topic,
                  max_total_payload_bytes = EXCLUDED.max_total_payload_bytes,
                  default_content_type_id = EXCLUDED.default_content_type_id,
                  aggressive_retention = EXCLUDED.aggressive_retention
                """,
                (
                    topic_name,
                    partition_count,
                    retention_max_age_seconds,
                    max_rows_per_topic,
                    max_total_payload_bytes,
                    default_ct_id,
                    aggressive_retention,
                ),
            )
        return self._refresh_topic_row_cache(conn, topic_name)[0]

    def _compute_stored_content_type_id(
        self,
        conn: PGConnection,
        topic_default_id: Optional[int],
        content_type: Optional[str],
    ) -> Optional[int]:
        if not content_type:
            return None
        cid = self._ensure_mime_registered(conn, content_type)
        if topic_default_id is not None and topic_default_id == cid:
            return None
        return cid

    def _enqueue_message_in_transaction(
        self,
        conn: PGConnection,
        *,
        topic_row: TopicRow,
        routing_key: str,
        priority: int,
        payload: bytes,
        content_type: Optional[str],
        headers: Sequence[Tuple[str, bytes]],
        payload_compression: int = 0,
    ) -> PgQueueMessageHandle:
        """Insert one row inside the caller's open transaction (no commit)."""
        topic_id, pc, topic_default_ct_id = topic_row
        if priority < 0 or priority > 9:
            raise ValueError(f"priority {priority} out of range [0, 9]")
        partition_id = stable_partition_id(routing_key, pc)
        stored_ct_id = self._compute_stored_content_type_id(
            conn, topic_default_ct_id, content_type
        )

        lock_k = advisory_lock_key(topic_id, partition_id)
        hdr_json = headers_to_json(headers)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_k,))
            cur.execute(
                f"""
                WITH mx AS (
                  SELECT COALESCE(MAX(enqueue_seq), 0) AS max_seq
                  FROM {self._message}
                  WHERE topic_id = %s AND partition_id = %s
                )
                INSERT INTO {self._message}
                  (topic_id, partition_id, routing_key, enqueue_seq, priority, payload, content_type_id, payload_compression, headers)
                SELECT %s, %s, %s, mx.max_seq + 1, %s, %s, %s, %s, CAST(%s AS jsonb)
                FROM mx
                RETURNING id, enqueued_at, enqueue_seq
                """,
                (
                    topic_id,
                    partition_id,
                    topic_id,
                    partition_id,
                    routing_key,
                    priority,
                    payload,
                    stored_ct_id,
                    payload_compression,
                    hdr_json,
                ),
            )
            row = cur.fetchone()
            assert row is not None
            mid = int(row[0])
            enq_at = row[1]
            enq_seq = int(row[2])

        return PgQueueMessageHandle(
            id=mid,
            enqueued_at=enq_at,
            topic_id=topic_id,
            partition_id=partition_id,
            enqueue_seq=enq_seq,
        )

    def enqueue(
        self,
        conn: PGConnection,
        *,
        topic_name: str,
        routing_key: str,
        partition_count: int,
        retention_max_age_seconds: int,
        max_rows_per_topic: int,
        max_total_payload_bytes: int,
        default_content_type_mime: Optional[str] = None,
        aggressive_retention: bool = False,
        priority: int,
        payload: bytes,
        content_type: Optional[str],
        headers: Sequence[Tuple[str, bytes]],
        payload_compression: int = 0,
    ) -> PgQueueMessageHandle:
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            self.ensure_topic(
                conn,
                topic_name,
                partition_count,
                retention_max_age_seconds,
                max_rows_per_topic,
                max_total_payload_bytes,
                default_content_type_mime=default_content_type_mime,
                aggressive_retention=aggressive_retention,
            )
            topic_row = self._get_topic_row(conn, topic_name)
            handle = self._enqueue_message_in_transaction(
                conn,
                topic_row=topic_row,
                routing_key=routing_key,
                priority=priority,
                payload=payload,
                content_type=content_type,
                headers=headers,
                payload_compression=payload_compression,
            )
            conn.commit()
            return handle
        except Exception:
            conn.rollback()
            raise
        finally:
            restore_pg_connection_autocommit(conn, old_autocommit)

    def enqueue_batch(
        self,
        conn: PGConnection,
        items: Sequence[EnqueueBatchItem],
        *,
        partition_count: int,
        retention_max_age_seconds: int,
        max_rows_per_topic: int,
        max_total_payload_bytes: int,
        default_content_type_mime: Optional[str] = None,
        aggressive_retention: bool = False,
    ) -> List[PgQueueMessageHandle]:
        """Enqueue many records in one PostgreSQL transaction (single commit)."""
        if not items:
            return []
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            handles: List[PgQueueMessageHandle] = []
            topic_rows_in_batch: Dict[str, TopicRow] = {}
            for it in items:
                topic_row = topic_rows_in_batch.get(it.topic_name)
                if topic_row is None:
                    self.ensure_topic(
                        conn,
                        it.topic_name,
                        partition_count,
                        retention_max_age_seconds,
                        max_rows_per_topic,
                        max_total_payload_bytes,
                        default_content_type_mime=default_content_type_mime,
                        aggressive_retention=aggressive_retention,
                    )
                    topic_row = self._get_topic_row(conn, it.topic_name)
                    topic_rows_in_batch[it.topic_name] = topic_row
                handles.append(
                    self._enqueue_message_in_transaction(
                        conn,
                        topic_row=topic_row,
                        routing_key=it.routing_key,
                        priority=it.priority,
                        payload=it.payload,
                        content_type=it.content_type,
                        headers=it.headers,
                        payload_compression=it.payload_compression,
                    )
                )
            conn.commit()
            return handles
        except Exception:
            conn.rollback()
            raise
        finally:
            restore_pg_connection_autocommit(conn, old_autocommit)

    def _receive_content_type_expr(self) -> str:
        return (
            f"(SELECT ct.mime FROM {self._content_type} ct WHERE ct.id = "
            f"COALESCE(m.content_type_id, (SELECT t.default_content_type_id FROM "
            f"{self._topic} t WHERE t.id = m.topic_id)))"
        )

    def _load_committed_offset(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT offset_value FROM {self._consumer_offset}
                WHERE consumer_group = %s AND topic_id = %s AND partition_id = %s
                """,
                (consumer_group, topic_id, partition_id),
            )
            row = cur.fetchone()
            if row is None:
                return 0
            return int(row[0])

    def _load_max_enqueue_seq(
        self, conn: PGConnection, topic_id: int, partition_id: int
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(MAX(enqueue_seq), 0) FROM {self._message}
                WHERE topic_id = %s AND partition_id = %s
                """,
                (topic_id, partition_id),
            )
            row = cur.fetchone()
            if row is None:
                return 0
            return int(row[0])

    def partition_max_enqueue_seqs(
        self, conn: PGConnection, topic_id: int, partition_count: int
    ) -> Dict[int, int]:
        out: Dict[int, int] = {p: 0 for p in range(partition_count)}
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT partition_id, COALESCE(MAX(enqueue_seq), 0)
                FROM {self._message}
                WHERE topic_id = %s
                GROUP BY partition_id
                """,
                (topic_id,),
            )
            for row in cur.fetchall():
                out[int(row[0])] = int(row[1])
        return out

    def get_committed_offset(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
    ) -> int:
        return self._load_committed_offset(conn, consumer_group, topic_id, partition_id)

    def _load_committed_offsets_for_topic(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_count: int,
    ) -> Dict[int, int]:
        """All committed offsets for a group/topic; missing partitions default to 0."""
        out: Dict[int, int] = {p: 0 for p in range(partition_count)}
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT partition_id, offset_value FROM {self._consumer_offset}
                WHERE consumer_group = %s AND topic_id = %s
                """,
                (consumer_group, topic_id),
            )
            for row in cur.fetchall():
                pid = int(row[0])
                if 0 <= pid < partition_count:
                    out[pid] = int(row[1])
        return out

    def detect_offset_ahead_of_log(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_count: int,
        *,
        topic_name: Optional[str] = None,
    ) -> List[PartitionOffsetSkew]:
        max_seqs = self.partition_max_enqueue_seqs(conn, topic_id, partition_count)
        committed_by_partition = self._load_committed_offsets_for_topic(
            conn, consumer_group, topic_id, partition_count
        )
        skewed: List[PartitionOffsetSkew] = []
        for p in range(partition_count):
            max_seq = max_seqs.get(p, 0)
            committed = committed_by_partition.get(p, 0)
            if committed > max_seq:
                skewed.append(
                    PartitionOffsetSkew(
                        consumer_group=consumer_group,
                        topic_id=topic_id,
                        topic_name=topic_name,
                        partition_id=p,
                        committed_offset=committed,
                        max_seq=max_seq,
                        ahead_by=committed - max_seq,
                    )
                )
        return skewed

    def _select_candidate_handles_for_priority(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        min_exclusive_seq: int,
        priority: int,
        limit: int,
    ) -> List[PgQueueMessageHandle]:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT m.id, m.enqueued_at, m.topic_id, m.partition_id, m.enqueue_seq
                FROM {self._message} m
                LEFT JOIN {self._lease} l
                  ON l.message_id = m.id
                 AND l.message_enqueued_at = m.enqueued_at
                 AND l.consumer_group = %s
                WHERE m.topic_id = %s AND m.partition_id = %s
                  AND m.enqueue_seq > %s
                  AND m.priority = %s
                  AND (l.id IS NULL OR l.lock_owner <> %s)
                ORDER BY m.enqueue_seq ASC
                LIMIT %s
                """,
                (
                    consumer_group,
                    topic_id,
                    partition_id,
                    min_exclusive_seq,
                    priority,
                    ACKED_LOCK_OWNER,
                    limit,
                ),
            )
            rows = cur.fetchall()
        return [
            PgQueueMessageHandle(
                id=int(r[0]),
                enqueued_at=r[1],
                topic_id=int(r[2]),
                partition_id=int(r[3]),
                enqueue_seq=int(r[4]),
            )
            for r in rows
        ]

    def _try_acquire_lease_for_group(
        self,
        conn: PGConnection,
        handle: PgQueueMessageHandle,
        consumer_group: str,
        lock_owner: str,
        visibility_seconds: float,
    ) -> bool:
        lease = self._lease
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {lease}
                  (message_id, message_enqueued_at, consumer_group, lock_owner, locked_until)
                VALUES (%s, %s, %s, %s, NOW() + %s * INTERVAL '1 second')
                ON CONFLICT (message_id, message_enqueued_at, consumer_group) DO UPDATE SET
                  lock_owner = EXCLUDED.lock_owner,
                  locked_until = EXCLUDED.locked_until
                WHERE {lease}.locked_until < NOW()
                  AND {lease}.lock_owner <> %s
                RETURNING {lease}.message_id
                """,
                (
                    handle.id,
                    handle.enqueued_at,
                    consumer_group,
                    lock_owner,
                    visibility_seconds,
                    ACKED_LOCK_OWNER,
                ),
            )
            row = cur.fetchone()
            return row is not None

    def _load_received_message_row(
        self,
        conn: PGConnection,
        handle: PgQueueMessageHandle,
        lock_owner: str,
    ) -> PgQueueReceivedMessage:
        ctype_expr = self._receive_content_type_expr()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT m.priority, m.payload, {ctype_expr} AS content_type,
                       m.payload_compression, m.headers, m.routing_key
                FROM {self._message} m
                WHERE m.id = %s AND m.enqueued_at = %s
                """,
                (handle.id, handle.enqueued_at),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"message row missing after lease: {handle}")
        pc_raw = int(row[3]) if row[3] is not None else 0
        hdrs = tuple(headers_from_db(row[4]))
        rk = row[5] if row[5] is not None else ""
        return PgQueueReceivedMessage(
            handle=handle,
            priority=int(row[0]),
            payload=bytes(row[1]),
            content_type=str(row[2]) if row[2] is not None else None,
            payload_compression=pc_raw,
            headers=hdrs,
            routing_key=str(rk),
            lock_owner=lock_owner,
        )

    def _receive_partition_for_group(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        lock_owner: str,
        visibility_seconds: float,
        limit: int,
        band_config: PriorityBandConfig,
    ) -> List[PgQueueReceivedMessage]:
        committed_offset = self._load_committed_offset(
            conn, consumer_group, topic_id, partition_id
        )
        max_seq = self._load_max_enqueue_seq(conn, topic_id, partition_id)
        if committed_offset > max_seq:
            warn_if_ahead(
                PartitionOffsetSkew(
                    consumer_group=consumer_group,
                    topic_id=topic_id,
                    partition_id=partition_id,
                    committed_offset=committed_offset,
                    max_seq=max_seq,
                    ahead_by=committed_offset - max_seq,
                )
            )
        out: List[PgQueueReceivedMessage] = []
        self._receive_partition_round(
            conn,
            consumer_group,
            topic_id,
            partition_id,
            committed_offset,
            lock_owner,
            visibility_seconds,
            limit,
            band_config,
            out,
        )
        return out

    def _receive_band_head_of_line(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        min_exclusive_seq: int,
        band: PriorityBand,
        band_limit: int,
        lock_owner: str,
        visibility_seconds: float,
        out: List[PgQueueReceivedMessage],
    ) -> int:
        if band_limit <= 0:
            return 0
        acquired = 0
        for priority in range(band.min_priority, band.max_priority + 1):
            if acquired >= band_limit:
                break
            seq_floor = min_exclusive_seq
            while acquired < band_limit:
                heads = self._select_candidate_handles_for_priority(
                    conn,
                    consumer_group,
                    topic_id,
                    partition_id,
                    seq_floor,
                    priority,
                    1,
                )
                if not heads:
                    break
                head = heads[0]
                if not self._try_acquire_lease_for_group(
                    conn, head, consumer_group, lock_owner, visibility_seconds
                ):
                    break
                out.append(self._load_received_message_row(conn, head, lock_owner))
                acquired += 1
                seq_floor = head.enqueue_seq
        return acquired

    def _receive_partition_round(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        min_exclusive_seq: int,
        lock_owner: str,
        visibility_seconds: float,
        remaining: int,
        band_config: PriorityBandConfig,
        out: List[PgQueueReceivedMessage],
    ) -> None:
        limits = band_config.batch_limits(remaining)
        band_remaining = remaining
        spare = 0
        for i, band in enumerate(band_config.bands):
            if band_remaining <= 0 and spare <= 0:
                break
            band_limit = limits[i] + spare
            spare = 0
            if band_limit <= 0 or band_remaining <= 0:
                continue
            cap = min(band_limit, band_remaining)
            acquired = self._receive_band_head_of_line(
                conn,
                consumer_group,
                topic_id,
                partition_id,
                min_exclusive_seq,
                band,
                cap,
                lock_owner,
                visibility_seconds,
                out,
            )
            band_remaining -= acquired
            if acquired < cap:
                spare += cap - acquired

    def receive_batch_for_group(
        self,
        conn: PGConnection,
        *,
        consumer_group: str,
        topic_id: int,
        partition_ids: Sequence[int],
        lock_owner: str,
        visibility_timeout: timedelta,
        max_messages: int,
        priority_bands_json: str = DEFAULT_BANDS_JSON,
    ) -> List[PgQueueReceivedMessage]:
        """Poll partitions in one transaction; acquire work via ``message_group_lease`` only."""
        if max_messages <= 0 or not partition_ids:
            return []

        band_config = PriorityBandConfig.parse(priority_bands_json)
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            vis_secs = visibility_timeout.total_seconds()
            combined: List[PgQueueReceivedMessage] = []
            remaining = max_messages
            for pid in partition_ids:
                if remaining <= 0:
                    break
                batch = self._receive_partition_for_group(
                    conn,
                    consumer_group,
                    topic_id,
                    int(pid),
                    lock_owner,
                    vis_secs,
                    remaining,
                    band_config,
                )
                combined.extend(batch)
                remaining = max_messages - len(combined)
            conn.commit()
            return combined
        except Exception:
            conn.rollback()
            raise
        finally:
            restore_pg_connection_autocommit(conn, old_autocommit)

    def _load_acked_enqueue_seqs(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        min_exclusive_seq: int,
    ) -> List[int]:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT m.enqueue_seq
                FROM {self._message} m
                INNER JOIN {self._lease} l
                  ON l.message_id = m.id
                 AND l.message_enqueued_at = m.enqueued_at
                 AND l.consumer_group = %s
                WHERE m.topic_id = %s AND m.partition_id = %s
                  AND m.enqueue_seq > %s
                  AND l.lock_owner = %s
                ORDER BY m.enqueue_seq
                """,
                (
                    consumer_group,
                    topic_id,
                    partition_id,
                    min_exclusive_seq,
                    ACKED_LOCK_OWNER,
                ),
            )
            rows = cur.fetchall()
        return [int(r[0]) for r in rows]

    def _mark_acked_for_group(
        self,
        conn: PGConnection,
        consumer_group: str,
        handles: Sequence[PgQueueMessageHandle],
    ) -> int:
        with conn.cursor() as cur:
            for h in handles:
                cur.execute(
                    f"""
                    INSERT INTO {self._lease}
                      (message_id, message_enqueued_at, consumer_group, lock_owner, locked_until)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (message_id, message_enqueued_at, consumer_group)
                    DO UPDATE SET lock_owner = EXCLUDED.lock_owner,
                                  locked_until = EXCLUDED.locked_until
                    """,
                    (
                        h.id,
                        h.enqueued_at,
                        consumer_group,
                        ACKED_LOCK_OWNER,
                        ACKED_LOCKED_UNTIL,
                    ),
                )
        return len(handles)

    def commit_for_group(
        self,
        conn: PGConnection,
        consumer_group: str,
        handles: Sequence[PgQueueMessageHandle],
    ) -> int:
        """Mark group leases acked and advance contiguous ``enqueue_seq`` watermarks."""
        if not handles:
            return 0
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            marked = self._mark_acked_for_group(conn, consumer_group, handles)

            partitions_by_topic: Dict[int, set[int]] = defaultdict(set)
            for h in handles:
                partitions_by_topic[h.topic_id].add(h.partition_id)
            with conn.cursor() as cur:
                for tid, part_ids in partitions_by_topic.items():
                    for part_id in part_ids:
                        current = self._load_committed_offset(
                            conn, consumer_group, tid, part_id
                        )
                        acked = self._load_acked_enqueue_seqs(
                            conn, consumer_group, tid, part_id, current
                        )
                        new_offset = advance_watermark(current, acked)
                        if new_offset <= current:
                            continue
                        cur.execute(
                            f"""
                            INSERT INTO {self._consumer_offset} AS co
                              (consumer_group, topic_id, partition_id, offset_value, epoch)
                            VALUES (%s, %s, %s, %s, 0)
                            ON CONFLICT (consumer_group, topic_id, partition_id)
                            DO UPDATE SET offset_value = EXCLUDED.offset_value
                            """,
                            (consumer_group, tid, part_id, new_offset),
                        )
            conn.commit()
            return int(marked)
        except Exception:
            conn.rollback()
            raise
        finally:
            restore_pg_connection_autocommit(conn, old_autocommit)

    def extend_visibility_for_group(
        self,
        conn: PGConnection,
        consumer_group: str,
        handles: Collection[PgQueueMessageHandle],
        *,
        lock_owner: str,
        extend_by: timedelta,
    ) -> int:
        """Extend ``locked_until`` on lease rows for this group and ``lock_owner``."""
        if not handles:
            return 0
        handle_list = list(handles)
        secs = extend_by.total_seconds()
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                values_sql = ", ".join(["(%s, %s::timestamptz)"] * len(handle_list))
                params: List[Any] = [secs]
                for h in handle_list:
                    params.extend([h.id, h.enqueued_at])
                params.extend([consumer_group, lock_owner])
                cur.execute(
                    f"""
                    UPDATE {self._lease} AS l
                    SET locked_until = NOW() + %s * INTERVAL '1 second'
                    FROM (VALUES {values_sql}) AS v(id, enqueued_at)
                    WHERE l.message_id = v.id
                      AND l.message_enqueued_at = v.enqueued_at
                      AND l.consumer_group = %s
                      AND l.lock_owner = %s
                    """,
                    tuple(params),
                )
                updated = cur.rowcount
            conn.commit()
            return int(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            restore_pg_connection_autocommit(conn, old_autocommit)

    def apply_topic_retention(self, conn: PGConnection) -> None:
        """Run SqlSetup retention (preserves per-partition MAX(enqueue_seq) anchor rows)."""
        flush_pg_connection(conn)
        apply_retention(conn, qualified_apply_retention=self._apply_retention)

    def register_consumer(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
    ) -> None:
        """Upsert a consumer registration row (heartbeat updates ``last_heartbeat_at``)."""
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._consumer_registration}
                      (consumer_group, topic_id)
                    VALUES (%s, %s)
                    ON CONFLICT (consumer_group, topic_id)
                    DO UPDATE SET last_heartbeat_at = NOW()
                    """,
                    (consumer_group, topic_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            restore_pg_connection_autocommit(conn, old_autocommit)
