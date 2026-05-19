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
   Dequeue acquires work only through leases (not row locks on ``message``).
   ``consumer_offset.offset_value`` records the highest ``enqueue_seq`` committed
   per group/partition using ``GREATEST`` so it never regresses.
3. Visibility / ``lock_owner`` string format expected by peer clients (lease rows).
4. Persisted ``routing_key`` on each message row (Kafka-style enqueue key; aligns with Java).
5. Topic upsert: ``partition_count`` is set with ``GREATEST`` so it never drops below the prior
   catalog value or below ``MAX(partition_id)+1`` over existing message rows (matches Java
   ``EbeanPostgresMetadataQueueStore`` / SqlSetup).
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
from datahub.pgqueue.headers import headers_from_db, headers_to_json
from datahub.pgqueue.offset_skew import PartitionOffsetSkew, warn_if_ahead
from datahub.pgqueue.priority_bands import DEFAULT_BANDS_JSON, PriorityBandConfig
from datahub.pgqueue.sql import qualified_table

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection

logger = logging.getLogger(__name__)


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
        self._lease = qualified_table(schema, table_prefix, "message_group_lease")

    def fetch_topic_row(
        self, conn: PGConnection, topic_name: str
    ) -> Optional[Tuple[int, int, Optional[int]]]:
        """Return ``(topic_id, partition_count, default_content_type_id)`` or None."""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, partition_count, default_content_type_id FROM {self._topic} WHERE topic_name = %s",
                (topic_name,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            dct = int(row[2]) if row[2] is not None else None
            return int(row[0]), int(row[1]), dct

    def _ensure_mime_registered(self, conn: PGConnection, mime: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._content_type} (mime) VALUES (%s) ON CONFLICT (mime) DO NOTHING",
                (mime,),
            )
            cur.execute(
                f"SELECT id FROM {self._content_type} WHERE mime = %s",
                (mime,),
            )
            row = cur.fetchone()
            assert row is not None
            return int(row[0])

    def ensure_topic(
        self,
        conn: PGConnection,
        topic_name: str,
        partition_count: int,
        retention_max_age_seconds: int,
        max_rows_per_topic: int,
        max_total_payload_bytes: int,
        default_content_type_mime: Optional[str] = None,
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
                   default_content_type_id)
                VALUES (%s, %s, %s, %s, %s, %s)
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
                  default_content_type_id = EXCLUDED.default_content_type_id
                """,
                (
                    topic_name,
                    partition_count,
                    retention_max_age_seconds,
                    max_rows_per_topic,
                    max_total_payload_bytes,
                    default_ct_id,
                ),
            )
            cur.execute(
                f"SELECT id FROM {self._topic} WHERE topic_name = %s",
                (topic_name,),
            )
            row = cur.fetchone()
            assert row is not None
            return int(row[0])

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
        topic_name: str,
        routing_key: str,
        partition_count: int,
        retention_max_age_seconds: int,
        max_rows_per_topic: int,
        max_total_payload_bytes: int,
        default_content_type_mime: Optional[str],
        priority: int,
        payload: bytes,
        content_type: Optional[str],
        headers: Sequence[Tuple[str, bytes]],
        payload_compression: int = 0,
    ) -> PgQueueMessageHandle:
        """Insert one row inside the caller's open transaction (no commit)."""
        self.ensure_topic(
            conn,
            topic_name,
            partition_count,
            retention_max_age_seconds,
            max_rows_per_topic,
            max_total_payload_bytes,
            default_content_type_mime=default_content_type_mime,
        )
        row_meta = self.fetch_topic_row(conn, topic_name)
        assert row_meta is not None
        topic_id, pc, topic_default_ct_id = row_meta
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
            handle = self._enqueue_message_in_transaction(
                conn,
                topic_name=topic_name,
                routing_key=routing_key,
                partition_count=partition_count,
                retention_max_age_seconds=retention_max_age_seconds,
                max_rows_per_topic=max_rows_per_topic,
                max_total_payload_bytes=max_total_payload_bytes,
                default_content_type_mime=default_content_type_mime,
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
    ) -> List[PgQueueMessageHandle]:
        """Enqueue many records in one PostgreSQL transaction (single commit)."""
        if not items:
            return []
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            handles: List[PgQueueMessageHandle] = []
            for it in items:
                handles.append(
                    self._enqueue_message_in_transaction(
                        conn,
                        topic_name=it.topic_name,
                        routing_key=it.routing_key,
                        partition_count=partition_count,
                        retention_max_age_seconds=retention_max_age_seconds,
                        max_rows_per_topic=max_rows_per_topic,
                        max_total_payload_bytes=max_total_payload_bytes,
                        default_content_type_mime=default_content_type_mime,
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
        skewed: List[PartitionOffsetSkew] = []
        for p in range(partition_count):
            max_seq = max_seqs.get(p, 0)
            committed = self.get_committed_offset(conn, consumer_group, topic_id, p)
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

    def _select_candidate_handles_band(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        min_exclusive_seq: int,
        min_priority: int,
        max_priority: int,
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
                  AND m.priority BETWEEN %s AND %s
                  AND (l.id IS NULL OR l.locked_until < NOW())
                ORDER BY m.priority ASC, m.enqueue_seq ASC
                LIMIT %s
                """,
                (
                    consumer_group,
                    topic_id,
                    partition_id,
                    min_exclusive_seq,
                    min_priority,
                    max_priority,
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

    def _select_candidate_handles_for_group(
        self,
        conn: PGConnection,
        consumer_group: str,
        topic_id: int,
        partition_id: int,
        min_exclusive_seq: int,
        candidate_limit: int,
        band_config: PriorityBandConfig,
    ) -> List[PgQueueMessageHandle]:
        from datahub.pgqueue.priority_bands import weighted_fair_fetch

        return weighted_fair_fetch(
            band_config,
            candidate_limit,
            lambda min_p, max_p, band_limit: self._select_candidate_handles_band(
                conn,
                consumer_group,
                topic_id,
                partition_id,
                min_exclusive_seq,
                min_p,
                max_p,
                band_limit,
            ),
        )

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
                RETURNING {lease}.message_id
                """,
                (
                    handle.id,
                    handle.enqueued_at,
                    consumer_group,
                    lock_owner,
                    visibility_seconds,
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
        safety = 0
        max_rounds = max(50, limit * 10)
        while len(out) < limit and safety < max_rounds:
            safety += 1
            candidates = self._select_candidate_handles_for_group(
                conn,
                consumer_group,
                topic_id,
                partition_id,
                committed_offset,
                max(limit * 2, 8),
                band_config,
            )
            if not candidates:
                break
            progressed = False
            for h in candidates:
                if len(out) >= limit:
                    break
                if self._try_acquire_lease_for_group(
                    conn, h, consumer_group, lock_owner, visibility_seconds
                ):
                    out.append(self._load_received_message_row(conn, h, lock_owner))
                    progressed = True
            if not progressed:
                break
        return out

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

    def commit_for_group(
        self,
        conn: PGConnection,
        consumer_group: str,
        handles: Sequence[PgQueueMessageHandle],
    ) -> int:
        """Delete group leases and advance committed offsets (max ``enqueue_seq`` per partition)."""
        if not handles:
            return 0
        flush_pg_connection(conn)
        old_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                placeholders = ", ".join(["(%s, %s::timestamptz)"] * len(handles))
                delete_params: List[Any] = [consumer_group]
                for h in handles:
                    delete_params.extend([h.id, h.enqueued_at])
                cur.execute(
                    f"""
                    DELETE FROM {self._lease}
                    WHERE consumer_group = %s
                      AND (message_id, message_enqueued_at) IN ({placeholders})
                    """,
                    tuple(delete_params),
                )
                deleted = cur.rowcount

                max_seq: Dict[int, Dict[int, int]] = defaultdict(dict)
                for h in handles:
                    part_map = max_seq[h.topic_id]
                    prev = part_map.get(h.partition_id)
                    part_map[h.partition_id] = (
                        h.enqueue_seq if prev is None else max(prev, h.enqueue_seq)
                    )
                for tid, pmap in max_seq.items():
                    for part_id, seq in pmap.items():
                        cur.execute(
                            f"""
                            INSERT INTO {self._consumer_offset} AS co
                              (consumer_group, topic_id, partition_id, offset_value, epoch)
                            VALUES (%s, %s, %s, %s, 0)
                            ON CONFLICT (consumer_group, topic_id, partition_id)
                            DO UPDATE SET offset_value = GREATEST(
                                co.offset_value, EXCLUDED.offset_value
                            )
                            """,
                            (consumer_group, tid, part_id, seq),
                        )
            conn.commit()
            return int(deleted)
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
