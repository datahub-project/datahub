"""Distributed lock for the DataHub Documents source.

The documents source is typically scheduled on a short interval (e.g. every 15
minutes). A full scroll + embedding pass can take longer than that interval, so two
runs could overlap and redundantly re-embed the same documents (and race on the
SemanticContent aspect). This module implements a lightweight lease-based lock backed
by a ``dataHubStepState`` entity.

Why ``dataHubStepState``:
  The ``dataHubStepState`` entity is a general-purpose key/value store in DataHub: each
  entity is keyed by an arbitrary string id, and its ``dataHubStepStateProperties``
  aspect holds a ``map[string, string]`` for arbitrary payloads. We use that map as the
  lock lease record (status, run_id, expiry, etc.). Writes go through the metadata API
  with ``EmitMode.SYNC_PRIMARY`` so the lease is committed to the primary store (MySQL)
  before returning, giving concurrent runners read-after-write visibility without relying
  on Elasticsearch (unlike execution-request lookups, which are search-backed and
  eventually consistent).

Design:
- One ``dataHubStepState`` entity per lock id; lease fields live in the properties map.
- **Cold start** (lock entity does not yet exist): ``CREATE_ENTITY`` + ``CREATE`` with
  ``If-None-Match: *`` so only one concurrent runner creates the entity; losers are
  dropped server-side without error (see ``CreateIfNotExistsValidator``).
- **Takeover** (entity exists but lease is released/expired): ``UPSERT`` the properties
  aspect, then re-read to confirm we won. There is no conditional-write primitive for
  "update only if expired", so this path retains a small race window; jitter + confirm
  keeps it negligible for a 15-minute schedule.
- **Renewal / release**: ``UPSERT`` while we hold the lock; ``heartbeat()`` extends expiry.
- Leases carry a TTL so a crashed run that never releases does not block forever. The
  holder renews periodically, so TTL only needs to exceed the renewal interval (not total
  run duration).
"""

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DataHubStepStatePropertiesClass,
)
from datahub.metadata.urns import DataHubStepStateUrn

logger = logging.getLogger(__name__)

# Actor recorded on the lease audit stamp. Not a real user; identifies the writer.
_LOCK_ACTOR = "urn:li:corpuser:__datahub_documents_ingestion"

_STATUS_RUNNING = "running"
_STATUS_RELEASED = "released"

# Conditional-create header: if the entity/aspect already exists the write is dropped
# server-side (not an error). See docs/advanced/mcp-mcl.md and CreateIfNotExistsValidator.
_IF_NONE_MATCH = {"If-None-Match": "*"}


@dataclass
class _LeaseState:
    status: Optional[str]
    run_id: Optional[str]
    acquired_at_ms: Optional[int]
    expires_at_ms: Optional[int]

    @property
    def is_active(self) -> bool:
        """A lease that is held and has not yet expired."""
        if self.status != _STATUS_RUNNING:
            return False
        if self.expires_at_ms is None:
            # Held with no expiry recorded; treat as active to be safe.
            return True
        return _now_ms() < self.expires_at_ms


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class DocumentIndexingLock:
    """Lease-based lock backed by a ``dataHubStepState`` entity."""

    def __init__(
        self,
        graph: DataHubGraph,
        lock_id: str,
        run_id: str,
        ttl_seconds: int,
        renewal_interval_seconds: int = 300,
    ):
        self.graph = graph
        self.run_id = run_id
        self.ttl_seconds = ttl_seconds
        self.renewal_interval_seconds = renewal_interval_seconds
        self.urn = DataHubStepStateUrn(lock_id)
        self._acquired = False
        self._acquired_at_ms: Optional[int] = None
        # Wall-clock (monotonic) of the last successful lease write, used to throttle
        # heartbeat renewals.
        self._last_renew_monotonic: float = 0.0

    def _read_lease(self) -> Optional[_LeaseState]:
        props = self.graph.get_aspect(
            entity_urn=str(self.urn),
            aspect_type=DataHubStepStatePropertiesClass,
        )
        if props is None:
            return None
        p = props.properties or {}
        return _LeaseState(
            status=p.get("status"),
            run_id=p.get("run_id"),
            acquired_at_ms=_safe_int(p.get("acquired_at_ms")),
            expires_at_ms=_safe_int(p.get("expires_at_ms")),
        )

    def _lease_properties(
        self, status: str, now: Optional[int] = None
    ) -> dict[str, str]:
        now = now if now is not None else _now_ms()
        if self._acquired_at_ms is None:
            self._acquired_at_ms = now
        return {
            "status": status,
            "run_id": self.run_id,
            "acquired_at_ms": str(self._acquired_at_ms),
            "expires_at_ms": str(now + self.ttl_seconds * 1000),
        }

    def _properties_aspect(self, status: str) -> DataHubStepStatePropertiesClass:
        now = _now_ms()
        return DataHubStepStatePropertiesClass(
            properties=self._lease_properties(status, now=now),
            lastModified=AuditStampClass(time=now, actor=_LOCK_ACTOR),
        )

    def _write_lease(self, status: str) -> None:
        """UPSERT the lease record (renewal, takeover, or release)."""
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(self.urn),
            aspect=self._properties_aspect(status),
        )
        # SYNC_PRIMARY: commit to the primary store (MySQL) before returning so a
        # concurrent runner reads our lease immediately, without waiting on the async
        # MCP/search-indexing pipeline.
        self.graph.emit_mcp(mcp, emit_mode=EmitMode.SYNC_PRIMARY)
        self._last_renew_monotonic = time.monotonic()

    def _try_create_lease_atomic(self) -> None:
        """Create the lock entity + properties with If-None-Match (cold-start only).

        When two runs race on first deploy, only one CREATE_ENTITY succeeds; the other is
        dropped per ``CreateIfNotExistsValidator`` without raising an error.
        """
        self._acquired_at_ms = _now_ms()
        key_aspect = self.urn.to_key_aspect()
        props_aspect = self._properties_aspect(_STATUS_RUNNING)
        mcps = [
            MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=key_aspect,
                changeType=ChangeTypeClass.CREATE_ENTITY,
                headers=_IF_NONE_MATCH,
            ),
            MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=props_aspect,
                changeType=ChangeTypeClass.CREATE,
                headers=_IF_NONE_MATCH,
            ),
        ]
        self.graph.emit_mcps(mcps, emit_mode=EmitMode.SYNC_PRIMARY)
        self._last_renew_monotonic = time.monotonic()

    def _confirm_acquired(self) -> tuple[bool, Optional[_LeaseState]]:
        """Re-read the lease; return (won, confirmed_state)."""
        confirmed = self._read_lease()
        if confirmed is not None and confirmed.run_id == self.run_id:
            self._acquired = True
            logger.info(
                f"Acquired document indexing lock {self.urn} for run {self.run_id}"
            )
            return True, confirmed
        winner = confirmed.run_id if confirmed else "unknown"
        logger.info(
            f"Lost race for document indexing lock {self.urn} to run {winner}; "
            "skipping this run."
        )
        return False, confirmed

    def acquire(self) -> bool:
        """Attempt to acquire the lock.

        Returns True if this run now holds the lock, False if another active run holds it.
        """
        existing = self._read_lease()
        if (
            existing is not None
            and existing.is_active
            and existing.run_id != self.run_id
        ):
            logger.info(
                f"Document indexing lock {self.urn} is held by run "
                f"{existing.run_id} (expires_at_ms={existing.expires_at_ms}); "
                "skipping this run."
            )
            return False

        if (
            existing is not None
            and existing.is_active
            and existing.run_id == self.run_id
        ):
            # Re-entrant within the same run.
            self._acquired = True
            return True

        time.sleep(random.uniform(0, 0.25))

        # Cold start: no lease record and the lock entity does not exist yet. Use
        # CREATE_ENTITY + CREATE with If-None-Match for an atomic first claim.
        if existing is None and not self.graph.exists(str(self.urn)):
            self._try_create_lease_atomic()
            won, confirmed = self._confirm_acquired()
            if won:
                return True
            # Lost the cold-start race; do not UPSERT over the winner's lease.
            if (
                confirmed is not None
                and confirmed.is_active
                and confirmed.run_id != self.run_id
            ):
                return False

        # Takeover path: entity exists but lease is released/expired/missing.
        self._write_lease(_STATUS_RUNNING)
        won, _ = self._confirm_acquired()
        return won

    def heartbeat(self) -> None:
        """Renew (extend) the lease if held and the renewal interval has elapsed.

        Call this frequently from long-running processing loops (e.g. per page / per
        document); it is throttled to write at most once per ``renewal_interval_seconds``,
        so it is cheap to call often. Renewing keeps a multi-hour run's lease alive even
        though the TTL is short, while a crashed run stops renewing and its lease expires.
        """
        if not self._acquired:
            return
        if (
            time.monotonic() - self._last_renew_monotonic
            < self.renewal_interval_seconds
        ):
            return
        try:
            self._write_lease(_STATUS_RUNNING)
            logger.debug(
                f"Renewed document indexing lock {self.urn} for run {self.run_id}"
            )
        except Exception as e:
            # A transient renewal failure is not fatal: the TTL still has margin over the
            # renewal interval, so a subsequent heartbeat can recover before expiry.
            logger.warning(f"Failed to renew document indexing lock {self.urn}: {e}")

    def release(self) -> None:
        """Release the lock if held by this run.

        We mark the lease released rather than deleting the entity so the slot stays
        free for future runs without requiring delete privileges, and so the last
        holder remains visible for debugging.
        """
        if not self._acquired:
            return
        try:
            current = self._read_lease()
            # Only release if we still own it; a takeover by another run after our TTL
            # expired must not be clobbered.
            if current is not None and current.run_id not in (None, self.run_id):
                logger.info(
                    f"Not releasing lock {self.urn}: now owned by run {current.run_id}"
                )
                return
            self._write_lease(_STATUS_RELEASED)
            logger.info(f"Released document indexing lock {self.urn}")
        except Exception as e:
            logger.warning(f"Failed to release document indexing lock {self.urn}: {e}")
        finally:
            self._acquired = False
            self._acquired_at_ms = None


def _safe_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
