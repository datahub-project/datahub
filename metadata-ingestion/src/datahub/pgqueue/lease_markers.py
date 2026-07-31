"""Sentinel values for ``message_group_lease`` rows that record an ack without deletion."""

from __future__ import annotations

from datetime import datetime, timezone

ACKED_LOCK_OWNER = "__acked__"
ACKED_LOCKED_UNTIL = datetime(1970, 1, 1, tzinfo=timezone.utc)


def is_acked(lock_owner: str) -> bool:
    return lock_owner == ACKED_LOCK_OWNER
