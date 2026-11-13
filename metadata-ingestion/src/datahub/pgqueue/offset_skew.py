"""STUCK_AHEAD detection and rate-limited warnings for pgQueue consumer offsets."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_WARN_INTERVAL_SEC = 60.0
_last_warn_at: Dict[str, float] = {}


@dataclass(frozen=True)
class PartitionOffsetSkew:
    consumer_group: str
    topic_id: int
    partition_id: int
    committed_offset: int
    max_seq: int
    ahead_by: int
    topic_name: Optional[str] = None


def warn_if_ahead(skew: PartitionOffsetSkew) -> None:
    key = f"{skew.consumer_group}:{skew.topic_id}:{skew.partition_id}"
    now = time.monotonic()
    last = _last_warn_at.get(key)
    if last is not None and now - last < _WARN_INTERVAL_SEC:
        return
    _last_warn_at[key] = now
    logger.warning(
        "pgQueue consumer offset ahead of message log (STUCK_AHEAD); consumer will not receive "
        "messages until an operator resets the offset (see docs/pgqueue-design.md). "
        "consumerGroup=%s topicId=%s topicName=%s partitionId=%s committedOffset=%s "
        "maxSeq=%s aheadBy=%s",
        skew.consumer_group,
        skew.topic_id,
        skew.topic_name,
        skew.partition_id,
        skew.committed_offset,
        skew.max_seq,
        skew.ahead_by,
    )


def warn_all(skews: List[PartitionOffsetSkew]) -> None:
    for skew in skews:
        warn_if_ahead(skew)
