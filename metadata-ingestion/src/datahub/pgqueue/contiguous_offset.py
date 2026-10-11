"""Contiguous ``enqueue_seq`` watermark advancement for pgQueue consumer offsets."""

from __future__ import annotations

from typing import Sequence


def advance_watermark(current_offset: int, acked_enqueue_seqs: Sequence[int]) -> int:
    """Advance only across a contiguous run starting at ``current_offset + 1``."""
    if not acked_enqueue_seqs:
        return current_offset
    watermark = current_offset
    for seq in sorted(acked_enqueue_seqs):
        if seq <= watermark:
            continue
        if seq == watermark + 1:
            watermark = seq
        else:
            break
    return watermark
