"""Exponential idle backoff for empty pgQueue polls.

First empty poll sleeps ``min_millis``; each subsequent empty poll doubles, capped at
``max_millis``. Call :meth:`EmptyPollBackoff.reset` after a non-empty poll.
"""

from __future__ import annotations


class EmptyPollBackoff:
    def __init__(self, min_millis: int, max_millis: int) -> None:
        if min_millis < 1:
            raise ValueError(f"empty poll min sleep must be >= 1, got {min_millis}")
        if max_millis < 1:
            raise ValueError(f"empty poll max sleep must be >= 1, got {max_millis}")
        self._max_millis = max_millis
        self._min_millis = min(min_millis, max_millis)
        self._current_millis = self._min_millis

    def next_sleep_millis(self) -> int:
        sleep = self._current_millis
        self._current_millis = self._multiply_capped(self._current_millis)
        return sleep

    def next_sleep_seconds(self) -> float:
        return self.next_sleep_millis() / 1000.0

    def reset(self) -> None:
        self._current_millis = self._min_millis

    def _multiply_capped(self, value: int) -> int:
        if value >= self._max_millis:
            return self._max_millis
        doubled = value * 2
        return min(self._max_millis, doubled)
