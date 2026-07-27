"""Suite identifiers for the ZDU end-to-end test framework.

Suites map 1:1 to the production ZDU upgrade phases they exercise:

* Blocking phase     → Suite B: ES Phase 1 reindexing
* Dual-write phase   → Suite D: ES Phase 2 reindexing
* Non-blocking phase → Suite N: Aspect schema migration & system sweep
* Concurrent operations → Suite C: Live Read/write and Swap

The enum value is the lowercase short code used as the pytest marker
(``suite_b``, ``suite_d``, ``suite_n``, ``suite_c``) and the ``--suite`` CLI argument.
"""

from __future__ import annotations

from enum import Enum


class Suite(Enum):
    B = "b"  # Blocking — ES Phase 1 reindexing (TC-101..TC-109)
    D = "d"  # Dual-write — ES Phase 2 reindexing (TC-201..TC-206)
    N = "n"  # Non-blocking — Aspect schema migration & system sweep (TC-301..TC-328)
    C = "c"  # Concurrent operation — Live Read/write and Swap (TC-401..TC-403)


# Explicit mapping — keeps the source of truth in one place and avoids a
# silent off-by-one if a TC range shifts. Tests exercise the boundaries.
_TC_RANGES: tuple[tuple[range, Suite], ...] = (
    (range(101, 110), Suite.B),
    (range(201, 207), Suite.D),
    (range(301, 332), Suite.N),
    (range(401, 404), Suite.C),
)


def suite_for_tc(tc_number: int) -> Suite | None:
    """Return the Suite that owns ``tc_number``, or None if it falls outside known ranges."""
    for tc_range, suite in _TC_RANGES:
        if tc_number in tc_range:
            return suite
    return None
