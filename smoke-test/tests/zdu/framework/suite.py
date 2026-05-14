"""Suite identifiers for the ZDU end-to-end test framework.

Each suite groups TCs that exercise a related ZDU concern. The enum value is
the lowercase short code used as the pytest marker (``suite_a``, ``suite_b``,
...) and the ``--suite`` CLI argument.
"""

from __future__ import annotations

from enum import Enum


class Suite(Enum):
    A = "a"  # Aspect schema migration (TC-001..TC-023)
    B = "b"  # ES Phase 1 — incremental reindex (TC-101..TC-112)
    C = "c"  # Rollback dual-write (TC-201..TC-208)
    D = "d"  # ES Phase 2 catch-up (TC-301..TC-309)
    E = "e"  # System-level sweep (TC-401..TC-408)
    F = "f"  # Live traffic (TC-501..TC-507)
    G = "g"  # Rollback (TC-601..TC-604)
    H = "h"  # Failure recovery (TC-701..TC-705)


# Explicit mapping — keeps the source of truth in one place and avoids a
# silent off-by-one if a TC range shifts. Tests exercise the boundaries.
_TC_RANGES: tuple[tuple[range, Suite], ...] = (
    (range(1, 24), Suite.A),
    (range(101, 113), Suite.B),
    (range(201, 209), Suite.C),
    (range(301, 310), Suite.D),
    (range(401, 409), Suite.E),
    (range(501, 508), Suite.F),
    (range(601, 605), Suite.G),
    (range(701, 706), Suite.H),
)


def suite_for_tc(tc_number: int) -> Suite | None:
    """Return the Suite that owns ``tc_number``, or None if it falls outside known ranges."""
    for tc_range, suite in _TC_RANGES:
        if tc_number in tc_range:
            return suite
    return None
