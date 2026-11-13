"""Priority band configuration and weighted fair queuing for pgQueue dequeue."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

MIN_PRIORITY = 0
MAX_PRIORITY = 9
DEFAULT_PRIORITY = 5

DEFAULT_BANDS_JSON = '[{"range":[0,3],"weight":70},{"range":[4,6],"weight":20},{"range":[7,9],"weight":10}]'


@dataclass(frozen=True)
class PriorityBand:
    min_priority: int
    max_priority: int
    weight: int

    def __post_init__(self) -> None:
        if self.min_priority < MIN_PRIORITY or self.min_priority > MAX_PRIORITY:
            raise ValueError(f"min_priority {self.min_priority} out of range [0, 9]")
        if self.max_priority < MIN_PRIORITY or self.max_priority > MAX_PRIORITY:
            raise ValueError(f"max_priority {self.max_priority} out of range [0, 9]")
        if self.min_priority > self.max_priority:
            raise ValueError(
                f"min_priority {self.min_priority} > max_priority {self.max_priority}"
            )
        if self.weight <= 0:
            raise ValueError(f"weight must be positive, got {self.weight}")


@dataclass(frozen=True)
class PriorityBandConfig:
    bands: List[PriorityBand]

    def __post_init__(self) -> None:
        if not self.bands:
            raise ValueError("At least one priority band is required")
        covered = [False] * (MAX_PRIORITY + 1)
        for band in self.bands:
            for p in range(band.min_priority, band.max_priority + 1):
                if covered[p]:
                    raise ValueError(
                        f"Priority {p} is covered by multiple bands (overlap)"
                    )
                covered[p] = True
        for p in range(MIN_PRIORITY, MAX_PRIORITY + 1):
            if not covered[p]:
                raise ValueError(
                    f"Priority {p} is not covered by any band (gap in [0, 9])"
                )

    @staticmethod
    def parse(json_str: str) -> PriorityBandConfig:
        raw = json.loads(json_str)
        bands = []
        for entry in raw:
            rng = entry.get("range")
            if not rng or len(rng) != 2:
                raise ValueError(
                    "Each band must have a 'range' array of exactly 2 integers"
                )
            weight = entry.get("weight")
            if weight is None:
                raise ValueError("Each band must have a 'weight' field")
            bands.append(PriorityBand(rng[0], rng[1], int(weight)))
        return PriorityBandConfig(bands=bands)

    def batch_limits(self, total: int) -> List[int]:
        """Proportional per-band limits. Remainders go round-robin from first band."""
        n = len(self.bands)
        if total <= 0:
            return [0] * n
        total_weight = sum(b.weight for b in self.bands)
        limits = [total * b.weight // total_weight for b in self.bands]
        allocated = sum(limits)
        remainder = total - allocated
        for i in range(remainder):
            limits[i % n] += 1
        return limits


PRIORITY_BANDS_ENV_VAR = "DATAHUB_PGQUEUE_PRIORITY_BANDS"


def resolve_priority_bands_json(
    server_config: Optional[Dict[str, Any]] = None,
    env_override: Optional[str] = None,
) -> str:
    """Resolve priority bands JSON using the precedence hierarchy:

    1. GMS server config (``/config`` endpoint → ``pgQueue.priorityBands``)
    2. Environment variable (``DATAHUB_PGQUEUE_PRIORITY_BANDS``)
    3. Hardcoded default (``DEFAULT_BANDS_JSON``)

    Args:
        server_config: Raw dict from GMS ``/config`` response (may be None if unavailable).
        env_override: Explicit env value; if None, reads from os.environ.
    """
    if server_config:
        pgqueue_cfg = server_config.get("pgQueue")
        if isinstance(pgqueue_cfg, dict):
            bands = pgqueue_cfg.get("priorityBands")
            if bands and isinstance(bands, str):
                return bands

    env_val = (
        env_override
        if env_override is not None
        else os.environ.get(PRIORITY_BANDS_ENV_VAR)
    )
    if env_val:
        return env_val

    return DEFAULT_BANDS_JSON
