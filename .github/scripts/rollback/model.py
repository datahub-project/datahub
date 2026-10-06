"""Finding data model, risk/impact vocabulary and the overall verdict."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


SAFE = "safe"
REQUIRES_ATTENTION = "requires_attention"
BLOCKS_ROLLBACK = "blocks_rollback"

DIM_PDL_SCHEMA = "pdl_schema"
DIM_MUTATOR = "mutator"
DIM_UPGRADE_STEP = "upgrade_step"
DIM_REINDEX = "reindex"
DIM_SCHEMA_VERSION = "schema_version"

VERDICT_FEASIBLE = "feasible_as_is"
VERDICT_MANUAL = "feasible_with_manual_intervention"
VERDICT_NOT_RECOMMENDED = "not_recommended"

VERDICT_LABELS = {
    VERDICT_FEASIBLE: "✅ Feasible as-is",
    VERDICT_MANUAL: "⚠️ Feasible with manual intervention",
    VERDICT_NOT_RECOMMENDED: "\U0001f6d1 Not recommended",
}

VERDICT_DESCRIPTIONS = {
    VERDICT_FEASIBLE: (
        "All changes are safe; rollback N → N-1 requires no manual steps."
    ),
    VERDICT_MANUAL: (
        "Some changes need review or manual action, but none categorically "
        "block rollback."
    ),
    VERDICT_NOT_RECOMMENDED: (
        "One or more changes block rollback without prior remediation."
    ),
}


@dataclass
class RollbackFinding:
    dimension: str
    risk: str
    path: str
    aspect_name: Optional[str]
    summary: str
    detail: Optional[str] = None
    pr_number: Optional[str] = None
    author: Optional[str] = None
    reindex_required: bool = False
    # Aspects a nested-record change reaches (empty for top-level findings).
    affected_aspects: list[str] = field(default_factory=list)
    # Impact on N-1 after rollback, assuming default write validation.
    read_impact: Optional[str] = None
    write_impact: Optional[str] = None
    data_loss: Optional[str] = None


DROPS_NEW_FIELD = "ok, drops N's new field"


def impact(read: str, write: str, data_loss: str) -> dict[str, str]:
    return {"read_impact": read, "write_impact": write, "data_loss": data_loss}


def compute_verdict(findings: list[RollbackFinding]) -> str:
    risks = {f.risk for f in findings}
    if BLOCKS_ROLLBACK in risks:
        return VERDICT_NOT_RECOMMENDED
    if REQUIRES_ATTENTION in risks:
        return VERDICT_MANUAL
    return VERDICT_FEASIBLE


# Worst first.
READ_SEVERITY = [
    "API fails",
    "UI/API fails",
    "restore-indices fails",
    "ok, may truncate",
    "ok, stale",
    "ok",
]
WRITE_SEVERITY = ["fails", DROPS_NEW_FIELD, "ok"]
LOSS_SEVERITY = ["yes", "if out of range", "no"]


def worst(values: list[Optional[str]], order: list[str]) -> str:
    """Most severe value. Values outside `order` ("not analysed", "unknown")
    mean the impact isn't known, so they win over any known value."""
    present = [v for v in values if v]
    unranked = [v for v in present if v not in order]
    if unranked:
        return unranked[0]
    return min(present, key=order.index) if present else order[-1]
