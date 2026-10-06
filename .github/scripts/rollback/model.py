"""Finding data model, risk/impact vocabulary and the overall verdict."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
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
    # Structure behind `summary`, used to render the report; not in the JSON.
    subject: Optional[str] = None  # the field, member, type or class changed
    record: Optional[str] = None  # nested record that holds `subject`
    hop: Optional[str] = None  # mutator version hop, e.g. "v1→v2"
    step_type: Optional[str] = None

    @property
    def change(self) -> str:
        """The summary without its " — <note>" suffix."""
        return self.summary.split(" — ")[0]


_INTERNAL_FIELDS = {"subject", "record", "hop", "step_type"}


def public_dict(f: RollbackFinding) -> dict:
    """The finding as written to the JSON report."""
    return {k: v for k, v in asdict(f).items() if k not in _INTERNAL_FIELDS}


@dataclass(frozen=True)
class Origin:
    """Where a finding comes from: the changed file, its aspect and its PR."""

    path: str
    aspect_name: Optional[str]
    pr: Optional[str]
    author: Optional[str]


# Impact on N-1 after rollback. Each *_SEVERITY list below ranks them.
OK = "ok"
API_FAILS = "API fails"
UI_API_FAILS = "UI/API fails"
RESTORE_FAILS = "restore-indices fails"
MAY_TRUNCATE = "ok, may truncate"
STALE = "ok, stale"
FAILS = "fails"
DROPS_NEW_FIELD = "ok, drops N's new field"
LOSS_YES = "yes"
LOSS_NO = "no"
LOSS_IF_OUT_OF_RANGE = "if out of range"
# Unranked: the impact isn't known, so `worst` lets these win.
UNKNOWN = "unknown"
NOT_ANALYSED = "not analysed"


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
READ_SEVERITY = [API_FAILS, UI_API_FAILS, RESTORE_FAILS, MAY_TRUNCATE, STALE, OK]
WRITE_SEVERITY = [FAILS, DROPS_NEW_FIELD, OK]
LOSS_SEVERITY = [LOSS_YES, LOSS_IF_OUT_OF_RANGE, LOSS_NO]


def worst(values: list[Optional[str]], order: list[str]) -> str:
    """Most severe value. Values outside `order` ("not analysed", "unknown")
    mean the impact isn't known, so they win over any known value."""
    present = [v for v in values if v]
    unranked = [v for v in present if v not in order]
    if unranked:
        return unranked[0]
    return min(present, key=order.index) if present else order[-1]
