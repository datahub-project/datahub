"""Finding data model, risk/impact vocabulary and the overall verdict."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Optional, TypedDict


SAFE = "safe"
# N's new data that N-1 drops or can't use: a known, expected side effect of
# rolling back a new feature, with no decision to make.
EXPECTED_LOSS = "expected_loss"
REQUIRES_ATTENTION = "requires_attention"
BLOCKS_ROLLBACK = "blocks_rollback"

DIM_PDL_SCHEMA = "pdl_schema"
DIM_MUTATOR = "mutator"
DIM_UPGRADE_STEP = "upgrade_step"
DIM_REINDEX = "reindex"
DIM_SCHEMA_VERSION = "schema_version"
# A type used by Kafka events but by no stored aspect.
DIM_EVENT_SCHEMA = "event_schema"


class CommitInfo(TypedDict):
    sha: str
    pr: Optional[str]
    url: Optional[str]  # commit page, when the repo URL is known
    pr_url: Optional[str]


VERDICT_FEASIBLE = "feasible_as_is"
VERDICT_EXPECTED_LOSS = "feasible_with_expected_loss"
VERDICT_MANUAL = "feasible_with_manual_intervention"
VERDICT_NOT_RECOMMENDED = "not_recommended"

VERDICT_LABELS = {
    VERDICT_FEASIBLE: "✅ Feasible as-is",
    VERDICT_EXPECTED_LOSS: "✅ Feasible, with expected loss",
    VERDICT_MANUAL: "⚠️ Feasible after review",
    VERDICT_NOT_RECOMMENDED: "\U0001f6d1 Not feasible until blockers are fixed",
}

# Lead-ins to the per-risk breakdown the report prints under the verdict.
VERDICT_DESCRIPTIONS = {
    VERDICT_FEASIBLE: "All changes are safe; rollback N → N-1 needs no manual steps.",
    VERDICT_EXPECTED_LOSS: (
        "Rollback needs no manual steps, but it removes the data of N's new features:"
    ),
    VERDICT_MANUAL: "Rollback can go ahead once the items that need a decision are checked:",
    VERDICT_NOT_RECOMMENDED: "Don't roll back until the blockers are fixed:",
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
    # Commits on N's first-parent history that changed this file, newest first.
    commits: list[CommitInfo] = field(default_factory=list)
    # Structure behind `summary`, used to render the report; not in the JSON.
    subject: Optional[str] = None  # the field, member, type or class changed
    record: Optional[str] = None  # nested record that holds `subject`
    hop: Optional[str] = None  # mutator version hop, e.g. "v1→v2"
    # For relationship changes N-1 has to reconcile: the relationship name and
    # whether N "removed", "renamed" or "added" it on this field.
    relationship: Optional[str] = None
    rel_change: Optional[str] = None
    rel_new: Optional[str] = None  # the new name, for a renamed relationship

    @property
    def change(self) -> str:
        """The summary without its " — <note>" suffix."""
        return self.summary.split(" — ")[0]


_INTERNAL_FIELDS = {"subject", "record", "hop", "relationship", "rel_change", "rel_new"}


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
MAY_TRUNCATE = "ok, may truncate"
STALE = "ok, stale"
FAILS = "fails"
DROPS_NEW_FIELD = "ok, drops N's new field"
LOSS_YES = "yes"
LOSS_NO = "no"
LOSS_IF_OUT_OF_RANGE = "if out of range"
LOSS_FRACTIONS = "drops fractions"
LOSS_PRECISION = "rounds large values"
# The stored record is intact, but graph edges derived from it are missing
# or stale until N-1 saves the record again.
LOSS_GRAPH_ONLY = "graph only"
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
    if EXPECTED_LOSS in risks:
        return VERDICT_EXPECTED_LOSS
    return VERDICT_FEASIBLE


# Worst first.
READ_SEVERITY = [API_FAILS, UI_API_FAILS, MAY_TRUNCATE, STALE, OK]
WRITE_SEVERITY = [FAILS, DROPS_NEW_FIELD, OK]
LOSS_SEVERITY = [
    LOSS_YES,
    LOSS_FRACTIONS,
    LOSS_IF_OUT_OF_RANGE,
    LOSS_PRECISION,
    LOSS_GRAPH_ONLY,
    LOSS_NO,
]


def worst(values: list[Optional[str]], order: list[str]) -> str:
    """Most severe value. Values outside `order` ("not analysed", "unknown")
    mean the impact isn't known, so they win over any known value."""
    present = [v for v in values if v]
    unranked = [v for v in present if v not in order]
    if unranked:
        return unranked[0]
    return min(present, key=order.index) if present else order[-1]
