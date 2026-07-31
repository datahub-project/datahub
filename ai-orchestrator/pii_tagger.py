"""Propose-then-apply orchestration.

Nothing is written until a human confirms, so the two calls are separate and the
proposal is held server-side in between. What makes this fast is what it avoids: the
rules settle most columns with no model call, the table description is only fetched when
the model actually has something to judge, and the write is a single request.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time

from pydantic import BaseModel

import pii_writer
from pii_classifier import classify
from pii_models import Column, Source, Verdict
from pii_rules import apply_rules
from pii_taxonomy import DEFAULT_CONFIDENCE_FLOOR, PROVENANCE_TAG, is_taxonomy_tag

logger = logging.getLogger("pii_tagger")

CONFIDENCE_FLOOR = float(
    os.environ.get("PII_CONFIDENCE_FLOOR", str(DEFAULT_CONFIDENCE_FLOOR))
)
PROPOSAL_TTL_SECONDS = float(os.environ.get("PII_PROPOSAL_TTL_SECONDS", "1800"))
SCHEMA_PAGE_SIZE = 100

_URN_TABLE = re.compile(r"\(([^,]+),(.+),([^,]+)\)$")


class TaggerError(RuntimeError):
    pass


class Proposal(BaseModel):
    dataset_urn: str
    dataset_name: str
    total_columns: int
    verdicts: list[Verdict]
    skipped: dict[str, list[str]] = {}
    fingerprint: str
    created_at: float

    @property
    def expired(self) -> bool:
        return time.time() - self.created_at > PROPOSAL_TTL_SECONDS

    @property
    def confident(self) -> list[Verdict]:
        return [v for v in self.verdicts if v.confidence >= CONFIDENCE_FLOOR]

    @property
    def uncertain(self) -> list[Verdict]:
        return [v for v in self.verdicts if v.confidence < CONFIDENCE_FLOOR]


_PROPOSALS: dict[str, Proposal] = {}


def short_name(dataset_urn: str) -> str:
    """`urn:li:dataset:(urn:li:dataPlatform:mysql,appdb.users,PROD)` -> `appdb.users`."""
    match = _URN_TABLE.search(dataset_urn.strip())
    return match.group(2) if match else dataset_urn.strip()


def _fingerprint(columns: list[Column]) -> str:
    material = "|".join(
        f"{c.field_path}:{','.join(sorted(c.existing_tags))}" for c in columns
    )
    return hashlib.sha256(material.encode()).hexdigest()[:16]


def _find(dataset_urn: str) -> Proposal | None:
    """Exact URN, else an unambiguous table-name match. Never raises.

    The name fallback exists because persisted history keeps only message text: the
    tool_use block holding the exact URN is gone by the confirmation turn, so the model
    retypes it and occasionally gets the platform wrong.
    """
    proposal = _PROPOSALS.get(dataset_urn)
    if proposal is None:
        wanted = short_name(dataset_urn)
        matches = [p for key, p in _PROPOSALS.items() if short_name(key) == wanted]
        if len(matches) != 1:
            return None
        proposal = matches[0]
        logger.info("Matched %s to cached proposal by table name", dataset_urn)
    if proposal.expired:
        _PROPOSALS.pop(proposal.dataset_urn, None)
        return None
    return proposal


def pending() -> list[Proposal]:
    return [p for p in _PROPOSALS.values() if not p.expired]


def pending_prompt_note() -> str:
    """Pending proposals, named for the system prompt.

    Without this the model cannot tell that it already proposed — history keeps no
    tool_use record — so it concludes it has no proposal on record and classifies again.
    Every turn reaches the same conclusion, so the reviewer never gets past the review
    step. Naming the datasets and their exact URNs is what lets a bare "apply the tags"
    resolve to an apply.
    """
    live = pending()
    if not live:
        return ""
    listed = "\n".join(f"- {p.dataset_name} — {p.dataset_urn}" for p in live)
    return (
        "\n\nProposals you have ALREADY shown the user, awaiting their decision:\n"
        f"{listed}\n"
        "If the user asks you to apply, write, confirm, or approve tags for one of these, "
        "call apply_pii_tags with the URN exactly as listed. Do NOT call propose_pii_tags "
        "for it again — that discards what they reviewed."
    )


def _resolve(dataset_urn: str) -> Proposal:
    proposal = _find(dataset_urn)
    if proposal is not None:
        return proposal
    if not _PROPOSALS:
        raise TaggerError(
            f"No proposal on record for {dataset_urn}. Call propose_pii_tags first and "
            "show the result to the user before applying anything."
        )
    held = ", ".join(p.dataset_name for p in _PROPOSALS.values())
    raise TaggerError(
        f"No proposal matches {dataset_urn}. Proposals are held for: {held}. Use one of "
        "those URNs, or call propose_pii_tags for this dataset first."
    )


async def _call_json(mcp, tool: str, args: dict) -> dict:
    raw = await mcp.execute_tool(tool, args)
    if "error" in raw:
        raise TaggerError(f"{tool} failed: {raw['error']}")
    try:
        return json.loads(raw.get("result") or "{}")
    except json.JSONDecodeError as exc:
        raise TaggerError(f"{tool} returned unparseable output: {exc}") from exc


async def _fetch_columns(mcp, dataset_urn: str) -> list[Column]:
    """Every schema field, following pagination.

    Paginated deliberately: a truncated read would classify part of a wide table and
    report success, which is worse than failing.
    """
    columns: list[Column] = []
    offset = 0
    while True:
        payload = await _call_json(
            mcp,
            "list_schema_fields",
            {"urn": dataset_urn, "limit": SCHEMA_PAGE_SIZE, "offset": offset},
        )
        fields = payload.get("fields") or []
        for entry in fields:
            tags = tuple(entry.get("editedTags") or ()) + tuple(entry.get("tags") or ())
            columns.append(
                Column(
                    field_path=entry["fieldPath"],
                    native_type=entry.get("nativeDataType") or "",
                    description=entry.get("description") or "",
                    existing_tags=tags,
                )
            )
        offset += len(fields)
        if not fields or not payload.get("remainingCount"):
            break

    if not columns:
        raise TaggerError(f"{dataset_urn} has no schema fields to classify")
    return columns


async def _fetch_description(mcp, dataset_urn: str) -> str:
    """Best-effort table description, only worth a round trip when the model will run."""
    try:
        payload = await _call_json(mcp, "get_entities", {"urns": [dataset_urn]})
    except TaggerError as exc:
        logger.info("No description for %s (%s); classifying without it", dataset_urn, exc)
        return ""
    entities = payload if isinstance(payload, list) else [payload]
    if not entities or not isinstance(entities[0], dict):
        return ""
    properties = entities[0].get("properties") or {}
    return properties.get("description") or ""


def _rows(verdicts: list[Verdict]) -> list[dict]:
    return [
        {
            "column": v.field,
            "tag": v.label,
            "confidence": round(v.confidence, 2),
            "reason": v.reason,
            "decided_by": v.source.value,
        }
        for v in verdicts
    ]


def _payload(proposal: Proposal, *, reused: bool) -> dict:
    confident = proposal.confident
    uncertain = proposal.uncertain
    if reused:
        next_step = (
            "This is the proposal the user has already seen; it was not re-classified. "
            "If they have just confirmed it, call apply_pii_tags now."
        )
    else:
        next_step = (
            "Show this to the user as a table, then call apply_pii_tags with the same "
            "dataset_urn once they confirm. Do not apply in the same turn as the proposal."
        )
    return {
        "dataset": proposal.dataset_name,
        "dataset_urn": proposal.dataset_urn,
        "columns_scanned": proposal.total_columns,
        "already_tagged": proposal.skipped,
        "proposed": _rows(confident),
        "uncertain": _rows(uncertain),
        "confidence_floor": CONFIDENCE_FLOOR,
        "provenance_tag": PROVENANCE_TAG,
        "nothing_to_do": not confident and not uncertain,
        "reused": reused,
        "next_step": next_step,
    }


async def propose(mcp, *, dataset_urn: str, api_key: str, model: str | None = None) -> dict:
    """Classify one dataset and cache the result. Writes nothing."""
    # Resolved before the schema read so a retyped URN still reaches the right entity:
    # reading with the caller's string would fail as "entity not found" even though the
    # proposal is cached under the correct one.
    existing = _find(dataset_urn)
    target_urn = existing.dataset_urn if existing is not None else dataset_urn

    columns = await _fetch_columns(mcp, target_urn)
    fingerprint = _fingerprint(columns)

    # Reuse a live proposal rather than reclassifying: the model re-proposes more often
    # than it should, and a fresh run would replace the exact rows the user is reading.
    # A changed schema invalidates it, since the reviewed rows no longer describe it.
    if existing is not None and existing.fingerprint == fingerprint:
        logger.info("Reusing pending proposal for %s", existing.dataset_name)
        return _payload(existing, reused=True)

    dataset_name = short_name(target_urn)
    already = {
        c.field_path: [t for t in c.existing_tags if is_taxonomy_tag(t)]
        for c in columns
        if c.already_labelled
    }
    candidates = [c for c in columns if not c.already_labelled]

    decision = apply_rules(candidates)
    verdicts = list(decision.verdicts)
    if decision.residual:
        description = await _fetch_description(mcp, target_urn)
        verdicts += await classify(
            dataset_name=dataset_name,
            dataset_description=description,
            columns=decision.residual,
            api_key=api_key,
            model=model,
        )

    proposal = Proposal(
        dataset_urn=target_urn,
        dataset_name=dataset_name,
        total_columns=len(columns),
        verdicts=verdicts,
        skipped=already,
        fingerprint=fingerprint,
        created_at=time.time(),
    )
    _PROPOSALS[target_urn] = proposal

    by_rule = sum(1 for v in verdicts if v.source is Source.RULE)
    logger.info(
        "Proposed %s: %d columns, %d flagged (%d by rule, %d by model), "
        "%d skipped as tagged, %d residual sent to model",
        dataset_name,
        len(columns),
        len(verdicts),
        by_rule,
        len(verdicts) - by_rule,
        len(already),
        len(decision.residual),
    )
    return _payload(proposal, reused=False)


async def apply(
    *,
    dataset_urn: str,
    exclude_columns: list[str] | None = None,
    include_uncertain: bool = False,
) -> dict:
    """Write the confirmed proposal in a single request."""
    proposal = _resolve(dataset_urn)
    excluded = {name.strip() for name in (exclude_columns or []) if name.strip()}

    selected = list(proposal.confident)
    if include_uncertain:
        selected += proposal.uncertain
    selected = [v for v in selected if v.field not in excluded]

    if not selected:
        return {
            "dataset": proposal.dataset_name,
            "written": [],
            "skipped_by_request": sorted(excluded),
            "note": "Nothing left to write once exclusions and the confidence floor "
            "were applied.",
        }

    # Writes target the proposal's own URN, never the caller's string, so a retyped
    # platform cannot send tags to a different entity than the one that was reviewed.
    tags_by_field = {v.field: pii_writer.tags_for(v.label) for v in selected}
    result = await pii_writer.apply_field_tags(proposal.dataset_urn, tags_by_field)

    applied = {v.field: v.label for v in selected if v.field in set(result.written)}
    if result.written:
        _PROPOSALS.pop(proposal.dataset_urn, None)

    return {
        "dataset": proposal.dataset_name,
        "dataset_urn": proposal.dataset_urn,
        "written": applied,
        "already_current": result.unchanged,
        "skipped_by_request": sorted(excluded),
        "provenance_tag": PROVENANCE_TAG,
    }
