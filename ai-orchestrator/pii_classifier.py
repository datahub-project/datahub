"""The model pass, for the columns the rules could not settle.

Runs on a small fast model with a forced tool schema, because the job here is narrow:
a handful of ambiguous column names, judged with the table for context. If the rules
left nothing ambiguous, no request is made at all.
"""
from __future__ import annotations

import logging
import os

import anthropic
from pydantic import ValidationError

from pii_models import Column, Source, Verdict
from pii_taxonomy import BY_NAME, guidance_block

logger = logging.getLogger("pii_classifier")

# Haiku, not the chat model: the residual set is tiny and this keeps classification
# quality fixed when someone switches the chat model.
CLASSIFIER_MODEL = os.environ.get("PII_CLASSIFIER_MODEL", "claude-haiku-4-5")
MAX_TOKENS = int(os.environ.get("PII_CLASSIFIER_MAX_TOKENS", "1024"))

_TOOL_NAME = "record_classification"

_TOOL = {
    "name": _TOOL_NAME,
    "description": "Record one verdict per column you were given.",
    "input_schema": {
        "type": "object",
        "properties": {
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "column": {
                            "type": "string",
                            "description": "The column name, exactly as given.",
                        },
                        "label": {
                            "type": "string",
                            "enum": [*BY_NAME, ""],
                            "description": "The PII label, or an empty string if the "
                            "column holds no personal data.",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "0.0 to 1.0. Below 0.6 is treated as needing "
                            "a human judgement call, so use a low value when unsure "
                            "rather than guessing high.",
                        },
                        "reason": {
                            "type": "string",
                            "description": "One short clause. Say what the column holds, "
                            "not what the label means.",
                        },
                    },
                    "required": ["column", "label", "confidence", "reason"],
                },
            }
        },
        "required": ["columns"],
    },
}

_INSTRUCTIONS = (
    "You classify database columns for a privacy catalog. You are given only the columns "
    "that simple name matching could not decide, so each one genuinely depends on the "
    "table it sits in: `name` on an employee table is a person, `name` on a product table "
    "is not.\n\n"
    "Labels:\n{labels}\n\n"
    "Judge from the column name, its type, and the table it belongs to. Return an empty "
    "label for anything that is not personal data — surrogate keys for non-person rows, "
    "audit timestamps, counters, and configuration are not personal data. Do not invent a "
    "label to seem useful; an empty label is the correct answer for most of these."
)


def _prompt(dataset_name: str, dataset_description: str, columns: list[Column]) -> str:
    lines = []
    for column in columns:
        parts = [f"- {column.field_path}"]
        if column.native_type:
            parts.append(f"type={column.native_type}")
        if column.description:
            parts.append(f"description={column.description!r}")
        lines.append(" | ".join(parts))

    header = f"Table: {dataset_name}"
    if dataset_description:
        header += f"\nTable description: {dataset_description}"
    return f"{header}\n\nColumns to classify:\n" + "\n".join(lines)


async def classify(
    *,
    dataset_name: str,
    dataset_description: str,
    columns: list[Column],
    api_key: str,
    model: str | None = None,
) -> list[Verdict]:
    """Verdicts for the residual columns. No request is made for an empty list."""
    if not columns:
        return []

    client = anthropic.AsyncAnthropic(api_key=api_key)
    response = await client.messages.create(
        model=model or CLASSIFIER_MODEL,
        max_tokens=MAX_TOKENS,
        system=_INSTRUCTIONS.format(labels=guidance_block()),
        tools=[_TOOL],
        tool_choice={"type": "tool", "name": _TOOL_NAME},
        messages=[
            {
                "role": "user",
                "content": _prompt(dataset_name, dataset_description, columns),
            }
        ],
    )

    rows: list[dict] = []
    for block in response.content:
        if block.type == "tool_use" and block.name == _TOOL_NAME:
            rows = (block.input or {}).get("columns") or []
            break

    known = {column.field_path for column in columns}
    verdicts: list[Verdict] = []
    for row in rows:
        field = row.get("column")
        # The model occasionally returns a column it was not asked about, or a
        # near-miss spelling. Writing to a field path we never read would be a silent
        # no-op at best, so unknown paths are dropped rather than trusted.
        if field not in known:
            logger.warning("Ignoring verdict for unrequested column %r", field)
            continue
        if not row.get("label"):
            continue
        try:
            verdicts.append(
                Verdict(
                    field=field,
                    label=row["label"],
                    confidence=float(row.get("confidence") or 0.0),
                    reason=(row.get("reason") or "").strip(),
                    source=Source.MODEL,
                )
            )
        except (ValidationError, TypeError, ValueError) as exc:
            logger.warning("Discarding malformed verdict for %r: %s", field, exc)

    logger.info(
        "Model pass on %s: %d/%d residual columns labelled",
        dataset_name,
        len(verdicts),
        len(columns),
    )
    return verdicts
