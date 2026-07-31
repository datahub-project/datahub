"""What the model is allowed to call: MCP's read tools plus the two guarded PII tools.

Raw write tools are filtered out even if the MCP server offers them, because a direct
`add_tags` would let the model write without the review step and without provenance.
Every write in this service goes through `apply_pii_tags`.
"""
from __future__ import annotations

import logging
from typing import Any

import pii_tagger
from pii_tagger import TaggerError

logger = logging.getLogger("local_tools")

# Defensive: MCP currently exposes reads only, but enabling mutations upstream must not
# silently hand the model a way around the confirmation gate.
_WRITE_PREFIXES = ("add_", "remove_", "update_", "set_", "create_", "delete_", "upsert_")

PROPOSE = "propose_pii_tags"
APPLY = "apply_pii_tags"

LOCAL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": PROPOSE,
        "description": (
            "Classify a dataset's columns for PII and return a proposal. Reads the schema "
            "itself, so do not look it up first. Writes nothing. Most columns are decided "
            "by deterministic rules; only ambiguous ones cost a model call."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_urn": {
                    "type": "string",
                    "description": "Full dataset URN, e.g. "
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,appdb.users,PROD)",
                }
            },
            "required": ["dataset_urn"],
        },
    },
    {
        "name": APPLY,
        "description": (
            "Write the tags from a proposal the user has already reviewed and confirmed. "
            "Only call this after they have seen the proposal and said yes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_urn": {
                    "type": "string",
                    "description": "The same dataset URN the proposal was made for.",
                },
                "exclude_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Columns the user rejected.",
                },
                "include_uncertain": {
                    "type": "boolean",
                    "description": "Also write rows below the confidence floor. Only when "
                    "the user explicitly approved those rows.",
                },
            },
            "required": ["dataset_urn"],
        },
    },
]


def _is_write_tool(name: str) -> bool:
    return name.startswith(_WRITE_PREFIXES)


class ToolRegistry:
    """One request's view of the tools, plus the state the gate needs.

    Instantiated per request: `_proposed_this_turn` must not outlive the turn it
    describes, or a later confirmation would be blocked.
    """

    def __init__(self, mcp, *, api_key: str) -> None:
        self._mcp = mcp
        self._api_key = api_key
        self._proposed_this_turn: set[str] = set()

        hidden = [d["name"] for d in mcp.definitions if _is_write_tool(d["name"])]
        if hidden:
            logger.info("Hiding MCP write tools from the model: %s", ", ".join(hidden))

        self.definitions = [
            d for d in mcp.definitions if not _is_write_tool(d["name"])
        ] + LOCAL_DEFINITIONS

    async def execute_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        if name in (PROPOSE, APPLY):
            return await self._execute_local(name, args)
        if _is_write_tool(name):
            return {"error": f"{name} is not available; use {APPLY} instead."}
        return await self._mcp.execute_tool(name, args)

    async def _execute_local(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        dataset_urn = (args.get("dataset_urn") or "").strip()
        if not dataset_urn:
            return {"error": "dataset_urn is required."}

        key = pii_tagger.short_name(dataset_urn)
        logger.info("%s(%s)", name, dataset_urn)

        try:
            if name == PROPOSE:
                payload = await pii_tagger.propose(
                    self._mcp, dataset_urn=dataset_urn, api_key=self._api_key
                )
                # Only a fresh classification arms the gate. A reused proposal was shown
                # in an earlier turn, so applying it now is exactly what was asked for —
                # arming the gate here would block the confirmation and loop the review.
                if not payload.get("reused"):
                    self._proposed_this_turn.add(key)
                return payload

            if key in self._proposed_this_turn:
                logger.warning("Refused same-turn apply for %s", dataset_urn)
                return {
                    "error": (
                        "This proposal was produced in the current turn, so the user has "
                        "not seen it yet. Show them the table and wait for them to "
                        "confirm; then apply on a later turn."
                    )
                }

            return await pii_tagger.apply(
                dataset_urn=dataset_urn,
                exclude_columns=args.get("exclude_columns"),
                include_uncertain=bool(args.get("include_uncertain")),
            )
        except TaggerError as exc:
            return {"error": str(exc)}
        except Exception as exc:  # surfaced to the model so it can report, not crash
            logger.exception("%s failed", name)
            return {"error": f"{name} failed: {exc}"}
