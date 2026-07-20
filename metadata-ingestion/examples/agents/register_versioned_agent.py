#!/usr/bin/env python3
"""
Register a *versioned* AI agent with the DataHub SDK.

Each version of an agent is its own ``aiAgent`` entity (a distinct ``id``), and
versions are grouped into a shared **version set**. DataHub orders them by a
lexicographic sort id (auto-derived from the version tag) and flags the highest
as *latest*, so the agent profile shows a version picker.

The only new inputs versus a plain registration are:
    version       — the version tag, e.g. "2.0"
    version_set   — the family the versions share (a bare id or a full
                    urn:li:versionSet:(...) urn). Pass the SAME value for every
                    version so they group together.

Run against any instance:
    export DATAHUB_GMS_URL=http://localhost:8080
    python register_versioned_agent.py
"""

from __future__ import annotations

from datahub.api.entities.agent.agent import Agent
from datahub.ingestion.graph.client import get_default_graph

# All versions of the Support Agent live in this family.
VERSION_SET = "support-agent"


def main() -> None:
    with get_default_graph() as emitter:
        # A historical snapshot — its own entity, but part of the same family.
        v1 = Agent(
            id="support-agent-v1.0",
            name="Support Agent",
            description="Answers customer order questions.",
            version="1.0",
            version_set=VERSION_SET,
            version_comment="Initial release: order status lookups.",
        )
        v1.emit(emitter)
        print(f"  ✓ {v1.name}  v1.0  → {v1.version_set_urn}")

        # The current production version. It keeps the stable id ("support-agent")
        # so existing links/lineage point at the canonical entity; being the
        # highest version, it is flagged as latest.
        v2 = Agent(
            id="support-agent",
            name="Support Agent",
            description=(
                "Answers customer order questions, delays, and delivery estimates."
            ),
            version="2.0",
            version_set=VERSION_SET,
            version_comment="Added delivery estimates + delay explanations.",
        )
        v2.emit(emitter)
        print(f"  ✓ {v2.name}  v2.0 (latest)  → {v2.version_set_urn}")

        print()
        print(
            f"Open → {emitter.config.server}/aiAgent/{v2.urn}"
            "  (version picker: v2.0, v1.0)"
        )


if __name__ == "__main__":
    main()
