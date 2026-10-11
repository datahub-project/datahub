#!/usr/bin/env python3
"""
Register the agent-framework data platforms (LangChain, Google ADK, Google) so
agents/tools/skills that carry a ``platform`` render a logo in DataHub.

Logos are embedded as data-URI SVGs on ``DataPlatformInfo.logoUrl`` — no asset
files or serving config needed; the frontend renders ``logoUrl`` directly.

    export DATAHUB_GMS_URL=http://localhost:8080
    python seed_agent_platforms.py
"""

from __future__ import annotations

import base64

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import DataPlatformInfoClass, PlatformTypeClass


def _logo(svg: str) -> str:
    b64 = base64.b64encode(svg.strip().encode()).decode()
    return f"data:image/svg+xml;base64,{b64}"


def _badge(bg: str, fg: str, label: str, size: int = 22) -> str:
    """A clean rounded-square lettermark badge."""
    return f"""
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="14" fill="{bg}"/>
  <text x="32" y="33" fill="{fg}" font-family="Inter,Arial,sans-serif"
        font-size="{size}" font-weight="700" text-anchor="middle"
        dominant-baseline="central">{label}</text>
</svg>"""


# name -> (displayName, logo svg)
PLATFORMS = {
    "langchain": ("LangChain", _badge("#1c3d34", "#ffffff", "LC")),
    "google-adk": ("Google ADK", _badge("#e8f0fe", "#1a73e8", "ADK")),
    "google": ("Google", _badge("#ffffff", "#1a73e8", "G", size=30)),
}


def main() -> None:
    with get_default_graph() as emitter:
        for name, (display_name, svg) in PLATFORMS.items():
            urn = make_data_platform_urn(name)
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=DataPlatformInfoClass(
                        name=name,
                        displayName=display_name,
                        type=PlatformTypeClass.OTHERS,
                        datasetNameDelimiter=".",
                        logoUrl=_logo(svg),
                    ),
                )
            )
            print(f"  ✓ dataPlatform {display_name}  ({urn})")


if __name__ == "__main__":
    main()
