from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.specific.agent_cli import agent
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AIAgentInfoClass,
    StructuredPropertiesClass,
)

AGENT_YAML = """
id: support-agent
name: Support Agent
description: Helps customers with support requests
tools:
  - urn:li:api:get_order
"""


def _write(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "agent.yml"
    f.write_text(content)
    return f


@patch("datahub.cli.specific.agent_cli.get_default_graph")
def test_upsert_from_file_emits_and_reports_urn(
    mock_graph_ctx: MagicMock, tmp_path: Path
) -> None:
    mock_graph = MagicMock()
    mock_graph_ctx.return_value.__enter__.return_value = mock_graph

    emitted: List[MetadataChangeProposalWrapper] = []
    mock_graph.emit.side_effect = lambda item, *a, **k: emitted.append(item)

    result = CliRunner().invoke(
        agent, ["upsert", "-f", str(_write(tmp_path, AGENT_YAML))]
    )

    assert result.exit_code == 0, result.output
    assert "Upserted AI Agent urn:li:aiAgent:support-agent" in result.output
    assert any(isinstance(m.aspect, AIAgentInfoClass) for m in emitted)


@patch("datahub.cli.specific.agent_cli.get_default_graph")
def test_register_assigns_framework_and_version_structured_properties(
    mock_graph_ctx: MagicMock, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    mock_graph = MagicMock()
    mock_graph_ctx.return_value.__enter__.return_value = mock_graph
    emitted: List[MetadataChangeProposalWrapper] = []
    mock_graph.emit.side_effect = lambda item, *a, **k: emitted.append(item)

    result = CliRunner().invoke(
        agent,
        [
            "register",
            "--id",
            "support-agent",
            "--name",
            "Support Agent",
            "--framework",
            "LangChain",
            "--version",
            "1.2",
        ],
    )

    assert result.exit_code == 0, result.output
    sp = [m.aspect for m in emitted if isinstance(m.aspect, StructuredPropertiesClass)]
    assert len(sp) == 1
    urns = {p.propertyUrn for p in sp[0].properties}
    assert "urn:li:structuredProperty:io.acryl.agent.framework" in urns
    assert "urn:li:structuredProperty:io.acryl.agent.version" in urns


def test_upsert_missing_file_errors() -> None:
    # click.Path(exists=True) rejects a nonexistent file before any emit.
    result = CliRunner().invoke(agent, ["upsert", "-f", "/nonexistent/agent.yml"])
    assert result.exit_code != 0


@patch("datahub.cli.specific.agent_cli.get_default_graph")
def test_upsert_list_yaml_rejected(mock_graph_ctx: MagicMock, tmp_path: Path) -> None:
    # A list document is not a single Agent definition.
    result = CliRunner().invoke(
        agent, ["upsert", "-f", str(_write(tmp_path, "- id: a\n  name: A\n"))]
    )
    assert result.exit_code != 0
    assert not mock_graph_ctx.called
