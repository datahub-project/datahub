from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.specific.agent_skill_cli import agent_skill
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import AgentSkillInfoClass

SKILL_YAML = """
id: refund-flow
name: Refund Flow
description: Handles customer refunds
source_repository:
  url: https://github.com/acme/skills
  path: refund/SKILL.md
required_tools:
  - urn:li:api:get_order
"""


def _write(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "skill.yml"
    f.write_text(content)
    return f


@patch("datahub.cli.specific.agent_skill_cli.get_default_graph")
def test_upsert_from_file_emits_info_and_reports_urn(
    mock_graph_ctx: MagicMock, tmp_path: Path
) -> None:
    mock_graph = MagicMock()
    mock_graph_ctx.return_value.__enter__.return_value = mock_graph
    emitted: List[MetadataChangeProposalWrapper] = []
    mock_graph.emit.side_effect = lambda item, *a, **k: emitted.append(item)

    result = CliRunner().invoke(
        agent_skill, ["upsert", "-f", str(_write(tmp_path, SKILL_YAML))]
    )

    assert result.exit_code == 0, result.output
    assert "Upserted AgentSkill urn:li:agentSkill:refund-flow" in result.output
    infos = [m.aspect for m in emitted if isinstance(m.aspect, AgentSkillInfoClass)]
    assert infos and infos[0].name == "Refund Flow"
    assert infos[0].sourceRepository is not None
    assert infos[0].requiredTools == ["urn:li:api:get_order"]


@patch("datahub.cli.specific.agent_skill_cli.get_default_graph")
def test_register_from_flags_emits_info_and_reports_urn(
    mock_graph_ctx: MagicMock,
) -> None:
    mock_graph = MagicMock()
    mock_graph_ctx.return_value.__enter__.return_value = mock_graph
    emitted: List[MetadataChangeProposalWrapper] = []
    mock_graph.emit.side_effect = lambda item, *a, **k: emitted.append(item)

    result = CliRunner().invoke(
        agent_skill,
        [
            "register",
            "--id",
            "refund-flow",
            "--name",
            "Refund Flow",
            "--source-repo",
            "https://github.com/acme/skills",
            "--source-path",
            "refund/SKILL.md",
            "--requires-tool",
            "urn:li:api:get_order",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Registered AgentSkill urn:li:agentSkill:refund-flow" in result.output
    infos = [m.aspect for m in emitted if isinstance(m.aspect, AgentSkillInfoClass)]
    assert infos and infos[0].name == "Refund Flow"
    assert infos[0].sourceRepository is not None
    assert infos[0].requiredTools == ["urn:li:api:get_order"]


def test_register_source_path_without_repo_errors() -> None:
    result = CliRunner().invoke(
        agent_skill,
        [
            "register",
            "--id",
            "refund-flow",
            "--name",
            "Refund Flow",
            "--source-path",
            "refund/SKILL.md",
        ],
    )
    assert result.exit_code != 0
    assert "--source-path requires --source-repo" in result.output


@patch("datahub.cli.specific.agent_skill_cli.get_default_graph")
def test_upsert_list_yaml_rejected(mock_graph_ctx: MagicMock, tmp_path: Path) -> None:
    result = CliRunner().invoke(
        agent_skill, ["upsert", "-f", str(_write(tmp_path, "- id: a\n  name: A\n"))]
    )
    assert result.exit_code != 0
    assert not mock_graph_ctx.called
