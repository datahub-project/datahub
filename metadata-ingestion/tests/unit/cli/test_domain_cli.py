from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.specific.domain_cli import domain


def test_upsert_exits_non_zero_when_emit_fails(tmp_path: Path) -> None:
    yaml_file = tmp_path / "domain.yaml"
    yaml_file.write_text("id: marketing\ndisplay_name: Marketing\n")

    graph = MagicMock()
    graph.exists.return_value = True
    graph.emit.side_effect = RuntimeError("GMS rejected the write")

    with (
        patch("datahub.cli.specific.domain_cli.get_default_graph") as get_default_graph,
        patch("datahub.upgrade.upgrade.check_upgrade_post"),
    ):
        get_default_graph.return_value.__enter__.return_value = graph
        result = CliRunner().invoke(domain, ["upsert", "-f", str(yaml_file)])

    assert result.exit_code != 0
    assert "GMS rejected the write" in result.output
