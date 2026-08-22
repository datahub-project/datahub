import json
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.get_cli import get


@patch("datahub.cli.get_cli.get_default_graph")
@patch("datahub.cli.get_cli.get_aspects_for_entity")
def test_document_default_output_remains_json(mock_get_aspects, mock_get_graph):
    mock_graph = MagicMock()
    mock_graph.exists.return_value = True
    mock_get_graph.return_value = mock_graph

    mock_get_aspects.return_value = {
        "documentInfo": {
            "title": "Test Document",
            "contents": {"text": "Test contents here"}
        }
    }

    runner = CliRunner()
    result = runner.invoke(
        get,
        [
            "urn",
            "--urn",
            "urn:li:document:test",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "documentInfo" in payload
    assert payload["documentInfo"]["title"] == "Test Document"
    assert "---" not in result.output
    assert "# Test Document" not in result.output


@patch("datahub.cli.get_cli.get_default_graph")
@patch("datahub.cli.get_cli.get_aspects_for_entity")
def test_pretty_document_renders_title_and_contents(mock_get_aspects, mock_get_graph):
    mock_graph = MagicMock()
    mock_graph.exists.return_value = True
    mock_get_graph.return_value = mock_graph

    mock_get_aspects.return_value = {
        "documentInfo": {
            "title": "Test Document",
            "contents": {"text": "Verified decision markdown"}
        }
    }

    runner = CliRunner()
    result = runner.invoke(
        get,
        [
            "urn",
            "--urn",
            "urn:li:document:test",
            "--pretty-document",
        ],
    )

    assert result.exit_code == 0
    assert "# Test Document" in result.output
    assert "Verified decision markdown" in result.output
    assert "--- (Metadata Below) ---" in result.output


@patch("datahub.cli.get_cli.get_default_graph")
@patch("datahub.cli.get_cli.get_aspects_for_entity")
def test_pretty_document_handles_missing_contents(mock_get_aspects, mock_get_graph):
    mock_graph = MagicMock()
    mock_graph.exists.return_value = True
    mock_get_graph.return_value = mock_graph

    mock_get_aspects.return_value = {
        "documentInfo": {
            "title": "Test Document Without Contents"
        }
    }

    runner = CliRunner()
    result = runner.invoke(
        get,
        [
            "urn",
            "--urn",
            "urn:li:document:test",
            "--pretty-document",
        ],
    )

    assert result.exit_code == 0
    # Should safely output JSON with no formatting errors if contents are missing
    payload_str = result.output.strip()
    assert payload_str.startswith("{") or payload_str.endswith("}")
    assert "Test Document Without Contents" in result.output


def test_pretty_document_rejects_non_document_urn():
    runner = CliRunner()
    result = runner.invoke(
        get,
        [
            "urn",
            "--urn",
            "urn:li:dataset:test",
            "--pretty-document",
        ],
    )

    assert result.exit_code != 0
    assert "--pretty-document requires a urn:li:document:* URN" in result.output


def test_pretty_document_rejects_aspect_projection():
    runner = CliRunner()
    result = runner.invoke(
        get,
        [
            "urn",
            "--urn",
            "urn:li:document:test",
            "--aspect",
            "documentInfo",
            "--pretty-document",
        ],
    )

    assert result.exit_code != 0
    assert "--pretty-document cannot be combined with --aspect" in result.output
