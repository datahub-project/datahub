from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.specific.api_cli import _parse_param, _parse_return, api
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ApiPropertiesClass,
    ApiSignatureClass,
    SubTypesClass,
)

API_YAML = """
id: get_order
name: Get Order
subtypes:
  - REST_ENDPOINT
description: Fetch an order by id
method: GET
path: /orders/{orderId}
"""


def _write(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "api.yml"
    f.write_text(content)
    return f


def test_parse_param_required_and_optional_and_type() -> None:
    required = _parse_param("order_id:string:required")
    assert required.name == "order_id"
    assert required.data_type == "string"
    assert required.required is True

    optional = _parse_param("order_id:string")
    assert optional.required is False

    bare = _parse_param("order_id")
    assert bare.data_type == "string"
    assert bare.required is False


def test_parse_return_is_always_required() -> None:
    ret = _parse_return("order:object")
    assert ret.name == "order"
    assert ret.data_type == "object"
    assert ret.required is True


@patch("datahub.cli.specific.api_cli.get_default_graph")
def test_upsert_from_file_emits_rest_and_reports_urn(
    mock_graph_ctx: MagicMock, tmp_path: Path
) -> None:
    mock_graph = MagicMock()
    mock_graph_ctx.return_value.__enter__.return_value = mock_graph
    emitted: List[MetadataChangeProposalWrapper] = []
    mock_graph.emit.side_effect = lambda item, *a, **k: emitted.append(item)

    result = CliRunner().invoke(api, ["upsert", "-f", str(_write(tmp_path, API_YAML))])

    assert result.exit_code == 0, result.output
    assert "Upserted API urn:li:api:get_order" in result.output
    props = [m.aspect for m in emitted if isinstance(m.aspect, ApiPropertiesClass)]
    assert props and props[0].name == "Get Order"
    subtypes = [m.aspect for m in emitted if isinstance(m.aspect, SubTypesClass)]
    assert subtypes and subtypes[0].typeNames == ["REST_ENDPOINT"]


@patch("datahub.cli.specific.api_cli.get_default_graph")
def test_register_with_params_and_returns_emits_signature(
    mock_graph_ctx: MagicMock, tmp_path: Path
) -> None:
    mock_graph = MagicMock()
    mock_graph_ctx.return_value.__enter__.return_value = mock_graph
    emitted: List[MetadataChangeProposalWrapper] = []
    mock_graph.emit.side_effect = lambda item, *a, **k: emitted.append(item)

    result = CliRunner().invoke(
        api,
        [
            "register",
            "--id",
            "get_order",
            "--name",
            "Get Order",
            "--param",
            "order_id:string:required",
            "--returns",
            "order:object",
        ],
    )

    assert result.exit_code == 0, result.output
    sigs = [m.aspect for m in emitted if isinstance(m.aspect, ApiSignatureClass)]
    assert len(sigs) == 1
    assert sigs[0].inputFields is not None and len(sigs[0].inputFields) == 1
    assert sigs[0].inputFields[0].nullable is False  # required param
    assert sigs[0].outputFields is not None and len(sigs[0].outputFields) == 1


def test_upsert_invalid_method_in_file_errors(tmp_path: Path) -> None:
    bad = "id: x\nname: X\nmethod: FETCH\npath: /x\n"
    with patch("datahub.cli.specific.api_cli.get_default_graph") as mock_graph_ctx:
        result = CliRunner().invoke(api, ["upsert", "-f", str(_write(tmp_path, bad))])
    assert result.exit_code != 0
    assert not mock_graph_ctx.called
