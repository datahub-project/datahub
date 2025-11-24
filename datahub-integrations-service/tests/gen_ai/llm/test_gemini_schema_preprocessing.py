from unittest.mock import MagicMock, patch

import pytest
from langchain_google_vertexai.functions_utils import (
    _format_to_gapic_function_declaration,
)

from datahub_integrations.gen_ai.llm.gemini import GeminiLLMWrapper
from datahub_integrations.mcp.mcp_server import mcp, register_all_tools
from datahub_integrations.mcp_integration.tool import tools_from_fastmcp


@pytest.fixture(autouse=True)
def mock_gemini_client():
    """Mock GeminiLLMWrapper._initialize_client to avoid requiring VERTEXAI_PROJECT env var."""
    with patch.object(GeminiLLMWrapper, "_initialize_client", return_value=MagicMock()):
        yield


def test_removes_null_from_anyof_and_flattens_single_type():
    wrapper = GeminiLLMWrapper(model_name="gemini-2.5-pro")

    schema = {
        "type": "object",
        "properties": {
            "multi": {
                "anyOf": [
                    {"items": {"type": "string"}, "type": "array"},
                    {"type": "string"},
                    {"type": "null"},
                ],
            },
            "single": {
                "anyOf": [
                    {"type": "string"},
                    {"type": "null"},
                ],
            },
        },
    }

    preprocessed = wrapper._preprocess_schema_for_vertex_ai(schema)

    multi = preprocessed["properties"]["multi"]
    assert "anyOf" in multi
    assert len(multi["anyOf"]) == 2
    assert not any(item.get("type") == "null" for item in multi["anyOf"])

    single = preprocessed["properties"]["single"]
    assert single["type"] == "string"
    assert "anyOf" not in single


def test_handles_all_null_anyof_and_allof():
    wrapper = GeminiLLMWrapper(model_name="gemini-2.5-pro")

    schema = {
        "type": "object",
        "properties": {
            "all_null": {
                "anyOf": [{"type": "null"}],
            },
            "allof": {
                "allOf": [
                    {"type": "string"},
                    {"type": "null"},
                ],
            },
        },
    }

    preprocessed = wrapper._preprocess_schema_for_vertex_ai(schema)

    assert "all_null" not in preprocessed["properties"]
    assert preprocessed["properties"]["allof"]["type"] == "string"
    assert "allOf" not in preprocessed["properties"]["allof"]


def test_recursively_processes_nested_schemas():
    wrapper = GeminiLLMWrapper(model_name="gemini-2.5-pro")

    schema = {
        "type": "object",
        "properties": {
            "nested": {
                "type": "object",
                "properties": {
                    "inner": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                    }
                },
            },
            "array": {
                "type": "array",
                "items": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                },
            },
        },
    }

    preprocessed = wrapper._preprocess_schema_for_vertex_ai(schema)

    assert (
        preprocessed["properties"]["nested"]["properties"]["inner"]["type"] == "string"
    )
    assert preprocessed["properties"]["array"]["items"]["type"] == "string"


def test_preserves_defs_and_other_keys():
    wrapper = GeminiLLMWrapper(model_name="gemini-2.5-pro")

    schema = {
        "$defs": {"Foo": {"type": "string"}},
        "type": "object",
        "properties": {
            "field": {
                "type": "string",
                "description": "A field",
                "default": "value",
            }
        },
        "required": ["field"],
    }

    result = wrapper._preprocess_schema_for_vertex_ai(schema)

    assert "$defs" in result
    assert result["$defs"]["Foo"]["type"] == "string"
    assert result["properties"]["field"]["description"] == "A field"
    assert result["properties"]["field"]["default"] == "value"
    assert "required" in result


def test_preprocesses_schema_before_conversion():
    wrapper = GeminiLLMWrapper(model_name="gemini-2.5-pro")

    bedrock_tool = {
        "toolSpec": {
            "name": "test_tool",
            "description": "Test tool",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "keywords": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "null"},
                            ],
                        }
                    },
                }
            },
        }
    }

    lc_tools = wrapper._convert_bedrock_tools_to_langchain([bedrock_tool])

    assert len(lc_tools) == 1
    assert lc_tools[0]["type"] == "function"
    assert lc_tools[0]["function"]["name"] == "test_tool"

    params = lc_tools[0]["function"]["parameters"]
    assert params["properties"]["keywords"]["type"] == "string"
    assert "anyOf" not in params["properties"]["keywords"]


def test_all_registered_tools_convert_to_protobuf():
    register_all_tools(is_oss=False)
    tools = tools_from_fastmcp(mcp)
    wrapper = GeminiLLMWrapper(model_name="gemini-2.5-pro")
    failed_tools = []

    for tool in tools:
        try:
            spec = tool.to_bedrock_spec()
            lc_tools = wrapper._convert_bedrock_tools_to_langchain([spec])
            result = _format_to_gapic_function_declaration(lc_tools[0])
            assert result.name == tool.name
            assert result.parameters.type == result.parameters.type.OBJECT
        except Exception as e:
            failed_tools.append((tool.name, str(e)))

    if failed_tools:
        error_msg = "\n".join([f"  - {name}: {error}" for name, error in failed_tools])
        pytest.fail(
            f"Protobuf conversion failed for {len(failed_tools)} tool(s):\n{error_msg}"
        )
