"""The ingestion group's recipe tools: list_connector_types, get_recipe_schema
and validate_recipe. They run against the connectors installed in this
process, where `file` and `demo-data` always load."""

import pytest
import yaml
from datahub.ingestion.agent import introspect
from datahub.ingestion.source.source_registry import source_registry
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tools.ingestion_recipes import (
    checked_recipe,
    get_recipe_schema,
    list_connector_types,
    literal_secret_paths,
    validate_recipe,
)

FILE_RECIPE = "source:\n  type: file\n  config:\n    path: /data/mces.json\n"


def test_literal_secret_paths_flags_only_literal_credentials() -> None:
    recipe = {
        "source": {
            "config": {
                "password": "hunter2",
                "token": "${API_TOKEN}",
                "private_key_path": "/keys/rsa.p8",
                "options": {"connect_args": {"client_secret": "s3cret"}},
                "use_password_auth": True,
            }
        },
        "sink": {"config": {"servers": [{"api_key": "abc"}]}},
    }

    assert literal_secret_paths(recipe) == [
        "source.config.password",
        "source.config.options.connect_args.client_secret",
        "sink.config.servers[0].api_key",
    ]


def test_a_recipe_with_a_literal_credential_is_refused() -> None:
    with pytest.raises(ToolError, match=r"source\.config\.password"):
        checked_recipe("source:\n  type: postgres\n  config:\n    password: pw\n")


def test_list_connector_types_filters_by_name() -> None:
    everything = list_connector_types()
    snow = list_connector_types(query="SNOW")

    assert {"file", "demo-data", "snowflake", "mysql"} <= set(everything["connectors"])
    assert everything["total"] == len(everything["connectors"])
    assert snow["connectors"] and all("snow" in n for n in snow["connectors"])
    assert everything["aliases"]["databricks"] == "unity-catalog"


def test_recipe_schema_describes_the_connector() -> None:
    schema = get_recipe_schema("file")

    fields = {f["name"]: f for f in schema["fields"]}
    assert schema["source_type"] == "file"
    assert fields["path"]["required"] is True
    assert "recipe_template" not in schema

    one = get_recipe_schema("file", field_name="path")
    assert one["field"]["name"] == "path"
    with pytest.raises(ToolError, match="no config field 'nope'"):
        get_recipe_schema("file", field_name="nope")


def test_recipe_template_holds_the_given_config() -> None:
    schema = get_recipe_schema("file", partial_config={"path": "/data/x.json"})

    template = yaml.safe_load(schema["recipe_template"])
    assert template == {"source": {"type": "file", "config": {"path": "/data/x.json"}}}
    with pytest.raises(ToolError, match="partial_config has literal credentials"):
        get_recipe_schema("file", partial_config={"password": "hunter2"})


def test_recipe_schema_of_an_unknown_or_unloadable_connector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ToolError, match="list_connector_types"):
        get_recipe_schema("no-such-connector")

    def missing(source_type: str) -> object:
        raise ValueError("file is disabled due to a missing dependency")

    monkeypatch.setattr(introspect, "describe_source", missing)
    with pytest.raises(ToolError, match="cannot load the file connector") as e:
        get_recipe_schema("file")
    assert "docs.datahub.com" in str(e.value)


def test_a_valid_recipe() -> None:
    assert validate_recipe(FILE_RECIPE) == {
        "valid": True,
        "errors": [],
        "schema_checked": True,
    }


@pytest.mark.parametrize(
    ("recipe", "error"),
    [
        ("source: [unclosed", "not valid YAML"),
        ("- a list", "must be a mapping"),
        ("source:\n  config: {}\n", "no `source.type`"),
        ("source:\n  type: no-such-connector\n", "list_connector_types"),
        (FILE_RECIPE + "    not_a_field: 1\n", "not_a_field"),
    ],
)
def test_an_invalid_recipe_is_a_verdict_not_an_error(recipe: str, error: str) -> None:
    result = validate_recipe(recipe)

    assert result["valid"] is False
    assert error in " ".join(result["errors"])


def test_secret_references_validate_without_this_servers_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Resolving from the environment would put this server's own value in the
    # config, and in any error that quotes it.
    monkeypatch.setenv("SERVER_ONLY_SECRET", "server-value")
    recipe = (
        "source:\n  type: file\n  config:\n"
        "    path: ${SERVER_ONLY_SECRET}\n    file_extension: 7\n"
    )

    result = validate_recipe(recipe)

    assert "server-value" not in str(result)


def test_literal_credentials_make_a_recipe_invalid() -> None:
    result = validate_recipe(FILE_RECIPE + "    token: abc\n")

    assert result["valid"] is False
    assert result["literal_secrets"] == ["source.config.token"]


def test_a_connector_this_server_cannot_load_is_not_checked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unloadable(key: str) -> object:
        raise ValueError(f"{key} is disabled due to a missing dependency")

    monkeypatch.setattr(source_registry, "get", unloadable)

    result = validate_recipe(FILE_RECIPE + "    not_a_field: 1\n")

    assert result["valid"] is True
    assert result["schema_checked"] is False
    assert "cannot load the file connector" in result["note"]


async def test_registered_recipe_tools_are_read_only_and_take_a_json_string() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True, groups=["ingestion"])

    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
        result = await client.call_tool(
            "get_recipe_schema",
            {"connector": "file", "partial_config": '{"path": "/data/x.json"}'},
        )

    for name in ("list_connector_types", "get_recipe_schema", "validate_recipe"):
        assert tools[name].annotations.read_only_hint is True
        assert tools[name].meta["fastmcp"]["tags"] == ["ingestion"]
    assert "/data/x.json" in result.data["recipe_template"]
