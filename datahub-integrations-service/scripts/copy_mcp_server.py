#! /usr/bin/env python3

import pathlib
import subprocess

import typer

mcp_replaces = {
    "from mcp.server.fastmcp import FastMCP": "from datahub_integrations.chat.tool import ToolRegistry",
    """\
is_dev_mode = _version.is_dev_mode()
datahub_package_dir = pathlib.Path(datahub.__file__).parent.parent.parent

mcp = FastMCP(
    name="datahub",
    dependencies=[
        (
            # No spaces, since MCP doesn't escape their commands properly :(
            f"acryl-datahub@{datahub_package_dir}" if is_dev_mode else "acryl-datahub"
        ),
    ],
)
""": """\
mcp = ToolRegistry()
""",
}


def main(mcp_server_dir: pathlib.Path) -> None:
    # Assumes there's just one main MCP server file.
    mcp_server_src_dir = mcp_server_dir / "src/mcp_server_datahub"

    chat_dir = pathlib.Path(__file__).parent.parent / "src/datahub_integrations/chat"

    mcp_file_contents = (mcp_server_src_dir / "mcp_server.py").read_text()
    for old, new in mcp_replaces.items():
        assert old in mcp_file_contents, (
            f"Old value {old} not found in {mcp_file_contents}"
        )
        mcp_file_contents = mcp_file_contents.replace(old, new)

    (chat_dir / "mcp_server.py").write_text(mcp_file_contents)

    # Copy the gql dir.
    gql_dir = chat_dir / "gql"
    for file in gql_dir.glob("*"):
        file.unlink()
    for gql_file in (mcp_server_src_dir / "gql").glob("*.gql"):
        (gql_dir / gql_file.name).write_text(gql_file.read_text())

    # Run ruff to fix import ordering.
    ruff_cmd = ["ruff", "check", "--fix", chat_dir]
    subprocess.run(ruff_cmd, check=True)

    print(f"Copied MCP server to {chat_dir}")


if __name__ == "__main__":
    typer.run(main)
