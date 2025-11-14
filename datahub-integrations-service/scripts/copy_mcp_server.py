#! /usr/bin/env python3

"""
OBSOLETE: This script copies only selected files from mcp-server-datahub.

For syncing improvements, it's now simpler to just copy the entire mcp/ folder:
    
    # Sync source code
    cp -r /path/to/datahub-fork/datahub-integrations-service/src/datahub_integrations/mcp/* \
          /path/to/mcp-server-datahub/src/mcp_server_datahub/
    
    # Sync tests
    cp -r /path/to/datahub-fork/datahub-integrations-service/tests/mcp/* \
          /path/to/mcp-server-datahub/tests/

See docs/mcp-syncing.md for the complete syncing guide.

This script is kept for backwards compatibility but may be removed in the future.
"""

import pathlib
import subprocess

import typer

# mcp_replaces = {}


def main(mcp_server_dir: pathlib.Path) -> None:
    # Assumes there's just one main MCP server file.
    mcp_server_src_dir = mcp_server_dir / "src/mcp_server_datahub"

    mcp_out_dir = pathlib.Path(__file__).parent.parent / "src/datahub_integrations/mcp"

    mcp_file_contents = (mcp_server_src_dir / "mcp_server.py").read_text()
    # for old, new in mcp_replaces.items():
    #     assert old in mcp_file_contents, (
    #         f"Old value {old} not found in {mcp_file_contents}"
    #     )
    #     mcp_file_contents = mcp_file_contents.replace(old, new)

    (mcp_out_dir / "mcp_server.py").write_text(mcp_file_contents)

    # Copy the gql dir.
    gql_dir = mcp_out_dir / "gql"
    for file in gql_dir.glob("*"):
        file.unlink()
    for gql_file in (mcp_server_src_dir / "gql").glob("*.gql"):
        (gql_dir / gql_file.name).write_text(gql_file.read_text())

    mcp_server_test_dir = mcp_server_dir / "tests"
    mcp_tests_dir = pathlib.Path(__file__).parent.parent / "tests/mcp"
    for test_file in mcp_server_test_dir.glob("*.py"):
        if test_file.name == "test_mcp_server.py":
            continue
        (mcp_tests_dir / test_file.name).write_text(
            test_file.read_text().replace(
                "mcp_server_datahub", "datahub_integrations.mcp"
            )
        )

    # Run ruff to fix import ordering.
    ruff_cmd = ["ruff", "format", mcp_out_dir, mcp_tests_dir]
    subprocess.run(ruff_cmd, check=True)

    # Run ruff to fix import ordering.
    ruff_cmd = ["ruff", "check", "--fix", mcp_out_dir, mcp_tests_dir]
    subprocess.run(ruff_cmd, check=True)

    print(f"Copied MCP server to {mcp_out_dir}, {mcp_tests_dir}")


if __name__ == "__main__":
    typer.run(main)
