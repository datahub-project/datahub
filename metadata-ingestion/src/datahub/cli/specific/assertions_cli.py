import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.assertion.assertion_config_spec import AssertionsConfigSpec
from datahub.api.entities.assertion.compiler_interface import (
    AssertionCompilationResult,
    CompileResultArtifact,
    CompileResultArtifactType,
)
from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.integrations.assertion.registry import ASSERTION_PLATFORMS
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

REPORT_FILE_NAME = "compile_report.json"


@click.group(cls=DefaultGroup, default="upsert")
def assertions() -> None:
    """A group of commands to interact with the Assertion entity in DataHub."""
    pass


@assertions.command()
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: str) -> None:
    """Upsert (create or update) a set of assertions in DataHub."""

    assertions_spec: AssertionsConfigSpec = AssertionsConfigSpec.from_yaml(file)

    with get_default_graph() as graph:
        for assertion_spec in assertions_spec.assertions:
            try:
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=make_assertion_urn(assertion_spec.get_id()),
                    aspect=assertion_spec.get_assertion_info_aspect(),
                )
                graph.emit_mcp(mcp)
                # TODO: Validate uniqueness of assertion ids. Report if duplicates found.
                # TODO: Use upsert graphql endpoints here instead of graph.emit_mcp.
                click.secho(f"Update succeeded for urn {mcp.entityUrn}.", fg="green")
            except Exception as e:
                logger.exception(e)
                click.secho(
                    f"Update failed for {mcp.entityUrn}: {e}",
                    fg="red",
                )


@assertions.command()
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option("-p", "--platform", required=True, type=str)
@click.option("-o", "--output-to", required=False, type=click.Path(exists=True))
@click.option(
    "-x",
    "--extras",
    required=False,
    multiple=True,
    default=[],
    help="Platform-specific extra key-value inputs in form key=value",
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def compile(
    file: str, platform: str, output_to: Optional[str], extras: List[str]
) -> None:
    """Compile a set of assertions for input assertion platform.
    Note that this does not run any code or execute any queries on assertion platform
    and only creates artifacts specific to assertion platform that can be executed manually.
    In future, we may introduce separate command to automatically apply these compiled changes
    in assertion platform. Currently, generated result artifacts are stored in target folder
    unless another folder is specified using option `--output-to <folder>`.
    """

    if platform not in ASSERTION_PLATFORMS:
        click.secho(
            f"Platform {platform} is not supported.",
            fg="red",
        )

    if output_to is None:
        output_to = f"{os.getcwd()}/target"

    if not os.path.isdir(output_to):
        os.mkdir(output_to)

    assertions_spec: AssertionsConfigSpec = AssertionsConfigSpec.from_yaml(file)

    try:
        compiler = ASSERTION_PLATFORMS[platform].create(
            output_dir=output_to, extras=extras_list_to_dict(extras)
        )
        result = compiler.compile(assertions_spec)

        write_report_file(output_to, result)
        click.secho("Compile report:", bold=True)
        click.echo(result.report.as_string())
        if result.status == "failure":
            click.secho("Failure", fg="yellow", bold=True)
        else:
            click.secho("Success", fg="green", bold=True)
    except Exception as e:
        logger.exception(e)
        click.secho(
            f"Compile failed: {e}",
            fg="red",
        )


def write_report_file(output_to: str, result: AssertionCompilationResult) -> None:
    report_path = Path(output_to) / REPORT_FILE_NAME
    with (report_path).open("w") as f:
        result.add_artifact(
            CompileResultArtifact(
                name=REPORT_FILE_NAME,
                path=report_path,
                type=CompileResultArtifactType.COMPILE_REPORT,
                description="Detailed report about compile status",
            )
        )
        f.write(result.report.as_json())


def extras_list_to_dict(extras: List[str]) -> Dict[str, str]:
    extra_properties: Dict[str, str] = dict()
    for x in extras:
        parts = x.split("=")
        assert (
            len(parts) == 2
        ), f"Invalid value for extras {x}, should be in format key=value"
        extra_properties[parts[0]] = parts[1]
    return extra_properties


# TODO: support for
# Immediate:
# 1. delete assertions (from datahub)
# Later:
# 3. execute compiled assertions on assertion platform (Later, requires connection details to platform),
# 4. cleanup assertions from assertion platform (generate artifacts. optionally execute)
