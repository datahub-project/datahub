import pprint
import shutil
import tempfile
from typing import List

import click

from datahub import __package_name__
from datahub.cli.json_file import check_mce_file
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry
from datahub.testing.compare_metadata_json import load_json_file, diff_metadata_json
from datahub.testing.mcp_diff import MCPDiff


@click.group()
def check() -> None:
    """Helper commands for checking various aspects of DataHub."""
    pass


@check.command()
@click.argument("json-file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--rewrite",
    default=False,
    is_flag=True,
    help="Rewrite the JSON file to it's canonical form.",
)
@click.option(
    "--unpack-mces", default=False, is_flag=True, help="Converts MCEs into MCPs"
)
def metadata_file(json_file: str, rewrite: bool, unpack_mces: bool) -> None:
    """Check the schema of a metadata (MCE or MCP) JSON file."""

    if not rewrite:
        report = check_mce_file(json_file)
        click.echo(report)

    else:
        with tempfile.NamedTemporaryFile() as out_file:
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "file",
                        "config": {"filename": json_file},
                        "extractor": "generic",
                        "extractor_config": {
                            "set_system_metadata": False,
                            "unpack_mces_into_mcps": unpack_mces,
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": out_file.name},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            shutil.copy(out_file.name, json_file)


@check.command(no_args_is_help=True)
@click.argument(
    "actual-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.argument(
    "expected-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.option(
    "--verbose",
    "-v",
    type=bool,
    default=False,
    help="Print full aspects that were changed, when comparing MCPs",
)
@click.option(
    "--ignore-path",
    multiple=True,
    type=str,
    default=(),
    help="[Advanced] Paths in the deepdiff object to ignore",
)
def metadata_diff(
    actual_file: str, expected_file: str, verbose: bool, ignore_path: List[str]
) -> None:
    """Compare two metadata (MCE or MCP) JSON files.

    Comparison is more sophisticated for files composed solely of MCPs.
    """

    actual = load_json_file(actual_file)
    expected = load_json_file(expected_file)

    diff = diff_metadata_json(output=actual, golden=expected, ignore_paths=ignore_path)
    if isinstance(diff, MCPDiff):
        click.echo(diff.pretty(verbose=verbose))
    else:
        click.echo(pprint.pformat(diff))


@check.command()
@click.option(
    "--verbose",
    "-v",
    type=bool,
    is_flag=True,
    default=False,
    help="Include extra information for each plugin.",
)
def plugins(verbose: bool) -> None:
    """List the enabled ingestion plugins."""

    click.secho("Sources:", bold=True)
    click.echo(source_registry.summary(verbose=verbose))
    click.echo()
    click.secho("Sinks:", bold=True)
    click.echo(sink_registry.summary(verbose=verbose))
    click.echo()
    click.secho("Transformers:", bold=True)
    click.echo(transform_registry.summary(verbose=verbose, col_width=30))
    click.echo()
    if not verbose:
        click.echo("For details on why a plugin is disabled, rerun with '--verbose'")
    click.echo(
        f"If a plugin is disabled, try running: pip install '{__package_name__}[<plugin>]'"
    )
