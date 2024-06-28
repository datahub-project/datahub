import json
import logging
import os
import time
from datetime import datetime
from typing import List, Optional

import click
from click.shell_completion import CompletionItem
from click_default_group import DefaultGroup

from datahub.cli.config_utils import (
    DATAHUB_ROOT_FOLDER,
    DatahubConfig,
    get_client_config,
    persist_datahub_config,
)
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.lite.duckdb_lite_config import DuckDBLiteConfig
from datahub.lite.lite_local import (
    AutoComplete,
    DataHubLiteLocal,
    PathNotFoundException,
    SearchFlavor,
)
from datahub.lite.lite_util import LiteLocalConfig, get_datahub_lite
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)

DEFAULT_LITE_IMPL = "duckdb"


class DuckDBLiteConfigWrapper(DuckDBLiteConfig):
    file: str = os.path.expanduser(f"{DATAHUB_ROOT_FOLDER}/lite/datahub.duckdb")


class LiteCliConfig(DatahubConfig):
    lite: LiteLocalConfig = LiteLocalConfig(
        type="duckdb", config=DuckDBLiteConfigWrapper().dict()
    )


def get_lite_config() -> LiteLocalConfig:
    client_config_dict = get_client_config(as_dict=True)
    lite_config = LiteCliConfig.parse_obj(client_config_dict)
    return lite_config.lite


def _get_datahub_lite(read_only: bool = False) -> DataHubLiteLocal:
    lite_config = get_lite_config()
    if lite_config.type == "duckdb":
        lite_config.config["read_only"] = read_only

    duckdb_lite = get_datahub_lite(config_dict=lite_config.dict(), read_only=read_only)
    return duckdb_lite


@click.group(cls=DefaultGroup, default="ls")
def lite() -> None:
    """A group of commands to work with a DataHub Lite instance"""
    pass


@lite.command()
@telemetry.with_telemetry()
def list_urns() -> None:
    """List all urns"""

    catalog = _get_datahub_lite(read_only=True)
    for result in catalog.list_ids():
        click.echo(result)


class CompleteablePath(click.ParamType):
    name = "path"

    def shell_complete(self, ctx, param, incomplete):
        path = incomplete or "/"
        lite = _get_datahub_lite(read_only=True)
        try:
            completions = lite.ls(path)
            return [
                CompletionItem(browseable.auto_complete.suggested_path, type="plain")
                if browseable.auto_complete
                else CompletionItem(
                    f"{incomplete}/{browseable.name}".replace("//", "/")
                )
                for browseable in completions
                if not browseable.leaf
            ]
        except Exception as e:
            logger.debug(f"failed with {e}")
            return []


@lite.command(context_settings=dict(allow_extra_args=True))
@click.option("--urn", required=False, type=str, help="Get metadata rooted at an urn")
@click.option(
    "--path",
    required=False,
    type=CompleteablePath(),
    help="Get metadata rooted at a path",
)
@click.option("-a", "--aspect", required=False, multiple=True, type=str)
@click.option("--asof", required=False, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--verbose", required=False, is_flag=True, default=False)
@click.pass_context
@telemetry.with_telemetry()
def get(
    ctx: click.Context,
    urn: Optional[str],
    path: Optional[str],
    aspect: List[str],
    asof: Optional[datetime],
    verbose: bool,
) -> None:
    """Get one or more metadata elements"""
    start_time = time.time()
    if urn is None and path is None:
        if not ctx.args:
            raise click.UsageError(
                "Nothing for me to get. Maybe provide an urn or a path? Use ls if you want to explore me."
            )
        urn_or_path = ctx.args[0]
        if urn_or_path.startswith("urn:"):
            urn = ctx.args[0]
            logger.debug(f"Using urn {urn} from arguments")
        else:
            path = ctx.args[0]
            logger.debug(f"Using path {path} from arguments")

    asof_millis = int(asof.timestamp() * 1000) if asof else None

    lite = _get_datahub_lite(read_only=True)
    if path:
        parents = set()
        for browseable in lite.ls(path):
            if browseable.id and browseable.leaf:
                click.echo(
                    json.dumps(
                        lite.get(
                            id=browseable.id,
                            aspects=aspect,
                            as_of=asof_millis,
                            details=verbose,
                        ),
                        indent=2,
                    )
                )
            else:
                parents.update(browseable.parents or [])
        if parents:
            for p in parents:
                click.echo(
                    json.dumps(
                        lite.get(
                            id=p, aspects=aspect, as_of=asof_millis, details=verbose
                        ),
                        indent=2,
                    )
                )

    if urn:
        click.echo(
            json.dumps(
                lite.get(id=urn, aspects=aspect, as_of=asof_millis, details=verbose),
                indent=2,
            )
        )
    end_time = time.time()
    logger.debug(f"Time taken: {int((end_time - start_time)*1000.0)} millis")


@lite.command()
@click.pass_context
@telemetry.with_telemetry()
def nuke(ctx: click.Context) -> None:
    """Nuke the instance"""
    lite = _get_datahub_lite()
    if click.confirm(
        f"This will permanently delete the DataHub Lite instance at: {lite.location()}. Are you sure?"
    ):
        lite.destroy()
        click.echo(f"DataHub Lite at {lite.location()} nuked!")


@lite.command(context_settings=dict(allow_extra_args=True))
@click.pass_context
@telemetry.with_telemetry()
def reindex(ctx: click.Context) -> None:
    """Reindex the catalog"""
    lite = _get_datahub_lite()
    lite.reindex()


@lite.command()
@click.option("--port", type=int, default=8979)
@telemetry.with_telemetry()
def serve(port: int) -> None:
    """Run a local server."""

    import uvicorn

    from datahub.lite.lite_server import app

    # TODO allow arbitrary args
    # TODO bind the lite instance to the app
    uvicorn.run(app, port=port)


@lite.command()
@click.argument("path", required=False, type=CompleteablePath())
@telemetry.with_telemetry()
def ls(path: Optional[str]) -> None:
    """List at a path"""

    start_time = time.time()
    path = path or "/"
    lite = _get_datahub_lite(read_only=True)
    try:
        browseables = lite.ls(path)
        end_time = time.time()
        logger.debug(f"Time taken: {int((end_time - start_time)*1000.0)} millis")
        auto_complete: List[AutoComplete] = [
            b.auto_complete for b in browseables if b.auto_complete is not None
        ]
        if auto_complete:
            click.echo(
                f"Path not found at {auto_complete[0].success_path}/".replace("//", "/")
                + click.style(f"{auto_complete[0].failed_token}", fg="red")
            )
            click.echo("Did you mean")
            for completable in auto_complete:
                click.secho(f"{completable.suggested_path}?")
        else:
            for browseable in [b for b in browseables if b.auto_complete is None]:
                click.secho(
                    browseable.name,
                    fg="white"
                    if browseable.leaf
                    else "green"
                    if browseable.id.startswith("urn:")
                    and not browseable.id.startswith("urn:li:systemNode")
                    else "cyan",
                )
    except PathNotFoundException:
        click.echo(f"Path not found: {path}")
    except Exception as e:
        logger.exception("Failed to process request", exc_info=e)


@lite.command(context_settings=dict(allow_extra_args=True))
@click.argument("query", required=True)
@click.option(
    "--flavor",
    required=False,
    type=click.Choice(
        choices=[x.lower() for x in SearchFlavor._member_names_], case_sensitive=False
    ),
    default=SearchFlavor.FREE_TEXT.name.lower(),
    help="the flavor of the query, defaults to free text. Set to exact if you want to pass in a specific query to the database engine.",
)
@click.option(
    "--aspect",
    required=False,
    multiple=True,
    help="Constrain the search to a specific set of aspects",
)
@click.option("--details/--no-details", required=False, is_flag=True, default=True)
@click.pass_context
@telemetry.with_telemetry()
def search(
    ctx: click.Context,
    query: str = "",
    flavor: str = SearchFlavor.FREE_TEXT.name.lower(),
    aspect: List[str] = [],
    details: bool = True,
) -> None:
    """Search with a free text or exact query string"""

    # query flavor should be sanitized by now, but we still need to convert it to a SearchFlavor
    try:
        search_flavor = SearchFlavor[flavor.upper()]
    except KeyError:
        raise click.UsageError(
            f"Failed to find a matching query flavor for {flavor}. Valid values are {[x.lower() for x in SearchFlavor._member_names_]}"
        )
    catalog = _get_datahub_lite(read_only=True)
    # sanitize query
    result_ids = set()
    try:
        for searchable in catalog.search(
            query=query, flavor=search_flavor, aspects=aspect
        ):
            result_str = searchable.id
            if details:
                result_str = json.dumps(searchable.dict())
            # suppress id if we have already seen it in the non-detailed response
            if details or searchable.id not in result_ids:
                click.secho(result_str)
                result_ids.add(searchable.id)

    except Exception as e:
        logger.exception("Failed to process request", exc_info=e)


def write_lite_config(lite_config: LiteLocalConfig) -> None:
    cli_config = get_client_config(as_dict=True)
    assert isinstance(cli_config, dict)
    cli_config["lite"] = lite_config.dict()
    persist_datahub_config(cli_config)


@lite.command(context_settings=dict(allow_extra_args=True))
@click.option("--type", required=False, default=DEFAULT_LITE_IMPL)
@click.option("--file", required=False)
@click.pass_context
@telemetry.with_telemetry()
def init(ctx: click.Context, type: Optional[str], file: Optional[str]) -> None:
    lite_config = get_lite_config()
    new_lite_config_dict = lite_config.dict()
    # Update the type and config sections only
    new_lite_config_dict["type"] = type
    if file:
        new_lite_config_dict["config"]["file"] = file
    new_lite_config = LiteLocalConfig.parse_obj(new_lite_config_dict)
    if lite_config != new_lite_config:
        if click.confirm(
            f"Will replace datahub lite config {lite_config} with {new_lite_config}"
        ):
            write_lite_config(new_lite_config)

    lite = _get_datahub_lite()
    click.echo(f"DataHub Lite inited at {lite.location()}")


@lite.command("import", context_settings=dict(allow_extra_args=True))
@click.option("--file", required=False)
@click.pass_context
@telemetry.with_telemetry()
def import_cmd(ctx: click.Context, file: Optional[str]) -> None:
    if file is None:
        if not ctx.args:
            raise click.UsageError(
                "Nothing for me to import. Maybe provide a metadata file?"
            )
        file = ctx.args[0]

    config_dict = {
        "source": {"type": "file", "config": {"path": file}},
        "sink": {"type": "datahub-lite", "config": {}},
    }
    Pipeline.create(config_dict).run()


@lite.command("export", context_settings=dict(allow_extra_args=True))
@click.option("--file", required=True, type=str)
@click.pass_context
@telemetry.with_telemetry()
def export(ctx: click.Context, file: str) -> None:
    if file is None:
        if not ctx.args:
            raise click.UsageError("Nothing for me to export to. Provide a file path")
        file = ctx.args[0]

    current_time = int(time.time() * 1000.0)
    pipeline_ctx: PipelineContext = PipelineContext(
        run_id=f"datahub-lite_{current_time}"
    )
    file = os.path.expanduser(file)
    base_dir = os.path.dirname(file)
    if base_dir:
        os.makedirs(base_dir, exist_ok=True)
    file_sink: FileSink = FileSink(
        ctx=pipeline_ctx, config=FileSinkConfig(filename=file)
    )
    datahub_lite = _get_datahub_lite(read_only=True)
    num_events = 0
    for mcp in datahub_lite.get_all_aspects():
        file_sink.write_record_async(
            RecordEnvelope(record=mcp, metadata={}), write_callback=NoopWriteCallback()
        )
        num_events += 1

    file_sink.close()
    click.echo(f"Successfully exported {num_events} events to {file}")
