import json
import logging
import time
from datetime import datetime
from typing import Any, List, Optional

import click
from click.exceptions import UsageError
from requests import Response
from termcolor import colored

import datahub.cli.cli_utils
from datahub.emitter.mce_builder import dataset_urn_to_key, schema_field_urn_to_key
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


def pretty_field_path(field_path: str) -> str:
    if not field_path.startswith("[version=2.0]"):
        return field_path
        # breakpoint()
        # parse schema field
    tokens = [
        t
        for t in field_path.split(".")
        if not t.startswith("[") and not t.endswith("]")
    ]

    return ".".join(tokens)


def pretty_id(id: Optional[str]) -> str:
    if not id:
        return ""
    # breakpoint()
    assert id is not None
    if id.startswith("urn:li:datasetField:") or id.startswith("urn:li:schemaField:"):
        schema_field_key = schema_field_urn_to_key(
            id.replace("urn:li:datasetField", "urn:li:schemaField")
        )
        if schema_field_key:
            assert schema_field_key is not None
            field_path = schema_field_key.fieldPath

            return f"{colored('field','cyan')}:{colored(pretty_field_path(field_path),'white')}"
    if id.startswith("[version=2.0]"):
        return f"{colored('field','cyan')}:{colored(pretty_field_path(id),'white')}"

    if id.startswith("urn:li:dataset"):
        dataset_key = dataset_urn_to_key(id)
        if dataset_key:
            return f"{colored('dataset','cyan')}:{colored(dataset_key.platform,'white')}:{colored(dataset_key.name,'white')}"
    # failed to prettify, return original
    return id


def get_timeline(
    urn: str,
    category: List[str],
    start_time: Optional[int],
    end_time: Optional[int],
    diff: bool,
) -> Any:
    session, host = datahub.cli.cli_utils.get_session_and_host()
    if urn.startswith("urn%3A"):
        # we assume the urn is already encoded
        encoded_urn: str = urn
    elif urn.startswith("urn:"):
        encoded_urn = Urn.url_encode(urn)
    else:
        raise Exception(
            f"urn {urn} does not seem to be a valid raw (starts with urn:) or encoded urn (starts with urn%3A)"
        )
    categories: str = ",".join([c.upper() for c in category])
    start_time_param: str = f"&startTime={start_time}" if start_time else ""
    end_time_param: str = f"&endTime={end_time}" if end_time else ""
    diff_param: str = f"&raw={diff}" if diff else ""
    endpoint: str = (
        host
        + f"/openapi/timeline/v1/{encoded_urn}?categories={categories}{start_time_param}{end_time_param}{diff_param}"
    )
    click.echo(endpoint)

    response: Response = session.get(endpoint)
    if response.status_code != 200:
        return {
            "status": response.status_code,
            "content": str(response.content),
        }
    else:
        return response.json()


@click.command(
    name="timeline",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option("--urn", required=True, type=str)
@click.option(
    "-c",
    "--category",
    required=True,
    multiple=True,
    type=str,
    help="One of tag, glossary_term, technical_schema, documentation, owner",
)
@click.option(
    "--start",
    required=False,
    type=str,
    help="The start time for the timeline query in milliseconds. Shorthand form like 7daysago is also supported. e.g. --start 7daysago implies a timestamp 7 days ago",
)
@click.option(
    "--end",
    required=False,
    type=str,
    help="The end time for the timeline query in milliseconds. Shorthand form like 7days is also supported. e.g. --end 7daysago implies a timestamp 7 days ago",
)
@click.option(
    "--verbose", "-v", type=bool, is_flag=True, help="Show the underlying http response"
)
@click.option("--raw", type=bool, is_flag=True, help="Show the raw diff")
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry
def timeline(
    ctx: Any,
    urn: str,
    category: List[str],
    start: Optional[str],
    end: Optional[str],
    verbose: bool,
    raw: bool,
) -> None:
    """Get timeline for an entity based on certain categories"""

    all_categories = [
        "TAG",
        "OWNER",
        "GLOSSARY_TERM",
        "TECHNICAL_SCHEMA",
        "DOCUMENTATION",
    ]
    for c in category:
        if c.upper() not in all_categories:
            raise click.UsageError(
                f"category: {c.upper()} is not one of {all_categories}"
            )

    if urn is None:
        if not ctx.args:
            raise UsageError("Nothing for me to get. Maybe provide an urn?")
        urn = ctx.args[0]
        logger.debug(f"Using urn from args {urn}")

    start_time_millis: Optional[int] = None
    if start:
        if start.endswith("daysago"):
            days_ago: int = int(start.replace("daysago", ""))
            start_time_millis = int((time.time() - days_ago * 24 * 60 * 60) * 1000)
        else:
            start_time_millis = int(start)

    end_time_millis: Optional[int] = None
    if end:
        if end.endswith("daysago"):
            days_ago = int(end.replace("daysago", ""))
            end_time_millis = int((time.time() - days_ago * 24 * 60 * 60) * 1000)
        else:
            end_time_millis = int(end)

    timeline = get_timeline(
        urn=urn,
        category=category,
        start_time=start_time_millis,
        end_time=end_time_millis,
        diff=raw,
    )

    if isinstance(timeline, list) and not verbose:
        for change_txn in timeline:
            change_instant = str(
                datetime.fromtimestamp(change_txn["timestamp"] // 1000)
            )
            change_color = (
                "green"
                if change_txn.get("semVerChange") in ["MINOR", "PATCH"]
                else "red"
            )

            print(
                f"{colored(change_instant,'cyan')} - {colored(change_txn['semVer'],change_color)}"
            )
            if change_txn["changeEvents"] is not None:
                for change_event in change_txn["changeEvents"]:
                    element_string = (
                        f"({pretty_id(change_event.get('elementId') or change_event.get('modifier'))})"
                        if change_event.get("elementId") or change_event.get("modifier")
                        else ""
                    )
                    event_change_color: str = (
                        "green"
                        if change_event.get("semVerChange") == "MINOR"
                        else "red"
                    )
                    target_string = pretty_id(
                        change_event.get("target")
                        or change_event.get("entityUrn")
                        or ""
                    )
                    print(
                        f"\t{colored(change_event.get('changeType') or change_event.get('operation'),event_change_color)} {change_event.get('category')} {target_string} {element_string}: {change_event['description']}"
                    )
    else:
        click.echo(
            json.dumps(
                timeline,
                sort_keys=True,
                indent=2,
            )
        )
