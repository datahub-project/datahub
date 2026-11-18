"""How a path_spec include is turned into the S3 listings that walk it.

Shared by S3Source (and GCSSource, which runs it) and the object-store probe,
so `probe run` lists exactly the prefixes ingestion lists. Kept apart from
s3/source.py so the probe and the path_spec verdicts do not load the S3
ingestion stack (smart_open, schema inference) to reach them.
"""

import logging
import re
from typing import Callable, Iterable, List, Optional, Tuple

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_boto_utils import list_folders_path

logger: logging.Logger = logging.getLogger(__name__)


def get_prefix(relative_path: str) -> str:
    index = re.search(r"[\*|\{]", relative_path)
    if index:
        return relative_path[: index.start()]
    else:
        return relative_path


def listing_prefix(include: str) -> Tuple[str, str]:
    """(dirname, basename_startswith) a simple path_spec lists objects under."""
    prefix = get_prefix(include)
    basename_startswith = prefix.split("/")[-1]
    return prefix.removesuffix(basename_startswith), basename_startswith


def table_marker_prefix(include: str) -> str:
    """The part of a templated include before `{table}`, with every placeholder
    before it widened to `*` so resolve_templated_folders can list it."""
    table_marker = "{table}"
    # Replace template placeholders with stars (except {table}) to enable folder
    # resolution.
    matches = re.finditer(r"{\s*\w+\s*}", include, re.MULTILINE)
    matches_list = list(matches)

    if matches_list:
        # Replace all templates with stars except keep {table} as the marker
        max_start: int = -1
        widened: str = include
        max_match: str = ""

        for match in matches_list:
            pos = widened.find(match.group())
            if pos > max_start:
                if max_match:
                    widened = widened.replace(max_match, "*")
                max_start = match.start()
                max_match = match.group()
                # We stop at {table}
                if max_match == "{table}":
                    break

        logger.info(f"Template replacement: {include} -> {widened}")
    else:
        widened = include

    # Split the path at {table} to get the prefix that needs wildcard resolution
    return widened.split(table_marker)[0]


def resolve_templated_folders(
    prefix: str,
    aws_config: Optional[AwsConnectionConfig],
    on_listing: Optional[Callable[[str], None]] = None,
) -> Iterable[str]:
    """Expand the `*`s in prefix into the folders that exist. `on_listing`, if
    given, is called with each folder about to be listed, so a caller can bound
    the walk: a dead end yields nothing but still costs a listing. It may raise
    to stop the walk."""
    folder_split: List[str] = prefix.split("*", 1)
    # If the len of split is 1 it means we don't have * in the prefix
    if len(folder_split) == 1:
        yield prefix
        return

    basename_startswith = folder_split[0].split("/")[-1]
    dirname = folder_split[0].removesuffix(basename_startswith)

    if on_listing is not None:
        on_listing(dirname)
    folders = list_folders_path(
        dirname,
        startswith=basename_startswith,
        aws_config=aws_config,
    )
    for folder in folders:
        # Ensure proper path joining - folders from list_folders path never include a
        # trailing slash, but we need to handle the case where folder_split[1] might
        # start with a slash
        remaining_pattern = folder_split[1]
        if remaining_pattern.startswith("/"):
            remaining_pattern = remaining_pattern[1:]

        yield from resolve_templated_folders(
            f"{folder.path}/{remaining_pattern}", aws_config, on_listing
        )
