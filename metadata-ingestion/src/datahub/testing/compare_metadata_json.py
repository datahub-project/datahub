"""Utilities for comparing MCE and MCP files."""

import json
import logging
import os
import pathlib
import pprint
import re
import shutil
import tempfile
from typing import Any, Dict, List, Sequence, Union

import pytest
from deepdiff import DeepDiff

from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.testing.mcp_diff import CannotCompareMCPs, MCPDiff, get_aspects_by_urn

logger = logging.getLogger(__name__)

MetadataJson = List[Dict[str, Any]]

default_exclude_paths = [
    r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
    r"root\[\d+\].*?\['systemMetadata'\]\['runId'\]",
    r"root\[\d+\].*?\['systemMetadata'\]\['lastRunId'\]",
]


def load_json_file(filename: Union[str, os.PathLike]) -> MetadataJson:
    with open(str(filename)) as f:
        return json.load(f)


def assert_metadata_files_equal(
    output_path: Union[str, os.PathLike],
    golden_path: Union[str, os.PathLike],
    update_golden: bool,
    copy_output: bool,
    ignore_paths: Sequence[str] = (),
    ignore_paths_v2: Sequence[str] = (),
    ignore_order: bool = True,
) -> None:
    golden_exists = os.path.isfile(golden_path)

    if copy_output:
        shutil.copyfile(str(output_path), str(golden_path) + ".output")
        logger.info(f"Copied output file to {golden_path}.output")

    if not update_golden and not golden_exists:
        raise FileNotFoundError(
            "Golden file does not exist. Please run with the --update-golden-files option to create."
        )

    output = load_json_file(output_path)

    if update_golden and not golden_exists:
        shutil.copyfile(str(output_path), str(golden_path))
        return
    else:
        # We have to "normalize" the golden file by reading and writing it back out.
        # This will clean up nulls, double serialization, and other formatting issues.
        with tempfile.NamedTemporaryFile() as temp:
            try:
                golden_metadata = read_metadata_file(pathlib.Path(golden_path))
                write_metadata_file(pathlib.Path(temp.name), golden_metadata)
                golden = load_json_file(temp.name)
            except (ValueError, AssertionError) as e:
                logger.info(f"Error reformatting golden file as MCP/MCEs: {e}")
                golden = load_json_file(golden_path)

    if ignore_paths_v2:
        golden_json = load_json_file(golden_path)
        for i, obj in enumerate(golden_json):
            aspect_json = obj.get("aspect", {}).get("json", [])
            for j, item in enumerate(aspect_json):
                if isinstance(item, dict):
                    if item.get("path") in ignore_paths_v2:
                        json_path = f"root[{i}]['aspect']['json'][{j}]['value']"
                        ignore_paths = (*ignore_paths, re.escape(json_path))

    ignore_paths = (*ignore_paths, *default_exclude_paths)

    diff = diff_metadata_json(output, golden, ignore_paths, ignore_order=ignore_order)
    if diff and update_golden:
        if isinstance(diff, MCPDiff) and diff.is_delta_valid:
            logger.info(f"Applying delta to golden file {golden_path}")
            diff.apply_delta(golden)
            write_metadata_file(pathlib.Path(golden_path), golden)
        else:
            # Fallback: just overwrite the golden file
            logger.info(f"Overwriting golden file {golden_path}")
            shutil.copyfile(str(output_path), str(golden_path))
        return

    if diff:
        # Call pytest.fail rather than raise an exception to omit stack trace
        message = (
            "Metadata files differ (use `pytest --update-golden-files` to update):\n"
        )
        if isinstance(diff, MCPDiff):
            logger.error(message + diff.pretty(verbose=True))
            pytest.fail(message + diff.pretty(), pytrace=False)
        else:
            logger.error(message + pprint.pformat(diff))
            pytest.fail(message + pprint.pformat(diff), pytrace=False)


def diff_metadata_json(
    output: MetadataJson,
    golden: MetadataJson,
    ignore_paths: Sequence[str] = (),
    ignore_order: bool = True,
) -> Union[DeepDiff, MCPDiff]:
    ignore_paths = [*ignore_paths, *default_exclude_paths, r"root\[\d+].delta_info"]
    try:
        if ignore_order:
            golden_map = get_aspects_by_urn(golden)
            output_map = get_aspects_by_urn(output)
            return MCPDiff.create(
                golden=golden_map,
                output=output_map,
                ignore_paths=ignore_paths,
            )
        # if ignore_order is False, always use DeepDiff
    except CannotCompareMCPs as e:
        logger.info(f"{e}, falling back to MCE diff")
    except (AssertionError, ValueError) as e:
        logger.warning(f"Reverting to old diff method: {e}")
        logger.debug("Error with new diff method", exc_info=True)

    return DeepDiff(
        golden,
        output,
        exclude_regex_paths=ignore_paths,
        ignore_order=ignore_order,
    )
