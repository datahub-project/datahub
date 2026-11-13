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

from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.schema_classes import MetadataChangeEventClass
from datahub.testing.mcp_diff import CannotCompareMCPs, MCPDiff, get_aspects_by_urn
from datahub.testing.pytest_hooks import get_golden_settings

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
    # Nested timestamps in upstreams array (from query usage lineage)
    r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['created'\]\['time'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['upstreams'\]\[\d+\]\['lastModified'\]\['time'\]",
    # Assertion source created timestamp (set at emit time)
    r"root\[\d+\]\['aspect'\]\['json'\]\['source'\]\['created'\]\['time'\]",
    # auditStamp.time is wall-clock time (time.time()*1000), not meaningful test data
    r"root\[\d+\].*\['auditStamp'\]\['time'\]",
]


def load_json_file(filename: Union[str, os.PathLike]) -> MetadataJson:
    with open(str(filename)) as f:
        return json.load(f)


def expand_mces(metadata: MetadataJson) -> MetadataJson:
    """Split each MCE record into one MCP record per aspect, serialized like the file sink's MCPs.

    The MCE envelope (proposedDelta, the snapshot type) is dropped, so it is no longer compared.
    """
    expanded: MetadataJson = []
    for record in metadata:
        if "proposedSnapshot" in record:
            mce = MetadataChangeEventClass.from_obj(record)
            expanded.extend(
                mcp.to_obj(simplified_structure=True) for mcp in mcps_from_mce(mce)
            )
        else:
            expanded.append(record)
    # to_obj returns OrderedDicts; DeepDiff would report them as type changes
    # against the plain dicts loaded from the file.
    return json.loads(json.dumps(expanded))


def assert_metadata_files_equal(
    output_path: Union[str, os.PathLike],
    golden_path: Union[str, os.PathLike],
    ignore_paths: Sequence[str] = (),
    ignore_paths_v2: Sequence[str] = (),
    ignore_order: bool = True,
    golden_mces_as_mcps: bool = False,
    fail_on_output_mces: bool = False,
) -> None:
    """Fail the test if the output file differs from the golden file.

    With golden_mces_as_mcps, MCEs in the golden are read as one MCP per aspect, so the
    output is expected to contain MCPs only; fail_on_output_mces enforces this. ignore_paths
    must use the MCP shape (root[i]['aspect']['json']...), and record indices renumber
    because one MCE becomes several MCPs. With --update-golden-files, a differing golden
    is rewritten as MCPs.

    With fail_on_output_mces, the test fails if the output contains any MCE, whatever
    the golden contains and even with --update-golden-files.
    """
    settings = get_golden_settings()

    golden_exists = os.path.isfile(golden_path)

    if settings.copy_output:
        shutil.copyfile(str(output_path), str(golden_path) + ".output")
        logger.info(f"Copied output file to {golden_path}.output")

    if not settings.update_golden and not golden_exists:
        raise FileNotFoundError(
            "Golden file does not exist. Please run with the --update-golden-files option to create."
        )

    output = load_json_file(output_path)

    if fail_on_output_mces:
        mce_count = sum("proposedSnapshot" in record for record in output)
        if mce_count:
            pytest.fail(
                f"Output contains {mce_count} MCE(s); this source must emit MCPs only",
                pytrace=False,
            )

    if settings.update_golden and not golden_exists:
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
                golden_parsed = True
            except (ValueError, AssertionError) as e:
                logger.info(f"Error reformatting golden file as MCP/MCEs: {e}")
                golden = load_json_file(golden_path)
                golden_parsed = False

    # Expanded here, not in diff_metadata_json, so apply_delta below indexes the list
    # that was diffed. A golden that does not parse is diffed raw so that
    # --update-golden-files can still overwrite it.
    if golden_mces_as_mcps and golden_parsed:
        golden = expand_mces(golden)

    if ignore_paths_v2:
        for i, obj in enumerate(golden):
            aspect_json = obj.get("aspect", {}).get("json", [])
            for j, item in enumerate(aspect_json):
                if isinstance(item, dict):
                    if item.get("path") in ignore_paths_v2:
                        json_path = f"root[{i}]['aspect']['json'][{j}]['value']"
                        ignore_paths = (*ignore_paths, re.escape(json_path))

    ignore_paths = (*ignore_paths, *default_exclude_paths)

    diff = diff_metadata_json(output, golden, ignore_paths, ignore_order=ignore_order)
    if diff and settings.update_golden:
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
