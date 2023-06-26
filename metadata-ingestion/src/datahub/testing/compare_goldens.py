import json
import logging
import os
import pathlib
import pprint
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Sequence, Union

import pytest
from deepdiff import DeepDiff

from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import read_metadata_file
from datahub.testing.golden_diff import GoldenDiff, get_aspects_by_urn

logger = logging.getLogger(__name__)


default_exclude_paths = [
    r"root\[\d+]\['systemMetadata']\['lastObserved']",
    r"root\[\d+]\['aspect']\['json']\['timestampMillis']",
    r"root\[\d+]\['aspect']\['json']\['lastUpdatedTimestamp']",
    r"root\[\d+]\['aspect']\['json']\['created']",
    r"root\[\d+]\['aspect']\['json']\['lastModified']",
]


def load_json_file(filename: Union[str, os.PathLike]) -> List[Dict[str, Any]]:
    with open(str(filename)) as f:
        return json.load(f)


def expand_mcp(mcpw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: (
            {
                ("json" if k1 == "value" else k1): (
                    json.loads(v1) if k1 == "value" else v1
                )
                for k1, v1 in v.items()
            }
            if k == "aspect"
            else v
        )
        for k, v in mcpw.items()
    }


def assert_goldens_equal(
    output_path: Union[str, os.PathLike],
    golden_path: Union[str, os.PathLike],
    update_golden: bool,
    copy_output: bool,
    ignore_paths: Sequence[str] = (),
) -> None:
    golden_exists = os.path.isfile(golden_path)

    if copy_output:
        shutil.copyfile(str(output_path), str(golden_path) + ".output")
        print(f"Copied output file to {golden_path}.output")

    if not update_golden and not golden_exists:
        raise FileNotFoundError(
            "Golden file does not exist. Please run with the --update-golden-files option to create."
        )

    output = load_json_file(output_path)

    # if updating a golden file that doesn't exist yet, load the output again
    if update_golden and not golden_exists:
        golden = load_json_file(output_path)
        shutil.copyfile(str(output_path), str(golden_path))
    else:
        # We have to "normalize" the golden file by reading and writing it back out.
        # This will clean up nulls, double serialization, and other formatting issues.
        with tempfile.NamedTemporaryFile() as temp:
            golden_metadata = read_metadata_file(pathlib.Path(golden_path))
            write_metadata_file(pathlib.Path(temp.name), golden_metadata)
            golden = load_json_file(temp.name)

    golden = [expand_mcp(e) for e in golden]
    output = [expand_mcp(e) for e in output]

    diff = check_mces_equal(output, golden, ignore_paths)
    if diff and update_golden:
        if isinstance(diff, GoldenDiff):
            diff.apply_delta(golden)
            write_metadata_file(pathlib.Path(golden_path), golden)
        else:
            shutil.copyfile(str(output_path), str(golden_path))
        return

    if diff:
        if isinstance(diff, GoldenDiff):
            print(diff.pretty(verbose=True))
            pytest.fail(diff.pretty(), pytrace=False)
        else:
            pytest.fail(pprint.pformat(diff), pytrace=False)


def check_mces_equal(
    output: List[Dict[str, Any]],
    golden: List[Dict[str, Any]],
    ignore_paths: Sequence[str],
) -> Union[DeepDiff, GoldenDiff]:
    ignore_paths = (*ignore_paths, *default_exclude_paths)
    try:
        golden_map = get_aspects_by_urn(golden)
        output_map = get_aspects_by_urn(output)
        return GoldenDiff.create(
            golden=golden_map,
            output=output_map,
            ignore_paths=ignore_paths,
        )
    except Exception as e:
        logger.warning(f"Reverting to old diff method: {e}")
        logger.debug("Error with new diff method", exc_info=True)
        return diff_mces(output, golden, ignore_paths)


def diff_mces(output: object, golden: object, ignore_paths: Sequence[str]) -> DeepDiff:
    return DeepDiff(
        golden,
        output,
        exclude_regex_paths=ignore_paths,
        ignore_order=True,
    )


def assert_mces_equal(
    output: object, golden: object, ignore_paths: Optional[List[str]] = None
) -> None:
    diff = diff_mces(output, golden, ignore_paths or ())
    assert not diff, f"MCEs differ\n{pprint.pformat(diff)}"
