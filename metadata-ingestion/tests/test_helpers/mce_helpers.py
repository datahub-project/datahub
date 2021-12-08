import json
import logging
import os
import pprint
import shutil
from typing import List, Optional, Union

import deepdiff

from tests.test_helpers.type_helpers import PytestConfig

logger = logging.getLogger(__name__)

IGNORE_PATH_TIMESTAMPS = [
    # Ignore timestamps from the ETL pipeline. A couple examples:
    # root[0]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][0]['com.linkedin.pegasus2avro.common.Ownership']['lastModified']['time']
    # root[69]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][0]['com.linkedin.pegasus2avro.schema.SchemaMetadata']['lastModified']['time']"
    # root[0]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][1]['com.linkedin.pegasus2avro.dataset.UpstreamLineage']['upstreams'][0]['auditStamp']['time']
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['lastModified'\]\['time'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['createStamp'\]\['time'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['auditStamp'\]\['time'\]",
]


def load_json_file(filename: Union[str, os.PathLike]) -> object:
    with open(str(filename)) as f:
        a = json.load(f)
    return a


def clean_nones(value):
    """
    Recursively remove all None values from dictionaries and lists, and returns
    the result as a new dictionary or list.
    """
    if isinstance(value, list):
        return [clean_nones(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {key: clean_nones(val) for key, val in value.items() if val is not None}
    else:
        return value


def assert_mces_equal(
    output: object, golden: object, ignore_paths: Optional[List[str]] = None
) -> None:
    # This method assumes we're given a list of MCE json objects.
    diff = deepdiff.DeepDiff(
        golden, output, exclude_regex_paths=ignore_paths, ignore_order=True
    )
    if diff:
        # Attempt a clean diff (removing None-s)
        assert isinstance(output, list)
        assert isinstance(golden, list)
        clean_output = [clean_nones(o) for o in output]
        clean_golden = [clean_nones(g) for g in golden]
        clean_diff = deepdiff.DeepDiff(
            clean_golden,
            clean_output,
            exclude_regex_paths=ignore_paths,
            ignore_order=True,
        )
        if clean_diff != diff:
            logger.warning(
                f"MCE-s differ, clean MCE-s are fine\n{pprint.pformat(diff)}"
            )
        diff = clean_diff

    assert not diff, f"MCEs differ\n{pprint.pformat(diff)}"


def check_golden_file(
    pytestconfig: PytestConfig,
    output_path: Union[str, os.PathLike],
    golden_path: Union[str, os.PathLike],
    ignore_paths: Optional[List[str]] = None,
) -> None:

    update_golden = pytestconfig.getoption("--update-golden-files")
    golden_exists = os.path.isfile(golden_path)

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
        golden = load_json_file(golden_path)

    try:
        assert_mces_equal(output, golden, ignore_paths)

    except AssertionError as e:
        # only update golden files if the diffs are not empty
        if update_golden:
            shutil.copyfile(str(output_path), str(golden_path))

        # raise the error if we're just running the test
        else:
            raise e
