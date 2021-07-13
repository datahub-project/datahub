import json
import os
import pprint
import shutil
from typing import List, Optional, Union

import deepdiff

from tests.test_helpers.type_helpers import PytestConfig

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


def assert_mces_equal(
    output: object, golden: object, ignore_paths: Optional[List[str]] = None
) -> None:
    # This method assumes we're given a list of MCE json objects.
    diff = deepdiff.DeepDiff(golden, output, exclude_regex_paths=ignore_paths)
    if diff:
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
