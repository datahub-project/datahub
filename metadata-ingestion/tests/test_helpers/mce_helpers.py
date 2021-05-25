import json
import os
from typing import Union

import deepdiff


def load_json_file(filename: Union[str, os.PathLike]) -> object:
    with open(str(filename)) as f:
        a = json.load(f)
    return a


def assert_mces_equal(output: object, golden: object) -> None:
    # This method assumes we're given a list of MCE json objects.

    ignore_paths = {
        # Ignore timestamps from the ETL pipeline. A couple examples:
        # root[0]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][0]['com.linkedin.pegasus2avro.common.Ownership']['lastModified']['time']
        # root[69]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][0]['com.linkedin.pegasus2avro.schema.SchemaMetadata']['lastModified']['time']"
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['lastModified'\]\['time'\]",
        r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['createStamp'\]\['time'\]",
    }
    diff = deepdiff.DeepDiff(golden, output, exclude_regex_paths=ignore_paths)
    assert not diff
