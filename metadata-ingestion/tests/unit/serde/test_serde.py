import io
import json

import fastavro
import pytest
from click.testing import CliRunner

from datahub.entrypoints import datahub
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.mce_file import iterate_mce_file
from datahub.metadata.schema_classes import SCHEMA_JSON_STR, MetadataChangeEventClass
from tests.test_helpers import mce_helpers


@pytest.mark.parametrize(
    "json_filename",
    [
        # Normal test.
        "tests/unit/serde/test_serde_large.json",
        # Ensure correct representation of chart info's input list.
        "tests/unit/serde/test_serde_chart_snapshot.json",
    ],
)
def test_serde_to_json(pytestconfig, tmp_path, json_filename):
    golden_file = pytestconfig.rootpath / json_filename

    output_filename = "output.json"
    output_file = tmp_path / output_filename

    pipeline = Pipeline.create(
        {
            "source": {"type": "file", "config": {"filename": str(golden_file)}},
            "sink": {"type": "file", "config": {"filename": str(output_file)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    output = mce_helpers.load_json_file(tmp_path / output_filename)
    golden = mce_helpers.load_json_file(golden_file)
    assert golden == output


@pytest.mark.parametrize(
    "json_filename",
    [
        "tests/unit/serde/test_serde_large.json",
        "tests/unit/serde/test_serde_chart_snapshot.json",
    ],
)
def test_serde_to_avro(pytestconfig, json_filename):
    # In this test, we want to read in from JSON -> MCE object.
    # Next we serialize from MCE to Avro and then deserialize back to MCE.
    # Finally, we want to compare the two MCE objects.

    json_path = pytestconfig.rootpath / json_filename
    mces = list(iterate_mce_file(str(json_path)))

    # Serialize to Avro.
    parsed_schema = fastavro.parse_schema(json.loads(SCHEMA_JSON_STR))
    fo = io.BytesIO()
    out_records = [mce.to_obj(tuples=True) for mce in mces]
    fastavro.writer(fo, parsed_schema, out_records)

    # Deserialized from Avro.
    fo.seek(0)
    in_records = list(fastavro.reader(fo))
    in_mces = [
        MetadataChangeEventClass.from_obj(record, tuples=True) for record in in_records
    ]

    # Check diff
    assert len(mces) == len(in_mces)
    for i in range(len(mces)):
        assert str(mces[i]) == str(in_mces[i])


@pytest.mark.parametrize(
    "json_filename",
    [
        # Normal test.
        "tests/unit/serde/test_serde_large.json",
        # Check for backwards compatability with specifying all union types.
        "tests/unit/serde/test_serde_backwards_compat.json",
        # Ensure sample MCE files are valid.
        "examples/mce_files/single_mce.json",
        "examples/mce_files/mce_list.json",
        "examples/mce_files/bootstrap_mce.json",
    ],
)
def test_check_mce_schema(pytestconfig, json_filename):
    json_file_path = pytestconfig.rootpath / json_filename

    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "mce-file", f"{json_file_path}"])
    assert result.exit_code == 0
