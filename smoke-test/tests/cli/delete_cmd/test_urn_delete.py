import json
import tempfile
import sys
import csv
from json import JSONDecodeError
from typing import Dict, List

from click.testing import CliRunner, Result

from datahub.entrypoints import datahub
from tests.utils import ingest_file_via_rest

runner = CliRunner(mix_stderr=False)

platform = "urn:li:dataPlatform:kafka"
dataset_name = "test-rollback"
env = "PROD"
dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

def datahub_get_and_verify(
    test_dataset_urn: str,
) -> None:
    get_args: List[str] = ["get", "--urn", test_dataset_urn]
    get_result: Result = runner.invoke(datahub, get_args)
    assert get_result.exit_code == 0
    try:
        get_result_output_obj: Dict = json.loads(get_result.stdout)
    except JSONDecodeError as e:
        print("Failed to decode: " + get_result.stdout, file=sys.stderr)
        raise e
    assert len(get_result_output_obj.keys()) == 1
    assert get_result_output_obj.get("datasetKey")


def datahub_delete(params: List[str]) -> None:
    args: List[str] = ["delete"]
    args.extend(params)
    args.append("--hard")
    delete_result: Result = runner.invoke(datahub, args, input="y\n")
    assert delete_result.exit_code == 0


def test_urn_csv_delete() -> None:
    ingest_file_via_rest("tests/cli/cli_test_data.json")
    with tempfile.NamedTemporaryFile("w+t", newline='') as file:
        file.writelines([dataset_urn])
        file.seek(0)
        datahub_delete(
            [
                "--urn-file",
                file.name,
            ],
        )
        datahub_get_and_verify(test_dataset_urn=dataset_urn)
