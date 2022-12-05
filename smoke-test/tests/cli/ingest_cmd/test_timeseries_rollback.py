import json
import time
from typing import Any, Dict, List, Optional

from click.testing import CliRunner, Result

import datahub.emitter.mce_builder as builder
from datahub.emitter.serialization_helper import post_json_transform
from datahub.entrypoints import datahub
from datahub.metadata.schema_classes import DatasetProfileClass
from tests.utils import ingest_file_via_rest
import requests_wrapper as requests

runner = CliRunner()


def sync_elastic() -> None:
    time.sleep(requests.ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)


def datahub_rollback(run_id: str) -> None:
    sync_elastic()
    rollback_args: List[str] = ["ingest", "rollback", "--run-id", run_id, "-f"]
    rollback_result: Result = runner.invoke(datahub, rollback_args)
    assert rollback_result.exit_code == 0


def datahub_get_and_verify_profile(
    urn: str,
    aspect_name: str,
    expected_profile: Optional[DatasetProfileClass],
) -> None:
    # Wait for writes to stabilize in elastic
    sync_elastic()
    get_args: List[str] = ["get", "--urn", urn, "-a", aspect_name]
    get_result: Result = runner.invoke(datahub, get_args)
    assert get_result.exit_code == 0
    get_result_output_obj: Dict = json.loads(get_result.output)
    if expected_profile is None:
        assert not get_result_output_obj
    else:
        profile_as_dict: Dict = post_json_transform(
            get_result_output_obj["datasetProfile"]
        )
        profile_from_get = DatasetProfileClass.from_obj(profile_as_dict)
        assert profile_from_get == expected_profile


def test_timeseries_rollback(wait_for_healthchecks: Any) -> None:
    pipeline = ingest_file_via_rest(
        "tests/cli/ingest_cmd/test_timeseries_rollback.json"
    )
    test_aspect_name: str = "datasetProfile"
    test_dataset_urn: str = builder.make_dataset_urn(
        "test_rollback",
        "rollback_test_dataset",
        "TEST",
    )
    datahub_rollback(pipeline.config.run_id)
    datahub_get_and_verify_profile(test_dataset_urn, test_aspect_name, None)
