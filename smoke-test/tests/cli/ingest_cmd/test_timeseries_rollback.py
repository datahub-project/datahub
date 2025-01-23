import json
from typing import Dict, List, Optional

from click.testing import CliRunner, Result

import datahub.emitter.mce_builder as builder
from datahub.emitter.serialization_helper import post_json_transform
from datahub.entrypoints import datahub
from datahub.metadata.schema_classes import DatasetProfileClass
from tests.utils import ingest_file_via_rest, wait_for_writes_to_sync

runner = CliRunner(mix_stderr=False)


def sync_elastic() -> None:
    wait_for_writes_to_sync()


def datahub_rollback(auth_session, run_id: str) -> None:
    sync_elastic()
    rollback_args: List[str] = ["ingest", "rollback", "--run-id", run_id, "-f"]
    rollback_result: Result = runner.invoke(
        datahub,
        rollback_args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )
    assert rollback_result.exit_code == 0


def datahub_get_and_verify_profile(
    auth_session,
    urn: str,
    aspect_name: str,
    expected_profile: Optional[DatasetProfileClass],
) -> None:
    # Wait for writes to stabilize in elastic
    sync_elastic()
    get_args: List[str] = ["get", "--urn", urn, "-a", aspect_name]
    get_result: Result = runner.invoke(
        datahub,
        get_args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )
    assert get_result.exit_code == 0
    get_result_output_obj: Dict = json.loads(get_result.stdout)
    if expected_profile is None:
        assert not get_result_output_obj
    else:
        profile_as_dict: Dict = post_json_transform(
            get_result_output_obj["datasetProfile"]
        )
        profile_from_get = DatasetProfileClass.from_obj(profile_as_dict)
        assert profile_from_get == expected_profile


def test_timeseries_rollback(auth_session) -> None:
    pipeline = ingest_file_via_rest(
        auth_session, "tests/cli/ingest_cmd/test_timeseries_rollback.json"
    )
    test_aspect_name: str = "datasetProfile"
    test_dataset_urn: str = builder.make_dataset_urn(
        "test_rollback",
        "rollback_test_dataset",
        "TEST",
    )
    datahub_rollback(auth_session, pipeline.config.run_id)
    datahub_get_and_verify_profile(
        auth_session, test_dataset_urn, test_aspect_name, None
    )
