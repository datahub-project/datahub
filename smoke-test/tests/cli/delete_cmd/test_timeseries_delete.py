import json
import logging
import sys
import tempfile
from json import JSONDecodeError
from typing import Any, Dict, List, Optional

from click.testing import CliRunner, Result

import datahub.emitter.mce_builder as builder
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.entrypoints import datahub
from datahub.metadata.schema_classes import DatasetProfileClass
from tests.aspect_generators.timeseries.dataset_profile_gen import gen_dataset_profiles
from tests.utils import get_strftime_from_timestamp_millis, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

test_aspect_name: str = "datasetProfile"
test_dataset_urn: str = builder.make_dataset_urn_with_platform_instance(
    "test_platform",
    "test_dataset",
    "test_platform_instance",
    "TEST",
)

runner = CliRunner(mix_stderr=False)


def sync_elastic() -> None:
    wait_for_writes_to_sync()


def datahub_put_profile(auth_session, dataset_profile: DatasetProfileClass) -> None:
    with tempfile.NamedTemporaryFile("w+t", suffix=".json") as aspect_file:
        aspect_text: str = json.dumps(pre_json_transform(dataset_profile.to_obj()))
        aspect_file.write(aspect_text)
        aspect_file.seek(0)
        put_args: List[str] = [
            "put",
            "--urn",
            test_dataset_urn,
            "-a",
            test_aspect_name,
            "-d",
            aspect_file.name,
        ]
        put_result = runner.invoke(
            datahub,
            put_args,
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        assert put_result.exit_code == 0


def datahub_get_and_verify_profile(
    auth_session,
    expected_profile: Optional[DatasetProfileClass],
) -> None:
    # Wait for writes to stabilize in elastic
    sync_elastic()
    get_args: List[str] = ["get", "--urn", test_dataset_urn, "-a", test_aspect_name]
    get_result: Result = runner.invoke(
        datahub,
        get_args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    if expected_profile is None:
        assert get_result.exit_code != 0
        assert (
            test_dataset_urn in get_result.stderr and "not found" in get_result.stderr
        ), f"Got stderr of {get_result.stderr} in get_and_verify_profile"
    else:
        assert get_result.exit_code == 0
        try:
            get_result_output_obj: Dict = json.loads(get_result.stdout)
        except JSONDecodeError as e:
            print("Failed to decode: " + get_result.stdout, file=sys.stderr)
            raise e

        profile_from_get = DatasetProfileClass.from_obj(
            get_result_output_obj["datasetProfile"]
        )
        assert profile_from_get == expected_profile


def datahub_delete(auth_session, params: List[str]) -> None:
    sync_elastic()

    args: List[str] = ["delete"]
    args.extend(params)
    args.append("--hard")
    logger.info(f"Running delete command with args: {args}")
    delete_result: Result = runner.invoke(
        datahub,
        args,
        input="y\ny\n",
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )
    logger.info(delete_result.stdout)
    if delete_result.stderr:
        logger.error(delete_result.stderr)
    assert delete_result.exit_code == 0


def test_timeseries_delete(auth_session: Any) -> None:
    num_test_profiles: int = 10
    verification_batch_size: int = int(num_test_profiles / 2)
    num_latest_profiles_to_delete = 2
    expected_profile_after_latest_deletion: DatasetProfileClass
    delete_ts_start: str
    delete_ts_end: str
    # 1. Ingest `num_test_profiles` datasetProfile aspects against the test_dataset_urn via put
    # and validate using get.
    for i, dataset_profile in enumerate(gen_dataset_profiles(num_test_profiles)):
        # Use put command to ingest the aspect value.
        datahub_put_profile(auth_session, dataset_profile)
        # Validate against all ingested values once every verification_batch_size to reduce overall test time. Since we
        # are ingesting  the aspects in the ascending order of timestampMillis, get should return the one just put.
        if (i % verification_batch_size) == 0:
            datahub_get_and_verify_profile(auth_session, dataset_profile)

        # Init the params for time-range based deletion.
        if i == (num_test_profiles - num_latest_profiles_to_delete - 1):
            expected_profile_after_latest_deletion = dataset_profile
        elif i == (num_test_profiles - num_latest_profiles_to_delete):
            delete_ts_start = get_strftime_from_timestamp_millis(
                dataset_profile.timestampMillis - 100
            )
        elif i == (num_test_profiles - 1):
            delete_ts_end = get_strftime_from_timestamp_millis(
                dataset_profile.timestampMillis + 100
            )
    # 2. Verify time-range based deletion.
    datahub_delete(
        auth_session,
        [
            "--urn",
            test_dataset_urn,
            "-a",
            test_aspect_name,
            "--start-time",
            delete_ts_start,
            "--end-time",
            delete_ts_end,
        ],
    )
    assert expected_profile_after_latest_deletion is not None
    datahub_get_and_verify_profile(auth_session, expected_profile_after_latest_deletion)

    # 3. Delete everything via the delete command & validate that we don't get any profiles back.
    datahub_delete(auth_session, ["-p", "test_platform"])
    datahub_get_and_verify_profile(auth_session, None)
