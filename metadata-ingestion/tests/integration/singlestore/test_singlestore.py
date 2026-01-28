import pytest
import time_machine

from datahub.ingestion.source.sql.singlestore import SingleStoreSource
from datahub.testing import mce_helpers
from datahub.testing.docker_utils import wait_for_port
from tests.test_helpers import test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd

FROZEN_TIME = "2020-04-14 07:00:00"
SINGLESTORE_PORT = 3306


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/singlestore"


@pytest.fixture(scope="module")
def singlestore_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "singlestore"
    ) as docker_services:
        wait_for_port(docker_services, "testsinglestore", SINGLESTORE_PORT, timeout=120)
        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file",
    [
        ("singlestore_to_file_with_db.yml", "singlestore_mces_with_db_golden.json"),
        (
            "singlestore_to_file_special_types.yml",
            "singlestore_mces_special_types_golden.json",
        ),
        ("singlestore_to_file_no_db.yml", "singlestore_mces_no_db_golden.json"),
        (
            "singlestore_profile_table_level_only.yml",
            "singlestore_table_level_only.json",
        ),
        (
            "singlestore_profile_table_row_count_estimate_only.yml",
            "singlestore_table_row_count_estimate_only.json",
        ),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_singlestore_ingest_no_db(
    singlestore_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
    mock_time,
    config_file,
    golden_file,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / config_file).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "singlestore_mces.json",
        golden_path=test_resources_dir / golden_file,
    )


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "host_port": "localhost:53308",
                "database": "northwind",
                "username": "root",
                "password": "example",
            },
            True,
        ),
        (
            {
                "host_port": "localhost:5330",
                "database": "wrong_db",
                "username": "wrong_user",
                "password": "wrong_pass",
            },
            False,
        ),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_singlestore_test_connection(singlestore_runner, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(SingleStoreSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Connection refused"
        )
