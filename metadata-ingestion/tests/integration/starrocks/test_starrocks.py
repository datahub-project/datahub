import subprocess

import pytest
import time_machine

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifierConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.file import FileSinkConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.testing import mce_helpers
from tests.test_helpers import fs_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_1

FROZEN_TIME = "2021-09-23 12:00:00"
STARROCKS_FE_PORT = 59030

data_platform = "starrocks"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/starrocks"


def is_starrocks_ready(container_name: str) -> bool:
    """Check if StarRocks FE is ready to accept connections"""
    try:
        # Check if StarRocks FE is ready via MySQL protocol
        result = subprocess.run(
            f"docker exec {container_name} mysql -h127.0.0.1 -P9030 -uroot -e 'SELECT 1'",
            shell=True,
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def starrocks_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "teststarrocks"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "teststarrocks",
            STARROCKS_FE_PORT,
            timeout=300,  # Increased timeout to 5 minutes
        )

        # Wait for StarRocks to be fully ready - StarRocks takes time to initialize
        docker_services.wait_until_responsive(
            timeout=240,  # Increased timeout to 4 minutes
            pause=10,  # Check every 10 seconds
            check=lambda: is_starrocks_ready("teststarrocks"),
        )

        yield docker_services


@pytest.fixture(scope="module")
def loaded_starrocks(starrocks_runner):
    # Set up the test data in StarRocks
    command = (
        "docker exec teststarrocks mysql -h127.0.0.1 -P9030 -uroot < /setup/setup.sql"
    )
    subprocess.run(command, shell=True, check=True)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_starrocks_ingest(
    loaded_starrocks, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline
    with fs_helpers.isolated_filesystem(tmp_path):
        mce_out_file = "starrocks_mces.json"
        events_file = tmp_path / mce_out_file

        pipeline_config = {
            "run_id": "starrocks-test",
            "source": {
                "type": data_platform,
                "config": MySQLConfig(
                    host_port="localhost:59030",
                    database="test_db",
                    username="root",
                    password="",
                    schema_pattern=AllowDenyPattern(allow=["^test_db"]),
                    profiling=GEProfilingConfig(
                        enabled=True,
                        include_field_null_count=True,
                        include_field_distinct_count=True,
                        include_field_min_value=True,
                        include_field_max_value=True,
                        include_field_mean_value=True,
                        include_field_median_value=True,
                        include_field_stddev_value=True,
                        include_field_quantiles=True,
                        include_field_distinct_value_frequencies=True,
                        include_field_histogram=True,
                        include_field_sample_values=True,
                    ),
                    classification=ClassificationConfig(
                        enabled=True,
                        classifiers=[
                            DynamicTypedClassifierConfig(
                                type="datahub",
                                config=DataHubClassifierConfig(
                                    minimum_values_threshold=1,
                                ),
                            )
                        ],
                        max_workers=1,
                    ),
                ).model_dump(),
            },
            "sink": {
                "type": "file",
                "config": FileSinkConfig(filename=str(events_file)).model_dump(),
            },
        }

        # Run the metadata ingestion pipeline
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=True)

        # Verify the output
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="starrocks_mces.json",
            golden_path=test_resources_dir / "starrocks_mces_golden.json",
        )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_starrocks_table_level_only(
    loaded_starrocks, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline with table-level profiling only
    with fs_helpers.isolated_filesystem(tmp_path):
        mce_out_file = "starrocks_table_level_mces.json"
        events_file = tmp_path / mce_out_file

        pipeline_config = {
            "run_id": "starrocks-table-level-test",
            "source": {
                "type": data_platform,
                "config": MySQLConfig(
                    host_port="localhost:59030",
                    database="test_db",
                    username="root",
                    password="",
                    schema_pattern=AllowDenyPattern(allow=["^test_db"]),
                    profiling=GEProfilingConfig(
                        enabled=True,
                        profile_table_level_only=True,
                    ),
                ).model_dump(),
            },
            "sink": {
                "type": "file",
                "config": FileSinkConfig(filename=str(events_file)).model_dump(),
            },
        }

        # Run the metadata ingestion pipeline
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=True)

        # Verify the output
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="starrocks_table_level_mces.json",
            golden_path=test_resources_dir / "starrocks_table_level_golden.json",
        )
