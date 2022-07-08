import re
import subprocess

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.file import FileSinkConfig
from datahub.ingestion.source.sql.hive import HiveConfig
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"

data_platform = "hive"


@pytest.fixture(scope="module")
def hive_runner(docker_compose_runner, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hive"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "hive"
    ) as docker_services:
        wait_for_port(docker_services, "testhiveserver2", 10000, timeout=120)
        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/hive"


@pytest.fixture(scope="module")
def loaded_hive(hive_runner):
    # Set up the container.
    command = "docker exec testhiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
    subprocess.run(command, shell=True, check=True)


def base_pipeline_config(events_file):

    return {
        "run_id": "hive-test",
        "source": {
            "type": data_platform,
            "config": HiveConfig(
                scheme="hive", database="db1", host_port="localhost:10000"
            ).dict(),
        },
        "sink": {
            "type": "file",
            "config": FileSinkConfig(filename=str(events_file)).dict(),
        },
    }


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_hive_ingest(
    loaded_hive, pytestconfig, test_resources_dir, tmp_path, mock_time
):
    mce_out_file = "test_hive_ingest.json"
    events_file = tmp_path / mce_out_file

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(base_pipeline_config(events_file))
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=events_file,
        golden_path=test_resources_dir / "hive_mces_golden.json",
        ignore_paths=[
            # example: root[1]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][0]['com.linkedin.pegasus2avro.dataset.DatasetProperties']['customProperties']['CreateTime:']
            # example: root[2]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][0]['com.linkedin.pegasus2avro.dataset.DatasetProperties']['customProperties']['Table Parameters: transient_lastDdlTime']
            r"root\[\d+\]\['proposedSnapshot'\]\['com\.linkedin\.pegasus2avro\.metadata\.snapshot\.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com\.linkedin\.pegasus2avro\.dataset\.DatasetProperties'\]\['customProperties'\]\['.*Time.*'\]",
            r"root\[6\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.schema.SchemaMetadata'\]\['fields'\]\[\d+\]\['nativeDataType'\]",
        ],
    )

    # Limitation - native data types for union does not show up as expected


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_hive_instance_check(loaded_hive, test_resources_dir, tmp_path, pytestconfig):
    instance: str = "production_warehouse"

    # Run the metadata ingestion pipeline.
    mce_out_file = "test_hive_instance.json"
    events_file = tmp_path / mce_out_file

    pipeline_config = base_pipeline_config(events_file)
    pipeline_config["source"]["config"]["platform_instance"] = instance

    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    # Assert that all events generated have instance specific urns
    urn_pattern = "^" + re.escape(
        f"urn:li:dataset:(urn:li:dataPlatform:{data_platform},{instance}."
    )
    mce_helpers.assert_mce_entity_urn(
        "ALL",
        entity_type="dataset",
        regex_pattern=urn_pattern,
        file=events_file,
    )

    mce_helpers.assert_mcp_entity_urn(
        "ALL",
        entity_type="dataset",
        regex_pattern=urn_pattern,
        file=events_file,
    )

    # all dataset entities emitted must have a dataPlatformInstance aspect emitted
    # there must be at least one entity emitted
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="dataPlatformInstance",
            aspect_field_matcher={
                "instance": f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:{data_platform},{instance})"
            },
            file=events_file,
        )
        >= 1
    )
