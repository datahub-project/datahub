import re
import subprocess
from typing import Dict, Sequence

import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_1
FROZEN_TIME = "2021-09-23 12:00:00"

data_platform = "hive-metastore"


@pytest.fixture(scope="module")
def hive_metastore_runner(docker_compose_runner, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/hive-metastore"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "hive-metastore"
    ) as docker_services:
        wait_for_port(docker_services, "presto", 8080)
        wait_for_port(docker_services, "hiveserver2", 10000, timeout=120)
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get("http://localhost:5300/v1/info").json()[
                "starting"
            ]
            is False,
        )

        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/hive-metastore"


@pytest.fixture(scope="module")
def loaded_hive_metastore(hive_metastore_runner):
    # Set up the container.
    command = "docker exec hiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
    subprocess.run(command, shell=True, check=True)
    # Create presto view so we can test
    command = "docker exec presto-cli /usr/bin/presto --server presto:8080 --catalog hivedb --execute 'CREATE VIEW db1.array_struct_test_presto_view as select * from db1.array_struct_test'"
    subprocess.run(command, shell=True, check=True)


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "mode,use_catalog_subtype,use_dataset_pascalcase_subtype,include_catalog_name_in_ids,simplify_nested_field_paths,"
    "test_suffix",
    [
        ("hive", False, False, False, False, "_1"),
        ("presto-on-hive", True, True, False, False, "_2"),
        ("hive", False, False, True, False, "_3"),
        ("presto-on-hive", True, True, True, False, "_4"),
        ("hive", False, False, False, True, "_5"),
    ],
)
def test_hive_metastore_ingest(
    loaded_hive_metastore,
    test_resources_dir,
    pytestconfig,
    tmp_path,
    mock_time,
    mode,
    use_catalog_subtype,
    use_dataset_pascalcase_subtype,
    include_catalog_name_in_ids,
    simplify_nested_field_paths,
    test_suffix,
):
    # Run the metadata ingestion pipeline.
    with fs_helpers.isolated_filesystem(tmp_path):
        # Run the metadata ingestion pipeline for presto catalog referring to postgres database
        mce_out_file = f"hive_metastore_mces{test_suffix}.json"
        events_file = tmp_path / mce_out_file

        pipeline_config: Dict = {
            "run_id": "hive-metastore-test",
            "source": {
                "type": data_platform,
                "config": {
                    "host_port": "localhost:5432",
                    "metastore_db_name": "metastore",
                    "database_pattern": {"allow": ["db1"]},
                    "username": "postgres",
                    "scheme": "postgresql+psycopg2",
                    "include_views": True,
                    "include_tables": True,
                    "include_catalog_name_in_ids": include_catalog_name_in_ids,
                    "schema_pattern": {"allow": ["^public"]},
                    "mode": mode,
                    "use_catalog_subtype": use_catalog_subtype,
                    "use_dataset_pascalcase_subtype": use_dataset_pascalcase_subtype,
                    "simplify_nested_field_paths": simplify_nested_field_paths,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(events_file),
                },
            },
        }

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=True)

        # Run the metadata ingestion pipeline for presto catalog referring to hive database
        # config_file = (test_resources_dir / "presto_on_hive_to_file.yml").resolve()
        # run_datahub_cmd(["ingest", "-c", f"{config_file}"])

        ignore_paths: Sequence[str] = [
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['transient_lastDdlTime'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['numfiles'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['totalsize'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['create_date'\]",
        ]

        ignore_paths_v2: Sequence[str] = [
            "/customProperties/create_date",
            "/customProperties/transient_lastDdlTime",
            "/customProperties/numfiles",
            "/customProperties/totalsize",
        ]

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"hive_metastore_mces{test_suffix}.json",
            golden_path=test_resources_dir
            / f"hive_metastore_mces_golden{test_suffix}.json",
            ignore_paths=ignore_paths,
            ignore_paths_v2=ignore_paths_v2,
        )


@freeze_time(FROZEN_TIME)
def test_hive_metastore_instance_ingest(
    loaded_hive_metastore, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    instance = "production_warehouse"
    platform = "hive"
    mce_out_file = "hive_metastore_instance_mces.json"
    events_file = tmp_path / mce_out_file

    pipeline_config: Dict = {
        "run_id": "hive-metastore-test-2",
        "source": {
            "type": data_platform,
            "config": {
                "host_port": "localhost:5432",
                "database": "metastore",
                "username": "postgres",
                "scheme": "postgresql+psycopg2",
                "include_views": True,
                "include_tables": True,
                "schema_pattern": {"allow": ["^public"]},
                "platform_instance": "production_warehouse",
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": str(events_file),
            },
        },
    }

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    # Assert that all events generated have instance specific urns
    urn_pattern = "^" + re.escape(
        f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}."
    )
    assert (
        mce_helpers.assert_mce_entity_urn(
            "ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        >= 0
    ), "There should be at least one match"

    assert (
        mce_helpers.assert_mcp_entity_urn(
            "ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        >= 0
    ), "There should be at least one MCP"

    # all dataset entities emitted must have a dataPlatformInstance aspect emitted
    # there must be at least one entity emitted
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="dataPlatformInstance",
            aspect_field_matcher={
                "instance": f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:{platform},{instance})"
            },
            file=events_file,
        )
        >= 1
    )
