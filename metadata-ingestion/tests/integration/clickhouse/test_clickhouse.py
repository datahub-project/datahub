from typing import List

import pytest
from freezegun import freeze_time

from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_clickhouse_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/clickhouse"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "clickhouse"
    ) as docker_services:
        wait_for_port(docker_services, "testclickhouse", 8123, timeout=120)
        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "clickhouse_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "-c", f"{config_file}"],
            tmp_path=tmp_path,
        )
        # These paths change from one instance run of the clickhouse docker to the other, and the FROZEN_TIME does not apply to these.
        ignore_paths: List[str] = [
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['metadata_modification_time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['data_paths'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['metadata_path'\]",
        ]
        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            ignore_paths=ignore_paths,
            output_path=tmp_path / "clickhouse_mces.json",
            golden_path=test_resources_dir / "clickhouse_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_clickhouse_ingest_uri_form(
    docker_compose_runner, pytestconfig, tmp_path, mock_time
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/clickhouse"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "clickhouse"
    ) as docker_services:
        wait_for_port(docker_services, "testclickhouse", 8123, timeout=120)

        # Run the metadata ingestion pipeline with uri form.
        config_file = (test_resources_dir / "clickhouse_to_file_uri_form.yml").resolve()
        run_datahub_cmd(
            ["ingest", "-c", f"{config_file}"],
            tmp_path=tmp_path,
        )
        # These paths change from one instance run of the clickhouse docker to the other, and the FROZEN_TIME does not apply to these.
        ignore_paths: List[str] = [
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['metadata_modification_time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['data_paths'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['metadata_path'\]",
        ]
        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            ignore_paths=ignore_paths,
            output_path=tmp_path / "clickhouse_mces_uri_form.json",
            golden_path=test_resources_dir / "clickhouse_mces_golden.json",
        )
