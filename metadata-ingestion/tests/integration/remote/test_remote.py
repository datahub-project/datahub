from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-08-24 09:00:00"


@freeze_time(FROZEN_TIME)
def test_remote_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    """
    Using Apache http server to host the files.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/remote"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "remote"
    ) as docker_services:
        wait_for_port(
            docker_services=docker_services,
            container_name="file-server",
            container_port=80,
            hostname="localhost",
            timeout=10,
            pause=5,
        )

        # Run the metadata ingestion pipeline for remote file.
        config_file = (test_resources_dir / "configs/remote_file_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "remote_file_output.json",
            golden_path=test_resources_dir / "golden/remote_file_golden.json",
        )

        # Run the metadata ingestion pipeline for remote glossary.
        config_file = (
            test_resources_dir / "configs/remote_glossary_to_file.yml"
        ).resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "remote_glossary_output.json",
            golden_path=test_resources_dir / "golden/remote_glossary_golden.json",
        )

        # Run the metadata ingestion pipeline for remote lineage.
        config_file = (
            test_resources_dir / "configs/remote_lineage_to_file.yml"
        ).resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "parsed_lineage_output.json",
            golden_path=test_resources_dir / "golden/remote_lineage_golden.json",
        )

        # Run the metadata ingestion pipeline for remote lineage.
        config_file = (
            test_resources_dir / "configs/remote_enricher_to_file.yml"
        ).resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "parsed_enriched_file.json",
            golden_path=test_resources_dir / "golden/remote_enricher_golden.json",
        )
