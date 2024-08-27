import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-08-24 09:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
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
            timeout=30,
            pause=5,
        )

        # try reading from a remote csv
        pipeline = Pipeline.create(
            {
                "run_id": "remote-1",
                "source": {
                    "type": "csv-enricher",
                    "config": {
                        "filename": "http://127.0.0.1/csv_enricher_test_data.csv",
                        "write_semantics": "OVERRIDE",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/parsed_enriched_file.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "parsed_enriched_file.json",
            golden_path=test_resources_dir / "golden/remote_enricher_golden.json",
        )

        # try reading from a remote file
        pipeline = Pipeline.create(
            {
                "run_id": "remote-2",
                "source": {
                    "type": "file",
                    "config": {
                        "path": "http://127.0.0.1/mce_list.json",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/remote_file_output.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "remote_file_output.json",
            golden_path=test_resources_dir / "golden/remote_file_golden.json",
        )

        # try reading from a remote lineage file
        pipeline = Pipeline.create(
            {
                "run_id": "remote-3",
                "source": {
                    "type": "datahub-lineage-file",
                    "config": {
                        "file": "http://127.0.0.1/file_lineage.yml",
                        "preserve_upstream": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/parsed_lineage_output.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "parsed_lineage_output.json",
            golden_path=test_resources_dir / "golden/remote_lineage_golden.json",
        )

        # try reading from a remote lineage file
        pipeline = Pipeline.create(
            {
                "run_id": "remote-4",
                "source": {
                    "type": "datahub-business-glossary",
                    "config": {
                        "file": "http://127.0.0.1/business_glossary.yml",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/remote_glossary_output.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "remote_glossary_output.json",
            golden_path=test_resources_dir / "golden/remote_glossary_golden.json",
        )
