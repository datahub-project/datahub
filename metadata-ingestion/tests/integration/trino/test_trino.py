import sys

import pytest
import requests
from click.testing import CliRunner
from freezegun import freeze_time

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.click_helpers import assert_result_ok
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-09-23 12:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="trino requires Python 3.7+")
@pytest.mark.integration
def test_trino_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/trino"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "trino"
    ) as docker_services:
        wait_for_port(docker_services, "testtrino", 8080)

        # wait until trino server has started - https://github.com/trinodb/trino/pull/213
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get("http://localhost:5300/v1/info").json()[
                "starting"
            ]
            is False,
        )

        # Run the metadata ingestion pipeline.
        runner = CliRunner()
        with fs_helpers.isolated_filesystem(tmp_path):
            print(tmp_path)

            config_file = (test_resources_dir / "trino_to_file.yml").resolve()
            result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
            assert_result_ok(result)

            # Verify the output.
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path="trino_mces.json",
                golden_path=test_resources_dir / "trino_mces_golden.json",
            )

            # Limitation 1  - MCE contains "nullable": true for all fields in trino database, irrespective of not null constraints present in underlying postgres database.
            # This is issue with trino, also reported here - https://github.com/trinodb/trino/issues/6400, Related : https://github.com/trinodb/trino/issues/4070

            # Limitation 2 - Dataset properties for postgres view (view query, etc) are not part of MCE from trino.
            # Postgres views are exposed as tables in trino. This setting depends on trino connector implementation - https://trino.io/episodes/18.html
