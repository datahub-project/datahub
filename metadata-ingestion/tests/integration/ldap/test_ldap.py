import time

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.slow
def test_ldap_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/ldap"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "ldap"
    ) as docker_services:
        # The openldap container loads the sample data after exposing the port publicly. As such,
        # we must wait a little bit extra to ensure that the sample data is loaded.
        wait_for_port(docker_services, "openldap", 389)
        time.sleep(5)

        pipeline = Pipeline.create(
            {
                "run_id": "ldap-test",
                "source": {
                    "type": "ldap",
                    "config": {
                        "ldap_server": "ldap://localhost",
                        "ldap_user": "cn=admin,dc=example,dc=org",
                        "ldap_password": "admin",
                        "base_dn": "dc=example,dc=org",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/ldap_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "ldap_mces.json",
            golden_path=test_resources_dir / "ldap_mces_golden.json",
        )
