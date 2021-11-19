import time
from typing import Any, Dict, List, cast

import pytest
import requests
from click.testing import CliRunner
from freezegun import freeze_time

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.click_helpers import assert_result_ok
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-11-11 07:00:00"
METABASE_URL = "http://localhost:3000"


def setup_user_and_get_id(setup_token):
    setup_response = requests.post(
        f"{METABASE_URL}/api/setup",
        None,
        {
            "token": f"{setup_token}",
            "prefs": {
                "site_name": "Acryl",
                "site_locale": "en",
                "allow_tracking": "false",
            },
            "database": None,
            "user": {
                "first_name": "admin",
                "last_name": "admin",
                "email": "admin@metabase.com",
                "password": "admin12345",
                "site_name": "Acryl",
            },
        },
    )

    if setup_response.status_code == 200:
        return setup_response.json()["id"]

    return None


def get_setup_token():
    session_response = requests.get(f"{METABASE_URL}/api/session/properties")
    if session_response.status_code == 200:
        return session_response.json()["setup-token"]

    return None


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_metabase_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/metabase"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "metabase/metabase"
    ) as docker_services:
        wait_for_port(docker_services, "testmetabase", 3000)

        # Delay API call for few seconds. Metabase is not usually ready to accept connections
        # even when port is available
        time.sleep(8)

        # create user for the first time
        setup_token = get_setup_token()
        access_token = setup_user_and_get_id(setup_token)
        session = requests.Session()
        session.headers.update(
            {
                "X-Metabase-Session": f"{access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # create dashboard
        create_dashboard_response = session.post(
            f"{METABASE_URL}/api/dashboard",
            None,
            {"name": "Dashboard", "description": "", "collection_id": None},
        )
        dashboard_id = create_dashboard_response.json()["id"]

        # create card and add to dashboard
        create_card_file_path = (
            test_resources_dir / "setup/create_card.json"
        ).resolve()
        create_card_json = mce_helpers.load_json_file(create_card_file_path)
        card_create_response = session.post(
            f"{METABASE_URL}/api/card", None, create_card_json
        )
        card_id = card_create_response.json()["id"]
        session.post(
            f"{METABASE_URL}/api/dashboard/{dashboard_id}/cards",
            None,
            {"cardId": card_id},
        )

        # Run the metadata ingestion pipeline.
        runner = CliRunner()
        with fs_helpers.isolated_filesystem(tmp_path):
            config_file = (test_resources_dir / "metabase_to_file.yml").resolve()
            result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
            assert_result_ok(result)

            # Verify the output.
            output: List[Dict[str, Any]] = cast(
                List[Dict[str, Any]], mce_helpers.load_json_file("metabase_mces.json")
            )
            golden = mce_helpers.load_json_file(
                test_resources_dir / "metabase_mces_golden.json"
            )

            # replace timestamps from output because these will always be different
            output[0]["proposedSnapshot"][
                "com.linkedin.pegasus2avro.metadata.snapshot.DashboardSnapshot"
            ]["aspects"][0]["com.linkedin.pegasus2avro.dashboard.DashboardInfo"][
                "lastModified"
            ][
                "created"
            ][
                "time"
            ] = None

            output[0]["proposedSnapshot"][
                "com.linkedin.pegasus2avro.metadata.snapshot.DashboardSnapshot"
            ]["aspects"][0]["com.linkedin.pegasus2avro.dashboard.DashboardInfo"][
                "lastModified"
            ][
                "lastModified"
            ][
                "time"
            ] = None

            output[1]["proposedSnapshot"][
                "com.linkedin.pegasus2avro.metadata.snapshot.ChartSnapshot"
            ]["aspects"][0]["com.linkedin.pegasus2avro.chart.ChartInfo"][
                "lastModified"
            ][
                "created"
            ][
                "time"
            ] = None

            output[1]["proposedSnapshot"][
                "com.linkedin.pegasus2avro.metadata.snapshot.ChartSnapshot"
            ]["aspects"][0]["com.linkedin.pegasus2avro.chart.ChartInfo"][
                "lastModified"
            ][
                "lastModified"
            ][
                "time"
            ] = None

            mce_helpers.assert_mces_equal(output, golden)
