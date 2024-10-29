import pathlib
import time
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2022-08-14 07:00:00"

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def ldap_ingest_common(
    docker_compose_runner,
    pytestconfig,
    tmp_path,
    golden_file_name,
    output_file_name,
    filter,
    mock_datahub_graph,
):
    test_resources_dir = pathlib.Path(pytestconfig.rootpath / "tests/integration/ldap")
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "ldap"
    ) as docker_services:
        # The openldap container loads the sample data after exposing the port publicly. As such,
        # we must wait a little bit extra to ensure that the sample data is loaded.
        wait_for_port(docker_services, "openldap", 389)
        # without this ldap server can provide empty results
        time.sleep(5)

        with mock.patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint:
            mock_checkpoint.return_value = mock_datahub_graph

            pipeline = Pipeline.create(
                {
                    "run_id": "ldap-test",
                    "pipeline_name": "ldap-test-pipeline",
                    "source": {
                        "type": "ldap",
                        "config": {
                            "ldap_server": "ldap://localhost",
                            "ldap_user": "cn=admin,dc=example,dc=org",
                            "ldap_password": "admin",
                            "base_dn": "dc=example,dc=org",
                            "filter": filter,
                            "attrs_list": ["+", "*"],
                            "stateful_ingestion": {
                                "enabled": True,
                                "remove_stale_metadata": True,
                                "fail_safe_threshold": 100.0,
                                "state_provider": {
                                    "type": "datahub",
                                    "config": {"datahub_api": {"server": GMS_SERVER}},
                                },
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {
                            "filename": f"{tmp_path}/{output_file_name}",
                        },
                    },
                }
            )
            pipeline.run()
            pipeline.raise_from_status()

            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=tmp_path / output_file_name,
                golden_path=test_resources_dir / golden_file_name,
                ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
            )
            return pipeline


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_ldap_stateful(
    docker_compose_runner, pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    golden_file_name: str = "ldap_mces_golden_stateful.json"
    output_file_name: str = "ldap_mces_stateful.json"

    golden_file_deleted_name: str = "ldap_mces_golden_deleted_stateful.json"
    output_file_deleted_name: str = "ldap_mces_deleted_stateful.json"

    golden_file_group: str = "ldap_mces_golden_group_stateful.json"
    output_file_group: str = "ldap_mces_group_stateful.json"

    golden_file_delete_group: str = "ldap_mces_golden_deleted_group_stateful.json"
    output_file_delete_group: str = "ldap_mces_deleted_group_stateful.json"

    filter1: str = "(|(cn=Bart Simpson)(cn=Homer Simpson))"
    filter2: str = "(|(cn=Bart Simpson))"
    filter3: str = "(&(objectClass=top)(|(cn=HR Department)(cn=Finance Department)))"
    filter4: str = "(&(objectClass=top)(|(cn=HR Department)))"

    pipeline_run1 = ldap_ingest_common(
        docker_compose_runner,
        pytestconfig,
        tmp_path,
        golden_file_name,
        output_file_name,
        filter1,
        mock_datahub_graph,
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = ldap_ingest_common(
        docker_compose_runner,
        pytestconfig,
        tmp_path,
        golden_file_deleted_name,
        output_file_deleted_name,
        filter2,
        mock_datahub_graph,
    )

    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
    assert checkpoint2
    assert checkpoint2.state

    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    state1 = checkpoint1.state
    state2 = checkpoint2.state

    difference_dataset_urns = list(
        state1.get_urns_not_in(type="corpuser", other_checkpoint_state=state2)
    )
    assert len(difference_dataset_urns) == 1
    deleted_dataset_urns = [
        "urn:li:corpuser:hsimpson",
    ]
    assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)

    pipeline_run3 = ldap_ingest_common(
        docker_compose_runner,
        pytestconfig,
        tmp_path,
        golden_file_group,
        output_file_group,
        filter3,
        mock_datahub_graph,
    )

    checkpoint3 = get_current_checkpoint_from_pipeline(pipeline_run3)
    assert checkpoint3
    assert checkpoint3.state

    pipeline_run4 = ldap_ingest_common(
        docker_compose_runner,
        pytestconfig,
        tmp_path,
        golden_file_delete_group,
        output_file_delete_group,
        filter4,
        mock_datahub_graph,
    )

    checkpoint4 = get_current_checkpoint_from_pipeline(pipeline_run4)
    assert checkpoint4
    assert checkpoint4.state

    state3 = checkpoint3.state
    state4 = checkpoint4.state

    difference_dataset_urns = list(
        state3.get_urns_not_in(type="corpGroup", other_checkpoint_state=state4)
    )
    assert len(difference_dataset_urns) == 1
    deleted_dataset_urns = [
        "urn:li:corpGroup:Finance Department",
    ]
    assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)
