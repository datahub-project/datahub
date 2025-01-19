import json
from typing import List

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
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


def test_view_properties_aspect_present(pytestconfig):
    """
    Verify that view definitions include the ViewProperties aspect with correct attributes.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/clickhouse"
    golden_file = test_resources_dir / "clickhouse_mces_golden.json"

    with golden_file.open() as f:
        mces = json.load(f)

    # Find all dataset entities that have View as a subType
    view_urns = []
    for mce in mces:
        if (
            "entityType" in mce
            and mce["entityType"] == "dataset"
            and "aspectName" in mce
            and mce["aspectName"] == "subTypes"
        ):
            if "View" in mce["aspect"]["json"]["typeNames"]:
                view_urns.append(mce["entityUrn"])

    assert len(view_urns) > 0, "No views found in golden file"

    # For each view, verify it has ViewProperties aspect
    for view_urn in view_urns:
        found_view_properties = False
        for mce in mces:
            if (
                "entityType" in mce
                and mce["entityType"] == "dataset"
                and "entityUrn" in mce
                and mce["entityUrn"] == view_urn
                and "aspectName" in mce
                and mce["aspectName"] == "viewProperties"
            ):
                found_view_properties = True
                view_props = mce["aspect"]["json"]

                # Check required fields
                assert (
                    "viewLogic" in view_props
                ), "viewLogic missing from ViewProperties"
                assert (
                    "viewLanguage" in view_props
                ), "viewLanguage missing from ViewProperties"
                assert (
                    "materialized" in view_props
                ), "materialized flag missing from ViewProperties"

                # Check values
                assert view_props["viewLanguage"] == "SQL", "viewLanguage should be SQL"
                assert isinstance(
                    view_props["materialized"], bool
                ), "materialized should be boolean"
                assert view_props["viewLogic"], "viewLogic should not be empty"

        assert (
            found_view_properties
        ), f"ViewProperties aspect missing for view {view_urn}"
