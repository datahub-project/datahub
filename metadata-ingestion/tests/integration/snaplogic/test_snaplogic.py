import json
import pathlib
from typing import Any
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"
ORG_NAME = "TEST_ORG"


def default_recipe(tmp_path, output_file_name="snaplogic_mces_default_config.json"):
    return {
        "run_id": "test-snaplogic",
        "pipeline_name": "snaplogic_ingest",
        "source": {
            "type": "snaplogic",
            "config": {
                "username": "example@snaplogic.com",
                "password": "dummy_password",
                "base_url": "https://elastic.snaplogic.com",
                "org_name": ORG_NAME,
                "stateful_ingestion": {
                    "enabled": False,
                    "remove_stale_metadata": False,
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


def register_mock_api(pytestconfig: Any, request_mock: Any) -> None:
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/snaplogic"
    )

    # Load the mock response from snaplogic_base_response.json
    with open(test_resources_dir / "snaplogic_base_response.json", "r") as f:
        snaplogic_response = json.load(f)

    api_vs_response = {
        f"https://elastic.snaplogic.com/api/1/rest/public/catalog/{ORG_NAME}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": snaplogic_response,
        },
    }

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def run_ingest(
    pytestconfig,
    mock_datahub_graph,
    recipe,
):
    with (
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph

        # Run an azure usage ingestion run.
        pipeline = Pipeline.create(recipe)
        pipeline.run()
        pipeline.raise_from_status()
        return pipeline


@freeze_time(FROZEN_TIME)
def test_snaplogic_source_default_configs(
    pytestconfig, mock_datahub_graph, tmp_path, requests_mock
):
    test_resources_dir: pathlib.Path = (
        pytestconfig.rootpath / "tests/integration/snaplogic"
    )
    register_mock_api(pytestconfig, requests_mock)

    run_ingest(
        pytestconfig=pytestconfig,
        mock_datahub_graph=mock_datahub_graph,
        recipe=default_recipe(tmp_path),
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "snaplogic_mces_default_config.json",
        golden_path=test_resources_dir / "snaplogic_base_golden.json",
    )
