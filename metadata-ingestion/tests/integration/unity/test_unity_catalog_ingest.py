from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline


FROZEN_TIME = "2021-12-07 07:00:00"


def register_mock_api(request_mock):
    api_vs_response = {
        "https://dummy/": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "value": [
                ]
            },
        },
    }

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def register_mock_data(unity_catalog_api_instance):
    unity_catalog_api_instance.list_metastores.return_value = {
        "metastores": [
            {
                "name": "production",
                "metastore_id": "9bc05fa3-b2ff-4464-b7ea-31d085caa2d4"
            }
        ]
    }

    unity_catalog_api_instance.list_catalogs.return_value = {
        "catalogs": [
            {
                "name": "hive_catalog",
                "metastore_id": "9bc05fa3-b2ff-4464-b7ea-31d085caa2d4",
            },
            {
                "name": "main_catalog",
                "metastore_id": "9bc05fa3-b2ff-4464-b7ea-31d085caa2c5",
            }
        ]
    }

    unity_catalog_api_instance.list_schemas.return_value = {
        "schemas": [
            {
                "name": "public_schema"
            }
        ]
    }


@freeze_time(FROZEN_TIME)
def test_ingestion(pytestconfig, tmp_path, requests_mock):

    register_mock_api(request_mock=requests_mock)

    output_file_name = "unity_catalog_mcps.json"
    with mock.patch("databricks_cli.unity_catalog.api.UnityCatalogApi") as UnityCatalogApi:
        unity_catalog_api_instance: mock.MagicMock = mock.MagicMock()
        UnityCatalogApi.return_value = unity_catalog_api_instance
        register_mock_data(unity_catalog_api_instance)
        pipeline = Pipeline.create(
            {
                "run_id": "unity-catalog-test",
                "pipeline_name": "unity-catalog-test-pipeline",
                "source": {
                    "type": "unity-catalog",
                    "config": {
                        "workspace_url": "https://dummy",
                        "token": "fake",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"/tmp/{output_file_name}",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

