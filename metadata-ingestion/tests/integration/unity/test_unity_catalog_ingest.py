from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"


def register_mock_api(request_mock):
    api_vs_response = {
        "https://dummy.cloud.databricks.com/": {
            "method": "GET",
            "status_code": 200,
            "json": {"value": []},
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
                "name": "acryl metastore",
                "storage_root": "s3://db-9063-5b7e3b6d3736-s3-root-bucket/metastore/2c983545-d403-4f87-9063-5b7e3b6d3736",
                "default_data_access_config_id": "9a9bacc4-cd82-409f-b55c-8ad1b2bde8da",
                "storage_root_credential_id": "9a9bacc4-cd82-409f-b55c-8ad1b2bde8da",
                "storage_root_credential_name": "2c983545-d403-4f87-9063-5b7e3b6d3736-data-access-config-1666185153576",
                "delta_sharing_scope": "INTERNAL",
                "owner": "abc@acryl.io",
                "privilege_model_version": "1.0",
                "region": "us-west-1",
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "created_at": 1666185153375,
                "created_by": "abc@acryl.io",
                "updated_at": 1666185154797,
                "updated_by": "abc@acryl.io",
                "cloud": "aws",
                "global_metastore_id": "aws:us-west-1:2c983545-d403-4f87-9063-5b7e3b6d3736",
            }
        ]
    }

    unity_catalog_api_instance.list_catalogs.return_value = {
        "catalogs": [
            {
                "name": "main",
                "owner": "account users",
                "comment": "Main catalog (auto-created)",
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "created_at": 1666185153376,
                "created_by": "abc@acryl.io",
                "updated_at": 1666186071115,
                "updated_by": "abc@acryl.io",
                "catalog_type": "MANAGED_CATALOG",
            },
            {
                "name": "quickstart_catalog",
                "owner": "account users",
                "comment": "",
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "created_at": 1666185610019,
                "created_by": "abc@acryl.io",
                "updated_at": 1666186064332,
                "updated_by": "abc@acryl.io",
                "catalog_type": "MANAGED_CATALOG",
            },
            {
                "name": "system",
                "owner": "System user",
                "comment": "System catalog (auto-created)",
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "created_at": 1666185153391,
                "created_by": "System user",
                "updated_at": 1666185153391,
                "updated_by": "System user",
                "catalog_type": "SYSTEM_CATALOG",
            },
        ]
    }

    unity_catalog_api_instance.list_schemas.return_value = {
        "schemas": [
            {
                "name": "default",
                "catalog_name": "quickstart_catalog",
                "owner": "abc@acryl.io",
                "comment": "Default schema (auto-created)",
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "full_name": "quickstart_catalog.default",
                "created_at": 1666185610021,
                "created_by": "abc@acryl.io",
                "updated_at": 1666185610021,
                "updated_by": "abc@acryl.io",
                "catalog_type": "MANAGED_CATALOG",
            },
            {
                "name": "information_schema",
                "catalog_name": "quickstart_catalog",
                "owner": "System user",
                "comment": "Information schema (auto-created)",
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "full_name": "quickstart_catalog.information_schema",
                "created_at": 1666185610024,
                "created_by": "System user",
                "updated_at": 1666185610024,
                "updated_by": "System user",
                "catalog_type": "MANAGED_CATALOG",
            },
            {
                "name": "quickstart_schema",
                "catalog_name": "quickstart_catalog",
                "owner": "account users",
                "comment": "A new Unity Catalog schema called quickstart_schema",
                "properties": {"owner": "root"},
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "full_name": "quickstart_catalog.quickstart_schema",
                "created_at": 1666185645311,
                "created_by": "abc@acryl.io",
                "updated_at": 1666186056973,
                "updated_by": "abc@acryl.io",
                "catalog_type": "MANAGED_CATALOG",
            },
        ]
    }

    unity_catalog_api_instance.list_tables.return_value = {
        "tables": [
            {
                "name": "quickstart_table",
                "catalog_name": "quickstart_catalog",
                "schema_name": "quickstart_schema",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "columns": [
                    {
                        "name": "columnA",
                        "type_text": "int",
                        "type_json": '{"name":"columnA","type":"integer","nullable":true,"metadata":{}}',
                        "type_name": "INT",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 0,
                        "nullable": True,
                    },
                    {
                        "name": "columnB",
                        "type_text": "string",
                        "type_json": '{"name":"columnB","type":"string","nullable":true,"metadata":{}}',
                        "type_name": "STRING",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 1,
                        "nullable": True,
                    },
                ],
                "storage_location": "s3://db-02eec1f70bfe4115445be9fdb1aac6ac-s3-root-bucket/metastore/2c983545-d403-4f87-9063-5b7e3b6d3736/tables/cff27aa1-1c6a-4d78-b713-562c660c2896",
                "owner": "account users",
                "properties": {
                    "delta.lastCommitTimestamp": "1666185711000",
                    "delta.lastUpdateVersion": "1",
                    "delta.minReaderVersion": "1",
                    "delta.minWriterVersion": "2",
                },
                "generation": 2,
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "full_name": "quickstart_catalog.quickstart_schema.quickstart_table",
                "data_access_configuration_id": "00000000-0000-0000-0000-000000000000",
                "created_at": 1666185698688,
                "created_by": "abc@acryl.io",
                "updated_at": 1666186049633,
                "updated_by": "abc@acryl.io",
                "table_id": "cff27aa1-1c6a-4d78-b713-562c660c2896",
            }
        ]
    }


@freeze_time(FROZEN_TIME)
def test_ingestion(pytestconfig, tmp_path, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/unity"

    register_mock_api(request_mock=requests_mock)

    output_file_name = "unity_catalog_mcps.json"

    with mock.patch(
        "databricks_cli.unity_catalog.api.UnityCatalogApi"
    ) as UnityCatalogApi:
        unity_catalog_api_instance: mock.MagicMock = mock.MagicMock()
        UnityCatalogApi.return_value = unity_catalog_api_instance
        register_mock_data(unity_catalog_api_instance)
        config_dict: dict = {
            "run_id": "unity-catalog-test",
            "pipeline_name": "unity-catalog-test-pipeline",
            "source": {
                "type": "unity-catalog",
                "config": {
                    "workspace_url": "https://dummy.cloud.databricks.com",
                    "token": "fake",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"/{tmp_path}/{output_file_name}",
                },
            },
        }
        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        mce_golden_file: str = "unity_catalog_mces_golden.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"/{tmp_path}/{output_file_name}",
            golden_path=f"{test_resources_dir}/{mce_golden_file}",
        )
