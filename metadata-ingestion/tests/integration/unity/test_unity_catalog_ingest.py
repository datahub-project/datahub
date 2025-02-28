import uuid
from collections import namedtuple
from unittest import mock
from unittest.mock import patch

import databricks
import pytest
from databricks.sdk.service.catalog import (
    CatalogInfo,
    GetMetastoreSummaryResponse,
    SchemaInfo,
)
from databricks.sdk.service.iam import ServicePrincipal
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.unity.hive_metastore_proxy import HiveMetastoreProxy
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2021-12-07 07:00:00"
SERVICE_PRINCIPAL_ID_1 = str(uuid.uuid4())
SERVICE_PRINCIPAL_ID_2 = str(uuid.uuid4())

pytestmark = pytest.mark.integration_batch_1


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


def register_mock_data(workspace_client):
    workspace_client.metastores.summary.return_value = GetMetastoreSummaryResponse.from_dict(
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
    )

    workspace_client.catalogs.list.return_value = [
        CatalogInfo.from_dict(d)
        for d in [
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
            }
        ]
    ]

    workspace_client.schemas.list.return_value = [
        SchemaInfo.from_dict(d)
        for d in [
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
    ]

    # Set as function so TableInfo can be patched
    workspace_client.tables.list = lambda *args, **kwargs: [
        databricks.sdk.service.catalog.TableInfo.from_dict(
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
                    "spark.sql.statistics.numRows": "10",
                    "spark.sql.statistics.totalSize": "512",
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
        ),
        databricks.sdk.service.catalog.TableInfo.from_dict(
            {
                "name": "quickstart_table_external",
                "catalog_name": "quickstart_catalog",
                "schema_name": "quickstart_schema",
                "table_type": "EXTERNAL",
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
                    "spark.sql.statistics.numRows": "10",
                    "spark.sql.statistics.totalSize": "512",
                },
                "generation": 2,
                "metastore_id": "2c983545-d403-4f87-9063-5b7e3b6d3736",
                "full_name": "quickstart_catalog.quickstart_schema.quickstart_table_external",
                "data_access_configuration_id": "00000000-0000-0000-0000-000000000000",
                "created_at": 1666185698688,
                "created_by": "abc@acryl.io",
                "updated_at": 1666186049633,
                "updated_by": "abc@acryl.io",
                "table_id": "cff27aa1-1c6a-4d78-b713-562c660c2896",
            }
        ),
    ]

    workspace_client.tables.get = (
        lambda *args, **kwargs: databricks.sdk.service.catalog.TableInfo.from_dict(
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
                    "spark.sql.statistics.numRows": "10",
                    "spark.sql.statistics.totalSize": "512",
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
        )
    )

    workspace_client.service_principals.list.return_value = [
        ServicePrincipal.from_dict(d)
        for d in [
            {
                "displayName": "Service Principal 1",
                "active": True,
                "id": "4940620261358622",
                "applicationId": SERVICE_PRINCIPAL_ID_1,
            },
            {
                "displayName": "Service Principal 2",
                "active": True,
                "id": "8202844266343024",
                "applicationId": SERVICE_PRINCIPAL_ID_2,
            },
        ]
    ]


TableEntry = namedtuple("TableEntry", ["database", "tableName", "isTemporary"])
ViewEntry = namedtuple(
    "ViewEntry", ["namespace", "viewName", "isTemporary", "isMaterialized"]
)


def mock_hive_sql(query):
    if query == "DESCRIBE EXTENDED `bronze_kambi`.`bet` betStatusId":
        return [
            ("col_name", "betStatusId"),
            ("data_type", "bigint"),
            ("comment", None),
            ("min", None),
            ("max", None),
            ("num_nulls", 0),
            ("distinct_count", 1),
            ("avg_col_len", 8),
            ("max_col_len", 8),
            ("histogram", None),
        ]
    elif query == "DESCRIBE EXTENDED `bronze_kambi`.`bet` channelId":
        return [
            ("col_name", "channelId"),
            ("data_type", "bigint"),
            ("comment", None),
            ("min", None),
            ("max", None),
            ("num_nulls", 0),
            ("distinct_count", 1),
            ("avg_col_len", 8),
            ("max_col_len", 8),
            ("histogram", None),
        ]
    elif query == "DESCRIBE EXTENDED `bronze_kambi`.`bet` combination":
        return [
            ("col_name", "combination"),
            (
                "data_type",
                "struct<combinationRef:bigint,currentOdds:double,eachWay:boolean,liveBetting:boolean,odds:double,outcomes:array<struct<betOfferTypeId:bigint,criterionId:bigint,criterionName:string,currentOdds:double,eventGroupId:bigint,eventGroupPath:array<struct<id:bigint,name:string>>,eventId:bigint,eventName:string,eventStartDate:string,live:boolean,odds:double,outcomeIds:array<bigint>,outcomeLabel:string,sportId:string,status:string,voidReason:string>>,payout:double,rewardExtraPayout:double,stake:double>",
            ),
            ("comment", None),
            ("min", None),
            ("max", None),
            ("num_nulls", None),
            ("distinct_count", None),
            ("avg_col_len", None),
            ("max_col_len", None),
            ("histogram", None),
        ]
    elif query == "DESCRIBE EXTENDED `bronze_kambi`.`bet`":
        return [
            ("betStatusId", "bigint", None),
            ("channelId", "bigint", None),
            (
                "combination",
                "struct<combinationRef:bigint,currentOdds:double,eachWay:boolean,liveBetting:boolean,odds:double,outcomes:array<struct<betOfferTypeId:bigint,criterionId:bigint,criterionName:string,currentOdds:double,eventGroupId:bigint,eventGroupPath:array<struct<id:bigint,name:string>>,eventId:bigint,eventName:string,eventStartDate:string,live:boolean,odds:double,outcomeIds:array<bigint>,outcomeLabel:string,sportId:string,status:string,voidReason:string>>,payout:double,rewardExtraPayout:double,stake:double>",
                None,
            ),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Catalog", "hive_metastore", ""),
            ("Database", "bronze_kambi", ""),
            ("Table", "bet", ""),
            ("Created Time", "Wed Jun 22 05:14:56 UTC 2022", ""),
            ("Last Access", "UNKNOWN", ""),
            ("Created By", "Spark 3.2.1", ""),
            ("Statistics", "1024 bytes, 3 rows", ""),
            ("Type", "MANAGED", ""),
            ("Location", "dbfs:/user/hive/warehouse/bronze_kambi.db/bet", ""),
            ("Provider", "delta", ""),
            ("Owner", "root", ""),
            ("Is_managed_location", "true", ""),
            (
                "Table Properties",
                "[delta.autoOptimize.autoCompact=true,delta.autoOptimize.optimizeWrite=true,delta.minReaderVersion=1,delta.minWriterVersion=2]",
                "",
            ),
        ]
    elif query == "DESCRIBE EXTENDED `bronze_kambi`.`external_metastore`":
        return [
            ("betStatusId", "bigint", None),
            ("channelId", "bigint", None),
            (
                "combination",
                "struct<combinationRef:bigint,currentOdds:double,eachWay:boolean,liveBetting:boolean,odds:double,outcomes:array<struct<betOfferTypeId:bigint,criterionId:bigint,criterionName:string,currentOdds:double,eventGroupId:bigint,eventGroupPath:array<struct<id:bigint,name:string>>,eventId:bigint,eventName:string,eventStartDate:string,live:boolean,odds:double,outcomeIds:array<bigint>,outcomeLabel:string,sportId:string,status:string,voidReason:string>>,payout:double,rewardExtraPayout:double,stake:double>",
                None,
            ),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Catalog", "hive_metastore", ""),
            ("Database", "bronze_kambi", ""),
            ("Table", "external_metastore", ""),
            ("Created Time", "Wed Jun 22 05:14:56 UTC 2022", ""),
            ("Last Access", "UNKNOWN", ""),
            ("Created By", "Spark 3.2.1", ""),
            ("Statistics", "1024 bytes, 3 rows", ""),
            ("Type", "EXTERNAL", ""),
            ("Location", "s3://external_metastore/", ""),
            ("Provider", "delta", ""),
            ("Owner", "root", ""),
            ("Is_managed_location", "true", ""),
            (
                "Table Properties",
                "[delta.autoOptimize.autoCompact=true,delta.autoOptimize.optimizeWrite=true,delta.minReaderVersion=1,delta.minWriterVersion=2]",
                "",
            ),
        ]
    elif query == "DESCRIBE EXTENDED `bronze_kambi`.`view1`":
        return [
            ("betStatusId", "bigint", None),
            ("channelId", "bigint", None),
            (
                "combination",
                "struct<combinationRef:bigint,currentOdds:double,eachWay:boolean,liveBetting:boolean,odds:double,outcomes:array<struct<betOfferTypeId:bigint,criterionId:bigint,criterionName:string,currentOdds:double,eventGroupId:bigint,eventGroupPath:array<struct<id:bigint,name:string>>,eventId:bigint,eventName:string,eventStartDate:string,live:boolean,odds:double,outcomeIds:array<bigint>,outcomeLabel:string,sportId:string,status:string,voidReason:string>>,payout:double,rewardExtraPayout:double,stake:double>",
                None,
            ),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Catalog", "hive_metastore", ""),
            ("Database", "bronze_kambi", ""),
            ("Table", "view1", ""),
            ("Created Time", "Wed Jun 22 05:14:56 UTC 2022", ""),
            ("Last Access", "UNKNOWN", ""),
            ("Created By", "Spark 3.2.1", ""),
            ("Type", "VIEW", ""),
            ("Owner", "root", ""),
        ]
    elif query == "DESCRIBE EXTENDED `bronze_kambi`.`delta_error_table`":
        raise Exception(
            "[DELTA_PATH_DOES_NOT_EXIST] doesn't exist, or is not a Delta table."
        )
    elif query == "SHOW CREATE TABLE `bronze_kambi`.`view1`":
        return [
            (
                "CREATE VIEW `hive_metastore`.`bronze_kambi`.`view1` AS SELECT * FROM `hive_metastore`.`bronze_kambi`.`bet`",
            )
        ]
    elif query == "SHOW TABLES FROM `bronze_kambi`":
        return [
            TableEntry("bronze_kambi", "bet", False),
            TableEntry("bronze_kambi", "external_metastore", False),
            TableEntry("bronze_kambi", "delta_error_table", False),
            TableEntry("bronze_kambi", "view1", False),
        ]
    elif query == "SHOW VIEWS FROM `bronze_kambi`":
        return [ViewEntry("bronze_kambi", "view1", False, False)]

    return []


@freeze_time(FROZEN_TIME)
def test_ingestion(pytestconfig, tmp_path, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/unity"

    register_mock_api(request_mock=requests_mock)

    output_file_name = "unity_catalog_mcps.json"

    with patch(
        "datahub.ingestion.source.unity.proxy.WorkspaceClient"
    ) as mock_client, patch.object(
        HiveMetastoreProxy, "get_inspector"
    ) as get_inspector, patch.object(HiveMetastoreProxy, "_execute_sql") as execute_sql:
        workspace_client: mock.MagicMock = mock.MagicMock()
        mock_client.return_value = workspace_client
        register_mock_data(workspace_client)

        inspector = mock.MagicMock()
        inspector.get_schema_names.return_value = ["bronze_kambi"]
        get_inspector.return_value = inspector

        execute_sql.side_effect = mock_hive_sql

        config_dict: dict = {
            "run_id": "unity-catalog-test",
            "pipeline_name": "unity-catalog-test-pipeline",
            "source": {
                "type": "unity-catalog",
                "config": {
                    "workspace_url": "https://dummy.cloud.databricks.com",
                    "token": "fake",
                    "include_ownership": True,
                    "include_hive_metastore": True,
                    "warehouse_id": "test",
                    "emit_siblings": True,
                    "delta_lake_options": {
                        "platform_instance_name": None,
                        "env": "PROD",
                    },
                    "profiling": {
                        "enabled": True,
                        "method": "analyze",
                        "call_analyze": False,
                    },
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
