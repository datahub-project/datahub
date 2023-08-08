from typing import Any

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


def register_mock_oss_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response = {
        "http://localhost:8000/api/v1/workspaces/list": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "workspaces": [
                    {
                        "workspaceId": "2de688a6-94ee-463a-949b-5a8e808641c0",
                        "customerId": "e6040db8-1f35-490b-b1aa-d1383ac190ba",
                        "email": "shubham.jagtap@gslab.com",
                        "name": "airbyte_workspace",
                        "slug": "airbyte_workspace",
                        "initialSetupComplete": True,
                        "displaySetupWizard": True,
                        "anonymousDataCollection": False,
                        "news": False,
                        "securityUpdates": False,
                        "notifications": [],
                        "notificationSettings": {
                            "sendOnSuccess": {"notificationType": []},
                            "sendOnFailure": {"notificationType": ["customerio"]},
                            "sendOnSyncDisabled": {"notificationType": ["customerio"]},
                            "sendOnSyncDisabledWarning": {
                                "notificationType": ["customerio"]
                            },
                            "sendOnConnectionUpdate": {
                                "notificationType": ["customerio"]
                            },
                            "sendOnConnectionUpdateActionRequired": {
                                "notificationType": ["customerio"]
                            },
                        },
                        "defaultGeography": "auto",
                        "webhookConfigs": [],
                    }
                ]
            },
        },
        "http://localhost:8000/api/v1/connections/list": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "connections": [
                    {
                        "connectionId": "337a8bf3-230f-4928-bb4d-ed510d7a211b",
                        "name": "oss_connection",
                        "namespaceDefinition": "destination",
                        "namespaceFormat": "${SOURCE_NAMESPACE}",
                        "prefix": "oss_",
                        "sourceId": "5fb3d595-6911-4c5f-be59-7098b3685062",
                        "destinationId": "533ad29e-729b-4695-b195-23a3d2a28c2b",
                        "operationIds": ["b1dc8799-b607-416e-869a-c047fd707b7f"],
                        "syncCatalog": {
                            "streams": [
                                {
                                    "stream": {
                                        "name": "employee",
                                        "jsonSchema": {
                                            "type": "object",
                                            "properties": {
                                                "id": {
                                                    "type": "number",
                                                    "airbyte_type": "integer",
                                                },
                                                "age": {
                                                    "type": "number",
                                                    "airbyte_type": "integer",
                                                },
                                                "name": {"type": "string"},
                                            },
                                        },
                                        "supportedSyncModes": [
                                            "full_refresh",
                                            "incremental",
                                        ],
                                        "sourceDefinedCursor": True,
                                        "defaultCursorField": [],
                                        "sourceDefinedPrimaryKey": [["id"]],
                                        "namespace": "public",
                                    },
                                    "config": {
                                        "syncMode": "incremental",
                                        "cursorField": [],
                                        "destinationSyncMode": "append_dedup",
                                        "primaryKey": [["id"]],
                                        "aliasName": "employee",
                                        "selected": True,
                                        "fieldSelectionEnabled": False,
                                    },
                                },
                                {
                                    "stream": {
                                        "name": "company",
                                        "jsonSchema": {
                                            "type": "object",
                                            "properties": {
                                                "id": {
                                                    "type": "number",
                                                    "airbyte_type": "integer",
                                                },
                                                "age": {
                                                    "type": "number",
                                                    "airbyte_type": "integer",
                                                },
                                                "name": {"type": "string"},
                                            },
                                        },
                                        "supportedSyncModes": [
                                            "full_refresh",
                                            "incremental",
                                        ],
                                        "sourceDefinedCursor": True,
                                        "defaultCursorField": [],
                                        "sourceDefinedPrimaryKey": [["id"]],
                                        "namespace": "public",
                                    },
                                    "config": {
                                        "syncMode": "incremental",
                                        "cursorField": [],
                                        "destinationSyncMode": "append_dedup",
                                        "primaryKey": [["id"]],
                                        "aliasName": "company",
                                        "selected": True,
                                        "fieldSelectionEnabled": False,
                                    },
                                },
                            ]
                        },
                        "schedule": {"units": 24, "timeUnit": "hours"},
                        "scheduleType": "basic",
                        "scheduleData": {
                            "basicSchedule": {"timeUnit": "hours", "units": 24}
                        },
                        "status": "active",
                        "sourceCatalogId": "2c353d29-0393-433f-8b56-4b6db6c3b2d5",
                        "geography": "auto",
                        "breakingChange": False,
                        "notifySchemaChanges": False,
                        "notifySchemaChangesByEmail": False,
                        "nonBreakingChangesPreference": "ignore",
                    }
                ]
            },
        },
        "http://localhost:8000/api/v1/sources/get": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "sourceDefinitionId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
                "sourceId": "5fb3d595-6911-4c5f-be59-7098b3685062",
                "workspaceId": "2de688a6-94ee-463a-949b-5a8e808641c0",
                "connectionConfiguration": {
                    "host": "mahmud.db.elephantsql.com",
                    "port": 5432,
                    "schemas": ["public"],
                    "database": "hxwraqld",
                    "password": "**********",
                    "ssl_mode": {"mode": "disable"},
                    "username": "hxwraqld",
                    "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
                    "replication_method": {"method": "Xmin"},
                },
                "name": "Postgres",
                "sourceName": "Postgres",
                "icon": "",
            },
        },
        "http://localhost:8000/api/v1/destinations/get": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "destinationDefinitionId": "25c5221d-dce2-4163-ade9-739ef790f503",
                "destinationId": "533ad29e-729b-4695-b195-23a3d2a28c2b",
                "workspaceId": "2de688a6-94ee-463a-949b-5a8e808641c0",
                "connectionConfiguration": {
                    "ssl": False,
                    "host": "mahmud.db.elephantsql.com",
                    "port": 5432,
                    "schema": "public",
                    "database": "hxwraqld",
                    "password": "**********",
                    "ssl_mode": {"mode": "disable"},
                    "username": "hxwraqld",
                    "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
                },
                "name": "Postgres",
                "destinationName": "Postgres",
                "icon": "",
            },
        },
        "http://localhost:8000/api/v1/jobs/list": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "jobs": [
                    {
                        "job": {
                            "id": 12,
                            "configType": "sync",
                            "configId": "337a8bf3-230f-4928-bb4d-ed510d7a211b",
                            "enabledStreams": [
                                {"name": "employee", "namespace": "public"},
                                {"name": "company", "namespace": "public"},
                            ],
                            "createdAt": 1690890462,
                            "updatedAt": 1690892150,
                            "status": "succeeded",
                        },
                        "attempts": [
                            {
                                "id": 0,
                                "status": "succeeded",
                                "createdAt": 1690890462,
                                "updatedAt": 1690892150,
                                "endedAt": 1690892150,
                                "bytesSynced": 0,
                                "recordsSynced": 0,
                                "totalStats": {
                                    "recordsEmitted": 0,
                                    "bytesEmitted": 0,
                                    "recordsCommitted": 0,
                                },
                                "streamStats": [],
                            }
                        ],
                    },
                    {
                        "job": {
                            "id": 11,
                            "configType": "sync",
                            "configId": "337a8bf3-230f-4928-bb4d-ed510d7a211b",
                            "enabledStreams": [
                                {"name": "employee", "namespace": "public"},
                                {"name": "company", "namespace": "public"},
                            ],
                            "createdAt": 1690804062,
                            "updatedAt": 1690804132,
                            "status": "failed",
                        },
                        "attempts": [
                            {
                                "id": 0,
                                "status": "failed",
                                "createdAt": 1690804062,
                                "updatedAt": 1690804132,
                                "endedAt": 1690804132,
                                "bytesSynced": 0,
                                "recordsSynced": 0,
                                "totalStats": {
                                    "recordsEmitted": 0,
                                    "bytesEmitted": 0,
                                    "recordsCommitted": 0,
                                },
                                "streamStats": [],
                                "failureSummary": {
                                    "failures": [
                                        {
                                            "failureOrigin": "source",
                                            "failureType": "config_error",
                                            "externalMessage": "Message: HikariPool-1 - Connection is not available, request timed out after 10003ms.",
                                            "internalMessage": "io.airbyte.commons.exceptions.ConnectionErrorException: java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 10003ms.",
                                            "stacktrace": "io.airbyte.commons.exceptions.ConnectionErrorException: java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 10003ms.\n\tat io.airbyte.db.jdbc.DefaultJdbcDatabase.getMetaData(DefaultJdbcDatabase.java:84)\n\tat io.airbyte.integrations.source.jdbc.AbstractJdbcSource.createDatabase(AbstractJdbcSource.java:434)\n\tat io.airbyte.integrations.source.postgres.PostgresSource.createDatabase(PostgresSource.java:305)\n\tat io.airbyte.integrations.source.postgres.PostgresSource.createDatabase(PostgresSource.java:127)\n\tat io.airbyte.integrations.source.relationaldb.AbstractDbSource.read(AbstractDbSource.java:145)\n\tat io.airbyte.integrations.base.ssh.SshWrappedSource.read(SshWrappedSource.java:74)\n\tat io.airbyte.integrations.base.IntegrationRunner.runInternal(IntegrationRunner.java:139)\n\tat io.airbyte.integrations.base.IntegrationRunner.run(IntegrationRunner.java:99)\n\tat io.airbyte.integrations.source.postgres.PostgresSource.main(PostgresSource.java:696)\nCaused by: java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 10003ms.\n\tat com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:696)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:181)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:146)\n\tat com.zaxxer.hikari.HikariDataSource.getConnection(HikariDataSource.java:100)\n\tat io.airbyte.db.jdbc.DefaultJdbcDatabase.getMetaData(DefaultJdbcDatabase.java:78)\n\t... 8 more\n",
                                            "timestamp": 1690804079039,
                                        },
                                        {
                                            "failureOrigin": "source",
                                            "externalMessage": "Something went wrong within the source connector",
                                            "internalMessage": "Source process exited with non-zero exit code 1",
                                            "stacktrace": "io.airbyte.workers.internal.exception.SourceException: Source process exited with non-zero exit code 1\n\tat io.airbyte.workers.general.BufferedReplicationWorker.readFromSource(BufferedReplicationWorker.java:306)\n\tat io.airbyte.workers.general.BufferedReplicationWorker.lambda$runAsyncWithHeartbeatCheck$3(BufferedReplicationWorker.java:210)\n\tat java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1804)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)\n\tat java.base/java.lang.Thread.run(Thread.java:1589)\n",
                                            "timestamp": 1690804079631,
                                        },
                                        {
                                            "failureOrigin": "destination",
                                            "externalMessage": "Something went wrong within the destination connector",
                                            "internalMessage": "Destination process exited with non-zero exit code 1",
                                            "stacktrace": "io.airbyte.workers.internal.exception.DestinationException: Destination process exited with non-zero exit code 1\n\tat io.airbyte.workers.general.BufferedReplicationWorker.readFromDestination(BufferedReplicationWorker.java:418)\n\tat io.airbyte.workers.general.BufferedReplicationWorker.lambda$runAsync$2(BufferedReplicationWorker.java:203)\n\tat java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1804)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)\n\tat java.base/java.lang.Thread.run(Thread.java:1589)\n",
                                            "timestamp": 1690804079736,
                                        },
                                    ],
                                    "partialSuccess": False,
                                },
                            }
                        ],
                    },
                    {
                        "job": {
                            "id": 10,
                            "configType": "sync",
                            "configId": "337a8bf3-230f-4928-bb4d-ed510d7a211b",
                            "enabledStreams": [
                                {"name": "employee", "namespace": "public"},
                                {"name": "company", "namespace": "public"},
                            ],
                            "createdAt": 1690795813,
                            "updatedAt": 1690795894,
                            "status": "succeeded",
                        },
                        "attempts": [
                            {
                                "id": 0,
                                "status": "succeeded",
                                "createdAt": 1690795813,
                                "updatedAt": 1690795894,
                                "endedAt": 1690795894,
                                "bytesSynced": 0,
                                "recordsSynced": 0,
                                "totalStats": {
                                    "recordsEmitted": 0,
                                    "bytesEmitted": 0,
                                    "recordsCommitted": 0,
                                },
                                "streamStats": [],
                            }
                        ],
                    },
                    {
                        "job": {
                            "id": 9,
                            "configType": "sync",
                            "configId": "337a8bf3-230f-4928-bb4d-ed510d7a211b",
                            "enabledStreams": [
                                {"name": "employee", "namespace": "public"},
                                {"name": "company", "namespace": "public"},
                            ],
                            "createdAt": 1690785044,
                            "updatedAt": 1690785097,
                            "status": "succeeded",
                        },
                        "attempts": [
                            {
                                "id": 0,
                                "status": "succeeded",
                                "createdAt": 1690785044,
                                "updatedAt": 1690785097,
                                "endedAt": 1690785097,
                                "bytesSynced": 132,
                                "recordsSynced": 4,
                                "totalStats": {
                                    "recordsEmitted": 4,
                                    "bytesEmitted": 132,
                                    "recordsCommitted": 4,
                                },
                                "streamStats": [
                                    {
                                        "streamName": "oss_company",
                                        "stats": {
                                            "recordsEmitted": 2,
                                            "bytesEmitted": 66,
                                            "recordsCommitted": 2,
                                        },
                                    },
                                    {
                                        "streamName": "oss_employee",
                                        "stats": {
                                            "recordsEmitted": 2,
                                            "bytesEmitted": 66,
                                            "recordsCommitted": 2,
                                        },
                                    },
                                ],
                            }
                        ],
                    },
                ],
                "totalJobCount": 4,
            },
        },
    }

    api_vs_response.update(override_data)

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def register_mock_cloud_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response = {
        "https://api.airbyte.com/v1/workspaces": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "data": [
                    {
                        "workspaceId": "5a8212cd-cc3f-4c35-a435-b35db7e00128",
                        "name": "test_workspace",
                        "dataResidency": "auto",
                    }
                ]
            },
        },
        "https://api.airbyte.com/v1/connections?workspaceIds=5a8212cd-cc3f-4c35-a435-b35db7e00128": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "data": [
                    {
                        "connectionId": "9c55843b-d33b-4e1c-964a-ced336eb194a",
                        "name": "postgres_to_postgres",
                        "sourceId": "326a71df-aece-41e4-8b8b-b26ff46305cf",
                        "destinationId": "f7d3caea-37e5-4641-8cb2-68f97283ff6f",
                        "workspaceId": "5a8212cd-cc3f-4c35-a435-b35db7e00128",
                        "status": "inactive",
                        "schedule": {
                            "scheduleType": "basic",
                            "basicTiming": "Every 24 hours",
                        },
                        "dataResidency": "auto",
                        "nonBreakingSchemaUpdatesBehavior": "ignore",
                        "namespaceDefinition": "destination",
                        "namespaceFormat": "${SOURCE_NAMESPACE}",
                        "prefix": "cloud_",
                        "configurations": {
                            "streams": [
                                {
                                    "name": "employee",
                                    "syncMode": "incremental_deduped_history",
                                    "primaryKey": [["id"]],
                                },
                                {
                                    "name": "company",
                                    "syncMode": "incremental_deduped_history",
                                    "primaryKey": [["id"]],
                                },
                            ]
                        },
                    }
                ]
            },
        },
        "https://api.airbyte.com/v1/sources/326a71df-aece-41e4-8b8b-b26ff46305cf": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "sourceId": "326a71df-aece-41e4-8b8b-b26ff46305cf",
                "name": "Postgres",
                "sourceType": "postgres",
                "workspaceId": "5a8212cd-cc3f-4c35-a435-b35db7e00128",
                "configuration": {
                    "host": "mahmud.db.elephantsql.com",
                    "port": 5432,
                    "schemas": ["public"],
                    "database": "hxwraqld",
                    "password": "**********",
                    "ssl_mode": {"mode": "require"},
                    "username": "hxwraqld",
                    "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
                    "replication_method": {"method": "Xmin"},
                },
            },
        },
        "https://api.airbyte.com/v1/destinations/f7d3caea-37e5-4641-8cb2-68f97283ff6f": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "destinationId": "f7d3caea-37e5-4641-8cb2-68f97283ff6f",
                "name": "Postgres",
                "destinationType": "postgres",
                "workspaceId": "5a8212cd-cc3f-4c35-a435-b35db7e00128",
                "configuration": {
                    "host": "mahmud.db.elephantsql.com",
                    "port": 5432,
                    "schema": "public",
                    "database": "hxwraqld",
                    "password": "**********",
                    "ssl_mode": {"mode": "require"},
                    "username": "hxwraqld",
                    "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
                },
            },
        },
        "https://api.airbyte.com/v1/jobs?connectionId=9c55843b-d33b-4e1c-964a-ced336eb194a": {
            "method": "GET",
            "status_code": 200,
            "json": {},
        },
    }

    api_vs_response.update(override_data)

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def default_oss_config():
    return {
        "cloud_deploy": False,
        "api_url": "http://localhost:8000/api",
        "username": "dummyusername",
        "password": "dummy",
    }


def default_cloud_config():
    return {
        "cloud_deploy": True,
        "api_url": "https://api.airbyte.com",
        "api_key": "dummyapikey",
    }


@pytest.mark.integration
def test_airbyte_oss_ingest(pytestconfig, tmp_path, requests_mock):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    register_mock_oss_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/airbyte_oss_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "airbyte-test",
            "source": {
                "type": "airbyte",
                "config": {
                    **default_oss_config(),
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_airbyte_oss_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_airbyte_cloud_ingest(pytestconfig, tmp_path, requests_mock):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    register_mock_cloud_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/airbyte_cloud_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "airbyte-test",
            "source": {
                "type": "airbyte",
                "config": {
                    **default_cloud_config(),
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_airbyte_cloud_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_platform_instance_ingest(pytestconfig, tmp_path, requests_mock):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    register_mock_oss_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/airbyte_platform_instace_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "airbyte-test",
            "source": {
                "type": "airbyte",
                "config": {
                    **default_oss_config(),
                    "platform_instance": "airbyte_oss_platform",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_platform_instace_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_server_to_platform_instance(pytestconfig, tmp_path, requests_mock):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/airbyte"

    register_mock_oss_api(request_mock=requests_mock)

    new_config: dict = {**default_oss_config()}
    new_config["server_to_platform_instance"] = {
        "mahmud.db.elephantsql.com": {
            "platform_instance": "cloud_postgres_instance",
            "env": "DEV",
        }
    }

    output_path: str = f"{tmp_path}/airbyte_server_to_platform_instance_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "airbyte-test",
            "source": {
                "type": "airbyte",
                "config": {
                    **new_config,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_server_to_platform_instance.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_wrong_cloud_api_url_error(pytestconfig, tmp_path, requests_mock):
    """
    Verify if wrong cloud api url provided in recipe then value error should get raised
    """
    register_mock_cloud_api(request_mock=requests_mock)

    new_config: dict = {**default_cloud_config()}
    new_config["api_url"] = "https://api.air.com"

    try:
        Pipeline.create(
            {
                "run_id": "airbyte-test",
                "source": {
                    "type": "airbyte",
                    "config": {
                        **new_config,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/airbyte_mces.json",
                    },
                },
            }
        )
    except Exception as e:
        assert (
            "To fetch metadata from Airbyte Cloud, user must set api_url as 'https://api.airbyte.com' in the recipe."
            in str(e)
        )


@pytest.mark.integration
def test_wrong_oss_api_url_error(pytestconfig, tmp_path, requests_mock):
    """
    Verify if wrong oss api url provided in recipe then value error should get raised
    """
    register_mock_oss_api(request_mock=requests_mock)

    new_config: dict = {**default_oss_config()}
    new_config["api_url"] = "http://localhost/api"

    try:
        Pipeline.create(
            {
                "run_id": "airbyte-test",
                "source": {
                    "type": "airbyte",
                    "config": {
                        **new_config,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/airbyte_mces.json",
                    },
                },
            }
        )
    except Exception as e:
        assert (
            "To fetch metadata from Airbyte OSS, user must set api_url as 'http://<host>:<port>/api' in the recipe."
            in str(e)
        )


@pytest.mark.integration
def test_cloud_auth_config_not_provided_error(pytestconfig, tmp_path, requests_mock):
    """
    Verify if cloud deploy is true and cloud auth configuration not provided then value error should get raised
    """
    register_mock_cloud_api(request_mock=requests_mock)

    new_config: dict = {**default_cloud_config()}
    del new_config["api_key"]

    try:
        Pipeline.create(
            {
                "run_id": "airbyte-test",
                "source": {
                    "type": "airbyte",
                    "config": {
                        **new_config,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/airbyte_mces.json",
                    },
                },
            }
        )
    except Exception as e:
        assert (
            "To fetch metadata from Airbyte Cloud, user must provide api_key in the recipe."
            in str(e)
        )


@pytest.mark.integration
def test_oss_auth_config_not_provided_error(pytestconfig, tmp_path, requests_mock):
    """
    Verify if cloud deploy is false and oss auth configuration not provided then value error should get raised
    """
    register_mock_oss_api(request_mock=requests_mock)

    new_config: dict = {**default_oss_config()}
    del new_config["password"]

    try:
        Pipeline.create(
            {
                "run_id": "airbyte-test",
                "source": {
                    "type": "airbyte",
                    "config": {
                        **new_config,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/airbyte_mces.json",
                    },
                },
            }
        )
    except Exception as e:
        assert (
            "To fetch metadata from Airbyte OSS, user must provide username and password in the recipe."
            in str(e)
        )
