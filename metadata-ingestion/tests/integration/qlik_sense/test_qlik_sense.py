from typing import Any
from unittest.mock import patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


def register_mock_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response = {
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/api-keys": {
            "method": "GET",
            "status_code": 200,
            "json": {},
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/spaces": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "data": [
                    {
                        "id": "659d0e41d1b0ecce6eebc9b1",
                        "type": "shared",
                        "ownerId": "657b5abe656297cec3d8b205",
                        "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                        "name": "test_space",
                        "description": "",
                        "meta": {
                            "actions": ["create", "delete", "read", "update"],
                            "roles": [],
                            "assignableRoles": [
                                "codeveloper",
                                "consumer",
                                "dataconsumer",
                                "facilitator",
                                "producer",
                            ],
                        },
                        "links": {
                            "self": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/spaces/659d0e41d1b0ecce6eebc9b1"
                            },
                            "assignments": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/spaces/659d0e41d1b0ecce6eebc9b1/assignments"
                            },
                        },
                        "createdAt": "2024-01-09T09:13:38.002Z",
                        "createdBy": "657b5abe656297cec3d8b205",
                        "updatedAt": "2024-01-09T09:13:38.002Z",
                    }
                ]
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "data": [
                    {
                        "name": "test_app_1",
                        "thumbnailId": "",
                        "resourceAttributes": {
                            "_resourcetype": "app",
                            "createdDate": "2024-01-09T08:39:28.928Z",
                            "description": "",
                            "dynamicColor": "",
                            "hasSectionAccess": False,
                            "id": "b90c4d4e-0d07-4c24-9458-b17d1492660b",
                            "isDirectQueryMode": False,
                            "lastReloadTime": "2024-01-09T09:03:59.227Z",
                            "modifiedDate": "2024-01-09T09:04:00.100Z",
                            "name": "test_app_1",
                            "originAppId": "",
                            "owner": "auth0|fd95ee6facf82e692d2eac4ccb5ddb18ef05c22a7575fcc4d26d7bc9aefedb4f",
                            "ownerId": "657b5abe656297cec3d8b205",
                            "publishTime": "",
                            "published": False,
                            "spaceId": "",
                            "thumbnail": "",
                            "usage": "ANALYTICS",
                        },
                        "resourceCustomAttributes": None,
                        "resourceUpdatedAt": "2024-01-09T09:04:00Z",
                        "resourceType": "app",
                        "resourceId": "b90c4d4e-0d07-4c24-9458-b17d1492660b",
                        "resourceCreatedAt": "2024-01-09T08:39:28Z",
                        "id": "659d064133e7d51dbcbb5911",
                        "createdAt": "2024-01-09T08:39:29Z",
                        "updatedAt": "2024-01-09T09:04:00Z",
                        "creatorId": "657b5abe656297cec3d8b205",
                        "updaterId": "657b5abe656297cec3d8b205",
                        "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                        "isFavorited": False,
                        "links": {
                            "self": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/659d064133e7d51dbcbb5911"
                            },
                            "collections": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/659d064133e7d51dbcbb5911/collections"
                            },
                            "open": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/sense/app/b90c4d4e-0d07-4c24-9458-b17d1492660b"
                            },
                        },
                        "actions": [
                            "change_owner",
                            "change_space",
                            "create",
                            "create_session_app",
                            "delete",
                            "delete_share",
                            "duplicate",
                            "export",
                            "export_reduced",
                            "exportappdata",
                            "import",
                            "read",
                            "reload",
                            "source",
                            "update",
                        ],
                        "collectionIds": ["659d0c6dad4c8e1b1e1f6ebe"],
                        "meta": {
                            "isFavorited": False,
                            "actions": [
                                "change_owner",
                                "change_space",
                                "create",
                                "create_session_app",
                                "delete",
                                "delete_share",
                                "duplicate",
                                "export",
                                "export_reduced",
                                "exportappdata",
                                "import",
                                "read",
                                "reload",
                                "source",
                                "update",
                            ],
                            "tags": [],
                            "collections": [
                                {
                                    "id": "659d0c6dad4c8e1b1e1f6ebe",
                                    "name": "test_collection",
                                }
                            ],
                        },
                        "ownerId": "657b5abe656297cec3d8b205",
                        "resourceReloadEndTime": "2024-01-09T09:03:59Z",
                        "resourceReloadStatus": "ok",
                        "resourceSize": {"appFile": 196608, "appMemory": 625},
                        "itemViews": {"total": 10, "trend": 0.5, "unique": 2},
                    },
                    {
                        "name": "IPL_Matches_2022.csv",
                        "spaceId": "659d0e41d1b0ecce6eebc9b1",
                        "resourceAttributes": {
                            "appType": "QIX-DF",
                            "dataStoreName": "DataFilesStore",
                            "dataStoreType": "qix-datafiles",
                            "qri": "qdf:qix-datafiles:ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A:sid@659d0e41d1b0ecce6eebc9b1:IPL_Matches_2022.csv",
                            "secureQri": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#cfl7k2dSDQT8_iAQ36wboHaodaoJTC9bE-sc7ZPM6q4",
                            "sourceSystemId": "QIX-DF_6fb35d21-24d0-474b-bf8b-536c6e9dc717",
                            "technicalDescription": "",
                            "technicalName": "IPL_Matches_2022.csv",
                            "type": "DELIMETED",
                            "version": "2",
                        },
                        "resourceCustomAttributes": None,
                        "resourceUpdatedAt": "2024-01-09T18:18:32Z",
                        "resourceType": "dataset",
                        "resourceSubType": "qix-df",
                        "resourceId": "659d8aef92df266ef3aa5a7c",
                        "resourceCreatedAt": "2024-01-09T18:05:35Z",
                        "id": "659d8aef12794f37026cb262",
                        "createdAt": "2024-01-09T18:05:35Z",
                        "updatedAt": "2024-01-09T18:18:32Z",
                        "creatorId": "657b5abe656297cec3d8b205",
                        "updaterId": "657b5abe656297cec3d8b205",
                        "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                        "isFavorited": False,
                        "links": {
                            "self": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/659d8aef12794f37026cb262"
                            },
                            "collections": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/659d8aef12794f37026cb262/collections"
                            },
                        },
                        "actions": [
                            "create",
                            "delete",
                            "list",
                            "profile",
                            "read",
                            "update",
                        ],
                        "collectionIds": [],
                        "meta": {
                            "isFavorited": False,
                            "actions": [
                                "create",
                                "delete",
                                "list",
                                "profile",
                                "read",
                                "update",
                            ],
                            "tags": [],
                            "collections": [],
                        },
                        "ownerId": "657b5abe656297cec3d8b205",
                        "resourceReloadEndTime": "",
                        "resourceReloadStatus": "",
                        "resourceSize": {"appFile": 0, "appMemory": 0},
                        "itemViews": {},
                    },
                    {
                        "name": "IPL_Matches_2022",
                        "spaceId": "659d0e41d1b0ecce6eebc9b1",
                        "thumbnailId": "",
                        "resourceAttributes": {
                            "_resourcetype": "app",
                            "createdDate": "2024-01-09T18:05:36.545Z",
                            "description": "",
                            "dynamicColor": "",
                            "hasSectionAccess": False,
                            "id": "e09a68e7-18c9-461d-b957-043e0c045dcd",
                            "isDirectQueryMode": False,
                            "lastReloadTime": "2024-01-15T11:06:53.070Z",
                            "modifiedDate": "2024-01-15T11:06:54.684Z",
                            "name": "IPL_Matches_2022",
                            "originAppId": "",
                            "owner": "auth0|fd95ee6facf82e692d2eac4ccb5ddb18ef05c22a7575fcc4d26d7bc9aefedb4f",
                            "ownerId": "657b5abe656297cec3d8b205",
                            "publishTime": "",
                            "published": False,
                            "spaceId": "659d0e41d1b0ecce6eebc9b1",
                            "thumbnail": "",
                            "usage": "ANALYTICS",
                        },
                        "resourceCustomAttributes": None,
                        "resourceUpdatedAt": "2024-01-15T11:06:54Z",
                        "resourceType": "app",
                        "resourceId": "e09a68e7-18c9-461d-b957-043e0c045dcd",
                        "resourceCreatedAt": "2024-01-09T18:05:36Z",
                        "id": "659d8af1ef4eadeead3ec0ec",
                        "createdAt": "2024-01-09T18:05:37Z",
                        "updatedAt": "2024-01-15T11:06:54Z",
                        "creatorId": "657b5abe656297cec3d8b205",
                        "updaterId": "657b5abe656297cec3d8b205",
                        "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                        "isFavorited": False,
                        "links": {
                            "self": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/659d8af1ef4eadeead3ec0ec"
                            },
                            "collections": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/659d8af1ef4eadeead3ec0ec/collections"
                            },
                            "open": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/sense/app/e09a68e7-18c9-461d-b957-043e0c045dcd"
                            },
                        },
                        "actions": [
                            "change_owner",
                            "change_space",
                            "create",
                            "delete",
                            "delete_share",
                            "duplicate",
                            "export",
                            "export_reduced",
                            "exportappdata",
                            "import",
                            "read",
                            "reload",
                            "share",
                            "source",
                            "update",
                        ],
                        "collectionIds": [],
                        "meta": {
                            "isFavorited": False,
                            "actions": [
                                "change_owner",
                                "change_space",
                                "create",
                                "delete",
                                "delete_share",
                                "duplicate",
                                "export",
                                "export_reduced",
                                "exportappdata",
                                "import",
                                "read",
                                "reload",
                                "share",
                                "source",
                                "update",
                            ],
                            "tags": [],
                            "collections": [],
                        },
                        "ownerId": "657b5abe656297cec3d8b205",
                        "resourceReloadEndTime": "2024-01-15T11:06:53Z",
                        "resourceReloadStatus": "ok",
                        "resourceSize": {"appFile": 17634, "appMemory": 36315},
                        "itemViews": {"total": 4, "trend": 0.5, "unique": 2},
                    },
                    {
                        "name": "test_tabl",
                        "resourceAttributes": {
                            "appType": "CONNECTION_BASED_DATASET",
                            "dataStoreName": "gbqqri:db:gbq://DU8iuDBF_ZFeSt2FN0x_P7ypp18VBc7gYNFAgDFVoY8#QgUSMctzlGjr47d2tMP35YlS93h78jVECRqtByqxPEE",
                            "dataStoreType": "gbq",
                            "qri": "a4d1b9ef-e629-4943-809a-2713aa3a5345",
                            "secureQri": "qri:db:gbq://DU8iuDBF_ZFeSt2FN0x_P7ypp18VBc7gYNFAgDFVoY8#Nkh5KRvqTwByLVl5up594IBQ2QA99-6N16Ux_O4qUBs",
                            "sourceSystemId": "",
                            "technicalDescription": "",
                            "technicalName": "harshal-playground-306419'.'test_dataset'.'test_table",
                            "type": "CONNECTION_BASED_DATASET",
                            "version": "2",
                        },
                        "resourceCustomAttributes": None,
                        "resourceUpdatedAt": "2024-01-12T13:11:36Z",
                        "resourceType": "dataset",
                        "resourceSubType": "connection_based_dataset",
                        "resourceId": "65a137c849f82a37c625151b",
                        "resourceCreatedAt": "2024-01-12T12:59:52Z",
                        "id": "65a137c8d5a03b02d359624a",
                        "createdAt": "2024-01-12T12:59:52Z",
                        "updatedAt": "2024-01-12T13:11:36Z",
                        "creatorId": "657b5abe656297cec3d8b205",
                        "updaterId": "657b5abe656297cec3d8b205",
                        "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                        "isFavorited": False,
                        "links": {
                            "self": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/65a137c8d5a03b02d359624a"
                            },
                            "collections": {
                                "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items/65a137c8d5a03b02d359624a/collections"
                            },
                        },
                        "actions": [
                            "create",
                            "delete",
                            "list",
                            "profile",
                            "read",
                            "update",
                        ],
                        "collectionIds": [],
                        "meta": {
                            "isFavorited": False,
                            "actions": [
                                "create",
                                "delete",
                                "list",
                                "profile",
                                "read",
                                "update",
                            ],
                            "tags": [],
                            "collections": [],
                        },
                        "ownerId": "657b5abe656297cec3d8b205",
                        "resourceReloadEndTime": "",
                        "resourceReloadStatus": "",
                        "resourceSize": {"appFile": 0, "appMemory": 0},
                        "itemViews": {
                            "total": 2,
                            "trend": 0.3,
                            "unique": 1,
                            "usedBy": 1,
                        },
                    },
                ],
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/data-sets/659d8aef92df266ef3aa5a7c": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "659d8aef92df266ef3aa5a7c",
                "name": "IPL_Matches_2022.csv",
                "description": "",
                "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                "spaceId": "659d0e41d1b0ecce6eebc9b1",
                "ownerId": "657b5abe656297cec3d8b205",
                "createdTime": "2024-01-09T18:05:35.527Z",
                "createdBy": "657b5abe656297cec3d8b205",
                "lastModifiedTime": "2024-01-09T18:18:32.453Z",
                "lastModifiedBy": "657b5abe656297cec3d8b205",
                "version": 2,
                "technicalDescription": "",
                "technicalName": "IPL_Matches_2022.csv",
                "properties": {"ModifiedByProfileService": False},
                "dataAssetInfo": {
                    "id": "659d8aef92df266ef3aa5a7b",
                    "name": "test_space",
                    "dataStoreInfo": {
                        "id": "659d8aef92df266ef3aa5a7a",
                        "name": "DataFilesStore",
                        "type": "qix-datafiles",
                    },
                },
                "operational": {
                    "size": 38168,
                    "rowCount": 74,
                    "lastLoadTime": "2024-01-09T18:05:35.527Z",
                    "logMessage": '{\n  "cloudEventsVersion": "0.1",\n  "source": "com.qlik/qix-datafiles",\n  "contentType": "application/json",\n  "eventID": "48c935d3-499c-4333-96e6-c8922af6e238",\n  "eventType": "com.qlik.datafile.created",\n  "eventTypeVersion": "0.0.1",\n  "eventTime": "2024-01-09T18:05:35.834061Z",\n  "extensions": {\n    "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",\n    "userId": "657b5abe656297cec3d8b205",\n    "header": {\n      "traceparent": [\n        "00-000000000000000063d931098d8f607c-7f2bea16c07c1c3a-01"\n      ]\n    }\n  },\n  "data": {\n    "id": "6fb35d21-24d0-474b-bf8b-536c6e9dc717",\n    "name": "IPL_Matches_2022.csv",\n    "createdDate": "2024-01-09T18:05:35.5279392Z",\n    "modifiedDate": "2024-01-09T18:05:35.828813Z",\n    "createdByUser": "657b5abe656297cec3d8b205",\n    "modifiedByUser": "657b5abe656297cec3d8b205",\n    "ownerId": "657b5abe656297cec3d8b205",\n    "spaceId": "659d0e41d1b0ecce6eebc9b1",\n    "size": 38168,\n    "contentUpdated": true,\n    "isInternal": false,\n    "qri": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#cfl7k2dSDQT8_iAQ36wboHaodaoJTC9bE-sc7ZPM6q4"\n  },\n  "messageTopic": "system-events.datafiles/Created/ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A/6fb35d21-24d0-474b-bf8b-536c6e9dc717"\n}',
                    "status": "com.qlik.datafile.created",
                    "location": "48c935d3-499c-4333-96e6-c8922af6e238",
                    "contentUpdated": True,
                    "lastUpdateTime": "2024-01-09T18:05:35.828Z",
                },
                "schema": {
                    "dataFields": [
                        {
                            "name": "ID",
                            "index": 0,
                            "dataType": {
                                "type": "INTEGER",
                                "properties": {"qType": "U", "qnDec": 0, "qUseThou": 0},
                            },
                            "tags": ["$integer", "$numeric"],
                            "encrypted": False,
                            "primaryKey": False,
                            "sensitive": False,
                            "orphan": False,
                            "nullable": False,
                        },
                        {
                            "name": "City",
                            "index": 1,
                            "dataType": {
                                "type": "STRING",
                                "properties": {"qType": "U", "qnDec": 0, "qUseThou": 0},
                            },
                            "tags": ["$text", "$ascii"],
                            "encrypted": False,
                            "primaryKey": False,
                            "sensitive": False,
                            "orphan": False,
                            "nullable": False,
                        },
                        {
                            "name": "Date",
                            "index": 2,
                            "dataType": {
                                "type": "DATE",
                                "properties": {"qType": "U", "qnDec": 0, "qUseThou": 0},
                            },
                            "tags": ["$integer", "$numeric", "$date", "$timestamp"],
                            "encrypted": False,
                            "primaryKey": False,
                            "sensitive": False,
                            "orphan": False,
                            "nullable": False,
                        },
                        {
                            "name": "Season",
                            "index": 3,
                            "dataType": {
                                "type": "INTEGER",
                                "properties": {"qType": "U", "qnDec": 0, "qUseThou": 0},
                            },
                            "tags": ["$integer", "$numeric"],
                            "encrypted": False,
                            "primaryKey": False,
                            "sensitive": False,
                            "orphan": False,
                            "nullable": False,
                        },
                    ],
                    "loadOptions": {
                        "qDataFormat": {
                            "qType": "CSV",
                            "qLabel": "embedded labels",
                            "qQuote": "msq",
                            "qDelimiter": {
                                "qName": "Comma",
                                "qScriptCode": "','",
                                "qNumber": 44,
                            },
                            "qCodePage": 28591,
                            "qHeaderSize": 0,
                            "qRecordSize": 0,
                            "qTabSize": 0,
                        }
                    },
                    "effectiveDate": "2024-01-09T18:05:39.713Z",
                    "overrideSchemaAnomalies": False,
                },
                "qri": "qdf:qix-datafiles:ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A:sid@659d0e41d1b0ecce6eebc9b1:IPL_Matches_2022.csv",
                "secureQri": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#cfl7k2dSDQT8_iAQ36wboHaodaoJTC9bE-sc7ZPM6q4",
                "type": "DELIMETED",
                "classifications": {
                    "personalInformation": [],
                    "sensitiveInformation": [],
                },
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/data-sets/65a137c849f82a37c625151b": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "65a137c849f82a37c625151b",
                "name": "test_tabl",
                "description": "",
                "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                "ownerId": "657b5abe656297cec3d8b205",
                "createdTime": "2024-01-12T12:59:52.288Z",
                "createdBy": "657b5abe656297cec3d8b205",
                "lastModifiedTime": "2024-01-12T13:11:36.325Z",
                "lastModifiedBy": "657b5abe656297cec3d8b205",
                "version": 2,
                "technicalDescription": "",
                "technicalName": "harshal-playground-306419'.'test_dataset'.'test_table",
                "properties": {"ModifiedByProfileService": False},
                "dataAssetInfo": {
                    "id": "65a137c849f82a37c625151a",
                    "name": "gbqqri:db:gbq://DU8iuDBF_ZFeSt2FN0x_P7ypp18VBc7gYNFAgDFVoY8#QgUSMctzlGjr47d2tMP35YlS93h78jVECRqtByqxPEE",
                    "dataStoreInfo": {
                        "id": "65a137c849f82a37c6251519",
                        "name": "gbqqri:db:gbq://DU8iuDBF_ZFeSt2FN0x_P7ypp18VBc7gYNFAgDFVoY8#QgUSMctzlGjr47d2tMP35YlS93h78jVECRqtByqxPEE",
                        "type": "gbq",
                    },
                },
                "operational": {
                    "rowCount": 1,
                    "contentUpdated": False,
                    "tableConnectionInfo": {
                        "tableName": "test_table",
                        "selectionScript": "[test_table]:\nSELECT name\nFROM `harshal-playground-306419`.`test_dataset`.`test_table`;",
                        "additionalProperties": {
                            "fields": '[{"name":"name","fullName":"name","nativeType":"STRING","nativeFieldInfo":{"dataType":12,"name":"name","nullable":1,"ordinalPostion":1,"scale":0,"size":65535,"typeName":"STRING"},"customProperties":[],"isSelected":true}]',
                            "tableRequestParameters": '[{\\"name\\":\\"database\\",\\"value\\":\\"harshal-playground-306419\\"},{\\"name\\":\\"owner\\",\\"value\\":\\"test_dataset\\"}]',
                        },
                    },
                },
                "schema": {
                    "dataFields": [
                        {
                            "name": "name",
                            "index": 0,
                            "dataType": {
                                "type": "STRING",
                                "properties": {"qType": "A", "qnDec": 0, "qUseThou": 0},
                            },
                            "tags": ["$text", "$ascii"],
                            "encrypted": False,
                            "primaryKey": False,
                            "sensitive": False,
                            "orphan": False,
                            "nullable": False,
                        }
                    ],
                    "loadOptions": {},
                    "effectiveDate": "2024-01-12T12:59:54.522Z",
                    "anomalies": ["$warning-single-text-column"],
                    "overrideSchemaAnomalies": False,
                },
                "qri": "a4d1b9ef-e629-4943-809a-2713aa3a5345",
                "secureQri": "qri:db:gbq://DU8iuDBF_ZFeSt2FN0x_P7ypp18VBc7gYNFAgDFVoY8#Nkh5KRvqTwByLVl5up594IBQ2QA99-6N16Ux_O4qUBs",
                "type": "CONNECTION_BASED_DATASET",
                "classifications": {
                    "personalInformation": [],
                    "sensitiveInformation": [],
                },
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


def mock_websocket_response(*args, **kwargs):
    request = kwargs["request"]
    if request == {
        "jsonrpc": "2.0",
        "id": 1,
        "handle": -1,
        "method": "OpenDoc",
        "params": {"qDocName": "b90c4d4e-0d07-4c24-9458-b17d1492660b"},
    }:
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "qReturn": {
                    "qType": "Doc",
                    "qHandle": 1,
                    "qGenericId": "b90c4d4e-0d07-4c24-9458-b17d1492660b",
                }
            },
            "change": [1],
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 2,
        "handle": 1,
        "method": "GetObjects",
        "params": {"qOptions": {"qTypes": ["sheet"]}},
    }:
        return {
            "jsonrpc": "2.0",
            "id": 2,
            "result": {
                "qList": [
                    {
                        "qInfo": {
                            "qId": "f4f57386-263a-4ec9-b40c-abcd2467f423",
                            "qType": "sheet",
                        },
                        "qMeta": {
                            "title": "New ds sheet",
                            "description": "",
                            "_resourcetype": "app.object",
                            "_objecttype": "sheet",
                            "id": "f4f57386-263a-4ec9-b40c-abcd2467f423",
                            "approved": False,
                            "published": False,
                            "owner": "auth0|fd95ee6facf82e692d2eac4ccb5ddb18ef05c22a7575fcc4d26d7bc9aefedb4f",
                            "ownerId": "657b5abe656297cec3d8b205",
                            "createdDate": "2024-01-15T11:01:49.704Z",
                            "modifiedDate": "2024-01-29T12:23:46.868Z",
                            "publishTime": True,
                            "privileges": [
                                "read",
                                "update",
                                "delete",
                                "publish",
                                "change_owner",
                            ],
                        },
                        "qData": {},
                    },
                ]
            },
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 3,
        "handle": 1,
        "method": "GetObject",
        "params": {"qId": "f4f57386-263a-4ec9-b40c-abcd2467f423"},
    }:
        return {
            "jsonrpc": "2.0",
            "id": 3,
            "result": {
                "qReturn": {
                    "qType": "GenericObject",
                    "qHandle": 2,
                    "qGenericType": "sheet",
                    "qGenericId": "f4f57386-263a-4ec9-b40c-abcd2467f423",
                }
            },
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 4,
        "handle": 2,
        "method": "GetLayout",
        "params": {},
    }:
        return {
            "jsonrpc": "2.0",
            "id": 4,
            "result": {
                "qLayout": {
                    "qInfo": {
                        "qId": "f4f57386-263a-4ec9-b40c-abcd2467f423",
                        "qType": "sheet",
                    },
                    "qMeta": {
                        "title": "New ds sheet",
                        "description": "",
                        "_resourcetype": "app.object",
                        "_objecttype": "sheet",
                        "id": "f4f57386-263a-4ec9-b40c-abcd2467f423",
                        "approved": False,
                        "published": False,
                        "owner": "auth0|fd95ee6facf82e692d2eac4ccb5ddb18ef05c22a7575fcc4d26d7bc9aefedb4f",
                        "ownerId": "657b5abe656297cec3d8b205",
                        "createdDate": "2024-01-15T11:01:49.704Z",
                        "modifiedDate": "2024-01-29T12:23:46.868Z",
                        "publishTime": True,
                        "privileges": [
                            "read",
                            "update",
                            "delete",
                            "publish",
                            "change_owner",
                        ],
                    },
                    "qSelectionInfo": {},
                    "rank": 0,
                    "thumbnail": {"qStaticContentUrl": {}},
                    "columns": 24,
                    "rows": 12,
                    "cells": [
                        {
                            "bounds": {"x": 0, "y": 0, "width": 100, "height": 100},
                            "col": 0,
                            "colspan": 24,
                            "name": "QYUUb",
                            "row": 0,
                            "rowspan": 12,
                            "smartGrid": {
                                "rowIdx": 0,
                                "itemIdx": 0,
                                "item": {"width": 1},
                            },
                            "type": "barchart",
                        }
                    ],
                    "qChildList": {
                        "qItems": [
                            {
                                "qInfo": {"qId": "QYUUb", "qType": "barchart"},
                                "qMeta": {"privileges": ["read", "update", "delete"]},
                                "qData": {"title": ""},
                            }
                        ]
                    },
                    "customRowBase": 12,
                    "gridResolution": "small",
                    "layoutOptions": {"mobileLayout": "LIST", "extendable": False},
                    "gridMode": "simpleEdit",
                }
            },
        }


@pytest.fixture(scope="module")
def mock_websocket_send_request():

    with patch(
        "datahub.ingestion.source.qlik_sense.qlik_api.QlikAPI._websocket_send_request"
    ) as mock_websocket_send_request, patch(
        "datahub.ingestion.source.qlik_sense.qlik_api.create_connection"
    ):
        mock_websocket_send_request.side_effect = mock_websocket_response
        yield mock_websocket_send_request


def default_config():
    return {
        "tenant_hostname": "iq37k6byr9lgam8.us.qlikcloud.com",
        "api_key": "qlik-api-key",
        "space_pattern": {
            "allow": [
                "test_space",
            ]
        },
        "extract_personal_entity": True,
    }


@pytest.mark.integration
def test_qlik_sense_ingest(
    pytestconfig, tmp_path, requests_mock, mock_websocket_send_request
):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/qlik_sense"

    register_mock_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/qlik_sense_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "qlik-sense-test",
            "source": {
                "type": "qlik-sense",
                "config": {
                    **default_config(),
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
    golden_file = "golden_test_qlik_sense_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_platform_instance_ingest(
    pytestconfig, tmp_path, requests_mock, mock_websocket_send_request
):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/qlik_sense"

    register_mock_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/qlik_platform_instace_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "qlik-sense-test",
            "source": {
                "type": "qlik-sense",
                "config": {
                    **default_config(),
                    "platform_instance": "qlik_sense_platform",
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
