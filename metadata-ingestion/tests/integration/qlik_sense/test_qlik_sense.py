from typing import Any, Dict
from unittest.mock import patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


def register_mock_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response: Dict[str, Dict] = {
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
                ],
                "links": {
                    "self": {
                        "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/spaces"
                    }
                },
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "data": [
                    {
                        "name": "test_app",
                        "spaceId": "659d0e41d1b0ecce6eebc9b1",
                        "thumbnailId": "",
                        "resourceAttributes": {
                            "_resourcetype": "app",
                            "createdDate": "2024-01-25T17:52:24.922Z",
                            "description": "",
                            "dynamicColor": "",
                            "hasSectionAccess": False,
                            "id": "f0714ca7-7093-49e4-8b58-47bb38563647",
                            "isDirectQueryMode": False,
                            "lastReloadTime": "2024-01-25T17:56:00.902Z",
                            "modifiedDate": "2024-01-25T17:56:02.045Z",
                            "name": "test_app",
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
                        "resourceUpdatedAt": "2024-01-25T17:56:02Z",
                        "resourceType": "app",
                        "resourceId": "f0714ca7-7093-49e4-8b58-47bb38563647",
                        "resourceCreatedAt": "2024-01-25T17:52:24Z",
                        "id": "65b29fd9435c545ed042a807",
                        "createdAt": "2024-01-25T17:52:25Z",
                        "updatedAt": "2024-01-25T17:56:02Z",
                        "creatorId": "657b5abe656297cec3d8b205",
                        "updaterId": "657b5abe656297cec3d8b205",
                        "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                        "isFavorited": False,
                        "collectionIds": [],
                        "meta": {
                            "isFavorited": False,
                            "tags": [
                                {"id": "659ce561640a2affcf0d629f", "name": "test_tag"}
                            ],
                            "collections": [],
                        },
                        "ownerId": "657b5abe656297cec3d8b205",
                        "resourceReloadEndTime": "2024-01-25T17:56:00Z",
                        "resourceReloadStatus": "ok",
                        "resourceSize": {"appFile": 2057, "appMemory": 78},
                        "itemViews": {"total": 3, "trend": 0.3, "unique": 1},
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
                        "collectionIds": [],
                        "meta": {
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
                        "collectionIds": [],
                        "meta": {
                            "tags": [],
                            "collections": [],
                        },
                        "ownerId": "657b5abe656297cec3d8b205",
                        "resourceReloadEndTime": "",
                        "resourceReloadStatus": "",
                        "resourceSize": {"appFile": 0, "appMemory": 0},
                    },
                ],
                "links": {
                    "self": {
                        "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/items"
                    }
                },
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/users/657b5abe656297cec3d8b205": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "657b5abe656297cec3d8b205",
                "tenantId": "ysA4KqhDrbdy36hO9wwo4HUvPxeaKT7A",
                "status": "active",
                "subject": "auth0|fd95ee6facf82e692d2eac4ccb5ddb18ef05c22a7575fcc4d26d7bc9aefedb4f",
                "name": "john doe",
                "email": "john.doe@example.com",
                "roles": [
                    "TenantAdmin",
                    "AnalyticsAdmin",
                    "AuditAdmin",
                    "DataAdmin",
                    "Developer",
                    "SharedSpaceCreator",
                    "PrivateAnalyticsContentCreator",
                    "DataServicesContributor",
                ],
                "groups": [],
                "createdAt": "2023-12-14T19:42:54.417Z",
                "lastUpdatedAt": "2024-01-25T06:28:22.629Z",
                "created": "2023-12-14T19:42:54.417Z",
                "lastUpdated": "2024-01-25T06:28:22.629Z",
                "links": {
                    "self": {
                        "href": "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/users/657b5abe656297cec3d8b205"
                    }
                },
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
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/lineage-graphs/nodes/qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647/actions/expand?node=qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647&level=TABLE": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "graph": {
                    "id": "",
                    "directed": True,
                    "type": "TABLE",
                    "label": "Expansion for qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647",
                    "nodes": {
                        "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc": {
                            "label": "IPL_Matches_2022",
                            "metadata": {"fields": 1, "type": "TABLE"},
                        },
                        "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE": {
                            "label": "test_table",
                            "metadata": {"fields": 1, "type": "TABLE"},
                        },
                    },
                    "edges": [
                        {
                            "relation": "from",
                            "source": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#t91dFUED_ebvKHOjauxSOOCnlRZQTwEkNHv_bjUl7AY#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc",
                            "target": "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc",
                        },
                    ],
                    "metadata": {},
                }
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/lineage-graphs/nodes/qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647/actions/expand?node=qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647%23FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc&level=FIELD": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "graph": {
                    "id": "",
                    "directed": True,
                    "type": "FIELD",
                    "label": "Expansion for qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc",
                    "nodes": {
                        "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc#-NAdo6895jVLliOXp13Do7e1kDNDDPwxZ0CDuGFJb8A": {
                            "label": "City",
                            "metadata": {"type": "FIELD"},
                        },
                        "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc#3DAdoo7e1kDNDDPwxZ0CD3Do7e1kDNDDPwxZ0CDuGFJ": {
                            "label": "Date",
                            "metadata": {"type": "FIELD"},
                        },
                    },
                    "edges": [
                        {
                            "relation": "from",
                            "source": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#fpA_oxHhnvRu-RrTC_INPo-TEykis1Y_6e-Qy0rKy2I#ti1Pyd82WD92n5Mafl23Gs1Pr_pwJ1tyFIWtaTJfslc#BcCb_mytkkVA-HSHbFLRYOHFY-O_55m8X_JG7DNzMNE",
                            "target": "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc#-NAdo6895jVLliOXp13Do7e1kDNDDPwxZ0CDuGFJb8A",
                        },
                        {
                            "relation": "from",
                            "source": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#fpA_oxHhnvRu-RrTC_INPo-TEykis1Y_6e-Qy0rKy2I#ti1Pyd82WD92n5Mafl23Gs1Pr_pwJ1tyFIWtaTJfslc#BcCb_mytkkVA-HSHbFLRYOHFY-O_58X8X_JG7DNzMNE",
                            "target": "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc#3DAdoo7e1kDNDDPwxZ0CD3Do7e1kDNDDPwxZ0CDuGFJ",
                        },
                    ],
                    "metadata": {},
                }
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/lineage-graphs/nodes/qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647/actions/expand?node=qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647%23Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE&level=FIELD": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "graph": {
                    "id": "",
                    "directed": True,
                    "type": "FIELD",
                    "label": "Expansion for qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE",
                    "nodes": {
                        "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE#gqNTf_Dbzn7sNdae3DoYnubxfYLzU6VT-aqWywvjzok": {
                            "label": "name",
                            "metadata": {"type": "FIELD"},
                        }
                    },
                    "edges": [
                        {
                            "relation": "read",
                            "source": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#JOKG8u7CvizvGXwrFsyXRU0yKr2rL2WFD5djpH9bj5Q#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE#gqNTf_Dbzn7sNdae3DoYnubxfYLzU6VT-aqWywvjzok",
                            "target": "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE#gqNTf_Dbzn7sNdae3DoYnubxfYLzU6VT-aqWywvjzok",
                        }
                    ],
                    "metadata": {},
                }
            },
        },
        "https://iq37k6byr9lgam8.us.qlikcloud.com/api/v1/lineage-graphs/nodes/qri%3Aapp%3Asense%3A%2F%2Ff0714ca7-7093-49e4-8b58-47bb38563647/overview": {
            "method": "POST",
            "response_list": [
                {
                    "status_code": 200,
                    "json": {
                        "resources": [
                            {
                                "qri": "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc#-NAdo6895jVLliOXp13Do7e1kDNDDPwxZ0CDuGFJb8A",
                                "lineage": [
                                    {
                                        "resourceLabel": "d7a6c03238b0c25b3d5c8631e1121807.qvd",
                                        "resourceQRI": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#_s2Ws5RVxAArzAZlmghwjHNGq8ZnjagxptAl6jlZOaw",
                                        "tableQRI": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#_s2Ws5RVxAArzAZlmghwjHNGq8ZnjagxptAl6jlZOaw#FcJ-H2TvmAyI--l6fn0VQGPtHf8kB2rj7sj0_ysRHgc",
                                        "tableLabel": "IPL_Matches_2022",
                                    },
                                ],
                            }
                        ]
                    },
                },
                {
                    "status_code": 200,
                    "json": {
                        "resources": [
                            {
                                "qri": "qri:app:sense://f0714ca7-7093-49e4-8b58-47bb38563647#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE#gqNTf_Dbzn7sNdae3DoYnubxfYLzU6VT-aqWywvjzok",
                                "lineage": [
                                    {
                                        "resourceLabel": "3fcbce17de9e55774ddedc345532c419.qvd",
                                        "resourceQRI": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#JOKG8u7CvizvGXwrFsyXRU0yKr2rL2WFD5djpH9bj5Q",
                                        "tableQRI": "qri:qdf:space://Ebw1EudUywmUi8p2bM7COr5OATHzuYxvT0BIrCc2irU#JOKG8u7CvizvGXwrFsyXRU0yKr2rL2WFD5djpH9bj5Q#Rrg6-1CeRbo4ews9o--QUP3tOXhm5moLizGY6_wCxJE",
                                        "tableLabel": "test_table",
                                    }
                                ],
                            }
                        ]
                    },
                },
            ],
        },
    }

    api_vs_response.update(override_data)

    for url in api_vs_response.keys():
        if api_vs_response[url].get("response_list"):
            request_mock.register_uri(
                api_vs_response[url]["method"],
                url,
                response_list=api_vs_response[url]["response_list"],
            )
        else:
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
        "params": {"qDocName": "f0714ca7-7093-49e4-8b58-47bb38563647"},
    }:
        return {
            "qReturn": {
                "qType": "Doc",
                "qHandle": 1,
                "qGenericId": "f0714ca7-7093-49e4-8b58-47bb38563647",
            }
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 2,
        "handle": 1,
        "method": "GetAppLayout",
        "params": {},
    }:
        return {
            "qLayout": {
                "qTitle": "IPL_Matches_2022",
                "qFileName": "f0714ca7-7093-49e4-8b58-47bb38563647",
                "qLastReloadTime": "2024-01-15T11:06:53.070Z",
                "qHasScript": True,
                "qStateNames": [],
                "qMeta": {},
                "qHasData": True,
                "qThumbnail": {},
                "qUnsupportedFeatures": [],
                "qUsage": "ANALYTICS",
                "encrypted": True,
                "id": "f0714ca7-7093-49e4-8b58-47bb38563647",
                "published": False,
                "owner": "auth0|fd95ee6facf82e692d2eac4ccb5ddb18ef05c22a7575fcc4d26d7bc9aefedb4f",
                "ownerId": "657b5abe656297cec3d8b205",
                "createdDate": "2024-01-09T18:05:36.545Z",
                "modifiedDate": "2024-01-15T11:07:00.333Z",
                "spaceId": "659d0e41d1b0ecce6eebc9b1",
                "hassectionaccess": False,
                "_resourcetype": "app",
                "privileges": [
                    "read",
                    "update",
                    "delete",
                    "reload",
                    "export",
                    "duplicate",
                    "change_owner",
                    "change_space",
                    "export_reduced",
                    "source",
                    "exportappdata",
                ],
            }
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 3,
        "handle": 1,
        "method": "GetObjects",
        "params": {"qOptions": {"qTypes": ["sheet"]}},
    }:
        return {
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
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 4,
        "handle": 1,
        "method": "GetObject",
        "params": {"qId": "f4f57386-263a-4ec9-b40c-abcd2467f423"},
    }:
        return {
            "qReturn": {
                "qType": "GenericObject",
                "qHandle": 2,
                "qGenericType": "sheet",
                "qGenericId": "f4f57386-263a-4ec9-b40c-abcd2467f423",
            }
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 5,
        "handle": 2,
        "method": "GetLayout",
        "params": {},
    }:
        return {
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
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 6,
        "handle": 2,
        "method": "GetChild",
        "params": {"qId": "QYUUb"},
    }:
        return {
            "qReturn": {
                "qType": "GenericObject",
                "qHandle": 3,
                "qGenericType": "scatterplot",
                "qGenericId": "QYUUb",
            }
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 7,
        "handle": 3,
        "method": "GetLayout",
        "params": {},
    }:
        return {
            "qLayout": {
                "qInfo": {"qId": "QYUUb", "qType": "scatterplot"},
                "qMeta": {"privileges": ["read", "update", "delete"]},
                "qSelectionInfo": {},
                "qHyperCube": {
                    "qSize": {"qcx": 3, "qcy": 5},
                    "qDimensionInfo": [
                        {
                            "qFallbackTitle": "City",
                            "qApprMaxGlyphCount": 11,
                            "qCardinal": 5,
                            "qSortIndicator": "A",
                            "qGroupFallbackTitles": ["City"],
                            "qGroupPos": 0,
                            "qStateCounts": {
                                "qLocked": 0,
                                "qSelected": 0,
                                "qOption": 5,
                                "qDeselected": 0,
                                "qAlternative": 0,
                                "qExcluded": 0,
                                "qSelectedExcluded": 0,
                                "qLockedExcluded": 0,
                            },
                            "qTags": [
                                "$ascii",
                                "$text",
                                "$geoname",
                                "$relates_IPL_Matches_2022.City_GeoInfo",
                            ],
                            "qDimensionType": "D",
                            "qGrouping": "N",
                            "qNumFormat": {"qType": "U", "qnDec": 0, "qUseThou": 0},
                            "qIsAutoFormat": True,
                            "qGroupFieldDefs": ["City"],
                            "qMin": "NaN",
                            "qMax": "NaN",
                            "qAttrExprInfo": [],
                            "qAttrDimInfo": [],
                            "qCardinalities": {
                                "qCardinal": 5,
                                "qHypercubeCardinal": 5,
                                "qAllValuesCardinal": -1,
                            },
                            "autoSort": True,
                            "cId": "FdErA",
                            "othersLabel": "Others",
                        }
                    ],
                    "qMeasureInfo": [
                        {
                            "qFallbackTitle": "Sum(Date)",
                            "qApprMaxGlyphCount": 7,
                            "qCardinal": 0,
                            "qSortIndicator": "D",
                            "qNumFormat": {"qType": "U", "qnDec": 0, "qUseThou": 0},
                            "qMin": 89411,
                            "qMax": 2144260,
                            "qIsAutoFormat": True,
                            "qAttrExprInfo": [],
                            "qAttrDimInfo": [],
                            "qTrendLines": [],
                            "autoSort": True,
                            "cId": "PtTpyG",
                            "numFormatFromTemplate": True,
                        },
                    ],
                },
                "script": "",
                "showTitles": True,
                "title": "Test_chart",
                "subtitle": "",
                "footnote": "",
                "disableNavMenu": False,
                "showDetails": True,
                "showDetailsExpression": False,
                "visualization": "scatterplot",
            }
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 8,
        "handle": 1,
        "method": "GetObject",
        "params": ["LoadModel"],
    }:
        return {
            "qReturn": {
                "qType": "GenericObject",
                "qHandle": 4,
                "qGenericType": "LoadModel",
                "qGenericId": "LoadModel",
            }
        }
    elif request == {
        "jsonrpc": "2.0",
        "id": 9,
        "handle": 4,
        "method": "GetLayout",
        "params": {},
    }:
        return {
            "qLayout": {
                "qInfo": {"qId": "LoadModel", "qType": "LoadModel"},
                "tables": [
                    {
                        "dataconnectorName": "Google_BigQuery_harshal-playground-306419",
                        "dataconnectorPrefix": "test_space:",
                        "boxType": "blackbox",
                        "databaseName": "",
                        "ownerName": "",
                        "tableName": "test_table",
                        "tableAlias": "test_table",
                        "loadProperties": {
                            "filterInfo": {"filterClause": "", "filterType": 1}
                        },
                        "key": "Google_BigQuery_harshal-playground-306419:::test_table",
                        "fields": [
                            {
                                "alias": "name",
                                "name": "name",
                                "selected": True,
                                "checked": True,
                                "id": "dsd.test_table.name",
                            }
                        ],
                        "connectionInfo": {
                            "name": "Google_BigQuery_harshal-playground-306419",
                            "displayName": "Google_BigQuery_harshal-playground-306419",
                            "id": "bb5be407-d3d3-4f19-858c-e71d593f09ae",
                            "type": {
                                "provider": "QvOdbcConnectorPackage.exe",
                                "type": "custom",
                                "name": "QvOdbcConnectorPackage",
                                "displayName": "Qlik® ODBC Connector Package",
                                "isStandardConnector": False,
                                "isIframeCompatible": True,
                                "needsConnect": True,
                                "connectDialog": "/customdata/64/QvOdbcConnectorPackage/web/standalone/connect-dialog.html?locale=en-US",
                                "selectDialog": "/customdata/64/QvOdbcConnectorPackage/web/standalone/select-dialog.html?locale=en-US",
                                "selectAddData": "/customdata/64/QvOdbcConnectorPackage/web/standalone/select-adddata.html?locale=en-US",
                                "credentialsDialog": "/customdata/64/QvOdbcConnectorPackage/web/standalone/credentials-dialog.html?locale=en-US",
                                "update": "/customdata/64/QvOdbcConnectorPackage/web/standalone/loadModelUpdate.js",
                                "architecture": {"text": "Common.undefinedbit"},
                                "connectorMain": "QvOdbcConnectorPackage.webroot/connector-main-iframe",
                            },
                            "typeName": "QvOdbcConnectorPackage.exe",
                            "privileges": [
                                "change_owner",
                                "change_space",
                                "delete",
                                "list",
                                "read",
                                "update",
                            ],
                            "sourceConnectorID": "gbq",
                            "dataconnectorPrefix": "test_space:",
                            "isInAppSpace": True,
                            "space": "659d0e41d1b0ecce6eebc9b1",
                            "connectionString": 'CUSTOM CONNECT TO "provider=QvOdbcConnectorPackage.exe;driver=gbq;OAuthMechanism=0;SupportOldClient=true;Catalog_Old=harshal-playground-306419;separateCredentials=false;Catalog=harshal-playground-306419;Min_TLS=1.2;SQLDialect=1;RowsFetchedPerBlock=16384;DefaultStringColumnLength=65535;AllowLargeResults=false;EnableHTAPI=false;HTAPI_MinResultsSize=1000;HTAPI_MinActivationRatio=3;allowNonSelectQueries=false;QueryTimeout=30;Timeout=300;useBulkReader=true;bulkFetchSize=50;rowBatchSize=1;bulkFetchColumnMode=true;maxStringLength=4096;logSQLStatements=false;"',
                            "hasEditableSeparatedCredentials": False,
                            "canDelete": True,
                            "connectorImagePath": "https://iq37k6byr9lgam8.us.qlikcloud.com/customdata/64/QvOdbcConnectorPackage/web/gbq-square.png",
                            "connectorDisplayName": "Google BigQuery",
                            "dbInfo": {},
                        },
                        "id": "dsd.test_table",
                        "tableGroupId": "",
                        "connectorProperties": {
                            "tableQualifiers": [
                                "harshal-playground-306419",
                                "test_dataset",
                            ],
                        },
                        "selectStatement": "SELECT name\nFROM `harshal-playground-306419`.`test_dataset`.`test_table`;",
                        "caching": {"enabled": True, "type": "qvd"},
                    },
                    {
                        "dataconnectorName": "DataFiles",
                        "dataconnectorPrefix": "test_space:",
                        "boxType": "load-file",
                        "databaseName": "IPL_Matches_2022.csv",
                        "ownerName": "TYPE9_CSV",
                        "tableName": "IPL_Matches_2022",
                        "tableAlias": "IPL_Matches_2022",
                        "key": "DataFiles:IPL_Matches_2022.csv:TYPE9_CSV:IPL_Matches_2022",
                        "fields": [
                            {
                                "id": "dsd.IPL_Matches_2022.City",
                                "name": "City",
                                "alias": "City",
                                "selected": True,
                            },
                            {
                                "id": "dsd.IPL_Matches_2022.Date",
                                "name": "Date",
                                "alias": "Date",
                                "selected": True,
                            },
                        ],
                        "connectionInfo": {
                            "type": {"isStorageProvider": False},
                            "id": "87d7bc7e-77d8-40dc-a251-3a35ec107b4e",
                            "name": "DataFiles",
                            "typeName": "qix-datafiles.exe",
                            "sourceConnectorID": "qix-datafiles.exe",
                            "connectionString": 'CUSTOM CONNECT TO "provider=qix-datafiles.exe;path=test_space:datafiles;"',
                            "space": "659d0e41d1b0ecce6eebc9b1",
                            "dataconnectorPrefix": "test_space:",
                            "caching": {"enabled": True, "type": "qvd"},
                        },
                        "id": "dsd.IPL_Matches_2022",
                        "loadSelectStatement": "LOAD [ID] AS [ID],\n\t[City] AS [City],\n\t[Date] AS [Date],\n\t[Season] AS [Season],\n\t[MatchNumber] AS [MatchNumber],\n\t[Team1] AS [Team1],\n\t[Team2] AS [Team2],\n\t[Venue] AS [Venue],\n\t[TossWinner] AS [TossWinner],\n\t[TossDecision] AS [TossDecision],\n\t[SuperOver] AS [SuperOver],\n\t[WinningTeam] AS [WinningTeam],\n\t[WonBy] AS [WonBy],\n\t[Margin] AS [Margin],\n\t[method] AS [method],\n\t[Player_of_Match] AS [Player_of_Match],\n\t[Team1Players] AS [Team1Players],\n\t[Team2Players] AS [Team2Players],\n\t[Umpire1] AS [Umpire1],\n\t[Umpire2] AS [Umpire2]\nFROM [lib://DataFiles/IPL_Matches_2022.csv]\n(txt, codepage is 28591, embedded labels, delimiter is ',', msq);\n",
                        "formatSpec": "(txt, codepage is 28591, embedded labels, delimiter is ',', msq)",
                        "caching": {"enabled": True, "type": "qvd"},
                    },
                ],
                "schemaVersion": 2.1,
            }
        }
    else:
        return {}


@pytest.fixture(scope="module")
def mock_websocket_send_request():
    with patch(
        "datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection._send_request"
    ) as mock_websocket_send_request, patch(
        "datahub.ingestion.source.qlik_sense.websocket_connection.create_connection"
    ):
        mock_websocket_send_request.side_effect = mock_websocket_response
        yield mock_websocket_send_request


def default_config():
    return {
        "tenant_hostname": "iq37k6byr9lgam8.us.qlikcloud.com",
        "api_key": "qlik-api-key",
        "space_pattern": {"allow": ["test_space", "personal_space"]},
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
                    "data_connection_to_platform_instance": {
                        "Google_BigQuery_harshal-playground-306419": {
                            "platform_instance": "google-cloud",
                            "env": "DEV",
                        }
                    },
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
    golden_file = "golden_test_platform_instance_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )
