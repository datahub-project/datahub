from typing import Any, Dict

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


def register_mock_api(request_mock: Any, override_data: dict = {}) -> None:
    api_vs_response: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/auth/token": {
            "method": "POST",
            "status_code": 200,
            "json": {
                "access_token": "717de8281754fe8e302b1ee69f1c9553faf0331cabd8712f459c",
                "refresh_token": "124de8281754fe8e302b1ee69f1c9553faf0331cabd8712f442v",
                "token_type": "bearer",
                "expires_in": 3599,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workspaces?limit=50": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "name": "Acryl Data",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-03-12T08:31:04.826Z",
                        "updatedAt": "2024-03-12T08:31:04.826Z",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=dataset": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": "8891fd40-5470-4ff2-a74f-6e61ee44d3fc",
                        "urlId": "49HFLTr6xytgrPly3PFsNC",
                        "name": "PETS",
                        "type": "dataset",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "isArchived": False,
                    },
                    {
                        "id": "bd6b86e8-cd4a-4b25-ab65-f258c2a68a8f",
                        "urlId": "5LqGLu14qUnqh3cN6wRJBd",
                        "name": "PET_PROFILES_JOINED_DYNAMIC",
                        "type": "dataset",
                        "parentId": "1b47afdb-db4e-4a2c-9fa4-fc1332f4a097",
                        "parentUrlId": "Ptyl1jrKEO18RDX9y1d4P",
                        "permission": "edit",
                        "path": "Acryl Data/New Folder",
                        "badge": "Deprecated",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:51:08.019Z",
                        "updatedAt": "2024-04-15T13:51:08.019Z",
                        "isArchived": False,
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=workbook": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": "9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b",
                        "urlId": "4JRFW1HThPI1K3YTjouXI7",
                        "name": "Acryl Workbook",
                        "type": "workbook",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": "Warning",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:44:51.477Z",
                        "updatedAt": "2024-04-15T13:51:57.302Z",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/datasets": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "datasetId": "8891fd40-5470-4ff2-a74f-6e61ee44d3fc",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "name": "PETS",
                        "description": "",
                        "url": "https://app.sigmacomputing.com/acryldata/b/49HFLTr6xytgrPly3PFsNC",
                        "isArchived": False,
                    },
                    {
                        "datasetId": "bd6b86e8-cd4a-4b25-ab65-f258c2a68a8f",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:51:08.019Z",
                        "updatedAt": "2024-04-15T13:51:08.019Z",
                        "name": "PET_PROFILES_JOINED_DYNAMIC",
                        "description": "",
                        "url": "https://app.sigmacomputing.com/acryldata/b/5LqGLu14qUnqh3cN6wRJBd",
                        "isArchived": False,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "path": "Acryl Data/New Folder",
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/files/1b47afdb-db4e-4a2c-9fa4-fc1332f4a097": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": "1b47afdb-db4e-4a2c-9fa4-fc1332f4a097",
                "urlId": "Ptyl1jrKEO18RDX9y1d4P",
                "name": "New Folder",
                "type": "folder",
                "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                "permission": "edit",
                "path": "Acryl Data",
                "badge": None,
                "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                "createdAt": "2024-04-15T13:35:39.512Z",
                "updatedAt": "2024-04-15T13:35:39.512Z",
                "isArchived": False,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "workbookId": "9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b",
                        "workbookUrlId": "4JRFW1HThPI1K3YTjouXI7",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:44:51.477Z",
                        "updatedAt": "2024-04-15T13:51:57.302Z",
                        "name": "Acryl Workbook",
                        "url": "https://app.sigmacomputing.com/acryldata/workbook/4JRFW1HThPI1K3YTjouXI7",
                        "path": "Acryl Data",
                        "latestVersion": 2,
                        "isArchived": False,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {"pageId": "OSnGLBzL1i", "name": "Page 1"},
                    {"pageId": "DFSieiAcgo", "name": "Page 2"},
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages/OSnGLBzL1i/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": "kH0MeihtGs",
                        "type": "table",
                        "name": "ADOPTIONS",
                        "columns": [
                            "Pk",
                            "Pet Fk",
                            "Human Fk",
                            "Status",
                            "Created At",
                            "Updated At",
                        ],
                        "vizualizationType": "levelTable",
                    },
                    {
                        "elementId": "Ml9C5ezT5W",
                        "type": "visualization",
                        "name": "Count of Profile Id by Status",
                        "columns": [
                            "Pk",
                            "Profile Id",
                            "Status",
                            "Created At",
                            "Updated At",
                            "Count of Profile Id",
                        ],
                        "vizualizationType": "bar",
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/kH0MeihtGs": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "mnJ7_k2sbt": {
                        "nodeId": "mnJ7_k2sbt",
                        "type": "sheet",
                        "name": "ADOPTIONS",
                        "elementId": "kH0MeihtGs",
                    },
                    "inode-2Fby2MBLPM5jUMfBB15On1": {
                        "nodeId": "inode-2Fby2MBLPM5jUMfBB15On1",
                        "type": "table",
                        "name": "ADOPTIONS",
                    },
                },
                "edges": [
                    {
                        "source": "inode-2Fby2MBLPM5jUMfBB15On1",
                        "target": "mnJ7_k2sbt",
                        "type": "source",
                    }
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/kH0MeihtGs/query": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "elementId": "kH0MeihtGs",
                "name": "ADOPTIONS",
                "sql": 'select PK "Pk", PET_FK "Pet Fk", HUMAN_FK "Human Fk", STATUS "Status", CAST_TIMESTAMP_TO_DATETIME_7 "Created At", CAST_TIMESTAMP_TO_DATETIME_8 "Updated At" from (select PK, PET_FK, HUMAN_FK, STATUS, CREATED_AT::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_7, UPDATED_AT::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_8 from (select * from LONG_TAIL_COMPANIONS.ADOPTION.ADOPTIONS ADOPTIONS limit 1000) Q1) Q2 limit 1000\n\n-- Sigma Σ {"request-id":"3d4bf15e-6a17-4967-ad2a-213341233a04","email":"john.doe@example.com"}',
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/Ml9C5ezT5W": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "qrL7BEq8LR": {
                        "nodeId": "qrL7BEq8LR",
                        "type": "sheet",
                        "name": "Count of Profile Id by Status",
                        "elementId": "Ml9C5ezT5W",
                    },
                    "inode-49HFLTr6xytgrPly3PFsNC": {
                        "nodeId": "inode-49HFLTr6xytgrPly3PFsNC",
                        "type": "dataset",
                        "name": "PETS",
                    },
                },
                "edges": [
                    {
                        "source": "inode-49HFLTr6xytgrPly3PFsNC",
                        "target": "qrL7BEq8LR",
                        "type": "source",
                    }
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/Ml9C5ezT5W/query": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "elementId": "Ml9C5ezT5W",
                "name": "Count of Profile Id by Status",
                "sql": 'with Q1 as (select PK, PROFILE_ID, STATUS, CREATED_AT::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_6, UPDATED_AT::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_7 from LONG_TAIL_COMPANIONS.ADOPTION.PETS PETS) select STATUS_10 "Status", COUNT_23 "Count of Profile Id", PK_8 "Pk", PROFILE_ID_9 "Profile Id", CAST_TIMESTAMP_TO_DATETIME_8 "Created At", CAST_TIMESTAMP_TO_DATETIME_9 "Updated At" from (select Q3.PK_8 PK_8, Q3.PROFILE_ID_9 PROFILE_ID_9, Q3.STATUS_10 STATUS_10, Q3.CAST_TIMESTAMP_TO_DATETIME_8 CAST_TIMESTAMP_TO_DATETIME_8, Q3.CAST_TIMESTAMP_TO_DATETIME_9 CAST_TIMESTAMP_TO_DATETIME_9, Q6.COUNT_23 COUNT_23, Q6.STATUS_11 STATUS_11 from (select PK PK_8, PROFILE_ID PROFILE_ID_9, STATUS STATUS_10, CAST_TIMESTAMP_TO_DATETIME_6 CAST_TIMESTAMP_TO_DATETIME_8, CAST_TIMESTAMP_TO_DATETIME_7 CAST_TIMESTAMP_TO_DATETIME_9 from Q1 Q2 order by STATUS_10 asc limit 1000) Q3 left join (select count(PROFILE_ID_9) COUNT_23, STATUS_10 STATUS_11 from (select PROFILE_ID PROFILE_ID_9, STATUS STATUS_10 from Q1 Q4) Q5 group by STATUS_10) Q6 on equal_null(Q3.STATUS_10, Q6.STATUS_11)) Q8 order by STATUS_10 asc limit 1000\n\n-- Sigma Σ {"request-id":"988dd6b5-0678-4421-ae14-21594c0ee97a","email":"john.doe@example.com"}',
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages/DFSieiAcgo/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": "tQJu5N1l81",
                        "type": "table",
                        "name": "PETS ADOPTIONS JOIN",
                        "columns": [
                            "Pk",
                            "Profile Id",
                            "Status",
                            "Created At",
                            "Updated At",
                            "Pk (ADOPTIONS)",
                            "Pet Fk",
                            "Human Fk",
                            "Status (ADOPTIONS)",
                            "Created At (ADOPTIONS)",
                            "Updated At (ADOPTIONS)",
                        ],
                        "vizualizationType": "levelTable",
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/tQJu5N1l81": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "QGgIlk8PQk": {
                        "nodeId": "QGgIlk8PQk",
                        "type": "sheet",
                        "name": "PETS ADOPTIONS JOIN",
                        "elementId": "tQJu5N1l81",
                    },
                    "2zTHG9wyvZ": {"nodeId": "2zTHG9wyvZ", "type": "join"},
                    "inode-49HFLTr6xytgrPly3PFsNC": {
                        "nodeId": "inode-49HFLTr6xytgrPly3PFsNC",
                        "type": "dataset",
                        "name": "PETS",
                    },
                    "inode-2Fby2MBLPM5jUMfBB15On1": {
                        "nodeId": "inode-2Fby2MBLPM5jUMfBB15On1",
                        "type": "table",
                        "name": "ADOPTIONS",
                    },
                },
                "edges": [
                    {"source": "2zTHG9wyvZ", "target": "QGgIlk8PQk", "type": "source"},
                    {
                        "source": "inode-49HFLTr6xytgrPly3PFsNC",
                        "target": "2zTHG9wyvZ",
                        "type": "source",
                    },
                    {
                        "source": "inode-2Fby2MBLPM5jUMfBB15On1",
                        "target": "2zTHG9wyvZ",
                        "type": "source",
                    },
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/tQJu5N1l81/query": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "elementId": "tQJu5N1l81",
                "name": "PETS ADOPTIONS JOIN",
                "sql": 'select PK_8 "Pk", PROFILE_ID_9 "Profile Id", STATUS_10 "Status", CAST_TIMESTAMP_TO_DATETIME_11 "Created At", CAST_TIMESTAMP_TO_DATETIME_12 "Updated At", PK_13 "Pk (ADOPTIONS)", PET_FK_14 "Pet Fk", HUMAN_FK_15 "Human Fk", STATUS_16 "Status (ADOPTIONS)", CAST_TIMESTAMP_TO_DATETIME_19 "Created At (ADOPTIONS)", CAST_TIMESTAMP_TO_DATETIME_20 "Updated At (ADOPTIONS)" from (select PK_8, PROFILE_ID_9, STATUS_10, CAST_TIMESTAMP_TO_DATETIME_11, CAST_TIMESTAMP_TO_DATETIME_12, PK_13, PET_FK_14, HUMAN_FK_15, STATUS_16, CREATED_AT_17::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_19, UPDATED_AT_18::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_20 from (select Q1.PK_8 PK_8, Q1.PROFILE_ID_9 PROFILE_ID_9, Q1.STATUS_10 STATUS_10, Q1.CAST_TIMESTAMP_TO_DATETIME_11 CAST_TIMESTAMP_TO_DATETIME_11, Q1.CAST_TIMESTAMP_TO_DATETIME_12 CAST_TIMESTAMP_TO_DATETIME_12, Q2.PK PK_13, Q2.PET_FK PET_FK_14, Q2.HUMAN_FK HUMAN_FK_15, Q2.STATUS STATUS_16, Q2.CREATED_AT CREATED_AT_17, Q2.UPDATED_AT UPDATED_AT_18 from (select PK PK_8, PROFILE_ID PROFILE_ID_9, STATUS STATUS_10, CREATED_AT::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_11, UPDATED_AT::timestamp_ltz CAST_TIMESTAMP_TO_DATETIME_12 from LONG_TAIL_COMPANIONS.ADOPTION.PETS PETS) Q1 inner join LONG_TAIL_COMPANIONS.ADOPTION.ADOPTIONS Q2 on (Q1.PK_8 = Q2.PET_FK) limit 1000) Q4) Q5 limit 1000\n\n-- Sigma Σ {"request-id":"f5a997ef-b80c-47f1-b32e-9cd0f50cd491","email":"john.doe@example.com"}',
            },
        },
        "https://aws-api.sigmacomputing.com/v2/members": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "organizationId": "b94da709-176c-4242-bea6-6760f34c9228",
                        "memberId": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "memberType": "admin",
                        "firstName": "Shubham",
                        "lastName": "Jagtap",
                        "email": "john.doe@example.com",
                        "profileImgUrl": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2023-11-28T10:59:20.957Z",
                        "updatedAt": "2024-03-12T21:21:17.996Z",
                        "homeFolderId": "9bb94df1-e8af-49eb-9c37-2bd40b0efb2e",
                        "userKind": "internal",
                        "isArchived": False,
                        "isInactive": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
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


@pytest.mark.integration
def test_sigma_ingest(pytestconfig, tmp_path, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    register_mock_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/sigma_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "chart_sources_platform_mapping": {
                        "Acryl Data/Acryl Workbook": {
                            "data_source_platform": "snowflake"
                        },
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
    golden_file = "golden_test_sigma_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_platform_instance_ingest(pytestconfig, tmp_path, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    register_mock_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/sigma_platform_instace_ingest_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "platform_instance": "cloud_instance",
                    "chart_sources_platform_mapping": {
                        "Acryl Data/Acryl Workbook": {
                            "data_source_platform": "snowflake",
                            "platform_instance": "dev_instance",
                            "env": "DEV",
                        },
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


@pytest.mark.integration
def test_sigma_ingest_shared_entities(pytestconfig, tmp_path, requests_mock):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    override_data = {
        "https://aws-api.sigmacomputing.com/v2/workbooks": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "workbookId": "9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b",
                        "workbookUrlId": "4JRFW1HThPI1K3YTjouXI7",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:44:51.477Z",
                        "updatedAt": "2024-04-15T13:51:57.302Z",
                        "name": "Acryl Workbook",
                        "url": "https://app.sigmacomputing.com/acryldata/workbook/4JRFW1HThPI1K3YTjouXI7",
                        "path": "New Acryl Data",
                        "latestVersion": 2,
                        "isArchived": False,
                        "workspaceId": "4pe61405-3be2-4000-ba72-60d36757b95b",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=workbook": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": "9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b",
                        "urlId": "4JRFW1HThPI1K3YTjouXI7",
                        "name": "Acryl Workbook",
                        "type": "workbook",
                        "parentId": "4pe61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "New Acryl Data",
                        "badge": "Warning",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:44:51.477Z",
                        "updatedAt": "2024-04-15T13:51:57.302Z",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workspaces/4pe61405-3be2-4000-ba72-60d36757b95b": {
            "method": "GET",
            "status_code": 403,
            "json": {},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path: str = f"{tmp_path}/sigma_ingest_shared_entities_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "ingest_shared_entities": True,
                    "chart_sources_platform_mapping": {
                        "Acryl Data/Acryl Workbook": {
                            "data_source_platform": "snowflake"
                        },
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
    golden_file = "golden_test_sigma_ingest_shared_entities_mces.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )
