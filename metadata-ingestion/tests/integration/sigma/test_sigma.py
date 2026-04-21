import json
from typing import Any, Dict, Optional

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sigma.config import SigmaSourceReport
from datahub.testing import mce_helpers


def register_mock_api(request_mock: Any, override_data: Optional[dict] = None) -> None:
    if override_data is None:
        override_data = {}
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
                        "ownerId": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
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
                        "ownerId": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
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

    for url in api_vs_response:
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
                    "ingest_data_models": False,
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
                    "ingest_data_models": False,
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
                        "ownerId": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
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
                        "ownerId": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
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
                    "ingest_data_models": False,
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


@pytest.mark.integration
def test_sigma_ingest_data_models(pytestconfig, tmp_path, requests_mock):
    """
    Exercises Data Model ingestion:
    - DM1 ("Pet Analytics Model"): Sigma Dataset upstream, 3 columns (2 passthrough + 1 calculated)
    - DM2 ("Chained Model"): DM1 as upstream (chained Data Model), 2 columns
    - Both Data Models live in the "Acryl Data" workspace
    - /sources uses nextPageToken pagination (DM1 sources span 2 pages)
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    dm1_id = "aa11bb22-1111-2222-3333-444444444444"
    dm2_id = "bb22cc33-2222-3333-4444-555555555555"
    workspace_id = "3ee61405-3be2-4000-ba72-60d36757b95b"
    pets_dataset_id = "8891fd40-5470-4ff2-a74f-6e61ee44d3fc"

    override_data: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": dm1_id,
                        "urlId": "dm1UrlId",
                        "name": "Pet Analytics Model",
                        "type": "dataModel",
                        "parentId": workspace_id,
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
                        "id": dm2_id,
                        "urlId": "dm2UrlId",
                        "name": "Chained Model",
                        "type": "dataModel",
                        "parentId": workspace_id,
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-16T09:00:00.000Z",
                        "updatedAt": "2024-04-16T09:00:00.000Z",
                        "isArchived": False,
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": dm1_id,
                        "name": "Pet Analytics Model",
                        "description": "Analytics model for pets data",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{dm1_id}",
                        "isArchived": False,
                    },
                    {
                        "dataModelId": dm2_id,
                        "name": "Chained Model",
                        "description": "Chained from Pet Analytics Model",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-16T09:00:00.000Z",
                        "updatedAt": "2024-04-16T09:00:00.000Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{dm2_id}",
                        "isArchived": False,
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        # DM1 columns: 2 passthrough + 1 calculated
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm1_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "columnId": f"inode-{dm1_id}/pet_id",
                        "name": "pet_id",
                        "label": "Pet ID",
                        "formula": "[PETS/pet_id]",
                    },
                    {
                        "columnId": f"inode-{dm1_id}/status",
                        "name": "status",
                        "label": "Status",
                        "formula": "[PETS/status]",
                    },
                    {
                        "columnId": "yM2rd2GcRO",
                        "name": "status_label",
                        "label": "Status Label",
                        "formula": 'If([status] = "active", "Active", "Inactive")',
                    },
                ],
                "total": 3,
                "nextPage": None,
            },
        },
        # DM2 columns
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm2_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "columnId": f"inode-{dm2_id}/pet_id",
                        "name": "pet_id",
                        "label": "Pet ID",
                        "formula": "[Pet Analytics Model/pet_id]",
                    },
                    {
                        "columnId": "kQ9mP3xVwN",
                        "name": "summary_count",
                        "label": "Summary Count",
                        "formula": "Count([pet_id])",
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        # DM1 sources: paginated with nextPageToken across 2 pages.
        # Page 1 has a Sigma Dataset source and a nextPageToken.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm1_id}/sources": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {"type": "dataset", "datasetId": pets_dataset_id},
                ],
                "total": 1,
                "nextPageToken": "tok_dm1_page2",
            },
        },
        # DM1 sources page 2: empty, pagination ends.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm1_id}/sources?nextPageToken=tok_dm1_page2": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [],
                "total": 0,
                "nextPageToken": None,
            },
        },
        # DM2 sources: chains from DM1
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm2_id}/sources": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {"type": "dataModel", "dataModelId": dm1_id},
                ],
                "total": 1,
                "nextPageToken": None,
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path: str = f"{tmp_path}/sigma_data_models_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "ingest_data_models": True,
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
    golden_file = "golden_test_sigma_data_models.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_dm_phantom_urn_skip(tmp_path, requests_mock):
    """C1: datasetId absent from dataset_id_to_urn_part must not produce a dangling upstream edge."""
    dm_id = "cc33dd44-aaaa-bbbb-cccc-dddddddddddd"
    missing_dataset_id = "00000000-ffff-ffff-ffff-000000000000"
    workspace_id = "3ee61405-3be2-4000-ba72-60d36757b95b"

    override_data: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": dm_id,
                        "urlId": "dmPhantomUrlId",
                        "name": "Phantom DM",
                        "type": "dataModel",
                        "parentId": workspace_id,
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
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": dm_id,
                        "name": "Phantom DM",
                        "description": "",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{dm_id}",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/sources": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [{"type": "dataset", "datasetId": missing_dataset_id}],
                "total": 1,
                "nextPageToken": None,
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = str(tmp_path / "phantom_urn_mces.json")
    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "ingest_data_models": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()

    report = pipeline.source.get_report()
    assert any(
        w.title == "Unresolved Sigma dataset upstream" for w in report.warnings
    ), (
        f"Expected unresolved-upstream warning; got: {[w.title for w in report.warnings]}"
    )
    assert any(dm_id in list(w.context) for w in report.warnings), (
        f"Expected warning context to contain {dm_id}; got: {[list(w.context) for w in report.warnings]}"
    )

    emitted = json.loads(open(output_path).read())
    all_urns = {mcp["entityUrn"] for mcp in emitted}
    phantom_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,{missing_dataset_id},PROD)"
    )
    assert phantom_urn not in all_urns, (
        "Phantom upstream URN must not appear in emitted MCPs"
    )


@pytest.mark.integration
def test_dm_personal_folder_ingest_shared_entities_false(tmp_path, requests_mock):
    """C3c: personal-folder DMs are dropped when ingest_shared_entities=False."""
    dm_id = "dd44ee55-aaaa-bbbb-cccc-dddddddddddd"
    personal_folder_id = "728394a6-0000-0000-0000-000000000000"

    override_data: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": dm_id,
                        "urlId": "dmPersonalUrlId",
                        "name": "Personal DM",
                        "type": "dataModel",
                        "parentId": personal_folder_id,
                        "parentUrlId": "personalFolderUrlId",
                        "permission": "edit",
                        "path": "My Documents",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/files/{personal_folder_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "id": personal_folder_id,
                "name": "My Documents",
                "type": "folder",
                "parentId": personal_folder_id,
                "path": "",
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": dm_id,
                        "name": "Personal DM",
                        "description": "",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{dm_id}",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = str(tmp_path / "personal_folder_mces.json")
    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "ingest_data_models": True,
                    "ingest_shared_entities": False,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()

    report = pipeline.source.get_report()
    assert isinstance(report, SigmaSourceReport)
    dropped = [str(e) for e in report.data_models.dropped_entities]
    assert any(dm_id in d for d in dropped), (
        f"Expected personal-folder DM {dm_id} to be dropped; dropped list: {dropped}"
    )

    emitted = json.loads(open(output_path).read())
    dm_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{dm_id},PROD)"
    all_urns = {mcp["entityUrn"] for mcp in emitted}
    assert dm_urn not in all_urns, (
        f"Personal-folder DM {dm_id} must not appear in emitted MCPs when ingest_shared_entities=False"
    )


@pytest.mark.integration
def test_dm_warehouse_table_upstream_warning(tmp_path, requests_mock):
    """M2: warehouse-table sources must emit a warning and increment the counter."""
    dm_id = "ee55ff66-aaaa-bbbb-cccc-dddddddddddd"
    workspace_id = "3ee61405-3be2-4000-ba72-60d36757b95b"

    override_data: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": dm_id,
                        "urlId": "dmWarehouseUrlId",
                        "name": "Warehouse DM",
                        "type": "dataModel",
                        "parentId": workspace_id,
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
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": dm_id,
                        "name": "Warehouse DM",
                        "description": "",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{dm_id}",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/sources": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [{"type": "table", "inodeId": "inode-abc123"}],
                "total": 1,
                "nextPageToken": None,
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = str(tmp_path / "warehouse_upstream_mces.json")
    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "ingest_data_models": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()

    report = pipeline.source.get_report()
    assert isinstance(report, SigmaSourceReport)
    assert report.skipped_warehouse_table_upstreams >= 1, (
        f"Expected skipped_warehouse_table_upstreams >= 1; got {report.skipped_warehouse_table_upstreams}"
    )
    assert any(
        w.title == "Warehouse table upstream skipped" for w in report.warnings
    ), f"Expected warehouse-table warning; got: {[w.title for w in report.warnings]}"

    emitted = json.loads(open(output_path).read())
    dm_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{dm_id},PROD)"
    dm_aspect_names = {
        mcp["aspectName"] for mcp in emitted if mcp.get("entityUrn") == dm_urn
    }
    assert "upstreamLineage" not in dm_aspect_names, (
        "upstreamLineage must not be emitted for a warehouse-only DM"
    )


@pytest.mark.integration
def test_dm_zero_columns(tmp_path, requests_mock):
    """Zero-columns DM: no schemaMetadata aspect, other aspects still emitted."""
    dm_id = "ff66aa77-aaaa-bbbb-cccc-dddddddddddd"
    workspace_id = "3ee61405-3be2-4000-ba72-60d36757b95b"

    override_data: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": dm_id,
                        "urlId": "dmZeroColsUrlId",
                        "name": "Empty Schema DM",
                        "type": "dataModel",
                        "parentId": workspace_id,
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
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": dm_id,
                        "name": "Empty Schema DM",
                        "description": "No columns yet",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-04-15T13:43:12.664Z",
                        "updatedAt": "2024-04-15T13:43:12.664Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{dm_id}",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/sources": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPageToken": None},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = str(tmp_path / "zero_cols_mces.json")
    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "ingest_data_models": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()

    emitted = json.loads(open(output_path).read())
    dm_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{dm_id},PROD)"
    dm_aspect_names = {
        mcp["aspectName"] for mcp in emitted if mcp.get("entityUrn") == dm_urn
    }
    assert "schemaMetadata" not in dm_aspect_names, (
        "schemaMetadata must not be emitted for a DM with zero columns"
    )
    assert "status" in dm_aspect_names, "status aspect must still be emitted"
    assert "datasetProperties" in dm_aspect_names, (
        "datasetProperties must still be emitted"
    )
    assert "subTypes" in dm_aspect_names, "subTypes aspect must still be emitted"
