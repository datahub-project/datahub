from typing import Any, Dict, List, Optional, cast

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sigma.config import SigmaSourceReport
from datahub.testing import mce_helpers


def _sigma_report(pipeline: Pipeline) -> SigmaSourceReport:
    """Narrow ``pipeline.source.get_report()`` (returns the abstract
    ``SourceReport`` base) to the Sigma-specific report so mypy can see the
    DM-related counters added in T2."""
    return cast(SigmaSourceReport, pipeline.source.get_report())


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
def test_sigma_ingest_intra_workbook_lineage(pytestconfig, tmp_path, requests_mock):
    """
    Exercises intra-workbook (element-to-element) lineage:
    - direct sheet→sheet edge (nodeId != elementId)
    - sheet upstream reached via a join pass-through node
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    override_data: Dict[str, Dict] = {
        # Add a third page containing the intra-workbook elements.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {"pageId": "OSnGLBzL1i", "name": "Page 1"},
                    {"pageId": "DFSieiAcgo", "name": "Page 2"},
                    {"pageId": "IntraWorkbookPage", "name": "Page 3"},
                ],
                "total": 3,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages/IntraWorkbookPage/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": "upstreamElem01",
                        "type": "table",
                        "name": "Upstream Table Element",
                        "columns": ["Col A", "Col B"],
                        "vizualizationType": "levelTable",
                    },
                    {
                        "elementId": "downstreamElem01",
                        "type": "visualization",
                        "name": "Downstream Chart",
                        "columns": ["Col A"],
                        "vizualizationType": "bar",
                    },
                    {
                        "elementId": "joinDownstreamElem",
                        "type": "visualization",
                        "name": "Join Downstream Chart",
                        "columns": ["Col A"],
                        "vizualizationType": "bar",
                    },
                    {
                        # Filtered by get_page_elements (not in allowlist) — never
                        # enters the elementId→chart_urn map.
                        "elementId": "pivotElem01",
                        "type": "pivot-table",
                        "name": "Pivot Table Element",
                        "columns": ["Col A"],
                        "vizualizationType": "pivot",
                    },
                    {
                        # References pivotElem01 as upstream — chart_urn lookup returns
                        # None, so no inputEdges should be emitted for this element.
                        "elementId": "filteredUpstreamElem",
                        "type": "visualization",
                        "name": "Downstream Of Filtered Element",
                        "columns": ["Col A"],
                        "vizualizationType": "bar",
                    },
                    {
                        # Cross-page reference: upstream is kH0MeihtGs on Page 1.
                        # Verifies that elementId→chart_urn is built at workbook scope,
                        # not page scope — a page-scoped map would miss Page 1 elements
                        # while processing Page 3 and produce no inputEdges here.
                        "elementId": "crossPageDownstreamElem",
                        "type": "visualization",
                        "name": "Cross-Page Downstream Chart",
                        "columns": ["Col A"],
                        "vizualizationType": "bar",
                    },
                ],
                "total": 6,
                "nextPage": None,
            },
        },
        # upstreamElem01: only a warehouse table upstream (no sheet edge).
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/upstreamElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_node_upstream": {
                        "nodeId": "tgt_node_upstream",
                        "elementId": "upstreamElem01",
                        "name": "Upstream Table Element",
                        "type": "sheet",
                    },
                    "inode-warehouse01": {
                        "nodeId": "inode-warehouse01",
                        "name": "SOME_WAREHOUSE_TABLE",
                        "type": "table",
                    },
                },
                "edges": [
                    {
                        "source": "inode-warehouse01",
                        "target": "tgt_node_upstream",
                        "type": "source",
                    }
                ],
            },
        },
        # downstreamElem01: direct sheet upstream; nodeId ("src_node_upstream") !=
        # elementId ("upstreamElem01") — the critical fixture-fiction guard.
        # Also includes an unrelated edge (unrelated_sheet_node → unrelated_target)
        # that is NOT reachable from tgt_node_downstream via reverse BFS. With the
        # old scrape-every-edge-source approach this would add a spurious upstream_sources
        # entry; the BFS implementation correctly excludes it.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/downstreamElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_node_downstream": {
                        "nodeId": "tgt_node_downstream",
                        "elementId": "downstreamElem01",
                        "name": "Downstream Chart",
                        "type": "sheet",
                    },
                    "src_node_upstream": {
                        "nodeId": "src_node_upstream",
                        "elementId": "upstreamElem01",
                        "name": "Upstream Table Element",
                        "type": "sheet",
                    },
                    "unrelated_sheet_node": {
                        "nodeId": "unrelated_sheet_node",
                        "elementId": "upstreamElem01",
                        "name": "Upstream Table Element",
                        "type": "sheet",
                    },
                    "unrelated_target_node": {
                        "nodeId": "unrelated_target_node",
                        "name": "Some Unrelated Target",
                        "type": "table",
                    },
                },
                "edges": [
                    {
                        "source": "src_node_upstream",
                        "target": "tgt_node_downstream",
                        "type": "lookup",
                    },
                    {
                        # This edge's source is not reachable from tgt_node_downstream.
                        "source": "unrelated_sheet_node",
                        "target": "unrelated_target_node",
                        "type": "source",
                    },
                ],
            },
        },
        # joinDownstreamElem: sheet upstream reached via a join pass-through node.
        # The sheet edge (upstreamElem01→join) appears directly in the edges list,
        # making the join transparent without any recursive traversal.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/joinDownstreamElem": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_node_join_downstream": {
                        "nodeId": "tgt_node_join_downstream",
                        "elementId": "joinDownstreamElem",
                        "name": "Join Downstream Chart",
                        "type": "sheet",
                    },
                    "join_node_01": {
                        "nodeId": "join_node_01",
                        "type": "join",
                    },
                    "src_node_upstream": {
                        "nodeId": "src_node_upstream",
                        "elementId": "upstreamElem01",
                        "name": "Upstream Table Element",
                        "type": "sheet",
                    },
                },
                "edges": [
                    {
                        "source": "src_node_upstream",
                        "target": "join_node_01",
                        "type": "lookup",
                    },
                    {
                        "source": "join_node_01",
                        "target": "tgt_node_join_downstream",
                        "type": "source",
                    },
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/upstreamElem01/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/downstreamElem01/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/joinDownstreamElem/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        # filteredUpstreamElem: sheet upstream points to pivotElem01, which was
        # filtered by get_page_elements and is absent from the chart map.
        # The chart should be emitted with no inputEdges.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/filteredUpstreamElem": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_node_filtered": {
                        "nodeId": "tgt_node_filtered",
                        "elementId": "filteredUpstreamElem",
                        "name": "Downstream Of Filtered Element",
                        "type": "sheet",
                    },
                    "src_node_pivot": {
                        "nodeId": "src_node_pivot",
                        "elementId": "pivotElem01",
                        "name": "Pivot Table Element",
                        "type": "sheet",
                    },
                },
                "edges": [
                    {
                        "source": "src_node_pivot",
                        "target": "tgt_node_filtered",
                        "type": "source",
                    }
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/filteredUpstreamElem/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        # crossPageDownstreamElem: sheet upstream is kH0MeihtGs on Page 1.
        # inputEdges must point to urn:li:chart:(sigma,kH0MeihtGs), proving the
        # elementId→chart_urn map is workbook-scoped (page-scoped would miss it).
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/crossPageDownstreamElem": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_node_cross_page": {
                        "nodeId": "tgt_node_cross_page",
                        "elementId": "crossPageDownstreamElem",
                        "name": "Cross-Page Downstream Chart",
                        "type": "sheet",
                    },
                    "src_node_page1_adoptions": {
                        "nodeId": "src_node_page1_adoptions",
                        "elementId": "kH0MeihtGs",
                        "name": "ADOPTIONS",
                        "type": "sheet",
                    },
                },
                "edges": [
                    {
                        "source": "src_node_page1_adoptions",
                        "target": "tgt_node_cross_page",
                        "type": "source",
                    }
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/crossPageDownstreamElem/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path: str = f"{tmp_path}/sigma_extract_lineage_mces.json"

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
    golden_file = "golden_test_intra_workbook_lineage.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


def get_mock_data_model_api() -> Dict[str, Dict]:
    """
    Register mocks for a multi-element Data Model (``My Data Model-2``)
    mirroring the live-tenant regression case from the T2 investigation:

    - 3 elements, two of which share the same name (duplicate-name case)
    - intra-DM element lineage (element 3 → element 1)
    - external upstream: element 1 sourced from an existing Sigma Dataset
      (``PETS`` with urlId ``49HFLTr6xytgrPly3PFsNC``)
    - workbook elements referencing DM elements via the ``data-model`` lineage
      node type — the workbook→DM bridge exercises both the name-match primary
      path and the Container fallback path (unknown DM element name)

    Returns the full mock dict so individual tests can further override it.
    """
    return {
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": "147a4d09-a686-4eea-b183-9b82aa0f7beb",
                        "urlId": "CDJLIyOhUoKBSEVI8Wr4n",
                        "name": "My Data Model-2",
                        "type": "data-model",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-05-10T09:00:00.000Z",
                        "updatedAt": "2024-05-12T10:00:00.000Z",
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
                        "dataModelId": "147a4d09-a686-4eea-b183-9b82aa0f7beb",
                        "urlId": "CDJLIyOhUoKBSEVI8Wr4n",
                        "name": "My Data Model-2",
                        "description": "Regression fixture for multi-element DM",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-05-10T09:00:00.000Z",
                        "updatedAt": "2024-05-12T10:00:00.000Z",
                        "url": "https://app.sigmacomputing.com/acryldata/dm/CDJLIyOhUoKBSEVI8Wr4n",
                        "latestVersion": 3,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "path": "Acryl Data",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                # The real Sigma /dataModels/{id}/elements endpoint ships
                # ``columns`` as a list of bare column-name strings, mirroring
                # the workbook /elements shape. We keep the bare-string list
                # here to regression-cover the pre-validator that discards it
                # (rich SigmaDataModelColumn objects come from the separate
                # /columns endpoint and are attached post-parse in
                # SigmaAPI._assemble_data_model).
                "entries": [
                    {
                        "elementId": "0ui59vLc38",
                        "name": "random data model",
                        "type": "table",
                        "vizualizationType": None,
                        "columns": ["id", "name"],
                    },
                    {
                        # Duplicate-name case: Sigma coalesces workbook refs to
                        # same-named DM elements. Orphan in this fixture (no
                        # workbook element references it).
                        "elementId": "xloKCITNsP",
                        "name": "random data model",
                        "type": "table",
                        "vizualizationType": None,
                        "columns": ["id", "name"],
                    },
                    {
                        "elementId": "4plNusNz75",
                        "name": "2313213123.test.231",
                        "type": "table",
                        "vizualizationType": None,
                        "columns": ["id", "price"],
                    },
                ],
                "total": 3,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    # Element 1 columns (share names with element 2 on purpose —
                    # verifies per-element schema URN scoping prevents collision).
                    {
                        "columnId": "col-0ui-team1",
                        "elementId": "0ui59vLc38",
                        "name": "team1",
                        "label": "Team 1",
                        "formula": "",
                    },
                    {
                        "columnId": "col-0ui-city",
                        "elementId": "0ui59vLc38",
                        "name": "city",
                        "label": "City",
                        "formula": "",
                    },
                    # Element 2 columns (same bare names as element 1).
                    {
                        "columnId": "col-xlo-team1",
                        "elementId": "xloKCITNsP",
                        "name": "team1",
                        "label": "Team 1",
                        "formula": "",
                    },
                    {
                        "columnId": "col-xlo-city",
                        "elementId": "xloKCITNsP",
                        "name": "city",
                        "label": "City",
                        "formula": "",
                    },
                    # Element 3 columns (references element 1 output).
                    {
                        "columnId": "col-4pl-team1",
                        "elementId": "4plNusNz75",
                        "name": "team1",
                        "label": "Team 1",
                        "formula": "[random data model/team1]",
                    },
                    {
                        "columnId": "col-4pl-calc",
                        "elementId": "4plNusNz75",
                        "name": "Calc (1)",
                        "label": "Calc",
                        "formula": "[team1] + 'x'",
                    },
                    # Column without elementId — silently dropped (no element to
                    # attach to). Covers the defensive branch in _assemble_data_model.
                    {
                        "columnId": "col-orphan",
                        "elementId": None,
                        "name": "orphan_col",
                        "label": None,
                        "formula": "",
                    },
                ],
                "total": 7,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    # External Sigma Dataset upstream for element 1 — matches the
                    # PETS dataset urlId emitted by the existing dataset fixtures,
                    # so resolves to the SigmaDataset URN already in the map.
                    {
                        "type": "dataset",
                        "name": "PETS",
                        "inodeId": "inode-49HFLTr6xytgrPly3PFsNC",
                    },
                    # Warehouse-table node for element 2 — not a Sigma Dataset,
                    # so resolution returns None and the upstream is counted
                    # as unresolved (ticket §"warehouse-table upstreams require
                    # SQL parsing that the DM API does not expose").
                    {
                        "type": "table",
                        "name": "SOME_WAREHOUSE_TABLE",
                        "inodeId": "inode-dmWarehouseTableX",
                    },
                    {
                        "type": "element",
                        "elementId": "0ui59vLc38",
                        "sourceIds": ["inode-49HFLTr6xytgrPly3PFsNC"],
                    },
                    {
                        "type": "element",
                        "elementId": "xloKCITNsP",
                        "sourceIds": ["inode-dmWarehouseTableX"],
                    },
                    {
                        "type": "element",
                        "elementId": "4plNusNz75",
                        "sourceIds": ["0ui59vLc38"],
                    },
                ],
                "total": 5,
                "nextPage": None,
            },
        },
    }


def _apply_dm_bridge_workbook_overrides(override_data: Dict[str, Dict]) -> None:
    """Add a workbook page whose elements reference the DM via ``data-model``
    lineage nodes. Exercises all three workbook→DM bridge outcomes:
    name-match, ambiguous-name, and name-fail (container-fallback / unresolved
    depending on whether the DM was emitted)."""
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {"pageId": "OSnGLBzL1i", "name": "Page 1"},
                {"pageId": "DFSieiAcgo", "name": "Page 2"},
                {"pageId": "DmBridgePage", "name": "DM Bridge Page"},
            ],
            "total": 3,
            "nextPage": None,
        },
    }
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages/DmBridgePage/elements"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "elementId": "dmRefElem01",
                    "type": "table",
                    "name": "Uses 2313213123",
                    "columns": ["Col"],
                    "vizualizationType": "levelTable",
                },
                {
                    "elementId": "dmRefElem02",
                    "type": "visualization",
                    "name": "Uses random model",
                    "columns": ["Col"],
                    "vizualizationType": "bar",
                },
                {
                    "elementId": "dmRefElem03",
                    "type": "visualization",
                    "name": "Uses unknown DM element",
                    "columns": ["Col"],
                    "vizualizationType": "bar",
                },
            ],
            "total": 3,
            "nextPage": None,
        },
    }
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem01"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "dependencies": {
                "tgt_dmref_01": {
                    "nodeId": "tgt_dmref_01",
                    "elementId": "dmRefElem01",
                    "name": "Uses 2313213123",
                    "type": "sheet",
                },
                "CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK",
                    "type": "data-model",
                    "name": "2313213123.test.231",
                },
            },
            "edges": [
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK",
                    "target": "tgt_dmref_01",
                    "type": "source",
                }
            ],
        },
    }
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem02"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "dependencies": {
                "tgt_dmref_02": {
                    "nodeId": "tgt_dmref_02",
                    "elementId": "dmRefElem02",
                    "name": "Uses random model",
                    "type": "sheet",
                },
                "CDJLIyOhUoKBSEVI8Wr4n/mdYJst_DFR": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/mdYJst_DFR",
                    "type": "data-model",
                    "name": "random data model",
                },
            },
            "edges": [
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/mdYJst_DFR",
                    "target": "tgt_dmref_02",
                    "type": "source",
                }
            ],
        },
    }
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem03"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "dependencies": {
                "tgt_dmref_03": {
                    "nodeId": "tgt_dmref_03",
                    "elementId": "dmRefElem03",
                    "name": "Uses unknown DM element",
                    "type": "sheet",
                },
                "CDJLIyOhUoKBSEVI8Wr4n/unknownSuffix": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/unknownSuffix",
                    "type": "data-model",
                    "name": "name_not_in_dm",
                },
            },
            "edges": [
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/unknownSuffix",
                    "target": "tgt_dmref_03",
                    "type": "source",
                }
            ],
        },
    }
    for elem_id in ("dmRefElem01", "dmRefElem02", "dmRefElem03"):
        override_data[
            f"https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/{elem_id}/query"
        ] = {"method": "GET", "status_code": 404, "json": {}}


@pytest.mark.integration
def test_sigma_ingest_data_models(pytestconfig, tmp_path, requests_mock):
    """
    Exercises the new Data Model ingestion path:

    - multi-element DM emits 1 Container + 3 Datasets (duplicate-named element
      included as an orphan)
    - per-element schemaMetadata (no cross-element column-name collision)
    - intra-DM element→element UpstreamLineage
    - external upstream resolves to an existing Sigma Dataset URN
    - workbook element bridges to a DM element via a ``data-model`` lineage
      node (name-match primary path + ambiguous-name counter)
    - workbook element with an unknown DM element name falls back to the DM
      Container URN
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    override_data: Dict[str, Dict] = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path: str = f"{tmp_path}/sigma_ingest_data_models_mces.json"

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
    golden_file = "golden_test_sigma_ingest_data_models.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_pattern_filter(pytestconfig, tmp_path, requests_mock):
    """``data_model_pattern`` denies the DM → no DM entities emitted and
    workbook elements previously bridging to the DM degrade to
    ``element_dm_edge_unresolved`` (bridge maps never registered)."""

    override_data: Dict[str, Dict] = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path: str = f"{tmp_path}/sigma_ingest_data_models_filtered_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "sigma-test",
            "source": {
                "type": "sigma",
                "config": {
                    "client_id": "CLIENTID",
                    "client_secret": "CLIENTSECRET",
                    "data_model_pattern": {"deny": ["My Data Model.*"]},
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
    # No golden needed: assert absence of any DM entities in the output.
    import json

    with open(output_path) as f:
        mces = json.load(f)
    dm_container_present = any(
        mce.get("entityType") == "container"
        and "147a4d09-a686-4eea-b183-9b82aa0f7beb" in mce.get("entityUrn", "")
        for mce in mces
    )
    assert not dm_container_present, (
        "DM Container should be filtered out by data_model_pattern"
    )
    # No element Datasets should have been emitted either — URN part encodes
    # the DM urlId, so a quick substring check is sufficient.
    dm_element_present = any(
        "CDJLIyOhUoKBSEVI8Wr4n" in mce.get("entityUrn", "") for mce in mces
    )
    assert not dm_element_present, (
        "DM element Datasets should be filtered out by data_model_pattern"
    )

    report = _sigma_report(pipeline)
    assert report.element_dm_edges_resolved == 0
    assert report.element_dm_edge_name_unmatched_but_dm_known == 0
    # All three DM-bridge workbook elements must end up as unresolved because
    # the bridge maps were never populated (DM was denied).
    assert report.element_dm_edge_unresolved == 3, (
        f"expected 3 unresolved DM edges, got {report.element_dm_edge_unresolved}"
    )
    # No DM-element Dataset URN should end up in any ChartInfo.inputs either.
    for mce in mces:
        if mce.get("aspectName") == "chartInfo":
            aspect_json = mce.get("aspect", {}).get("json", mce.get("aspect", {}))
            for inp in aspect_json.get("inputs", []):
                assert "CDJLIyOhUoKBSEVI8Wr4n" not in inp.get("string", ""), (
                    f"DM URN leaked into ChartInfo.inputs for {mce.get('entityUrn')}"
                )


@pytest.mark.integration
def test_sigma_ingest_data_models_disabled(pytestconfig, tmp_path, requests_mock):
    """``ingest_data_models=False`` short-circuits DM fetch entirely — verified
    by omitting DM mocks and relying on strict requests_mock to fail if the DM
    endpoint is hit."""
    register_mock_api(request_mock=requests_mock)

    output_path: str = f"{tmp_path}/sigma_ingest_data_models_disabled_mces.json"

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
    # Same golden as the baseline test — the ingest_data_models=False path
    # should produce identical output to the unconfigured default when no DM
    # mocks are registered.
    import json

    with open(output_path) as f:
        mces = json.load(f)
    assert not any("CDJLIyOhUoKBSEVI8Wr4n" in mce.get("entityUrn", "") for mce in mces)

    # Prove the DM endpoints were never hit — stronger than the absence check
    # above. If ingest_data_models=False, /v2/dataModels should not be
    # requested at all.
    dm_endpoint_hits = [
        req
        for req in requests_mock.request_history
        if "/v2/dataModels" in req.url or "typeFilters=data-model" in req.url
    ]
    assert dm_endpoint_hits == [], (
        f"expected ingest_data_models=False to short-circuit DM fetch, "
        f"but got {len(dm_endpoint_hits)} requests: "
        f"{[r.url for r in dm_endpoint_hits]}"
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


def _minimal_sigma_pipeline_config(output_path: str, **extra: Any) -> Dict[str, Any]:
    return {
        "run_id": "sigma-test",
        "source": {
            "type": "sigma",
            "config": {
                "client_id": "CLIENTID",
                "client_secret": "CLIENTSECRET",
                "chart_sources_platform_mapping": {
                    "Acryl Data/Acryl Workbook": {"data_source_platform": "snowflake"},
                },
                **extra,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


@pytest.mark.integration
def test_sigma_ingest_data_models_external_dataset_not_ingested(
    pytestconfig, tmp_path, requests_mock
):
    """Case 2 of ``_resolve_dm_element_external_upstream``: DM /lineage
    records a ``type: dataset`` node whose ``inode-<urlId>`` does *not* match
    any ingested SigmaDataset (e.g. filtered out). The resolver should
    synthesize a Sigma Dataset URN from the suffix rather than dropping the
    edge or falling through to None."""

    override_data = get_mock_data_model_api()
    # Replace the lineage so element 2 (``xloKCITNsP``) sources from an
    # unregistered Sigma Dataset inode. Element 1 keeps the PETS (Case 1)
    # upstream; element 3 keeps its intra-DM edge.
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/lineage"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "type": "dataset",
                    "name": "PETS",
                    "inodeId": "inode-49HFLTr6xytgrPly3PFsNC",
                },
                {
                    "type": "dataset",
                    "name": "UnregisteredDataset",
                    "inodeId": "inode-unregDs000000001",
                },
                {
                    "type": "element",
                    "elementId": "0ui59vLc38",
                    "sourceIds": ["inode-49HFLTr6xytgrPly3PFsNC"],
                },
                {
                    "type": "element",
                    "elementId": "xloKCITNsP",
                    "sourceIds": ["inode-unregDs000000001"],
                },
                {
                    "type": "element",
                    "elementId": "4plNusNz75",
                    "sourceIds": ["0ui59vLc38"],
                },
            ],
            "total": 5,
            "nextPage": None,
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_case2_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    import json

    with open(output_path) as f:
        mces = json.load(f)

    # The xloKCITNsP element should carry a synthesized Sigma Dataset URN as
    # its upstream — built from ``inode-unregDs000000001`` suffix.
    expected_synth_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:sigma,unregDs000000001,PROD)"
    )
    element2_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:sigma,"
        "147a4d09-a686-4eea-b183-9b82aa0f7beb.xloKCITNsP,PROD)"
    )
    found_edge = False
    for mce in mces:
        if (
            mce.get("entityUrn") == element2_urn
            and mce.get("aspectName") == "upstreamLineage"
        ):
            aspect_json = mce.get("aspect", {}).get("json", mce.get("aspect", {}))
            for up in aspect_json.get("upstreams", []):
                if up.get("dataset") == expected_synth_urn:
                    found_edge = True
    assert found_edge, (
        f"expected synthesized Sigma Dataset URN {expected_synth_urn} "
        f"as upstream of {element2_urn}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_lineage_http_error(
    pytestconfig, tmp_path, requests_mock
):
    """DM /lineage returning 500 is handled gracefully: the DM Container and
    element Datasets are still emitted, but no UpstreamLineage aspects."""

    override_data = get_mock_data_model_api()
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/lineage"
    ] = {"method": "GET", "status_code": 500, "json": {}}

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_lineage_err_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    import json

    with open(output_path) as f:
        mces = json.load(f)

    dm_urn = "urn:li:container:0466d89b8ce5ac9b2cd1deecdffe42c1"
    assert any(
        mce.get("entityUrn") == dm_urn
        and mce.get("aspectName") == "containerProperties"
        for mce in mces
    ), "DM Container should still be emitted despite lineage 500"

    # No upstreamLineage aspects should exist for any DM element.
    dm_element_upstreams = [
        mce
        for mce in mces
        if "CDJLIyOhUoKBSEVI8Wr4n" in mce.get("entityUrn", "")
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert dm_element_upstreams == [], (
        "no UpstreamLineage should be emitted when /lineage returns 500"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_next_page_token_pagination(
    pytestconfig, tmp_path, requests_mock
):
    """``get_data_models`` should paginate via ``nextPageToken`` when the API
    omits ``nextPage``. Covers the second branch of the pagination loop."""

    override_data = get_mock_data_model_api()

    # Add a second DM returned via a nextPageToken-driven second page.
    second_dm_id = "247a4d09-a686-4eea-b183-9b82aa0f7beb"
    second_dm_url_id = "SecondPagTok00000000"

    override_data["https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model"][
        "json"
    ]["entries"].append(
        {
            "id": second_dm_id,
            "urlId": second_dm_url_id,
            "name": "Second DM (token-paginated)",
            "type": "data-model",
            "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
            "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
            "permission": "edit",
            "path": "Acryl Data",
            "badge": None,
            "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
            "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
            "createdAt": "2024-05-10T09:00:00.000Z",
            "updatedAt": "2024-05-12T10:00:00.000Z",
            "isArchived": False,
        }
    )
    override_data["https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model"][
        "json"
    ]["total"] = 2

    # First /dataModels page: advertises a token, no nextPage.
    override_data["https://aws-api.sigmacomputing.com/v2/dataModels"]["json"][
        "nextPageToken"
    ] = "tok-page-2"
    # Token-driven second /dataModels page with the second DM.
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels?nextPageToken=tok-page-2"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "dataModelId": second_dm_id,
                    "urlId": second_dm_url_id,
                    "name": "Second DM (token-paginated)",
                    "description": "",
                    "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                    "createdAt": "2024-05-10T09:00:00.000Z",
                    "updatedAt": "2024-05-12T10:00:00.000Z",
                    "url": f"https://app.sigmacomputing.com/acryldata/dm/{second_dm_url_id}",
                    "latestVersion": 1,
                    "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                    "path": "Acryl Data",
                }
            ],
            "total": 1,
            "nextPage": None,
        },
    }
    # Empty elements/columns/lineage for the second DM — exercises the
    # pagination path, not element-level assembly.
    for endpoint in ("elements", "columns", "lineage"):
        override_data[
            f"https://aws-api.sigmacomputing.com/v2/dataModels/{second_dm_id}/{endpoint}"
        ] = {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_next_page_token_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    import json

    with open(output_path) as f:
        mces = json.load(f)

    dm_container_urns = {
        mce.get("entityUrn")
        for mce in mces
        if mce.get("entityType") == "container"
        and mce.get("aspectName") == "subTypes"
        and "Sigma Data Model"
        in mce.get("aspect", {}).get("json", mce.get("aspect", {})).get("typeNames", [])
    }
    assert len(dm_container_urns) == 2, (
        f"expected both DMs emitted via nextPage + nextPageToken, got {dm_container_urns}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_shared_entity_no_workspace(
    pytestconfig, tmp_path, requests_mock
):
    """DM whose workspace is not returned from /workspaces should be ingested
    when ``ingest_shared_entities=True`` and counted under
    ``data_models_without_workspace`` — mirrors the existing shared-workbook
    path but for DMs."""

    override_data = get_mock_data_model_api()
    # Point the DM at a workspace the tenant does not list in /workspaces.
    override_data["https://aws-api.sigmacomputing.com/v2/dataModels"]["json"][
        "entries"
    ][0]["workspaceId"] = "99999999-0000-0000-0000-000000000000"
    override_data["https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model"][
        "json"
    ]["entries"][0]["parentId"] = "99999999-0000-0000-0000-000000000000"

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_shared_no_ws_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    assert report.data_models_without_workspace == 1

    import json

    with open(output_path) as f:
        mces = json.load(f)
    assert any(
        mce.get("entityUrn") == "urn:li:container:0466d89b8ce5ac9b2cd1deecdffe42c1"
        for mce in mces
    ), "DM Container should be emitted under ingest_shared_entities=True"


@pytest.mark.integration
def test_sigma_ingest_data_models_workspace_pattern_deny(
    pytestconfig, tmp_path, requests_mock
):
    """``workspace_pattern`` deny should drop the DM even though
    ``data_model_pattern`` allows it — verifies the workspace-scoped filter
    branch specific to DMs."""

    override_data = get_mock_data_model_api()

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_ws_deny_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(
            output_path, workspace_pattern={"deny": ["Acryl Data"]}
        )
    )
    pipeline.run()
    pipeline.raise_from_status()

    import json

    with open(output_path) as f:
        mces = json.load(f)
    assert not any(
        "CDJLIyOhUoKBSEVI8Wr4n" in mce.get("entityUrn", "")
        or "0466d89b8ce5ac9b2cd1deecdffe42c1" in mce.get("entityUrn", "")
        for mce in mces
    ), "DM entities should be dropped by workspace_pattern deny"


@pytest.mark.integration
def test_sigma_ingest_data_models_lineage_node_missing_name(
    pytestconfig, tmp_path, requests_mock
):
    """Workbook lineage ``data-model`` node without a ``name`` field: the
    resolver's ``if name_map and upstream.name`` guard should skip the
    name-match branch and book it as
    ``element_dm_edge_name_unmatched_but_dm_known``."""

    override_data = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)

    # Rewrite dmRefElem01's lineage to strip the ``name`` from the DM node.
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem01"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "dependencies": {
                "tgt_dmref_01": {
                    "nodeId": "tgt_dmref_01",
                    "elementId": "dmRefElem01",
                    "name": "Uses 2313213123",
                    "type": "sheet",
                },
                "CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK",
                    "type": "data-model",
                    # ``name`` intentionally omitted to exercise the
                    # ``upstream.name is None`` guard.
                },
            },
            "edges": [
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK",
                    "target": "tgt_dmref_01",
                    "type": "source",
                }
            ],
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_name_none_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    # dmRefElem01 misses, dmRefElem02 resolves (name="random data model",
    # ambiguous but resolved), dmRefElem03 misses (unknown element name).
    assert report.element_dm_edge_name_unmatched_but_dm_known == 2, (
        f"expected 2 name-unmatched edges, got "
        f"{report.element_dm_edge_name_unmatched_but_dm_known}"
    )
    assert report.element_dm_edges_resolved == 1


@pytest.mark.integration
def test_sigma_ingest_data_models_ambiguous_name_deterministic_pick(
    pytestconfig, tmp_path, requests_mock
):
    """When multiple DM elements share a name, the resolver must pick the
    same URN regardless of Sigma ``/elements`` response order (sorted by
    elementId). Regression pin for m2 fix."""

    override_data = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)

    # Reverse the /elements response: put ``xloKCITNsP`` before
    # ``0ui59vLc38``. Without the sort-before-pick, this would flip which
    # candidate is chosen for the ambiguous-name workbook reference.
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/elements"
    ]["json"]["entries"] = [
        {
            "elementId": "xloKCITNsP",
            "name": "random data model",
            "type": "table",
            "vizualizationType": None,
            "columns": ["id", "name"],
        },
        {
            "elementId": "0ui59vLc38",
            "name": "random data model",
            "type": "table",
            "vizualizationType": None,
            "columns": ["id", "name"],
        },
        {
            "elementId": "4plNusNz75",
            "name": "2313213123.test.231",
            "type": "table",
            "vizualizationType": None,
            "columns": ["id", "price"],
        },
    ]

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_ambig_deterministic_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    import json

    with open(output_path) as f:
        mces = json.load(f)

    # dmRefElem02 references "random data model" — must resolve to the
    # URN for the element with the *sorted-smallest* elementId, i.e.
    # ``0ui59vLc38`` (sorts before ``xloKCITNsP``).
    dm_uuid = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    expected_resolved_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,{dm_uuid}.0ui59vLc38,PROD)"
    )
    chart_info = next(
        mce
        for mce in mces
        if mce.get("entityUrn", "").endswith("dmRefElem02)")
        and mce.get("aspectName") == "chartInfo"
    )
    aspect_json = chart_info.get("aspect", {}).get("json", chart_info.get("aspect", {}))
    resolved_urns = [inp.get("string", "") for inp in aspect_json.get("inputs", [])]
    assert expected_resolved_urn in resolved_urns, (
        f"expected deterministic pick of {expected_resolved_urn} regardless of "
        f"API element order, got {resolved_urns}"
    )

    report = _sigma_report(pipeline)
    assert report.element_dm_edge_ambiguous >= 1


@pytest.mark.integration
def test_sigma_ingest_data_models_edges_only_dm_ref_synthesized(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for the real Sigma API shape of workbook→DM element
    lineage (live-probed 2026-04-22 on a tenant workbook).

    Real tenants return the DM-reference node ``<dmUrlId>/<suffix>`` ONLY
    as an edge source — the node is NOT a key in the ``dependencies`` dict.
    Before the synthesis fix, the BFS loop raised ``KeyError`` when looking
    up the missing dependency entry, blanking the entire element's lineage
    and silently dropping every workbook→DM edge in production. This test
    reproduces that shape and asserts that:

    1. ``DataModelElementUpstream`` is synthesized from the edge alone,
    2. the workbook element's own ``name`` is used as the DM element name
       (Sigma's default — user rename degrades to the existing
       ``element_dm_edge_name_unmatched_but_dm_known`` counter), and
    3. the ``element_dm_edge_synthesized_from_edge_only`` counter tracks
       how many refs travelled this path, so the legacy "DM in dependencies"
       path's coverage — retained defensively — stays distinguishable.
    """

    override_data = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)

    # Rename the workbook elements so their ``name`` matches the target
    # DM element name (Sigma's default behaviour — see test docstring).
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages/DmBridgePage/elements"
    ]["json"]["entries"] = [
        {
            "elementId": "dmRefElem01",
            "type": "table",
            "name": "2313213123.test.231",
            "columns": ["Col"],
            "vizualizationType": "levelTable",
        },
        {
            "elementId": "dmRefElem02",
            "type": "visualization",
            "name": "random data model",
            "columns": ["Col"],
            "vizualizationType": "bar",
        },
        {
            "elementId": "dmRefElem03",
            "type": "visualization",
            "name": "name_renamed_by_user_no_longer_matches",
            "columns": ["Col"],
            "vizualizationType": "bar",
        },
    ]

    # Rewrite each element's lineage to the real API shape: the DM-shaped
    # ``<dmUrlId>/<suffix>`` node appears ONLY as an edge source; it is
    # deliberately absent from ``dependencies``. Seed sheet entry names
    # mirror element.name per observed API behaviour.
    def _edges_only_lineage(
        elem_id: str, elem_name: str, seed_node_id: str, dm_source_id: str
    ) -> Dict[str, Any]:
        return {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    seed_node_id: {
                        "nodeId": seed_node_id,
                        "elementId": elem_id,
                        "name": elem_name,
                        "type": "sheet",
                    }
                },
                "edges": [
                    {
                        "source": dm_source_id,
                        "target": seed_node_id,
                        "type": "source",
                    }
                ],
            },
        }

    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem01"
    ] = _edges_only_lineage(
        elem_id="dmRefElem01",
        elem_name="2313213123.test.231",
        seed_node_id="seed_dmref_01",
        dm_source_id="CDJLIyOhUoKBSEVI8Wr4n/pwxVRJHBSK",
    )
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem02"
    ] = _edges_only_lineage(
        elem_id="dmRefElem02",
        elem_name="random data model",
        seed_node_id="seed_dmref_02",
        dm_source_id="CDJLIyOhUoKBSEVI8Wr4n/mdYJst_DFR",
    )
    override_data[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/dmRefElem03"
    ] = _edges_only_lineage(
        elem_id="dmRefElem03",
        elem_name="name_renamed_by_user_no_longer_matches",
        seed_node_id="seed_dmref_03",
        dm_source_id="CDJLIyOhUoKBSEVI8Wr4n/someOtherSuffix",
    )

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_edges_only_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)

    # All 3 workbook→DM refs travelled the synthesized path (none were
    # present in dependencies), so the counter bumps once per ref.
    assert report.element_dm_edge_synthesized_from_edge_only == 3, (
        f"expected 3 synthesized DM refs, got "
        f"{report.element_dm_edge_synthesized_from_edge_only}"
    )

    # dmRefElem01 → "2313213123.test.231" (unique name, single DM element
    #                match → resolves to 4plNusNz75).
    # dmRefElem02 → "random data model" (ambiguous across 2 DM elements;
    #                deterministic pick = lowest elementId = 0ui59vLc38).
    # dmRefElem03 → user-renamed name → unmatched; DM is known to this run
    #                so the name-unmatched-but-known counter bumps.
    assert report.element_dm_edges_resolved == 2, (
        f"expected 2 resolved bridge edges (01 + 02), got "
        f"{report.element_dm_edges_resolved}"
    )
    assert report.element_dm_edge_name_unmatched_but_dm_known == 1, (
        f"expected 1 rename-induced unmatched, got "
        f"{report.element_dm_edge_name_unmatched_but_dm_known}"
    )

    import json

    with open(output_path) as f:
        mces = json.load(f)

    dm_uuid = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    expected_01_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,{dm_uuid}.4plNusNz75,PROD)"
    )
    expected_02_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,{dm_uuid}.0ui59vLc38,PROD)"
    )

    def _chart_inputs(elem_id: str) -> List[str]:
        chart_info = next(
            mce
            for mce in mces
            if mce.get("entityUrn", "").endswith(f"{elem_id})")
            and mce.get("aspectName") == "chartInfo"
        )
        aspect_json = chart_info.get("aspect", {}).get(
            "json", chart_info.get("aspect", {})
        )
        return [inp.get("string", "") for inp in aspect_json.get("inputs", [])]

    assert expected_01_urn in _chart_inputs("dmRefElem01"), (
        f"dmRefElem01 should link to unique-name DM element {expected_01_urn}; "
        f"got inputs={_chart_inputs('dmRefElem01')}"
    )
    assert expected_02_urn in _chart_inputs("dmRefElem02"), (
        f"dmRefElem02 should link to ambiguous-name deterministic pick "
        f"{expected_02_urn}; got inputs={_chart_inputs('dmRefElem02')}"
    )
    # dmRefElem03 is renamed by the user; the resolver must NOT fall back
    # to the DM Container URN (schema-invalid on ChartInfo.inputs).
    elem03_inputs = _chart_inputs("dmRefElem03")
    assert not any("CDJLIyOhUoKBSEVI8Wr4n" in inp for inp in elem03_inputs), (
        f"dmRefElem03 must NOT emit a DM Container URN as a chart input; "
        f"got inputs={elem03_inputs}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_cross_dm_upstream(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for DM→DM cross-reference upstream lineage.

    When a Sigma user imports a table from Data-Model-A into Data-Model-B,
    Sigma's ``/dataModels/{B_id}/lineage`` returns the consuming element's
    ``sourceIds`` in the shape ``<A_urlId>/<opaque_suffix>`` — the same
    shape as workbook→DM references (live-probed 2026-04-22 against a
    real tenant: ``Test Source`` DM importing ``Test Data`` from
    ``Test Model``). The suffix is opaque (Sigma does not expose a public
    endpoint that maps it to an ``elementId``), so resolution falls back
    to the name-bridge keyed by the consuming element's own ``name``
    (Sigma's default names the imported table after the source element).

    This test asserts:
    1. Consumer DM's element emits ``UpstreamLineage`` pointing at the
       source DM's element Dataset URN (not the source DM Container).
    2. ``data_model_element_cross_dm_upstreams_resolved`` counter bumps.
    3. The two-pass bridge pre-population in ``get_workunits_internal``
       correctly resolves forward references — the consuming DM is
       returned by ``/dataModels`` *before* the producing DM, which
       would have broken a single-pass resolver.
    """

    producer_dm_id = "aaaa1111-aaaa-1111-aaaa-1111aaaa1111"
    producer_dm_url_id = "ProducerDMurlId00000"
    producer_element_id = "1DYf5I08WO"

    consumer_dm_id = "bbbb2222-bbbb-2222-bbbb-2222bbbb2222"
    consumer_dm_url_id = "ConsumerDMurlId00000"
    consumer_element_id = "HdbgI9D-Ci"
    # The suffix Sigma assigns inside the consumer's /lineage sourceIds —
    # opaque per-element identifier, not equal to producer_element_id.
    cross_dm_suffix = "idfniR_6Jx"

    workspace_json = {
        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
        "name": "Acryl Data",
        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
        "createdAt": "2024-05-10T09:00:00.000Z",
        "updatedAt": "2024-05-12T10:00:00.000Z",
    }
    override_data: Dict[str, Dict[str, Any]] = {
        # Minimal workspace + workbook scaffolding so the pipeline runs.
        "https://aws-api.sigmacomputing.com/v2/workspaces": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [workspace_json],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workspaces/3ee61405-3be2-4000-ba72-60d36757b95b": {
            "method": "GET",
            "status_code": 200,
            "json": workspace_json,
        },
        "https://aws-api.sigmacomputing.com/v2/members": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=dataset": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        # Both DMs appear in /files (required for shared-entity workspace
        # reconciliation inside the ingestion loop).
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Test Source",
                        "type": "data-model",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-05-10T09:00:00.000Z",
                        "updatedAt": "2024-05-12T10:00:00.000Z",
                        "isArchived": False,
                    },
                    {
                        "id": producer_dm_id,
                        "urlId": producer_dm_url_id,
                        "name": "Test Model",
                        "type": "data-model",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-05-10T09:00:00.000Z",
                        "updatedAt": "2024-05-12T10:00:00.000Z",
                        "isArchived": False,
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        # Consumer DM appears FIRST in /dataModels so that a single-pass
        # resolver would encounter the forward reference before the
        # producer's bridge map is populated — this is what exercises the
        # two-pass pre-population contract in get_workunits_internal.
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Test Source",
                        "description": "Consumer DM for cross-DM lineage",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-05-10T09:00:00.000Z",
                        "updatedAt": "2024-05-12T10:00:00.000Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/dm/{consumer_dm_url_id}",
                        "latestVersion": 1,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "path": "Acryl Data",
                    },
                    {
                        "dataModelId": producer_dm_id,
                        "urlId": producer_dm_url_id,
                        "name": "Test Model",
                        "description": "Producer DM with the source element",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2024-05-10T09:00:00.000Z",
                        "updatedAt": "2024-05-12T10:00:00.000Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/dm/{producer_dm_url_id}",
                        "latestVersion": 1,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "path": "Acryl Data",
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        # Producer DM: a single "Test Data" element, no upstream.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": producer_element_id,
                        "name": "Test Data",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": ["id", "name"],
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "columnId": f"col-{producer_element_id}-id",
                        "elementId": producer_element_id,
                        "name": "id",
                        "label": "ID",
                        "formula": "",
                    },
                    {
                        "columnId": f"col-{producer_element_id}-name",
                        "elementId": producer_element_id,
                        "name": "name",
                        "label": "Name",
                        "formula": "",
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        # Consumer DM: a single "Test Data" element whose sourceIds contain
        # the real cross-DM wire shape "<producerUrlId>/<suffix>". The
        # top-level "data-model" entry carrying producer's dataModelId is
        # reproduced verbatim from the probe but not required for our
        # resolver — it names what's referenced, not how to resolve it.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": consumer_element_id,
                        "name": "Test Data",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": ["id", "name"],
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "columnId": f"col-{consumer_element_id}-id",
                        "elementId": consumer_element_id,
                        "name": "id",
                        "label": "ID",
                        "formula": "",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "type": "data-model",
                        "dataModelId": producer_dm_id,
                        "name": "Test Data",
                        "connectionId": "1aff342f-6157-4589-ab9b-947c16b0bd7e",
                    },
                    {
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [
                            f"{producer_dm_url_id}/{cross_dm_suffix}",
                        ],
                        "dataSourceIds": [
                            f"{producer_dm_url_id}/{cross_dm_suffix}",
                        ],
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_cross_dm_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    assert report.data_model_element_cross_dm_upstreams_resolved == 1, (
        f"expected 1 cross-DM upstream resolved, got "
        f"{report.data_model_element_cross_dm_upstreams_resolved}"
    )
    assert report.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known == 0
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 0

    import json

    with open(output_path) as f:
        mces = json.load(f)

    consumer_element_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{consumer_dm_id}.{consumer_element_id},PROD)"
    expected_upstream_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{producer_dm_id}.{producer_element_id},PROD)"

    upstream_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == consumer_element_urn
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert len(upstream_mces) == 1, (
        f"expected exactly 1 UpstreamLineage aspect on consumer element "
        f"{consumer_element_urn}, got {len(upstream_mces)}"
    )

    aspect_json = (
        upstream_mces[0]
        .get("aspect", {})
        .get("json", upstream_mces[0].get("aspect", {}))
    )
    upstream_urns = [u.get("dataset") for u in aspect_json.get("upstreams", [])]
    assert expected_upstream_urn in upstream_urns, (
        f"consumer element should have upstream {expected_upstream_urn} "
        f"(producer DM element), got {upstream_urns}"
    )

    # The upstream must be the producer's *element* URN, never the producer's
    # *Container* URN (UpstreamLineage.upstreams accepts Dataset URNs; a
    # Container URN would be schema-valid but semantically wrong — it would
    # show an entire DM as a single upstream node rather than the specific
    # table that was imported).
    producer_container_urn_prefix = "urn:li:container:"
    assert not any(
        u.startswith(producer_container_urn_prefix) for u in upstream_urns
    ), f"consumer element must not point at a DM Container; got {upstream_urns}"


@pytest.mark.integration
def test_sigma_ingest_data_models_orphan_dm_discovery(
    pytestconfig, tmp_path, requests_mock
):
    """Personal-space ("orphan") DM discovery via on-demand GET /v2/dataModels/{urlId}.

    When a workspace-scoped DM references an element from a personal-space DM
    (``path: "My Documents"``, ``workspaceId: null``), the personal-space DM is
    not returned by the ``/v2/dataModels`` listing. The discovery loop in
    ``get_workunits_internal`` should detect the unresolved cross-DM prefix,
    fetch the DM by urlId, and emit the full Container + element Dataset
    entities so the lineage edge resolves end-to-end.

    This also exercises the single-element fallback: the consumer element is
    named "Test Data" but the producer element is named "data.csv". Because the
    producer has exactly one element, the resolver falls back to that element
    unambiguously.

    Assertions:
    1. Producer Container emitted with isPersonalDataModel="true" and path.
    2. Producer element Dataset emitted with SchemaMetadata + correct subtype.
    3. Consumer element Dataset has UpstreamLineage pointing at producer element.
    4. Reporter counters: discovered=1, unresolved=0, single_element_fallback=1,
       cross_dm_resolved=1.
    5. Producer Container has no workspace parent Container.
    6. /v2/dataModels listing called once; /v2/dataModels/{urlId} called once.
    """
    import json

    consumer_dm_id = "b584ddca-cfd6-4b72-97da-367fc0a5606d"
    consumer_dm_url_id = "5wwkxte74KSUpjT0C0b0sZ"
    consumer_element_id = "1DYf5I08WO"

    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    producer_dm_url_id = "3BtEwqctAlmKlYTJIQ8QFC"
    producer_element_id = "wcUd3nEUAv"
    cross_dm_suffix = "vACRd1GzJS"

    workspace_json = {
        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
        "name": "Acryl Data",
        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
        "createdAt": "2024-05-10T09:00:00.000Z",
        "updatedAt": "2024-05-12T10:00:00.000Z",
    }

    override_data: Dict[str, Dict[str, Any]] = {
        "https://aws-api.sigmacomputing.com/v2/workspaces": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [workspace_json],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workspaces/3ee61405-3be2-4000-ba72-60d36757b95b": {
            "method": "GET",
            "status_code": 200,
            "json": workspace_json,
        },
        "https://aws-api.sigmacomputing.com/v2/members": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=dataset": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        # Only the consumer DM is returned by the /files listing — the producer
        # is a personal-space DM not tracked in /files.
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Test Model",
                        "type": "data-model",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2026-04-16T18:57:31.928Z",
                        "updatedAt": "2026-04-16T19:02:22.085Z",
                        "isArchived": False,
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        # /v2/dataModels listing — only the consumer workspace DM.
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Test Model",
                        "description": "Consumer DM",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2026-04-16T18:57:31.928Z",
                        "updatedAt": "2026-04-16T19:02:22.085Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{consumer_dm_url_id}",
                        "latestVersion": 1,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "path": "Acryl Data",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        # Consumer DM elements / columns / lineage.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": consumer_element_id,
                        "name": "Test Data",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": [],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "type": "data-model",
                        "dataModelId": producer_dm_id,
                        "name": "data.csv",
                        "connectionId": "1aff342f-6157-4589-ab9b-947c16b0bd7e",
                    },
                    {
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [f"{producer_dm_url_id}/{cross_dm_suffix}"],
                        "dataSourceIds": [f"{producer_dm_url_id}/{cross_dm_suffix}"],
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        # Personal-space producer DM — fetched on demand via GET /v2/dataModels/{urlId}.
        # Response uses ``dataModelUrlId`` (not ``urlId``) per the real API shape
        # probed 2026-04-22; our helper normalizes this.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_url_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dataModelId": producer_dm_id,
                "dataModelUrlId": producer_dm_url_id,
                "name": "My Data Model",
                "url": f"https://app.sigmacomputing.com/acryldata/data-model/{producer_dm_url_id}",
                "path": "My Documents",
                "latestVersion": 2,
                "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdAt": "2026-04-16T18:57:31.928Z",
                "updatedAt": "2026-04-16T19:02:22.085Z",
                # workspaceId intentionally absent / null
            },
        },
        # Producer DM elements / columns / lineage.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": producer_element_id,
                        "name": "data.csv",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": [],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "columnId": f"col-{producer_element_id}-val",
                        "elementId": producer_element_id,
                        "name": "value",
                        "label": "Value",
                        "formula": "",
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_dm_discovery_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)

    # --- Counter assertions ---
    assert report.data_model_external_references_discovered == 1, (
        f"expected 1 personal-space DM discovered, got "
        f"{report.data_model_external_references_discovered}"
    )
    assert report.data_model_external_reference_unresolved == 0
    assert report.data_model_element_cross_dm_upstreams_single_element_fallback == 1, (
        f"expected 1 single-element fallback, got "
        f"{report.data_model_element_cross_dm_upstreams_single_element_fallback}"
    )
    assert report.data_model_element_cross_dm_upstreams_resolved == 1
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 0

    with open(output_path) as f:
        mces = json.load(f)

    producer_element_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{producer_dm_id}.{producer_element_id},PROD)"
    consumer_element_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{consumer_dm_id}.{consumer_element_id},PROD)"

    # --- Assertion 1: producer Container emitted with isPersonalDataModel + path ---
    container_props_mces = [
        mce
        for mce in mces
        if mce.get("aspectName") == "containerProperties"
        and mce.get("aspect", {}).get("json", {}).get("customProperties", {}).get("dataModelId") == producer_dm_id
    ]
    assert len(container_props_mces) == 1, (
        f"expected 1 containerProperties for producer DM, got {len(container_props_mces)}"
    )
    producer_container_custom_props = (
        container_props_mces[0]["aspect"]["json"]["customProperties"]
    )
    assert producer_container_custom_props.get("isPersonalDataModel") == "true", (
        f"producer Container should have isPersonalDataModel=true, got "
        f"{producer_container_custom_props}"
    )
    assert producer_container_custom_props.get("path") == "My Documents", (
        f"producer Container should have path='My Documents', got "
        f"{producer_container_custom_props}"
    )

    # --- Assertion 2: producer element Dataset emitted with SchemaMetadata ---
    schema_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == producer_element_urn
        and mce.get("aspectName") == "schemaMetadata"
    ]
    assert len(schema_mces) == 1, (
        f"expected SchemaMetadata on producer element {producer_element_urn}, "
        f"got {len(schema_mces)}"
    )

    # --- Assertion 3: consumer element Dataset has UpstreamLineage → producer ---
    upstream_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == consumer_element_urn
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert len(upstream_mces) == 1, (
        f"expected 1 UpstreamLineage on consumer element, got {len(upstream_mces)}"
    )
    aspect_json = upstream_mces[0]["aspect"].get("json", upstream_mces[0]["aspect"])
    upstream_urns = [u.get("dataset") for u in aspect_json.get("upstreams", [])]
    assert producer_element_urn in upstream_urns, (
        f"consumer element should have upstream {producer_element_urn}, got {upstream_urns}"
    )

    # --- Assertion 5: producer Container has no workspace parent Container ---
    # The producer Container entity URN is derived from DataModelKey(producer_dm_id).
    # We verify no containerKey aspect on the producer Container references a
    # Workspace URN (i.e. no parent container is set for the orphan DM).
    producer_container_urn = next(
        (
            mce["entityUrn"]
            for mce in mces
            if mce.get("aspectName") == "containerProperties"
            and mce.get("aspect", {}).get("json", {}).get("customProperties", {}).get("dataModelId") == producer_dm_id
        ),
        None,
    )
    assert producer_container_urn is not None
    container_key_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == producer_container_urn
        and mce.get("aspectName") == "container"
    ]
    # container aspect presence means "has a parent container". For personal-space
    # DMs (workspaceId=None), no workspace parent should be emitted.
    assert len(container_key_mces) == 0, (
        f"orphan DM Container should have no workspace parent Container aspect, "
        f"but found: {container_key_mces}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_orphan_dm_unreachable(
    pytestconfig, tmp_path, requests_mock
):
    """Personal-space DM discovery when the DM is unreachable (HTTP 403).

    When ``GET /v2/dataModels/{urlId}`` returns 403 (service-account lacks
    permission to the user's personal space), the discovery loop should count
    the prefix as unresolvable and continue without raising. The consumer
    element is emitted without ``UpstreamLineage``; no producer entities appear.

    Assertions:
    1. data_model_external_reference_unresolved == 1.
    2. data_model_element_cross_dm_upstreams_dm_unknown == 1.
    3. Consumer Dataset is emitted without upstreamLineage aspect.
    4. No producer Container / Dataset URNs in the MCE stream.
    5. Pipeline completes without raising.
    """
    import json

    consumer_dm_id = "b584ddca-cfd6-4b72-97da-367fc0a5606d"
    consumer_dm_url_id = "5wwkxte74KSUpjT0C0b0sZ"
    consumer_element_id = "1DYf5I08WO"

    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    producer_dm_url_id = "3BtEwqctAlmKlYTJIQ8QFC"
    cross_dm_suffix = "vACRd1GzJS"

    workspace_json = {
        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
        "name": "Acryl Data",
        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
        "createdAt": "2024-05-10T09:00:00.000Z",
        "updatedAt": "2024-05-12T10:00:00.000Z",
    }

    override_data: Dict[str, Dict[str, Any]] = {
        "https://aws-api.sigmacomputing.com/v2/workspaces": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [workspace_json],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workspaces/3ee61405-3be2-4000-ba72-60d36757b95b": {
            "method": "GET",
            "status_code": 200,
            "json": workspace_json,
        },
        "https://aws-api.sigmacomputing.com/v2/members": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=dataset": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "id": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Test Model",
                        "type": "data-model",
                        "parentId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "parentUrlId": "1UGFyEQCHqwPfQoAec3xJ9",
                        "permission": "edit",
                        "path": "Acryl Data",
                        "badge": None,
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2026-04-16T18:57:31.928Z",
                        "updatedAt": "2026-04-16T19:02:22.085Z",
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
                        "dataModelId": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Test Model",
                        "description": "Consumer DM",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2026-04-16T18:57:31.928Z",
                        "updatedAt": "2026-04-16T19:02:22.085Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{consumer_dm_url_id}",
                        "latestVersion": 1,
                        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
                        "path": "Acryl Data",
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": consumer_element_id,
                        "name": "Test Data",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": [],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "type": "data-model",
                        "dataModelId": producer_dm_id,
                        "name": "data.csv",
                        "connectionId": "1aff342f-6157-4589-ab9b-947c16b0bd7e",
                    },
                    {
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [f"{producer_dm_url_id}/{cross_dm_suffix}"],
                        "dataSourceIds": [f"{producer_dm_url_id}/{cross_dm_suffix}"],
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
        # The personal-space producer DM returns 403 — service-account cannot see it.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_url_id}": {
            "method": "GET",
            "status_code": 403,
            "json": {"message": "Forbidden"},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_dm_unreachable_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    # Pipeline must complete without raising even when producer DM is 403.
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)

    # --- Assertion 1: unresolved counter ---
    assert report.data_model_external_reference_unresolved == 1, (
        f"expected 1 unresolved, got {report.data_model_external_reference_unresolved}"
    )
    # --- Assertion 2: cross-DM unknown counter ---
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 1, (
        f"expected 1 dm_unknown, got {report.data_model_element_cross_dm_upstreams_dm_unknown}"
    )
    assert report.data_model_external_references_discovered == 0

    with open(output_path) as f:
        mces = json.load(f)

    consumer_element_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{consumer_dm_id}.{consumer_element_id},PROD)"

    # --- Assertion 3: consumer Dataset emitted without UpstreamLineage ---
    consumer_upstream_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == consumer_element_urn
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert len(consumer_upstream_mces) == 0, (
        f"consumer Dataset should have no upstreamLineage when producer is 403, "
        f"got {len(consumer_upstream_mces)}"
    )

    # --- Assertion 4: no producer Container / Dataset URNs in the stream ---
    producer_prefix = producer_dm_id
    producer_mces = [
        mce
        for mce in mces
        if producer_prefix in mce.get("entityUrn", "")
    ]
    assert len(producer_mces) == 0, (
        f"no producer entities should be emitted when DM is 403, "
        f"got {len(producer_mces)}"
    )
