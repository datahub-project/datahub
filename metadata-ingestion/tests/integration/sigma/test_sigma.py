import json
from typing import Any, Dict, Optional, cast

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
    - direct sheet-to-sheet edge (nodeId != elementId)
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
                        # enters the elementId-to-chart_urn map.
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
                        # Verifies that elementId-to-chart_urn is built at workbook scope,
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
        # Also includes an unrelated edge (unrelated_sheet_node to unrelated_target)
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
        # The sheet edge (upstreamElem01 to join) appears directly in the edges list,
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
        # elementId-to-chart_urn map is workbook-scoped (page-scoped would miss it).
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
    - intra-DM element lineage (element 3 to element 1)
    - external upstream: element 1 sourced from an existing Sigma Dataset
      (``PETS`` with urlId ``49HFLTr6xytgrPly3PFsNC``)
    - workbook elements referencing DM elements via the ``data-model`` lineage
      node type — the workbook-to-DM bridge exercises both the name-match primary
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
    # Every caller of this helper is a DM-focused test; set
    # ``ingest_data_models: True`` here (the source default is False per the
    # opt-in release behavior) so the tests don't have to repeat the flag.
    # Callers that want to override it can still pass ingest_data_models=False
    # via ``**extra``.
    config: Dict[str, Any] = {
        "client_id": "CLIENTID",
        "client_secret": "CLIENTSECRET",
        "chart_sources_platform_mapping": {
            "Acryl Data/Acryl Workbook": {"data_source_platform": "snowflake"},
        },
        "ingest_data_models": True,
    }
    config.update(extra)
    return {
        "run_id": "sigma-test",
        "source": {"type": "sigma", "config": config},
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


@pytest.mark.integration
def test_sigma_ingest_data_models_external_dataset_not_ingested(
    pytestconfig, tmp_path, requests_mock
):
    """Previously-synthesized Sigma Dataset URN path is **not** taken anymore.

    Previously, when DM /lineage recorded a ``type: dataset`` node whose
    ``inode-<urlId>`` did not match any Sigma Dataset ingested in the same
    run (filtered out by ``dataset_pattern`` / ``workspace_pattern`` /
    ``ingest_shared_entities=False``), the resolver fabricated a Sigma
    Dataset URN from the suffix. Code review flagged this as a source of
    dangling nodes in the graph (the URN had no schema, no owners, no other
    aspects). This test now pins the correct behavior: **no** upstream edge
    is emitted for such references, and the ``data_model_element_upstreams_unresolved``
    counter is bumped once so operators can still observe the drop in
    ingestion telemetry.
    """

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

    with open(output_path) as f:
        mces = json.load(f)

    # The xloKCITNsP element references inode-unregDs000000001, a dataset
    # node that was not ingested this run. Previously the resolver fabricated
    # a Sigma Dataset URN from the suffix; that behavior was removed to
    # avoid emitting dangling graph nodes. No UpstreamLineage edge should
    # point at the fabricated URN, and the unresolved counter should capture
    # the drop.
    phantom_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,unregDs000000001,PROD)"
    element2_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:sigma,"
        "147a4d09-a686-4eea-b183-9b82aa0f7beb.xloKCITNsP,PROD)"
    )
    for mce in mces:
        if (
            mce.get("entityUrn") == element2_urn
            and mce.get("aspectName") == "upstreamLineage"
        ):
            aspect_json = mce.get("aspect", {}).get("json", mce.get("aspect", {}))
            assert all(
                up.get("dataset") != phantom_urn
                for up in aspect_json.get("upstreams", [])
            ), (
                f"fabricated Sigma Dataset URN {phantom_urn} should no longer "
                f"appear as upstream of {element2_urn}"
            )

    # The phantom URN must also not appear as the ``entityUrn`` of any
    # emitted MCE — dangling entities are precisely what this fix prevents.
    assert all(mce.get("entityUrn") != phantom_urn for mce in mces), (
        f"no entity should be emitted for the un-ingested dataset {phantom_urn}"
    )

    report = _sigma_report(pipeline)
    # Pin exact counter values so C1 (shape-aware dedup in
    # ``_get_data_model_lineage_entries``) cannot regress silently:
    # the fixture has exactly 3 elements -> 1 intra-DM edge (4plNus…
    # -> 0ui59vLc38), 1 resolved external edge (0ui59vLc38 -> PETS),
    # 1 unresolved external edge (xloKCITNsP -> un-ingested
    # UnregisteredDataset), and 0 unknown-shape entries.
    assert report.data_model_element_intra_upstreams == 1, (
        f"expected exactly 1 intra-DM upstream, got "
        f"{report.data_model_element_intra_upstreams}"
    )
    assert report.data_model_element_external_upstreams == 1, (
        f"expected exactly 1 resolved external upstream (PETS), got "
        f"{report.data_model_element_external_upstreams}"
    )
    assert report.data_model_element_upstreams_unresolved == 1, (
        f"expected exactly 1 unresolved upstream (un-ingested dataset), got "
        f"{report.data_model_element_upstreams_unresolved}"
    )
    assert report.data_model_element_upstreams_unresolved_external == 1, (
        f"unresolved entry is an ``inode-`` external, not an unknown shape; "
        f"got _unresolved_external="
        f"{report.data_model_element_upstreams_unresolved_external}"
    )
    assert report.data_model_element_upstreams_unknown_shape == 0, (
        f"fixture has no cross-DM or unknown-shape source_ids; got "
        f"_unknown_shape={report.data_model_element_upstreams_unknown_shape}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_unresolved_counters_dedup_duplicate_source_id(
    pytestconfig, tmp_path, requests_mock
):
    """Unresolved counters must dedup by ``source_id`` to mirror the
    success-path dedup.

    Regression guard: a vendor payload that repeats the same failed
    ``inode-X`` inside one element's ``sourceIds`` (diamond reference to
    the same un-ingested target) previously bumped
    ``data_model_element_upstreams_unresolved`` and
    ``data_model_element_upstreams_unresolved_external`` once per
    repetition, while the successful counters above correctly counted
    once per unique URN. Over-counting makes the "Sigma is partially
    broken" dashboards noisy. The fix tracks an ``unresolved_seen`` set
    keyed by ``source_id`` so unresolved buckets count once per distinct
    failure too.
    """

    override_data = get_mock_data_model_api()
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/lineage"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "type": "dataset",
                    "name": "UnregisteredDataset",
                    "inodeId": "inode-unregDs000000001",
                },
                {
                    "type": "element",
                    "elementId": "0ui59vLc38",
                    # Same un-ingested inode referenced three times --
                    # the counter fix must collapse these into one bump.
                    "sourceIds": [
                        "inode-unregDs000000001",
                        "inode-unregDs000000001",
                        "inode-unregDs000000001",
                    ],
                },
            ],
            "total": 2,
            "nextPage": None,
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_unresolved_dedup_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    assert report.data_model_element_upstreams_unresolved_external == 1, (
        f"duplicate source_id must count once in _unresolved_external, "
        f"got {report.data_model_element_upstreams_unresolved_external}"
    )
    assert report.data_model_element_upstreams_unresolved == 1, (
        f"duplicate source_id must count once in the aggregate unresolved "
        f"counter, got {report.data_model_element_upstreams_unresolved}"
    )
    assert report.data_model_element_upstreams_unknown_shape == 0
    assert report.data_model_element_external_upstreams == 0


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

    with open(output_path) as f:
        mces = json.load(f)

    dm_urn = "urn:li:container:0466d89b8ce5ac9b2cd1deecdffe42c1"
    assert any(
        mce.get("entityUrn") == dm_urn
        and mce.get("aspectName") == "containerProperties"
        for mce in mces
    ), "DM Container should still be emitted despite lineage 500"

    # No upstreamLineage aspects should exist for any DM element. Filter
    # on the ``dataModelId`` UUID (the URN-keying component) so this is a
    # real regression guard -- ``urlId`` would never appear in a URN and
    # make the assertion pass tautologically.
    dm_uuid = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    dm_element_upstreams = [
        mce
        for mce in mces
        if dm_uuid in mce.get("entityUrn", "")
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert dm_element_upstreams == [], (
        "no UpstreamLineage should be emitted when /lineage returns 500"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_extract_lineage_false(
    pytestconfig, tmp_path, requests_mock
):
    """``ingest_data_models=True`` + ``extract_lineage=False`` is the
    documented "catalog without upstreams" opt-out.

    Pins three invariants at once so the gating cannot regress silently:

    1. No ``/dataModels/{id}/lineage`` HTTP call is issued (the opt-out is
       enforced in ``_assemble_data_model``, not just on the consumer side).
    2. DM Containers, element Datasets, and their ``SchemaMetadata`` are
       still emitted -- operators still get a DM catalog.
    3. No ``upstreamLineage`` aspect is emitted for any DM element -- users
       who opt out are not surprised by empty-upstream aspects overwriting
       graph edges sourced from other connectors.
    """

    register_mock_api(
        request_mock=requests_mock, override_data=get_mock_data_model_api()
    )

    output_path = f"{tmp_path}/sigma_dm_extract_lineage_false_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, extract_lineage=False)
    )
    pipeline.run()
    pipeline.raise_from_status()

    lineage_hits = [
        req
        for req in requests_mock.request_history
        if "/dataModels/" in req.url and req.url.endswith("/lineage")
    ]
    assert lineage_hits == [], (
        f"expected extract_lineage=False to short-circuit the DM /lineage "
        f"call, but got {len(lineage_hits)} requests: "
        f"{[r.url for r in lineage_hits]}"
    )

    with open(output_path) as f:
        mces = json.load(f)

    dm_urn = "urn:li:container:0466d89b8ce5ac9b2cd1deecdffe42c1"
    assert any(
        mce.get("entityUrn") == dm_urn
        and mce.get("aspectName") == "containerProperties"
        for mce in mces
    ), "DM Container should still be emitted with extract_lineage=False"

    # DM element URNs embed the immutable ``dataModelId`` UUID (not the
    # mutable ``urlId``) per the URN-keying invariant documented in
    # updating-datahub.md -- filter on the UUID so this is a real positive
    # assertion, not one that tautologically collapses to ``[]``.
    dm_uuid = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    element_schema_aspects = [
        mce
        for mce in mces
        if dm_uuid in mce.get("entityUrn", "")
        and mce.get("aspectName") == "schemaMetadata"
    ]
    assert element_schema_aspects, (
        "DM element SchemaMetadata should still be emitted with "
        "extract_lineage=False (catalog-without-upstreams shape)"
    )

    dm_element_upstreams = [
        mce
        for mce in mces
        if dm_uuid in mce.get("entityUrn", "")
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert dm_element_upstreams == [], (
        "no UpstreamLineage should be emitted when extract_lineage=False; "
        "users opting out must not get empty-upstream aspects that would "
        "overwrite edges from other connectors"
    )

    report = _sigma_report(pipeline)
    assert report.data_model_element_intra_upstreams == 0
    assert report.data_model_element_external_upstreams == 0
    assert report.data_model_element_upstreams_unresolved == 0


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

    with open(output_path) as f:
        mces = json.load(f)
    assert any(
        mce.get("entityUrn") == "urn:li:container:0466d89b8ce5ac9b2cd1deecdffe42c1"
        for mce in mces
    ), "DM Container should be emitted under ingest_shared_entities=True"


@pytest.mark.integration
def test_sigma_ingest_data_models_pattern_filter(pytestconfig, tmp_path, requests_mock):
    """``data_model_pattern`` deny should drop the DM: no Container and no
    element Datasets emitted for it."""

    override_data = get_mock_data_model_api()
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_pattern_deny_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(
            output_path, data_model_pattern={"deny": ["My Data Model.*"]}
        )
    )
    pipeline.run()
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)
    assert not any(
        "147a4d09-a686-4eea-b183-9b82aa0f7beb" in mce.get("entityUrn", "")
        or "CDJLIyOhUoKBSEVI8Wr4n" in mce.get("entityUrn", "")
        for mce in mces
    ), "DM Container and element Datasets should be filtered out by data_model_pattern"


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

    with open(output_path) as f:
        mces = json.load(f)
    assert not any(
        "CDJLIyOhUoKBSEVI8Wr4n" in mce.get("entityUrn", "")
        or "0466d89b8ce5ac9b2cd1deecdffe42c1" in mce.get("entityUrn", "")
        for mce in mces
    ), "DM entities should be dropped by workspace_pattern deny"


@pytest.mark.integration
def test_sigma_ingest_data_models_default_off(pytestconfig, tmp_path, requests_mock):
    """Regression pin for the default value of ``ingest_data_models``.

    The default is ``False`` (opt-in) per the release classification. This
    test instantiates the source with **no** explicit ``ingest_data_models``
    flag, runs against a full fixture that includes DM mocks, and asserts
    that neither the DM listing nor any per-DM endpoint is hit. If the
    default ever flips back to ``True``, this test fails fast so the change
    is intentional and gets a Breaking-Changes release note.
    """

    # Use the DM-enabled baseline fixture so an accidental flip to ``True``
    # would actually make requests.
    override_data = get_mock_data_model_api()
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_default_off_mces.json"
    # Construct the config without the helper (the helper injects
    # ``ingest_data_models: True`` — this test specifically exercises the
    # unset / default path).
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
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    dm_endpoint_hits = [
        req
        for req in requests_mock.request_history
        if "/v2/dataModels" in req.url or "typeFilters=data-model" in req.url
    ]
    assert dm_endpoint_hits == [], (
        f"expected ingest_data_models default=False to short-circuit DM fetch, "
        f"but got {len(dm_endpoint_hits)} requests: "
        f"{[r.url for r in dm_endpoint_hits]}"
    )

    with open(output_path) as f:
        mces = json.load(f)
    # No DM-Container or DM-element URNs should appear in the output.
    assert not any(
        "147a4d09-a686-4eea-b183-9b82aa0f7beb" in mce.get("entityUrn", "")
        for mce in mces
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_elements_http_error(
    pytestconfig, tmp_path, requests_mock
):
    """Partial-failure regression: ``/dataModels/{id}/elements`` returning 500
    leaves the rest of the run healthy. The DM Container should still be
    emitted with zero elements, and the pipeline must not raise.
    """

    override_data = get_mock_data_model_api()
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/elements"
    ] = {"method": "GET", "status_code": 500, "json": {}}

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_elements_5xx_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    # Must not raise — _paginated_entries swallows and returns [].
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)
    dm_id = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    # The DM Container should still be emitted.
    container_mces = [
        mce
        for mce in mces
        if mce.get("entityType") == "container"
        and mce.get("aspectName") == "containerProperties"
        and mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("dataModelId")
        == dm_id
    ]
    assert len(container_mces) == 1, (
        f"DM Container should still be emitted on /elements 5xx, got "
        f"{len(container_mces)}"
    )
    # No element Dataset URNs should have been emitted (elements list is empty).
    element_mces = [
        mce
        for mce in mces
        if mce.get("entityType") == "dataset"
        and f"{dm_id}." in mce.get("entityUrn", "")
    ]
    assert element_mces == [], (
        f"no DM-element Datasets should be emitted on /elements 5xx, "
        f"got {len(element_mces)}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_columns_http_error(
    pytestconfig, tmp_path, requests_mock
):
    """Partial-failure regression: ``/dataModels/{id}/columns`` returning 500
    leaves the rest of the run healthy. The DM Container and element
    Datasets are still emitted; element ``SchemaMetadata`` is simply empty
    (no columns). The pipeline must not raise.

    Parallel to the ``/elements 500`` and ``/lineage 500`` coverage —
    confirms the third DM-sub-endpoint failure shape is handled via the
    same ``_paginated_entries`` swallow path rather than crashing the run.
    """

    override_data = get_mock_data_model_api()
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/147a4d09-a686-4eea-b183-9b82aa0f7beb/columns"
    ] = {"method": "GET", "status_code": 500, "json": {}}

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_columns_5xx_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)

    dm_id = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    container_mces = [
        mce
        for mce in mces
        if mce.get("entityType") == "container"
        and mce.get("aspectName") == "containerProperties"
        and mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("dataModelId")
        == dm_id
    ]
    assert len(container_mces) == 1, (
        f"DM Container should still be emitted on /columns 5xx, got "
        f"{len(container_mces)}"
    )
    element_datasets = [
        mce
        for mce in mces
        if mce.get("entityType") == "dataset"
        and f"{dm_id}." in mce.get("entityUrn", "")
        and mce.get("aspectName") == "datasetProperties"
    ]
    assert len(element_datasets) >= 1, (
        "DM-element Datasets should still be emitted even when /columns fails"
    )
    # SchemaMetadata aspects should have empty fields because columns
    # endpoint returned [] — pin the degraded-but-healthy shape.
    schema_metas = [
        mce
        for mce in mces
        if mce.get("entityType") == "dataset"
        and f"{dm_id}." in mce.get("entityUrn", "")
        and mce.get("aspectName") == "schemaMetadata"
    ]
    for sm in schema_metas:
        aspect = sm.get("aspect", {}).get("json", sm.get("aspect", {}))
        assert aspect.get("fields", []) == [], (
            f"schemaMetadata should have no fields on /columns 5xx, "
            f"got {aspect.get('fields')} on {sm.get('entityUrn')}"
        )


@pytest.mark.integration
def test_sigma_ingest_data_models_workspaceId_mismatch_uses_files(
    pytestconfig, tmp_path, requests_mock
):
    """C2 regression: when ``/files`` reports workspaceId=A but the
    ``/dataModels`` payload carries workspaceId=B (a DM that was moved,
    or a Sigma-side inconsistency between the two endpoints),
    ``get_data_models`` filters under workspace A (the ``/files``
    preference is documented as "authoritative for the folder tree")
    and :meth:`_assemble_data_model` must now also *render* under
    workspace A. Previously a ``if data_model.workspaceId is None``
    guard short-circuited the fill-from-file, leaving
    ``data_model.workspaceId = B`` for per-workspace counters and
    browse-path construction -- filtered in under A, rendered under B.

    Pinned invariants:

    1. The DM ends up in the workspace counters under the ``/files``
       workspace (A), not the payload workspace (B).
    2. The ``/dataModels`` payload workspace id (B) does not acquire
       any DM-related count -- otherwise a single DM would show up
       twice in two workspaces' dashboards.
    """
    FILES_WS = "3ee61405-3be2-4000-ba72-60d36757b95b"
    PAYLOAD_WS = "payload-ws-22222222-3333-4444-5555-666666666666"

    override_data = get_mock_data_model_api()
    # /files for this DM reports FILES_WS (the authoritative one).
    # The base fixture already uses FILES_WS in /files, so leave it.
    # /dataModels payload, however, diverges to PAYLOAD_WS -- this is
    # the Sigma-side inconsistency the C2 reviewer called out.
    override_data["https://aws-api.sigmacomputing.com/v2/dataModels"]["json"][
        "entries"
    ][0]["workspaceId"] = PAYLOAD_WS

    # Mock /workspaces/{PAYLOAD_WS} so even if the bug regresses and
    # the code tries to look up the payload workspace, the lookup
    # succeeds (otherwise the test would pass vacuously because both
    # pre- and post-fix code would drop the DM for "no workspace").
    override_data[f"https://aws-api.sigmacomputing.com/v2/workspaces/{PAYLOAD_WS}"] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "workspaceId": PAYLOAD_WS,
            "name": "Payload Workspace (should NOT receive the DM)",
            "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
            "updatedBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
            "createdAt": "2024-03-12T08:31:04.826Z",
            "updatedAt": "2024-03-12T08:31:04.826Z",
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_ws_mismatch_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    files_counts = report.workspaces.workspace_counts.get(FILES_WS)
    payload_counts = report.workspaces.workspace_counts.get(PAYLOAD_WS)

    assert files_counts is not None and files_counts.data_models_count == 1, (
        "DM must be counted under the /files workspace (authoritative for "
        "the folder tree), not the /dataModels payload workspace. Got "
        f"files_counts={files_counts}"
    )
    assert files_counts.data_model_elements_count == 3, (
        "All 3 DM elements must be counted under the /files workspace so "
        "filtering, rendering, and per-workspace telemetry agree on a "
        "single workspace. Got "
        f"files_counts.data_model_elements_count={files_counts.data_model_elements_count}"
    )
    assert payload_counts is None or payload_counts.data_models_count == 0, (
        "Payload-workspace counters must stay at zero -- a DM that appears "
        "in both workspaces' dashboards is precisely the user-visible "
        "split-brain C2 is protecting against. Got "
        f"payload_counts={payload_counts}"
    )

    with open(output_path) as f:
        mces = json.load(f)

    # The DM element BrowsePathsV2 must reference the /files workspace
    # container URN as its root, not the payload workspace's.
    # Construct both expected container URNs so the assertion fails
    # loudly if rendering regresses onto the payload workspace.
    from datahub.emitter.mce_builder import make_container_urn
    from datahub.ingestion.source.sigma.data_classes import WorkspaceKey

    files_ws_container_urn = make_container_urn(
        WorkspaceKey(workspaceId=FILES_WS, platform="sigma")
    )
    payload_ws_container_urn = make_container_urn(
        WorkspaceKey(workspaceId=PAYLOAD_WS, platform="sigma")
    )
    element_urn_prefix = (
        "urn:li:dataset:(urn:li:dataPlatform:sigma,"
        "147a4d09-a686-4eea-b183-9b82aa0f7beb."
    )
    element_browse_paths_seen = 0
    for mce in mces:
        entity_urn = mce.get("entityUrn", "")
        if not entity_urn.startswith(element_urn_prefix):
            continue
        if mce.get("aspectName") != "browsePathsV2":
            continue
        aspect_json = mce.get("aspect", {}).get("json", mce.get("aspect", {}))
        path_ids = [entry.get("id") for entry in aspect_json.get("path", [])]
        assert files_ws_container_urn in path_ids, (
            f"DM element {entity_urn} BrowsePathsV2 must root at the "
            f"/files workspace container URN ({files_ws_container_urn}); "
            f"got path_ids={path_ids}"
        )
        assert payload_ws_container_urn not in path_ids, (
            f"DM element {entity_urn} must NOT have the /dataModels "
            f"payload workspace ({payload_ws_container_urn}) anywhere in "
            f"its BrowsePathsV2; got path_ids={path_ids}"
        )
        element_browse_paths_seen += 1
    assert element_browse_paths_seen == 3, (
        f"expected BrowsePathsV2 for all 3 DM elements; got {element_browse_paths_seen}"
    )
