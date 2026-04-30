import json
from typing import Any, Dict, List, Optional, cast

import pytest

from datahub.emitter.mce_builder import make_container_urn
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sigma.config import SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import WorkspaceKey
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

    # Default empty columns response — tests that don't exercise formula resolution
    # override this; tests that do add their own entry via override_data.
    api_vs_response[
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/columns"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {"entries": [], "total": 0, "nextPage": None},
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


def _apply_dm_bridge_workbook_overrides(override_data: Dict[str, Dict]) -> None:
    """Add a workbook page whose elements reference the DM via ``data-model``
    lineage nodes. Exercises all three workbook-to-DM bridge outcomes:
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
    - intra-DM element-to-element UpstreamLineage
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
                    "ingest_data_models": True,
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
    """``data_model_pattern`` denies the DM, so no DM entities emitted and
    workbook elements previously bridging to the DM degrade to
    ``element_dm_edge.unresolved`` (bridge maps never registered)."""

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
                    "ingest_data_models": True,
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
    # the DM dataModelId UUID (not the urlId slug), so check by UUID.
    dm_element_present = any(
        "147a4d09-a686-4eea-b183-9b82aa0f7beb" in mce.get("entityUrn", "")
        for mce in mces
    )
    assert not dm_element_present, (
        "DM element Datasets should be filtered out by data_model_pattern"
    )

    report = _sigma_report(pipeline)
    assert report.element_dm_edge.resolved == 0
    assert report.element_dm_edge.name_unmatched_but_dm_known == 0
    # All three DM-bridge workbook elements must end up as unresolved because
    # the bridge maps were never populated (DM was denied).
    assert report.element_dm_edge.unresolved == 3, (
        f"expected 3 unresolved DM edges, got {report.element_dm_edge.unresolved}"
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
    with open(output_path) as f:
        mces = json.load(f)
    assert not any(
        "147a4d09-a686-4eea-b183-9b82aa0f7beb" in mce.get("entityUrn", "")
        for mce in mces
    )

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
def test_sigma_chart_input_fields(pytestconfig, tmp_path, requests_mock):
    """
    Exercises chart InputFields with formula-resolved upstreams.

    Fixture topology (all on page "InputFieldsPage"):
      - sourceElem   : table, upstream of downstream elements (warehouse table lineage)
      - warehouseElem: table, column "[FIVETRAN_LOG__SAMPLE/some_col]" -> warehouse URN
      - downstreamElem: table, column "[T Source/col]" -> sourceElem chart URN
      - noFormulaElem : table, columns have no formula -> zero InputFields entries
      - paramSiblingElem: columns "[P_param]" + "[bare_col]" -> zero entries (param + sibling)
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sigma"

    override_data: Dict[str, Dict] = {
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {"pageId": "InputFieldsPage", "name": "InputFields Page"},
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/pages/InputFieldsPage/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        # Source element — downstream's upstream via intra-workbook lineage.
                        "elementId": "sourceElem01",
                        "type": "table",
                        "name": "T Source",
                        "columns": ["col"],
                        "vizualizationType": "levelTable",
                    },
                    {
                        # Intra-workbook downstream: [T Source/col] -> sourceElem01 URN.
                        "elementId": "downstreamElem01",
                        "type": "table",
                        "name": "T Downstream",
                        "columns": ["col"],
                        "vizualizationType": "levelTable",
                    },
                    {
                        # Warehouse ref: [FIVETRAN_LOG__SAMPLE/some_col] resolved via SQL.
                        "elementId": "warehouseElem01",
                        "type": "table",
                        "name": "T Warehouse",
                        "columns": ["some_col"],
                        "vizualizationType": "levelTable",
                    },
                    {
                        # Columns without formulas -> zero InputFields entries.
                        "elementId": "noFormulaElem01",
                        "type": "table",
                        "name": "T No Formula",
                        "columns": ["col_a", "col_b"],
                        "vizualizationType": "levelTable",
                    },
                    {
                        # Parameter + sibling refs -> zero InputFields entries.
                        "elementId": "paramSiblingElem01",
                        "type": "table",
                        "name": "T Param Sibling",
                        "columns": ["monthly_target", "derived"],
                        "vizualizationType": "levelTable",
                    },
                ],
                "total": 5,
                "nextPage": None,
            },
        },
        # Real production path: formula data comes from GET /workbooks/{id}/columns.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": "sourceElem01",
                        "name": "col",
                        "formula": None,
                    },
                    {
                        "elementId": "downstreamElem01",
                        "name": "col",
                        "formula": "[T Source/col]",
                    },
                    {
                        "elementId": "warehouseElem01",
                        "name": "some_col",
                        "formula": "[FIVETRAN_LOG__SAMPLE/some_col]",
                    },
                    {
                        "elementId": "paramSiblingElem01",
                        "name": "monthly_target",
                        "formula": "[P_Monthly_Spend_Target]",
                    },
                    {
                        "elementId": "paramSiblingElem01",
                        "name": "derived",
                        "formula": "[monthly_target]",
                    },
                ],
                "total": 5,
                "nextPage": None,
            },
        },
        # sourceElem01: warehouse table upstream only (no sheet upstreams to resolve).
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/sourceElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_source": {
                        "nodeId": "tgt_source",
                        "elementId": "sourceElem01",
                        "name": "T Source",
                        "type": "sheet",
                    },
                    "inode-warehouseA": {
                        "nodeId": "inode-warehouseA",
                        "name": "SOME_WAREHOUSE_TABLE",
                        "type": "table",
                    },
                },
                "edges": [
                    {
                        "source": "inode-warehouseA",
                        "target": "tgt_source",
                        "type": "source",
                    }
                ],
            },
        },
        # downstreamElem01: intra-workbook sheet upstream -> sourceElem01.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/downstreamElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_downstream": {
                        "nodeId": "tgt_downstream",
                        "elementId": "downstreamElem01",
                        "name": "T Downstream",
                        "type": "sheet",
                    },
                    "src_source": {
                        "nodeId": "src_source",
                        "elementId": "sourceElem01",
                        "name": "T Source",
                        "type": "sheet",
                    },
                },
                "edges": [
                    {
                        "source": "src_source",
                        "target": "tgt_downstream",
                        "type": "source",
                    }
                ],
            },
        },
        # warehouseElem01: warehouse table upstream with a known SQL short-name.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/warehouseElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_warehouse": {
                        "nodeId": "tgt_warehouse",
                        "elementId": "warehouseElem01",
                        "name": "T Warehouse",
                        "type": "sheet",
                    },
                    "inode-fivetran": {
                        "nodeId": "inode-fivetran",
                        "name": "FIVETRAN_LOG__SAMPLE",
                        "type": "table",
                    },
                },
                "edges": [
                    {
                        "source": "inode-fivetran",
                        "target": "tgt_warehouse",
                        "type": "source",
                    }
                ],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/noFormulaElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_noformula": {
                        "nodeId": "tgt_noformula",
                        "elementId": "noFormulaElem01",
                        "name": "T No Formula",
                        "type": "sheet",
                    }
                },
                "edges": [],
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/lineage/elements/paramSiblingElem01": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dependencies": {
                    "tgt_paramsib": {
                        "nodeId": "tgt_paramsib",
                        "elementId": "paramSiblingElem01",
                        "name": "T Param Sibling",
                        "type": "sheet",
                    }
                },
                "edges": [],
            },
        },
        # SQL queries
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/sourceElem01/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/downstreamElem01/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        # warehouseElem01 has a SQL query so the parser can resolve the warehouse URN.
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/warehouseElem01/query": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "elementId": "warehouseElem01",
                "name": "T Warehouse",
                "sql": "select SOME_COL from LONG_TAIL_COMPANIONS.FIVETRAN.FIVETRAN_LOG__SAMPLE limit 1000",
            },
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/noFormulaElem01/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks/9bbbe3b0-c0c8-4fac-b6f1-8dfebfe74f8b/elements/paramSiblingElem01/query": {
            "method": "GET",
            "status_code": 404,
            "json": {},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path: str = f"{tmp_path}/sigma_chart_input_fields_mces.json"

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
    golden_file = "golden_test_sigma_extract_lineage.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )

    report = _sigma_report(pipeline)
    assert report.chart_input_fields_resolved == 2
    # 3 self-ref fallbacks: sourceElem01.col (formula=None), noFormulaElem01.col_a, .col_b
    assert report.chart_input_fields_self_ref_fallback == 3
    assert report.chart_input_fields_skipped_parameter == 1
    assert report.chart_input_fields_skipped_sibling == 1


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
        "147a4d09-a686-4eea-b183-9b82aa0f7beb" in mce.get("entityUrn", "")
        or "0466d89b8ce5ac9b2cd1deecdffe42c1" in mce.get("entityUrn", "")
        for mce in mces
    ), "DM entities should be dropped by workspace_pattern deny"


@pytest.mark.integration
def test_sigma_ingest_data_models_lineage_node_missing_name(
    pytestconfig, tmp_path, requests_mock
):
    """Workbook lineage ``data-model`` node without a ``name`` field counts
    under ``element_dm_edge.upstream_name_missing`` (not
    ``_name_unmatched_but_dm_known``). This mirrors the cross-DM path's
    ``_consumer_name_missing`` split so triage can distinguish "API gave
    us no name" from "a user renamed the DM element after linking"."""

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
    # dmRefElem01: DM node has no name, counts as element_dm_edge.upstream_name_missing.
    # dmRefElem02: name="random data model", resolves (ambiguous, but a hit).
    # dmRefElem03: name="name_not_in_dm", DM known but no matching element
    #              counts as element_dm_edge.name_unmatched_but_dm_known.
    assert report.element_dm_edge.upstream_name_missing == 1, (
        f"expected 1 upstream_name_missing, got "
        f"{report.element_dm_edge.upstream_name_missing}"
    )
    assert report.element_dm_edge.name_unmatched_but_dm_known == 1, (
        f"expected 1 name_unmatched_but_dm_known, got "
        f"{report.element_dm_edge.name_unmatched_but_dm_known}"
    )
    assert report.element_dm_edge.resolved == 1


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
    # One ambiguous chart-to-DM edge bumps the counter exactly once. The
    # diamond-pattern test
    # (``test_sigma_ingest_data_models_ambiguous_name_counter_not_duplicated_on_diamond``)
    # pins the no-over-count behavior for repeated sourceIds.
    assert report.element_dm_edge.ambiguous == 1, (
        f"expected element_dm_edge.ambiguous=1, got {report.element_dm_edge.ambiguous}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_edges_only_dm_ref_synthesized(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for the real Sigma API shape of workbook-to-DM element
    lineage (live-probed 2026-04-22 on a tenant workbook).

    Real tenants return the DM-reference node ``<dmUrlId>/<suffix>`` ONLY
    as an edge source — the node is NOT a key in the ``dependencies`` dict.
    Before the synthesis fix, the BFS loop raised ``KeyError`` when looking
    up the missing dependency entry, blanking the entire element's lineage
    and silently dropping every workbook-to-DM edge in production. This test
    reproduces that shape and asserts that:

    1. ``DataModelElementUpstream`` is synthesized from the edge alone,
    2. the workbook element's own ``name`` is used as the DM element name
       (Sigma's default — user rename degrades to the existing
       ``element_dm_edge.name_unmatched_but_dm_known`` counter), and
    3. the ``element_dm_edge.synthesized_from_edge_only`` counter tracks
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

    # All 3 workbook-to-DM refs travelled the synthesized path (none were
    # present in dependencies), so the counter bumps once per ref.
    assert report.element_dm_edge.synthesized_from_edge_only == 3, (
        f"expected 3 synthesized DM refs, got "
        f"{report.element_dm_edge.synthesized_from_edge_only}"
    )

    # dmRefElem01: "2313213123.test.231" (unique name, single DM element
    #              match resolves to 4plNusNz75).
    # dmRefElem02: "random data model" (ambiguous across 2 DM elements;
    #              deterministic pick-lowest resolves to 0ui59vLc38).
    # dmRefElem03: user-renamed name, unmatched; DM is known to this run
    #                so the name-unmatched-but-known counter bumps.
    assert report.element_dm_edge.resolved == 2, (
        f"expected 2 resolved bridge edges (01 + 02), got "
        f"{report.element_dm_edge.resolved}"
    )
    assert report.element_dm_edge.name_unmatched_but_dm_known == 1, (
        f"expected 1 rename-induced unmatched, got "
        f"{report.element_dm_edge.name_unmatched_but_dm_known}"
    )

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
    """Regression pin for DM-to-DM cross-reference upstream lineage.

    When a Sigma user imports a table from Data-Model-A into Data-Model-B,
    Sigma's ``/dataModels/{B_id}/lineage`` returns the consuming element's
    ``sourceIds`` in the shape ``<A_urlId>/<opaque_suffix>`` — the same
    shape as workbook-to-DM references (live-probed 2026-04-22 against a
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
    # Baseline case has one producer element named "Test Data" and one
    # consumer element; name-bridge must not trip the ambiguous branch.
    assert report.data_model_element_cross_dm_upstreams_ambiguous == 0
    assert report.data_model_element_cross_dm_upstreams_single_element_fallback == 0

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
    # ``ingest_shared_entities=True`` enables the discovery loop: orphan DMs
    # (personal-space, no workspace) can never emit under the default
    # ``False`` setting because ``get_data_models`` gates on workspace.
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
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
        and mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("dataModelId")
        == producer_dm_id
    ]
    assert len(container_props_mces) == 1, (
        f"expected 1 containerProperties for producer DM, got {len(container_props_mces)}"
    )
    producer_container_custom_props = container_props_mces[0]["aspect"]["json"][
        "customProperties"
    ]
    assert producer_container_custom_props.get("isPersonalDataModel") == "true", (
        f"producer Container should have isPersonalDataModel='true', got "
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

    # --- Assertion 3: consumer element Dataset has UpstreamLineage to producer ---
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

    # --- Assertion 4: producer element BrowsePathsV2 references the DM
    # Container URN exactly once. Regression pin for the "duplicate DM
    # Container in browse path" bug: when a DM has no workspace, the
    # browse-path builder previously added the DM Container once as the
    # parent fallback and a second time as the typed leaf, breaking
    # breadcrumbs in the UI.
    producer_browse_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == producer_element_urn
        and mce.get("aspectName") == "browsePathsV2"
    ]
    assert len(producer_browse_mces) == 1, (
        f"expected 1 browsePathsV2 aspect on producer element, got "
        f"{len(producer_browse_mces)}"
    )
    browse_path_entries = (
        producer_browse_mces[0]["aspect"]
        .get("json", producer_browse_mces[0]["aspect"])
        .get("path", [])
    )
    typed_container_urns = [
        entry.get("urn")
        for entry in browse_path_entries
        if entry.get("urn") and "container" in entry.get("urn", "")
    ]
    assert len(typed_container_urns) == len(set(typed_container_urns)), (
        f"producer element browse path should not repeat any Container URN, "
        f"got {typed_container_urns}"
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
            and mce.get("aspect", {})
            .get("json", {})
            .get("customProperties", {})
            .get("dataModelId")
            == producer_dm_id
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
    # ``ingest_shared_entities=True`` to reach the discovery loop; the 403
    # is what degrades the edge to ``dm_unknown`` + bumps unresolved.
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
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
    producer_mces = [mce for mce in mces if producer_prefix in mce.get("entityUrn", "")]
    assert len(producer_mces) == 0, (
        f"no producer entities should be emitted when DM is 403, "
        f"got {len(producer_mces)}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_orphan_dm_two_hop_discovery(
    pytestconfig, tmp_path, requests_mock
):
    """Two-hop personal-space DM discovery terminates cleanly.

    Pins the termination invariant of the discovery loop: each newly-fetched
    personal-space DM is itself scanned for cross-DM references, so a chain
    ``consumer (workspace) -> orphan B -> orphan C`` must be fully resolved
    across two loop iterations without re-fetching already-seen prefixes.

    This is the belt-and-braces check behind
    ``max_personal_dm_discovery_rounds``: the monotonically-growing
    ``unresolved_seen`` set guarantees termination under a well-behaved API;
    this test exercises a real two-hop payload to make that contract
    explicit.

    Assertions:
    1. Both orphan DMs are discovered (``data_model_external_references_discovered == 2``)
       and none are marked unresolved.
    2. ``GET /v2/dataModels/{urlId}`` is called exactly once per orphan
       prefix -- the second iteration must not re-fetch orphan B.
    3. Both orphan Containers are emitted with ``isPersonalDataModel="true"``.
    4. ``consumer -> orphan_b`` and ``orphan_b -> orphan_c`` cross-DM edges
       resolve (``data_model_element_cross_dm_upstreams_resolved == 2``).
    """

    consumer_dm_id = "b584ddca-cfd6-4b72-97da-367fc0a5606d"
    consumer_dm_url_id = "5wwkxte74KSUpjT0C0b0sZ"
    consumer_element_id = "1DYf5I08WO"

    orphan_b_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    orphan_b_dm_url_id = "3BtEwqctAlmKlYTJIQ8QFC"
    orphan_b_element_id = "wcUd3nEUAv"
    orphan_b_suffix_in_consumer = "vACRd1GzJS"

    orphan_c_dm_id = "a7c1c1f0-1111-2222-3333-444455556666"
    orphan_c_dm_url_id = "OrphanCUrlId000000000"
    orphan_c_element_id = "OrphanCElem"
    orphan_c_suffix_in_b = "TTTTTTTTTT"

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
            "json": {"entries": [workspace_json], "total": 1, "nextPage": None},
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
        # --- Consumer DM (round 0) ---
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
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [
                            f"{orphan_b_dm_url_id}/{orphan_b_suffix_in_consumer}"
                        ],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        # --- Orphan B (discovered round 1) -- references orphan C ---
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_b_dm_url_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dataModelId": orphan_b_dm_id,
                "dataModelUrlId": orphan_b_dm_url_id,
                "name": "Test Data",
                "url": f"https://app.sigmacomputing.com/acryldata/data-model/{orphan_b_dm_url_id}",
                "path": "My Documents",
                "latestVersion": 1,
                "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdAt": "2026-04-16T18:57:31.928Z",
                "updatedAt": "2026-04-16T19:02:22.085Z",
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_b_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": orphan_b_element_id,
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
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_b_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_b_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": orphan_b_element_id,
                        "type": "element",
                        "sourceIds": [f"{orphan_c_dm_url_id}/{orphan_c_suffix_in_b}"],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        # --- Orphan C (discovered round 2) -- terminal, no further cross-DM refs ---
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_c_dm_url_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dataModelId": orphan_c_dm_id,
                "dataModelUrlId": orphan_c_dm_url_id,
                "name": "Test Data",
                "url": f"https://app.sigmacomputing.com/acryldata/data-model/{orphan_c_dm_url_id}",
                "path": "My Documents",
                "latestVersion": 1,
                "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdAt": "2026-04-16T18:57:31.928Z",
                "updatedAt": "2026-04-16T19:02:22.085Z",
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_c_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": orphan_c_element_id,
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
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_c_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_c_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_dm_two_hop_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)

    assert report.data_model_external_references_discovered == 2, (
        f"expected 2 orphans discovered across two hops, got "
        f"{report.data_model_external_references_discovered}"
    )
    assert report.data_model_external_reference_unresolved == 0
    assert report.data_model_element_cross_dm_upstreams_resolved == 2, (
        f"expected 2 cross-DM edges resolved (consumer->B and B->C), got "
        f"{report.data_model_element_cross_dm_upstreams_resolved}"
    )
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 0

    # Termination-invariant guard: each orphan ``urlId`` must be fetched
    # exactly once. If the loop ever re-queued an already-seen prefix,
    # this count would jump to >=2 for at least one orphan.
    by_url_id_hits_b = [
        req
        for req in requests_mock.request_history
        if req.url.endswith(f"/dataModels/{orphan_b_dm_url_id}")
    ]
    by_url_id_hits_c = [
        req
        for req in requests_mock.request_history
        if req.url.endswith(f"/dataModels/{orphan_c_dm_url_id}")
    ]
    assert len(by_url_id_hits_b) == 1, (
        f"orphan B /dataModels/{{urlId}} must be fetched exactly once, "
        f"got {len(by_url_id_hits_b)}"
    )
    assert len(by_url_id_hits_c) == 1, (
        f"orphan C /dataModels/{{urlId}} must be fetched exactly once, "
        f"got {len(by_url_id_hits_c)}"
    )

    with open(output_path) as f:
        mces = json.load(f)

    # Both orphans should emit a Container with isPersonalDataModel="true".
    orphan_container_props_mces = [
        mce
        for mce in mces
        if mce.get("aspectName") == "containerProperties"
        and mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("isPersonalDataModel")
        == "true"
    ]
    assert len(orphan_container_props_mces) == 2, (
        f"expected 2 personal-space Container props across B and C, got "
        f"{len(orphan_container_props_mces)}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_orphan_dm_discovery_cap_surfaces_warning(
    pytestconfig, tmp_path, requests_mock
):
    """``max_personal_dm_discovery_rounds`` cap fires a loud
    ``SourceReport.warning`` and stops further orphan fetches, but
    only when there are unresolved prefixes to abandon (so the warning
    never fires spuriously when no cross-DM prefixes need fetching).

    This is the belt-and-braces sibling to
    ``test_sigma_ingest_data_models_orphan_dm_two_hop_discovery``: that
    test pins natural termination, this one pins the explicit cap. Uses
    the same consumer -> orphan B -> orphan C chain but sets the cap at
    ``1`` so the loop collects unresolved prefixes in round 1 (bridge
    prepop), sees orphan B as unresolved, and breaks with a warning
    before issuing any per-prefix fetch.
    Expected:
    - 0 orphans discovered (cap fires before the first per-prefix fetch).
    - Cap warning surfaced in ``SourceReport.warnings`` (non-empty
      unresolved set triggers it).
    - Consumer's cross-DM edge to orphan B degrades to ``dm_unknown``
      (never registered in bridges).
    - No producer Container / Dataset entities emitted (cap aborted fetch).
    - ``GET /v2/dataModels/{orphan_b_url_id}`` is NEVER called.
    """

    consumer_dm_id = "b584ddca-cfd6-4b72-97da-367fc0a5606d"
    consumer_dm_url_id = "5wwkxte74KSUpjT0C0b0sZ"
    consumer_element_id = "1DYf5I08WO"

    orphan_b_dm_url_id = "3BtEwqctAlmKlYTJIQ8QFC"
    orphan_b_suffix_in_consumer = "vACRd1GzJS"

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
            "json": {"entries": [workspace_json], "total": 1, "nextPage": None},
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
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [
                            f"{orphan_b_dm_url_id}/{orphan_b_suffix_in_consumer}"
                        ],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        # Orphan B by-urlId is wired up but must NEVER be fetched when the
        # cap is 1 -- the loop breaks before issuing any orphan fetch.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{orphan_b_dm_url_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dataModelId": "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624",
                "dataModelUrlId": orphan_b_dm_url_id,
                "name": "Should Not Be Fetched",
                "url": f"https://app.sigmacomputing.com/acryldata/data-model/{orphan_b_dm_url_id}",
                "path": "My Documents",
                "latestVersion": 1,
                "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdAt": "2026-04-16T18:57:31.928Z",
                "updatedAt": "2026-04-16T19:02:22.085Z",
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_dm_cap_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(
            output_path,
            ingest_shared_entities=True,
            max_personal_dm_discovery_rounds=1,
        )
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)

    # No orphan gets fetched -- the cap aborts the discovery loop before
    # the first per-prefix fetch.
    assert report.data_model_external_references_discovered == 0, (
        f"expected 0 orphans discovered when cap=1 fires, got "
        f"{report.data_model_external_references_discovered}"
    )
    # Consumer's cross-DM edge degrades to dm_unknown (DM never registered).
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 1

    # Cap warning must be surfaced -- operators need to know discovery was
    # aborted mid-run so they can raise the cap or investigate a cycle.
    assert any(
        "discovery cap reached" in w.title.lower()
        for w in report.warnings
        if w.title is not None
    ), (
        f"expected a ``discovery cap reached`` warning, got "
        f"{[w.title for w in report.warnings]}"
    )

    # Orphan B must NEVER be fetched -- the cap fires before the loop
    # issues any ``/dataModels/{urlId}`` call.
    orphan_b_hits = [
        req
        for req in requests_mock.request_history
        if req.url.endswith(f"/dataModels/{orphan_b_dm_url_id}")
    ]
    assert len(orphan_b_hits) == 0, (
        f"cap at round 1 must prevent the orphan B fetch, but saw "
        f"{len(orphan_b_hits)} fetches"
    )

    with open(output_path) as f:
        mces = json.load(f)

    # No producer URNs may appear in the output stream.
    producer_mces = [
        mce
        for mce in mces
        if "Should Not Be Fetched" in json.dumps(mce)
        or orphan_b_dm_url_id in mce.get("entityUrn", "")
    ]
    assert len(producer_mces) == 0, (
        f"no orphan B entities should be emitted when cap blocks the fetch, "
        f"got {len(producer_mces)}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_orphan_dm_malformed_payload_safe(
    pytestconfig, tmp_path, requests_mock
):
    """``get_data_model_by_url_id`` returning a payload that fails Pydantic
    validation (e.g. missing ``dataModelId`` on the server side) must not
    crash the ingestion.

    Contract pin: the by-urlId path's broad ``except Exception`` path
    returns None (so the caller bumps ``data_model_external_reference_unresolved``
    and the consumer's cross-DM edge degrades to ``dm_unknown``) AND emits
    a ``SourceReport.warning`` with a stable title so the failure is visible
    in the ingestion report — parity with the non-200 path. This guards the
    discovery loop against a Sigma-side regression we don't control (e.g.
    experimental endpoints that ship inconsistent field names).
    """

    consumer_dm_id = "b584ddca-cfd6-4b72-97da-367fc0a5606d"
    consumer_dm_url_id = "5wwkxte74KSUpjT0C0b0sZ"
    consumer_element_id = "1DYf5I08WO"

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
            "json": {"entries": [workspace_json], "total": 1, "nextPage": None},
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
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [f"{producer_dm_url_id}/{cross_dm_suffix}"],
                    }
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        # Malformed by-urlId payload: 200 OK, but ``dataModelId`` is absent.
        # ``SigmaDataModel.model_validate`` will raise ValidationError; the
        # broad ``except Exception`` in ``get_data_model_by_url_id`` must
        # swallow it and return None so ingestion continues.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_url_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dataModelUrlId": producer_dm_url_id,
                "name": "Corrupted Producer",
                "url": f"https://app.sigmacomputing.com/acryldata/data-model/{producer_dm_url_id}",
                # ``dataModelId`` intentionally omitted -- Pydantic must reject.
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_dm_malformed_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    # Pipeline MUST NOT raise even on a malformed by-urlId payload.
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)

    # The malformed payload path degrades identically to the non-200 path:
    # counter bumped, cross-DM edge dm_unknown, AND a report.warning emitted.
    assert report.data_model_external_reference_unresolved == 1, (
        f"expected 1 unresolved on malformed payload, got "
        f"{report.data_model_external_reference_unresolved}"
    )
    assert report.data_model_external_references_discovered == 0
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 1
    assert any(
        "exception" in (w.title or "").lower()
        or "orphan data model fetch raised" in (w.title or "").lower()
        for w in report.warnings
    ), (
        f"expected a report.warning from the except branch of "
        f"get_data_model_by_url_id, got: {[w.title for w in report.warnings]}"
    )

    with open(output_path) as f:
        mces = json.load(f)

    consumer_element_urn = f"urn:li:dataset:(urn:li:dataPlatform:sigma,{consumer_dm_id}.{consumer_element_id},PROD)"
    # Consumer Dataset must emit without a producer-pointing upstream.
    consumer_upstream_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == consumer_element_urn
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert len(consumer_upstream_mces) == 0, (
        f"consumer must have no upstreamLineage when producer payload is "
        f"malformed, got {len(consumer_upstream_mces)}"
    )


def _orphan_dm_mock_fixture() -> Dict[str, Dict[str, Any]]:
    """Build the orphan-DM discovery mock fixture: one workspace-listed
    consumer DM that references an element in a personal-space producer DM
    reachable by urlId. Returned as an ``override_data`` dict so callers can
    add/override entries before passing it to ``register_mock_api``.

    Used by the orphan-DM discovery tests to exercise the filter-bypass
    regressions (``ingest_shared_entities`` / ``data_model_pattern``).
    """
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

    return {
        "https://aws-api.sigmacomputing.com/v2/workspaces": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [workspace_json], "total": 1, "nextPage": None},
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
                    }
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
                    }
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
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_url_id}": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "dataModelId": producer_dm_id,
                "dataModelUrlId": producer_dm_url_id,
                "name": "My Personal DM",
                "url": f"https://app.sigmacomputing.com/acryldata/data-model/{producer_dm_url_id}",
                "path": "My Documents",
                "latestVersion": 2,
                "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                "createdAt": "2026-04-16T18:57:31.928Z",
                "updatedAt": "2026-04-16T19:02:22.085Z",
            },
        },
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
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
    }


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
def test_sigma_ingest_data_models_orphan_dm_blocked_by_ingest_shared_entities(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for the orphan-DM filter-bypass review finding.

    Personal-space DMs (``workspaceId=None``) live outside the workspace
    listing and used to be ingested unconditionally by the discovery loop —
    even when ``ingest_shared_entities=False`` (which is the default). This
    test asserts that with ``ingest_shared_entities=False`` the discovered
    orphan DM is dropped before any entity emission, its Container /
    Datasets never appear, and the cross-DM lineage edge degrades to
    ``data_model_element_cross_dm_upstreams_dm_unknown`` (the producer
    bridge was never registered).
    """

    override_data = _orphan_dm_mock_fixture()
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_filter_shared_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=False)
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    # Orphan was fetched (we observed the prefix) but then dropped by filter;
    # discovered counter must NOT increment (matches get_data_models' gating
    # behavior, which bumps ``dropped`` instead).
    assert report.data_model_external_references_discovered == 0, (
        f"expected 0 orphan DMs discovered with ingest_shared_entities=False, "
        f"got {report.data_model_external_references_discovered}"
    )
    # And the cross-DM edge should surface as "DM unknown" — the producer
    # bridge was never registered, so no name match is possible.
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 1, (
        f"expected 1 dm_unknown (filter-gated producer), got "
        f"{report.data_model_element_cross_dm_upstreams_dm_unknown}"
    )

    with open(output_path) as f:
        mces = json.load(f)
    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    producer_entities = [
        mce for mce in mces if producer_dm_id in mce.get("entityUrn", "")
    ]
    assert producer_entities == [], (
        f"no producer (orphan) entities should be emitted when filtered out, "
        f"got {len(producer_entities)}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_orphan_dm_blocked_by_data_model_pattern(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for the orphan-DM filter-bypass review finding (part 2).

    With ``ingest_shared_entities=True`` and ``data_model_pattern.deny``
    matching the personal-space DM's name, the discovered orphan DM must
    be dropped before any entity emission. Before the gate fix, the DM
    would slip through since discovery bypassed all filters.
    """

    override_data = _orphan_dm_mock_fixture()
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_orphan_filter_pattern_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(
            output_path,
            ingest_shared_entities=True,
            data_model_pattern={"deny": ["My Personal.*"]},
        )
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    assert report.data_model_external_references_discovered == 0, (
        f"expected 0 orphan DMs discovered when name is denied by "
        f"data_model_pattern, got {report.data_model_external_references_discovered}"
    )
    assert report.data_model_element_cross_dm_upstreams_dm_unknown == 1

    with open(output_path) as f:
        mces = json.load(f)
    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    producer_entities = [
        mce for mce in mces if producer_dm_id in mce.get("entityUrn", "")
    ]
    assert producer_entities == [], (
        f"no producer entities should be emitted when filtered by pattern, "
        f"got {len(producer_entities)}"
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
def test_sigma_ingest_data_models_cross_dm_self_reference_guarded(
    pytestconfig, tmp_path, requests_mock
):
    """A ``sourceId`` of shape ``<selfUrlId>/<suffix>`` — i.e. a DM element
    that references its own DM by urlId prefix — is defensively treated as
    unresolved rather than resolving to the same DM. This shape is not
    expected from the real API but the guard at
    ``_resolve_dm_element_cross_dm_upstream`` exists to protect against a
    malformed response. Pin the behavior here so the guard cannot be
    silently removed.
    """
    self_dm_id = "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    self_dm_url_id = "CDJLIyOhUoKBSEVI8Wr4n"
    self_element_id = "0ui59vLc38"

    override_data = get_mock_data_model_api()
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{self_dm_id}/lineage"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "type": "element",
                    "elementId": self_element_id,
                    "sourceIds": [f"{self_dm_url_id}/fakeSelfSuffix"],
                }
            ],
            "total": 1,
            "nextPage": None,
        },
    }
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_self_reference_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    # Self-reference must not resolve as a cross-DM upstream.
    assert report.data_model_element_cross_dm_upstreams_resolved == 0
    # Dedicated self-reference counter distinguishes this API-payload anomaly
    # from a generic unresolved failure (triage signal would otherwise be
    # lost in the shared unresolved bucket).
    assert report.data_model_element_cross_dm_upstreams_self_reference == 1
    # And must bump the standard unresolved counter (neither intra nor
    # external nor valid cross-DM).
    assert report.data_model_element_upstreams_unresolved >= 1


@pytest.mark.integration
def test_sigma_ingest_data_models_cross_dm_diamond_counter_not_inflated(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for M1 (DM-element side): when a consumer DM element's
    ``sourceIds`` contains multiple entries that all resolve to the same
    cross-DM producer element URN (a "diamond"), the success counters must
    count the edge once, not once per ``sourceId``.

    Before the dedup-gating fix, two source_ids sharing the same resolved
    URN would bump ``data_model_element_cross_dm_upstreams_resolved`` twice
    even though the emitted ``UpstreamLineage`` (deduped via ``seen``)
    carries only one ``Upstream`` entry, breaking the operator-facing
    "edges emitted" signal and the ``_resolved − _ambiguous`` arithmetic
    in config.py.

    Construction: consumer DM has one element whose ``sourceIds`` carry two
    distinct cross-DM suffixes against the same producer DM. The name-bridge
    resolves both to the same producer element URN.
    """
    producer_dm_id = "aaaa1111-aaaa-1111-aaaa-1111aaaa1111"
    producer_dm_url_id = "ProducerDMurlId00000"
    producer_element_id = "1DYf5I08WO"

    consumer_dm_id = "bbbb2222-bbbb-2222-bbbb-2222bbbb2222"
    consumer_dm_url_id = "ConsumerDMurlId00000"
    consumer_element_id = "HdbgI9D-Ci"

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
            "json": {"entries": [workspace_json], "total": 1, "nextPage": None},
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
                        "name": "Consumer DM",
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
                        "name": "Producer DM",
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
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": consumer_dm_id,
                        "urlId": consumer_dm_url_id,
                        "name": "Consumer DM",
                        "description": "",
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
                        "name": "Producer DM",
                        "description": "",
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
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": producer_element_id,
                        "name": "Shared Data",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": ["id"],
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/columns": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/elements": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": consumer_element_id,
                        "name": "Shared Data",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": ["id"],
                    },
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
        # Consumer element has TWO distinct cross-DM sourceIds against the
        # same producer DM. The name-bridge resolves both to the same URN.
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/lineage": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "elementId": consumer_element_id,
                        "type": "element",
                        "sourceIds": [
                            f"{producer_dm_url_id}/suffixOne",
                            f"{producer_dm_url_id}/suffixTwo",
                        ],
                    },
                ],
                "total": 1,
                "nextPage": None,
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_diamond_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    # Two source_ids collapsed to one URN. Counter must bump once, not twice.
    assert report.data_model_element_cross_dm_upstreams_resolved == 1, (
        f"diamond source_ids must dedupe to a single _resolved bump; got "
        f"{report.data_model_element_cross_dm_upstreams_resolved}"
    )
    # Baseline producer has one element named "Shared Data" matching
    # consumer name; neither sub-shape (ambiguous / single_element_fallback)
    # should fire.
    assert report.data_model_element_cross_dm_upstreams_ambiguous == 0
    assert report.data_model_element_cross_dm_upstreams_single_element_fallback == 0

    consumer_element_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,"
        f"{consumer_dm_id}.{consumer_element_id},PROD)"
    )
    expected_upstream_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,"
        f"{producer_dm_id}.{producer_element_id},PROD)"
    )
    with open(output_path) as f:
        mces = json.load(f)
    upstream_mces = [
        mce
        for mce in mces
        if mce.get("entityUrn") == consumer_element_urn
        and mce.get("aspectName") == "upstreamLineage"
    ]
    assert len(upstream_mces) == 1
    aspect_json = (
        upstream_mces[0]
        .get("aspect", {})
        .get("json", upstream_mces[0].get("aspect", {}))
    )
    upstream_urns = [u.get("dataset") for u in aspect_json.get("upstreams", [])]
    # Emitted lineage must carry exactly one upstream (already deduped via
    # ``seen``); the test pins that the counter matches the emitted shape.
    assert upstream_urns == [expected_upstream_urn], (
        f"consumer element should carry a single deduped upstream; got {upstream_urns}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_cross_dm_single_element_fallback_requires_total_count(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for M3: the single-element fallback must require the
    producer DM to have exactly one element **total**, not just one *named*
    element. Blank-named elements are excluded from
    ``dm_element_urn_by_name`` (see ``_prepopulate_dm_bridge_maps``), so a
    DM with 1 named + N blank-named elements would previously have
    spuriously triggered the fallback and attributed a cross-DM edge to
    the single named element even though Sigma could legitimately be
    pointing at any of the anonymous ones.

    Construction: producer DM with 1 named element ("data.csv") and 1
    blank-named element. Consumer element name "Test Data" does not match.
    The fallback must refuse (name_unmatched_but_dm_known, not
    single_element_fallback) because the producer has 2 elements.
    """

    override_data = _orphan_dm_mock_fixture()
    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    # Add a second, blank-named element to the producer DM. The fallback
    # must refuse to pick the single named element because the producer
    # genuinely has two elements; Sigma could be pointing at either.
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{producer_dm_id}/elements"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "elementId": "wcUd3nEUAv",
                    "name": "data.csv",
                    "type": "table",
                    "vizualizationType": "levelTable",
                    "columns": [],
                },
                {
                    "elementId": "anonBlank001",
                    "name": "",
                    "type": "table",
                    "vizualizationType": None,
                    "columns": [],
                },
            ],
            "total": 2,
            "nextPage": None,
        },
    }
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_fallback_total_count_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    # Fallback must NOT fire — producer has 2 elements total.
    assert report.data_model_element_cross_dm_upstreams_single_element_fallback == 0, (
        f"single-element fallback must refuse when DM has >1 element total "
        f"(even if only 1 is named); got "
        f"{report.data_model_element_cross_dm_upstreams_single_element_fallback}"
    )
    # Degrades to name_unmatched_but_dm_known instead.
    assert report.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known == 1

    # And no upstream edge should point at the producer element.
    consumer_element_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:sigma,"
        "b584ddca-cfd6-4b72-97da-367fc0a5606d.1DYf5I08WO,PROD)"
    )
    producer_element_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:sigma,{producer_dm_id}.wcUd3nEUAv,PROD)"
    )
    with open(output_path) as f:
        mces = json.load(f)
    for mce in mces:
        if (
            mce.get("entityUrn") == consumer_element_urn
            and mce.get("aspectName") == "upstreamLineage"
        ):
            aspect_json = mce.get("aspect", {}).get("json", mce.get("aspect", {}))
            for up in aspect_json.get("upstreams", []):
                assert up.get("dataset") != producer_element_urn, (
                    f"fallback must not attribute cross-DM edge to single named "
                    f"element when DM has blank-named siblings; got edge "
                    f"{consumer_element_urn} -> {producer_element_urn}"
                )


@pytest.mark.integration
def test_sigma_ingest_data_models_isPersonalDataModel_lowercase(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for M4: ``customProperties.isPersonalDataModel`` is
    emitted as the lowercase string ``"true"`` (JSON boolean convention
    used elsewhere in DataHub). Previous revisions flipped between ``"True"``
    and ``"true"`` across doc and code; this test locks the current
    contract so future drift is caught at CI time.
    """

    override_data = _orphan_dm_mock_fixture()
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_personal_lowercase_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)

    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    producer_container_props = next(
        mce
        for mce in mces
        if mce.get("aspectName") == "containerProperties"
        and mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("dataModelId")
        == producer_dm_id
    )
    custom_props = (
        producer_container_props.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
    )
    # Exact value and exact casing — both matter (previous drift emitted "True").
    assert custom_props.get("isPersonalDataModel") == "true", (
        f"isPersonalDataModel must be exact lowercase 'true' (JSON convention); "
        f"got {custom_props.get('isPersonalDataModel')!r}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_bridge_key_collision_first_wins(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for the ``_prepopulate_dm_bridge_maps`` collision
    branch: two DMs claiming the same ``urlId`` (a documented corner case
    on older Sigma tenants where a slug is reissued after the original
    asset is deleted).

    Behavior: the first registration wins the bridge key, and the second
    DM is **skipped from emission entirely**. Previously, the second DM
    would emit a Container + element Datasets that cross-DM and workbook-to-DM
    references could never link to (every lineage edge routes to the
    first DM by the bridge key), quietly polluting the graph with an
    orphan DM. The ``data_models_bridge_key_collision`` counter must
    bump so operators can audit affected tenants.
    """

    override_data = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)

    # Inject a second DM listing that reuses the same urlId as the
    # baseline fixture's DM ("CDJLIyOhUoKBSEVI8Wr4n"). Different UUID,
    # different name, but identical urlId. The second registration
    # should be rejected (first wins).
    duplicate_dm_id = "ddddddd-dddd-dddd-dddd-dddddddddddd"
    existing_listing_url = "https://aws-api.sigmacomputing.com/v2/dataModels"
    existing_listing = override_data[existing_listing_url]["json"]
    existing_listing["entries"].append(
        {
            "dataModelId": duplicate_dm_id,
            "urlId": "CDJLIyOhUoKBSEVI8Wr4n",
            "name": "Reissued Slug DM",
            "description": "Same urlId, different UUID",
            "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
            "createdAt": "2026-04-16T18:57:31.928Z",
            "updatedAt": "2026-04-16T19:02:22.085Z",
            "url": "https://app.sigmacomputing.com/acryldata/data-model/CDJLIyOhUoKBSEVI8Wr4n",
            "latestVersion": 1,
            "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
            "path": "Acryl Data",
        }
    )
    existing_listing["total"] = len(existing_listing["entries"])
    # get_data_models gates on file_meta.workspaceId (not the listing's
    # workspaceId), so the duplicate also needs a /files entry or it is
    # dropped as "no workspace" before reaching the bridge-map collision.
    files_listing = override_data[
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model"
    ]["json"]
    files_listing["entries"].append(
        {
            "id": duplicate_dm_id,
            "urlId": "CDJLIyOhUoKBSEVI8Wr4n",
            "name": "Reissued Slug DM",
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
        }
    )
    files_listing["total"] = len(files_listing["entries"])
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{duplicate_dm_id}/elements"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "elementId": "dupElemId00",
                    "name": "dup element",
                    "type": "table",
                    "vizualizationType": None,
                    "columns": [],
                }
            ],
            "total": 1,
            "nextPage": None,
        },
    }
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{duplicate_dm_id}/columns"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {"entries": [], "total": 0, "nextPage": None},
    }
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{duplicate_dm_id}/lineage"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {"entries": [], "total": 0, "nextPage": None},
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_collision_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)
    original_container = [
        mce
        for mce in mces
        if mce.get("aspectName") == "containerProperties"
        and mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("dataModelId")
        == "147a4d09-a686-4eea-b183-9b82aa0f7beb"
    ]
    duplicate_entities = [
        mce
        for mce in mces
        if duplicate_dm_id in mce.get("entityUrn", "")
        or mce.get("aspect", {})
        .get("json", {})
        .get("customProperties", {})
        .get("dataModelId")
        == duplicate_dm_id
    ]
    assert len(original_container) == 1, (
        "original DM container should still be emitted despite bridge collision"
    )
    assert duplicate_entities == [], (
        "colliding DM must not emit any entities — it cannot be linked by "
        "cross-DM or workbook-to-DM lineage (the first DM owns the bridge "
        f"key), so emitting it would create an orphan node. Got: "
        f"{[mce.get('entityUrn') for mce in duplicate_entities]}"
    )

    report = _sigma_report(pipeline)
    assert report.data_models_bridge_key_collision == 1, (
        f"expected collision counter to increment, got "
        f"{report.data_models_bridge_key_collision}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_cross_dm_consumer_blank_name(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for M5: a consuming DM element with a blank name
    bumps a **distinct** counter (``consumer_name_missing``) rather than
    the rename-adjacent ``name_unmatched_but_dm_known`` counter. The
    separation exists so report triage can distinguish "consumer element
    has no name at all" from "DM element rename broke the bridge".
    """
    override_data = _orphan_dm_mock_fixture()
    consumer_dm_id = "b584ddca-cfd6-4b72-97da-367fc0a5606d"
    # Flip the consumer element's name to blank — the cross-DM ref will
    # short-circuit at the ``not consuming_element.name`` branch.
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/dataModels/{consumer_dm_id}/elements"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "entries": [
                {
                    "elementId": "1DYf5I08WO",
                    "name": "",
                    "type": "table",
                    "vizualizationType": "levelTable",
                    "columns": [],
                }
            ],
            "total": 1,
            "nextPage": None,
        },
    }
    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_blank_consumer_name_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    # Distinct counter must bump, and the rename-adjacent counter must NOT.
    assert report.data_model_element_cross_dm_upstreams_consumer_name_missing == 1, (
        f"expected 1 consumer_name_missing, got "
        f"{report.data_model_element_cross_dm_upstreams_consumer_name_missing}"
    )
    assert (
        report.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known == 0
    ), (
        f"name_unmatched_but_dm_known must not conflate blank-consumer case; "
        f"got {report.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known}"
    )
    assert report.data_model_element_cross_dm_upstreams_resolved == 0
    assert report.data_model_element_cross_dm_upstreams_single_element_fallback == 0


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
def test_sigma_ingest_data_models_ambiguous_name_counter_not_duplicated_on_diamond(
    pytestconfig, tmp_path, requests_mock
):
    """Diamond-pattern ambiguity: a single workbook chart references an
    ambiguous-named DM element via **multiple** ``data-model`` lineage
    nodes (same DM, same name, different opaque suffixes). All nodes
    resolve to the same DM element URN, so only the first becomes a
    ``ChartInfo.inputs`` entry and the rest hit the dedupe branch.

    Regression pin for M3 fix: ``element_dm_edge.ambiguous`` should bump
    **once** (the ambiguity is a DM-side property — one ambiguous DM
    element reached from one chart), not once per sourceId — otherwise
    report triage over-counts and the ambiguity-warning log spams.
    """

    override_data = get_mock_data_model_api()
    _apply_dm_bridge_workbook_overrides(override_data)

    # Two DM elements share the ambiguous name "random data model".
    # dmRefElem02 reaches *both* via 3 diamond-pattern data-model nodes —
    # all 3 resolve to the sorted-smallest elementId (``0ui59vLc38``).
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
                "CDJLIyOhUoKBSEVI8Wr4n/diamond_a": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/diamond_a",
                    "type": "data-model",
                    "name": "random data model",
                },
                "CDJLIyOhUoKBSEVI8Wr4n/diamond_b": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/diamond_b",
                    "type": "data-model",
                    "name": "random data model",
                },
                "CDJLIyOhUoKBSEVI8Wr4n/diamond_c": {
                    "nodeId": "CDJLIyOhUoKBSEVI8Wr4n/diamond_c",
                    "type": "data-model",
                    "name": "random data model",
                },
            },
            "edges": [
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/diamond_a",
                    "target": "tgt_dmref_02",
                    "type": "source",
                },
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/diamond_b",
                    "target": "tgt_dmref_02",
                    "type": "source",
                },
                {
                    "source": "CDJLIyOhUoKBSEVI8Wr4n/diamond_c",
                    "target": "tgt_dmref_02",
                    "type": "source",
                },
            ],
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_dm_diamond_ambig_mces.json"
    pipeline = Pipeline.create(_minimal_sigma_pipeline_config(output_path))
    pipeline.run()
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)

    # One chart-to-DM-element edge should land (the dedupe collapses the
    # 3 diamond sourceIds into 1 ChartInfo.inputs entry).
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
    # Exactly one occurrence of the resolved DM URN — no diamond duplication.
    assert resolved_urns.count(expected_resolved_urn) == 1, (
        f"expected exactly 1 inputs entry for ambiguous DM element, got "
        f"{resolved_urns.count(expected_resolved_urn)}"
    )

    report = _sigma_report(pipeline)
    # Ambiguity is a property of the DM element (not per-sourceId), so
    # the counter must bump exactly once for this chart-to-DM edge.
    assert report.element_dm_edge.ambiguous == 1, (
        f"expected element_dm_edge.ambiguous=1 (per-unique-chart-to-DM-edge), "
        f"got {report.element_dm_edge.ambiguous}"
    )
    # The other two diamond paths are dedupe hits.
    assert report.element_dm_edge.deduped >= 2, (
        f"expected >=2 element_dm_edge.deduped hits on a 3-way diamond, "
        f"got {report.element_dm_edge.deduped}"
    )
    # Two chart-to-DM edges resolve across the workbook: dmRefElem01
    # (single non-diamond ref, from _apply_dm_bridge_workbook_overrides)
    # and dmRefElem02 (the diamond — 3 sourceIds collapse to 1 edge).
    # dmRefElem03 name-unmatches. The diamond dedup is validated above by
    # ``element_dm_edge.deduped >= 2`` and the ChartInfo.inputs count.
    assert report.element_dm_edge.resolved == 2


@pytest.mark.integration
def test_sigma_ingest_data_models_workspace_bypass_via_discovery(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for the discovery-path workspace_pattern bypass.

    A workspace-scoped DM in a workspace denied by ``workspace_pattern``
    is not returned by ``get_data_models`` (which gates on workspace
    before bridge registration). When an allowed DM cross-references an
    element in the denied-workspace DM, the discovery loop previously
    fetched the DM by ``urlId`` (``/v2/dataModels/{urlId}``) and emitted
    it unconditionally — workspace_pattern was never re-applied.

    After the fix, the discovery branch mirrors ``get_data_models``'s
    gating: if the fetched DM has a ``workspaceId`` and the workspace is
    denied, drop it. Emitted entities for the denied DM must stay empty
    and the cross-DM edge degrades to ``dm_unknown``.
    """

    override_data = _orphan_dm_mock_fixture()

    # Promote the producer from orphan (workspaceId=None, "My Documents")
    # to a real workspace-scoped DM that workspace_pattern would deny.
    denied_workspace_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    override_data[
        "https://aws-api.sigmacomputing.com/v2/dataModels/3BtEwqctAlmKlYTJIQ8QFC"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "dataModelId": "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624",
            "dataModelUrlId": "3BtEwqctAlmKlYTJIQ8QFC",
            "name": "Workspace-scoped producer",
            "url": "https://app.sigmacomputing.com/acryldata/data-model/3BtEwqctAlmKlYTJIQ8QFC",
            "path": "Denied Workspace",
            "latestVersion": 2,
            "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
            "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
            "createdAt": "2026-04-16T18:57:31.928Z",
            "updatedAt": "2026-04-16T19:02:22.085Z",
            "workspaceId": denied_workspace_id,
        },
    }
    override_data[
        f"https://aws-api.sigmacomputing.com/v2/workspaces/{denied_workspace_id}"
    ] = {
        "method": "GET",
        "status_code": 200,
        "json": {
            "workspaceId": denied_workspace_id,
            "name": "Denied Workspace",
            "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
            "createdAt": "2024-05-10T09:00:00.000Z",
            "updatedAt": "2024-05-12T10:00:00.000Z",
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_workspace_bypass_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(
            output_path,
            ingest_shared_entities=True,
            workspace_pattern={"deny": ["Denied Workspace"]},
        )
    )
    pipeline.run()
    pipeline.raise_from_status()

    report = _sigma_report(pipeline)
    assert report.data_model_external_references_discovered == 0, (
        f"workspace_pattern must gate discovered DMs, got "
        f"{report.data_model_external_references_discovered} "
        f"discovered in a denied workspace"
    )

    with open(output_path) as f:
        mces = json.load(f)
    producer_dm_id = "766ea1d1-5ee0-4a9c-9b68-b8ba19a7f624"
    producer_entities = [
        mce for mce in mces if producer_dm_id in mce.get("entityUrn", "")
    ]
    assert producer_entities == [], (
        f"producer DM in workspace-denied workspace must not emit any "
        f"entities via the discovery backdoor; got "
        f"{[m.get('entityUrn') for m in producer_entities]}"
    )


@pytest.mark.integration
def test_sigma_ingest_data_models_discovery_order_deterministic(
    pytestconfig, tmp_path, requests_mock
):
    """Regression pin for hash-randomized discovery ordering.

    ``_collect_unresolved_cross_dm_prefixes`` returns a ``Set[str]`` that
    the discovery loop iterates. Python set iteration is hash-randomized
    across interpreter runs, so two personal-space DMs discovered in the
    same iteration previously landed in ``all_data_models`` in
    run-to-run-varying order. That ordering flows straight into workunit
    emission, affecting golden files and any first-write-wins downstream
    behavior. Fix: ``sorted(unresolved)``.

    This test builds two orphan DMs with lexicographic-inverse urlIds
    (``Aaa…`` < ``Zzz…``) and asserts the ``A`` DM's container workunit
    is emitted before the ``Z`` DM's — which only holds if the loop
    iterates sorted.
    """

    consumer_dm_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    consumer_url_id = "ConsumerUrlId000000"

    orphan_a_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    orphan_a_url_id = "AaaOrderedOrphan000"
    orphan_z_id = "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"
    orphan_z_url_id = "ZzzOrderedOrphan000"

    workspace_json = {
        "workspaceId": "3ee61405-3be2-4000-ba72-60d36757b95b",
        "name": "Acryl Data",
        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
        "createdAt": "2024-05-10T09:00:00.000Z",
        "updatedAt": "2024-05-12T10:00:00.000Z",
    }

    def _orphan_endpoints(dm_id: str, url_id: str, name: str) -> Dict[str, Any]:
        return {
            f"https://aws-api.sigmacomputing.com/v2/dataModels/{url_id}": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "dataModelId": dm_id,
                    "dataModelUrlId": url_id,
                    "name": name,
                    "url": f"https://app.sigmacomputing.com/acryldata/data-model/{url_id}",
                    "path": "My Documents",
                    "latestVersion": 1,
                    "ownerId": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                    "createdBy": "awUuH3HDr10r2c41vSZ5MNcyCDYZl",
                    "createdAt": "2026-04-16T18:57:31.928Z",
                    "updatedAt": "2026-04-16T19:02:22.085Z",
                },
            },
            f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/elements": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "entries": [
                        {
                            "elementId": "elem01",
                            "name": name,
                            "type": "table",
                            "vizualizationType": "levelTable",
                            "columns": [],
                        }
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
            f"https://aws-api.sigmacomputing.com/v2/dataModels/{dm_id}/lineage": {
                "method": "GET",
                "status_code": 200,
                "json": {"entries": [], "total": 0, "nextPage": None},
            },
        }

    override_data: Dict[str, Dict[str, Any]] = {
        "https://aws-api.sigmacomputing.com/v2/workspaces": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [workspace_json], "total": 1, "nextPage": None},
        },
        f"https://aws-api.sigmacomputing.com/v2/workspaces/{workspace_json['workspaceId']}": {
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
        "https://aws-api.sigmacomputing.com/v2/files?typeFilters=data-model": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/workbooks": {
            "method": "GET",
            "status_code": 200,
            "json": {"entries": [], "total": 0, "nextPage": None},
        },
        "https://aws-api.sigmacomputing.com/v2/dataModels": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "entries": [
                    {
                        "dataModelId": consumer_dm_id,
                        "urlId": consumer_url_id,
                        "name": "Consumer DM",
                        "description": "",
                        "createdBy": "CPbEdA26GNQ2cM2Ra2BeO0fa5Awz1",
                        "createdAt": "2026-04-16T18:57:31.928Z",
                        "updatedAt": "2026-04-16T19:02:22.085Z",
                        "url": f"https://app.sigmacomputing.com/acryldata/data-model/{consumer_url_id}",
                        "latestVersion": 1,
                        "workspaceId": workspace_json["workspaceId"],
                        "path": "Acryl Data",
                    }
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
                        "elementId": "consumerElemA",
                        "name": "AaaOrderedOrphan000",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": [],
                    },
                    {
                        "elementId": "consumerElemZ",
                        "name": "ZzzOrderedOrphan000",
                        "type": "table",
                        "vizualizationType": "levelTable",
                        "columns": [],
                    },
                ],
                "total": 2,
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
                        "elementId": "consumerElemA",
                        "type": "element",
                        "sourceIds": [f"{orphan_a_url_id}/suffixA"],
                        "dataSourceIds": [f"{orphan_a_url_id}/suffixA"],
                    },
                    {
                        "elementId": "consumerElemZ",
                        "type": "element",
                        "sourceIds": [f"{orphan_z_url_id}/suffixZ"],
                        "dataSourceIds": [f"{orphan_z_url_id}/suffixZ"],
                    },
                ],
                "total": 2,
                "nextPage": None,
            },
        },
    }
    override_data.update(
        _orphan_endpoints(orphan_a_id, orphan_a_url_id, "AaaOrderedOrphan000")
    )
    override_data.update(
        _orphan_endpoints(orphan_z_id, orphan_z_url_id, "ZzzOrderedOrphan000")
    )

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    output_path = f"{tmp_path}/sigma_discovery_order_mces.json"
    pipeline = Pipeline.create(
        _minimal_sigma_pipeline_config(output_path, ingest_shared_entities=True)
    )
    pipeline.run()
    pipeline.raise_from_status()

    with open(output_path) as f:
        mces = json.load(f)

    # Emission order in the MCE stream for the two orphan Containers.
    orphan_container_emission: List[str] = []
    for mce in mces:
        if mce.get("aspectName") != "containerProperties":
            continue
        custom = mce.get("aspect", {}).get("json", {}).get("customProperties", {})
        dm_id = custom.get("dataModelId")
        if dm_id == orphan_a_id:
            orphan_container_emission.append("A")
        elif dm_id == orphan_z_id:
            orphan_container_emission.append("Z")

    assert orphan_container_emission == ["A", "Z"], (
        f"discovered DMs must emit in sorted(urlId) order, got "
        f"{orphan_container_emission}"
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
