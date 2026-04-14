"""
Mock-based integration test for the MicroStrategy connector.

Uses requests_mock to intercept all HTTP calls and returns fixture data
covering all entity types: project, folder, dashboard (dossier), report,
cube (with schema), and dataset.

Run with:
    pytest tests/integration/microstrategy/test_microstrategy_mock.py -v
"""

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-03-12 14:00:00"
BASE_URL = "http://mstr-mock.example.com/MicroStrategyLibrary"
AUTH_TOKEN = "mock-auth-token-abc123"

# ── Fixture IDs ───────────────────────────────────────────────────────────────
PROJECT_ID = "AABBCCDD1234567890AABBCCDD123456"
FOLDER_ID = "FF001122334455667788990011223344"
DASHBOARD_ID = "DD001122334455667788990011223344"
REPORT_ID = "RR001122334455667788990011223344"
CUBE_ID = "CC001122334455667788990011223344"
DATASET_ID = "DATSET001122334455667788990011"

# ── Fixture payloads ──────────────────────────────────────────────────────────

PROJECTS_RESPONSE = [
    {
        "id": PROJECT_ID,
        "name": "Mock Project",
        "description": "A mock project for testing.",
        "status": 0,
        "alias": "",
        "owner": {"id": "USR0001", "name": "Admin"},
        "dateCreated": "2023-01-01T00:00:00.000+0000",
        "dateModified": "2024-01-01T00:00:00.000+0000",
    }
]

PROJECT_DATASOURCES_RESPONSE = [
    {
        "id": "DS0001",
        "name": "Snowflake Prod",
        "datasourceType": "normal",
        "database": {"type": "snow_flake"},
        "dbms": {"name": "Snowflake"},
    }
]

FOLDERS_RESPONSE = [
    {
        "id": FOLDER_ID,
        "name": "Public Reports",
        "description": "Shared report folder.",
        "type": 8,
        "subtype": 8192,
        "dateCreated": "2023-01-01T00:00:00.000+0000",
        "dateModified": "2023-06-01T00:00:00.000+0000",
        "owner": {"id": "USR0001", "name": "Admin"},
    }
]

# type=55 search — dashboards/dossiers
SEARCH_TYPE55_RESPONSE = [
    {
        "id": DASHBOARD_ID,
        "name": "Sales Dashboard",
        "description": "Top-level sales overview dossier.",
        "type": 55,
        "subtype": 14336,  # modern dossier
        "dateCreated": "2023-03-01T00:00:00.000+0000",
        "dateModified": "2024-02-01T00:00:00.000+0000",
        "owner": {"id": "USR0001", "name": "Admin"},
    }
]

# type=3 search — reports
SEARCH_TYPE3_RESPONSE = [
    {
        "id": REPORT_ID,
        "name": "Revenue by Region",
        "description": "Monthly revenue breakdown.",
        "type": 3,
        "subtype": 768,  # Grid Report
        "dateCreated": "2023-02-01T00:00:00.000+0000",
        "dateModified": "2024-01-15T00:00:00.000+0000",
        "owner": {"id": "USR0001", "name": "Admin"},
    }
]

# type=3 search — reports with dataSource for lineage test (report → cube → warehouse)
SEARCH_TYPE3_LINEAGE_RESPONSE = [
    {
        "id": REPORT_ID,
        "name": "Revenue by Region",
        "description": "Monthly revenue breakdown.",
        "type": 3,
        "subtype": 768,  # Grid Report
        "dateCreated": "2023-02-01T00:00:00.000+0000",
        "dateModified": "2024-01-15T00:00:00.000+0000",
        "owner": {"id": "USR0001", "name": "Admin"},
        "dataSource": {"id": CUBE_ID, "name": "Revenue Cube"},
    }
]

# type=776 search — cubes (used by _build_registries + _yield_cube_workunits)
SEARCH_TYPE776_RESPONSE = [
    {
        "id": CUBE_ID,
        "name": "Revenue Cube",
        "description": "Intelligent cube for revenue analysis.",
        "type": 3,
        "subtype": 776,
        "dateCreated": "2023-01-15T00:00:00.000+0000",
        "dateModified": "2024-01-10T00:00:00.000+0000",
        "owner": {"id": "USR0001", "name": "Admin"},
    }
]

# Dossier definition for the dashboard
DOSSIER_DEFINITION = {
    "id": DASHBOARD_ID,
    "name": "Sales Dashboard",
    "chapters": [
        {
            "name": "Chapter 1",
            "pages": [
                {
                    "name": "Page 1",
                    "visualizations": [
                        {
                            "key": "W59F0EAC3D4EC4EDAA6E94AC17FA3BE4",
                            "name": "Revenue Chart",
                            "type": "chart",
                            "datasetPositions": [
                                {
                                    "datasetId": CUBE_ID,
                                    "datasetName": "Revenue Cube",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ],
    "datasets": [
        {
            "id": CUBE_ID,
            "name": "Revenue Cube",
            "type": "normal_dataset",
        }
    ],
}

# Cube schema definition
CUBE_SCHEMA = {
    "id": CUBE_ID,
    "definition": {
        "availableObjects": {
            "attributes": [
                {
                    "id": "ATT001",
                    "name": "Region",
                    "description": "Sales region",
                    "forms": [
                        {
                            "id": "F001",
                            "name": "ID",
                            "dataType": "integer",
                            "baseFormCategory": "ID",
                        }
                    ],
                },
                {
                    "id": "ATT002",
                    "name": "Product Category",
                    "description": "Product hierarchy",
                    "forms": [
                        {
                            "id": "F002",
                            "name": "DESC",
                            "dataType": "varChar",
                            "baseFormCategory": "DESC",
                        },
                        {
                            "id": "F003",
                            "name": "ID",
                            "dataType": "integer",
                            "baseFormCategory": "ID",
                        },
                    ],
                },
            ],
            "metrics": [
                {
                    "id": "MET001",
                    "name": "Revenue",
                    "description": "Total revenue",
                }
            ],
        }
    },
}

# Dataset (library dataset, not cube)
DATASETS_RESPONSE = [
    {
        "id": DATASET_ID,
        "name": "Customer Dataset",
        "description": "Customer dimension dataset.",
        "type": 3,
        "dateCreated": "2023-04-01T00:00:00.000+0000",
        "dateModified": "2024-01-20T00:00:00.000+0000",
        "owner": {"id": "USR0001", "name": "Admin"},
    }
]


# Paginated wrapper helper
def _paginated(items):
    return {"result": items, "total": len(items)}


def _empty_paginated():
    return {"result": [], "total": 0}


# ── Test ──────────────────────────────────────────────────────────────────────


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_microstrategy_ingest_mock(pytestconfig, tmp_path, requests_mock):
    """
    Mock-based end-to-end test for the MicroStrategy connector.

    Mocks all HTTP endpoints and validates connector output against a committed
    golden file covering: project container, folder container, dashboard entity,
    chart stub (visualization), report entity, cube dataset + schema, and library dataset.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/microstrategy"

    # ── Auth ──────────────────────────────────────────────────────────────────
    requests_mock.post(
        f"{BASE_URL}/api/auth/login",
        headers={"X-MSTR-AuthToken": AUTH_TOKEN},
        json={},
    )
    requests_mock.put(f"{BASE_URL}/api/sessions", json={})
    requests_mock.post(f"{BASE_URL}/api/auth/logout", json={})

    # ── Projects ──────────────────────────────────────────────────────────────
    requests_mock.get(f"{BASE_URL}/api/projects", json=PROJECTS_RESPONSE)

    # ── Datasources (warehouse platform detection) ────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/projects/{PROJECT_ID}/datasources",
        json=PROJECT_DATASOURCES_RESPONSE,
    )

    # ── Folders ───────────────────────────────────────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/v2/projects/{PROJECT_ID}/folders",
        json=_paginated(FOLDERS_RESPONSE),
    )

    # ── Search results — route by ?type= query param so call order doesn't matter ─
    _search_mapping = {
        "776": _paginated(SEARCH_TYPE776_RESPONSE),
        "55": _paginated(SEARCH_TYPE55_RESPONSE),
        "3": _paginated(SEARCH_TYPE3_RESPONSE),
    }

    def _search_callback(request, context):
        type_val = request.qs.get("type", [""])[0]
        return _search_mapping.get(type_val, _empty_paginated())

    requests_mock.get(
        f"{BASE_URL}/api/searches/results",
        json=_search_callback,
    )

    # ── Dossier definition (lineage for dashboard) ────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/v2/dossiers/{DASHBOARD_ID}/definition",
        json=DOSSIER_DEFINITION,
    )

    # ── Cube schema ───────────────────────────────────────────────────────────
    # get_cube_schema calls GET /api/v2/cubes/{id} (NOT /definition).
    # The cube definition endpoint returns definition.availableObjects with schema.
    requests_mock.get(
        f"{BASE_URL}/api/v2/cubes/{CUBE_ID}",
        json=CUBE_SCHEMA,
    )

    # ── Datasets (library datasets via _build_registries + _yield_library_dataset) ─
    requests_mock.get(
        f"{BASE_URL}/api/v2/projects/{PROJECT_ID}/datasets",
        json=_paginated(DATASETS_RESPONSE),
    )

    # ── Run pipeline ──────────────────────────────────────────────────────────
    pipeline = Pipeline.create(
        {
            "run_id": "microstrategy-mock-test",
            "source": {
                "type": "microstrategy",
                "config": {
                    "connection": {
                        "base_url": BASE_URL,
                        "username": "admin",
                        "password": "admin",
                    },
                    "include_lineage": True,
                    "include_ownership": True,
                    "include_folders": True,
                    "include_dashboards": True,
                    "include_reports": True,
                    "include_cubes": True,
                    "include_cube_schema": True,
                    "include_datasets": True,
                    "include_warehouse_lineage": False,  # Keep mock surface small
                    "include_report_definitions": False,
                    "preflight_dashboard_exists": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(tmp_path / "microstrategy_mock_mces.json")},
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "microstrategy_mock_mces.json",
        golden_path=test_resources_dir / "microstrategy_mces_golden_mock.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_microstrategy_ingest_mock_warehouse_lineage(
    pytestconfig, tmp_path, requests_mock
):
    """
    Mock-based test variant with warehouse lineage enabled.

    Verifies that the cube emits an upstreamLineage aspect pointing to the
    Snowflake warehouse table parsed from the cube's SQL view.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/microstrategy"

    # ── Auth ──────────────────────────────────────────────────────────────────
    requests_mock.post(
        f"{BASE_URL}/api/auth/login",
        headers={"X-MSTR-AuthToken": AUTH_TOKEN},
        json={},
    )
    requests_mock.put(f"{BASE_URL}/api/sessions", json={})
    requests_mock.post(f"{BASE_URL}/api/auth/logout", json={})

    # ── Projects ──────────────────────────────────────────────────────────────
    requests_mock.get(f"{BASE_URL}/api/projects", json=PROJECTS_RESPONSE)

    # ── Datasources — resolves to snowflake ──────────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/projects/{PROJECT_ID}/datasources",
        json=PROJECT_DATASOURCES_RESPONSE,
    )

    # ── Folders ───────────────────────────────────────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/v2/projects/{PROJECT_ID}/folders",
        json=_paginated(FOLDERS_RESPONSE),
    )

    # ── Search results — reports include dataSource for full chain lineage ──────
    _search_mapping_lineage = {
        "776": _paginated(SEARCH_TYPE776_RESPONSE),
        "55": _paginated(SEARCH_TYPE55_RESPONSE),
        "3": _paginated(SEARCH_TYPE3_LINEAGE_RESPONSE),
    }

    def _search_callback_lineage(request, context):
        type_val = request.qs.get("type", [""])[0]
        return _search_mapping_lineage.get(type_val, _empty_paginated())

    requests_mock.get(
        f"{BASE_URL}/api/searches/results",
        json=_search_callback_lineage,
    )

    # ── Dossier definition ────────────────────────────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/v2/dossiers/{DASHBOARD_ID}/definition",
        json=DOSSIER_DEFINITION,
    )

    # ── Cube schema + SQL view ────────────────────────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/v2/cubes/{CUBE_ID}",
        json=CUBE_SCHEMA,
    )
    requests_mock.get(
        f"{BASE_URL}/api/v2/cubes/{CUBE_ID}/sqlView",
        json={
            "sqlStatement": 'SELECT region_id, revenue FROM "analytics"."fact_revenue"'
        },
    )

    # ── Datasets ──────────────────────────────────────────────────────────────
    requests_mock.get(
        f"{BASE_URL}/api/v2/projects/{PROJECT_ID}/datasets",
        json=_paginated(DATASETS_RESPONSE),
    )

    pipeline = Pipeline.create(
        {
            "run_id": "microstrategy-mock-lineage-test",
            "source": {
                "type": "microstrategy",
                "config": {
                    "connection": {
                        "base_url": BASE_URL,
                        "username": "admin",
                        "password": "admin",
                    },
                    "include_lineage": True,
                    "include_ownership": True,
                    "include_folders": False,
                    "include_dashboards": False,
                    "include_reports": True,
                    "include_cubes": True,
                    "include_cube_schema": False,
                    "include_datasets": False,
                    "include_warehouse_lineage": True,
                    "warehouse_lineage_database": "prod_db",
                    "warehouse_lineage_schema": "analytics",
                    "include_report_definitions": False,
                    "preflight_dashboard_exists": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "microstrategy_mock_lineage_mces.json")
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "microstrategy_mock_lineage_mces.json",
        golden_path=test_resources_dir / "microstrategy_mces_golden_mock_lineage.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )
