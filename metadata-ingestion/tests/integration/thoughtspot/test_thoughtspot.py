"""Integration tests for ThoughtSpot connector.

Mocked API responses are wired up in ``conftest.py``'s
``mock_thoughtspot_api`` fixture; this module contains only the
test bodies and pipeline-level assertions.
"""

import json

import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.thoughtspot.source import ThoughtSpotSource
from datahub.testing import mce_helpers
from tests.test_helpers import test_connection_helpers

# FROZEN_TIME is re-imported from conftest via pytest's auto-discovery,
# but @time_machine.travel decorators below need the literal value at
# module import time — keep it in sync with conftest.py.
FROZEN_TIME = "2024-01-15 09:00:00"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_ingest(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test end-to-end ThoughtSpot ingestion with golden file validation.

    This test:
    1. Runs full ingestion pipeline with mocked API responses
    2. Validates output against golden file
    3. Verifies all entity types are extracted (containers, dashboards, charts, datasets)
    """
    output_file = tmp_path / "thoughtspot_mces.json"
    golden_file = test_resources_dir / "thoughtspot_mces_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                    "platform_instance": "prod",
                    "include_ownership": True,  # Enable ownership extraction
                    # Exercise the metadata_search include_stats path
                    # end-to-end. Fixtures above carry top-level ``stats``
                    # blocks so the rewritten _process_usage_stats emits
                    # DashboardUsageStatistics / ChartUsageStatistics
                    # aspects on each Liveboard/Answer.
                    "include_usage_stats": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_ingest_with_filters(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test ThoughtSpot ingestion with entity filtering patterns.

    Verifies that workspace_pattern, liveboard_pattern, etc. correctly filter entities.
    """
    output_file = tmp_path / "thoughtspot_filtered_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-filtered-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                    "workspace_pattern": {
                        "allow": ["^Sales.*"],
                    },
                    "liveboard_pattern": {
                        "allow": [".*"],
                        "deny": ["^Draft.*"],
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify filtered output
    with open(output_file) as f:
        output = json.load(f)

    # Should only have Sales Analytics workspace (not Marketing Dashboard)
    # Find container names to verify filtering
    workspace_names_found = set()
    for mcp in output:
        if (
            mcp.get("entityType") == "container"
            and mcp.get("aspectName") == "containerProperties"
        ):
            name = mcp.get("aspect", {}).get("json", {}).get("name")
            if name:
                workspace_names_found.add(name)

    assert "Sales Analytics" in workspace_names_found, (
        "Sales Analytics workspace should be present"
    )
    assert "Marketing Dashboard" not in workspace_names_found, (
        "Marketing Dashboard should be filtered out"
    )


def test_thoughtspot_connection_success(mock_thoughtspot_api):
    """Test that valid credentials succeed connection test."""
    report = test_connection_helpers.run_test_connection(
        ThoughtSpotSource,
        {
            "connection": {
                "base_url": "https://test.thoughtspot.cloud",
                "auth": {
                    "type": "trusted",
                    "username": "testuser",
                    "secret_key": "test-token-12345",
                },
            },
        },
    )
    test_connection_helpers.assert_basic_connectivity_success(report)


# Connection error tests removed - the client.test_connection() method catches
# all exceptions internally and returns False, so we can't test specific error
# scenarios through the test_connection interface. Error handling is verified
# through unit tests in test_thoughtspot_source.py instead.


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_schema_extraction(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test that schema metadata (columns) are extracted from logical tables.

    This verifies schema metadata extraction from the TML export endpoint.
    Datasets should have schemaMetadata aspects with column definitions.
    """
    output_file = tmp_path / "thoughtspot_schema_test.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-schema-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Load output and verify schema metadata exists
    with open(output_file) as f:
        output = json.load(f)

    # Find schemaMetadata aspects
    schema_aspects = [
        mcp for mcp in output if mcp.get("aspectName") == "schemaMetadata"
    ]

    assert len(schema_aspects) > 0, "No schemaMetadata aspects found"

    # Verify at least one schema has column information
    found_columns = False
    for schema_aspect in schema_aspects:
        fields = schema_aspect.get("aspect", {}).get("json", {}).get("fields", [])
        if len(fields) > 0:
            found_columns = True
            # Verify column structure
            first_field = fields[0]
            assert "fieldPath" in first_field
            assert "nativeDataType" in first_field
            break

    assert found_columns, "No columns found in schemaMetadata aspects"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_lineage_extraction(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test that lineage is extracted for Answers and Visualizations.

    This verifies:
    - Answer → Dataset lineage (from source_tables field)
    - Visualization → Answer lineage (from answer_id field)
    - Dashboard → Chart references (via chartEdges in dashboardInfo)
    """
    output_file = tmp_path / "thoughtspot_lineage_test.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-lineage-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Load output and verify lineage exists
    with open(output_file) as f:
        output = json.load(f)

    # SDK V2 stores lineage in chartInfo.inputs field, not separate upstreamLineage aspects
    # Find chartInfo aspects with inputs (lineage)
    chart_infos_with_inputs = [
        mcp
        for mcp in output
        if mcp.get("aspectName") == "chartInfo"
        and mcp.get("aspect", {}).get("json", {}).get("inputs")
    ]

    assert len(chart_infos_with_inputs) > 0, "No charts with lineage inputs found"

    # Verify at least one chart has upstream references in inputs field
    found_lineage = False
    for chart_info in chart_infos_with_inputs:
        inputs = chart_info.get("aspect", {}).get("json", {}).get("inputs", [])
        if len(inputs) > 0:
            found_lineage = True
            # Verify input structure (SDK V2 format: [{"string": "urn:..."}])
            first_input = inputs[0]
            assert "string" in first_input, "Input should have 'string' field"
            assert "urn:li:" in first_input["string"], "Input should be a valid URN"
            break

    assert found_lineage, "No lineage inputs found in chartInfo aspects"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_thoughtspot_container_hierarchy(
    pytestconfig, tmp_path, mock_thoughtspot_api, test_resources_dir, mock_time
):
    """
    Test that container hierarchy is correctly established: workspace→liveboard→visualization.

    This test verifies:
    1. Workspaces are emitted as container entities
    2. Liveboards reference their parent workspace container
    3. Visualizations are emitted as charts within liveboards
    4. The container relationship chain is properly maintained
    """
    output_file = tmp_path / "thoughtspot_container_hierarchy_test.json"

    pipeline = Pipeline.create(
        {
            "run_id": "thoughtspot-container-hierarchy-test",
            "source": {
                "type": "thoughtspot",
                "config": {
                    "connection": {
                        "base_url": "https://test.thoughtspot.cloud",
                        "auth": {
                            "type": "trusted",
                            "username": "testuser",
                            "secret_key": "test-token-12345",
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Load output and analyze container hierarchy
    with open(output_file) as f:
        output = json.load(f)

    # Step 1: Verify workspace containers exist
    workspace_containers = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "container"
        and mcp.get("aspectName") == "containerProperties"
    ]
    assert len(workspace_containers) > 0, "No workspace containers found"

    # Extract workspace container URNs
    workspace_urns = {mcp.get("entityUrn") for mcp in workspace_containers}
    assert len(workspace_urns) > 0, "No workspace container URNs found"

    # Step 2: Verify liveboards reference workspace containers
    liveboard_containers = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "dashboard" and mcp.get("aspectName") == "container"
    ]
    assert len(liveboard_containers) > 0, "No liveboards with container aspects found"

    # Verify at least one liveboard references a workspace
    found_liveboard_workspace_link = False
    for liveboard_container in liveboard_containers:
        container_urn = (
            liveboard_container.get("aspect", {}).get("json", {}).get("container")
        )
        if container_urn and container_urn in workspace_urns:
            found_liveboard_workspace_link = True
            break

    assert found_liveboard_workspace_link, (
        "No liveboards reference workspace containers"
    )

    # Step 3: Verify visualizations are emitted as charts
    chart_info = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "chart" and mcp.get("aspectName") == "chartInfo"
    ]
    assert len(chart_info) > 0, "No chart entities found for visualizations"

    # Step 4: Verify dashboards reference their charts
    dashboard_info = [
        mcp
        for mcp in output
        if mcp.get("entityType") == "dashboard"
        and mcp.get("aspectName") == "dashboardInfo"
    ]
    assert len(dashboard_info) > 0, "No dashboard info aspects found"

    # Verify at least one dashboard has chart references
    # SDK V2 uses chartEdges, not charts field
    found_dashboard_chart_link = False
    for dashboard in dashboard_info:
        chart_edges = dashboard.get("aspect", {}).get("json", {}).get("chartEdges", [])
        if len(chart_edges) > 0:
            found_dashboard_chart_link = True
            # Verify the chart URN format is correct
            first_edge = chart_edges[0]
            dest_urn = first_edge.get("destinationUrn", "")
            assert dest_urn.startswith("urn:li:chart:"), (
                f"Invalid chart URN format: {dest_urn}"
            )
            break

    assert found_dashboard_chart_link, "No dashboards reference chart entities"
