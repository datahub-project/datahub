"""Integration tests for Fabric OneLake source.

These tests use mocked REST API responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricLakehouse,
    FabricTable,
    FabricWorkspace,
)
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-15 12:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fabric_onelake_workspace_ingestion() -> None:
    """Test ingestion of a single workspace."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[
                    FabricWorkspace(
                        id="ws-123",
                        name="Test Workspace",
                        description="Test description",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_lakehouses", return_value=[]),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
        ):
            # Run pipeline
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            }
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            # Verify output file was created and contains expected data
            assert Path(output_file).exists()
            with open(output_file) as f:
                data = json.load(f)
                # Should contain at least one workspace container
                assert len(data) > 0

    finally:
        Path(output_file).unlink(missing_ok=True)


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fabric_onelake_lakehouse_with_tables(pytestconfig: pytest.Config) -> None:
    """Test ingestion of a lakehouse with tables."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                return_value=[
                    FabricLakehouse(
                        id="lh-456",
                        name="Test Lakehouse",
                        workspace_id="ws-123",
                        type="Lakehouse",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
            patch.object(
                OneLakeClient,
                "list_lakehouse_tables",
                return_value=[
                    FabricTable(
                        name="customers",
                        schema_name="dbo",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                    FabricTable(
                        name="orders",
                        schema_name="dbo",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                ],
            ),
        ):
            # Run pipeline
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            }
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            # Validate against golden file
            golden_path = (
                Path(__file__).parent
                / "golden"
                / "test_fabric_onelake_lakehouse_with_tables_golden.json"
            )
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=output_file,
                golden_path=str(golden_path),
            )

    finally:
        Path(output_file).unlink(missing_ok=True)
