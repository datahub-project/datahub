import json
import logging
import os
import tempfile
from pathlib import Path
from random import randint

import pytest
import yaml
from click.testing import CliRunner

from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.entrypoints import datahub
from datahub.ingestion.graph.client import DataHubGraph
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, get_sleep_info

logger = logging.getLogger(__name__)

# Generate random dataset IDs to avoid test interference
start_index = randint(10, 10000)
dataset_id = f"test_dataset_sync_{start_index}"
dataset_urn = make_dataset_urn("snowflake", dataset_id)

sleep_sec, sleep_times = get_sleep_info()
runner = CliRunner(mix_stderr=False)


@pytest.fixture(scope="module")
def setup_teardown_dataset(graph_client: DataHubGraph):
    """Fixture to setup and teardown the test dataset"""
    # Setup: Create empty environment
    try:
        # Teardown any existing dataset first to ensure clean state
        if graph_client.exists(dataset_urn):
            delete_urns(graph_client, [dataset_urn])
            wait_for_writes_to_sync()

        yield
    finally:
        # Teardown: Clean up created dataset
        if graph_client.exists(dataset_urn):
            delete_urns(graph_client, [dataset_urn])
            wait_for_writes_to_sync()


def create_dataset_yaml(file_path: Path, additional_properties=None):
    """Create a test dataset YAML file"""
    dataset_yaml = {
        "id": dataset_id,
        "platform": "snowflake",
        "env": "PROD",
        "description": "Test dataset for CLI sync smoke test",
        "schema": {
            "fields": [
                {"id": "id", "type": "number", "description": "Primary key"},
                {"id": "name", "type": "string", "description": "User name"},
            ]
        },
        "tags": ["test", "smoke_test"],
        "properties": {"origin": "cli_test"},
    }

    # Add any additional properties
    if additional_properties:
        for key, value in additional_properties.items():
            dataset_yaml[key] = value

    # Write YAML file
    with open(file_path, "w") as f:
        f.write("# Dataset sync test\n")  # Add a comment to test comment preservation
        yaml.dump(dataset_yaml, f, indent=2)


def run_cli_command(cmd, auth_session):
    """Run a DataHub CLI command using CliRunner and auth_session"""
    args = cmd.split()
    result = runner.invoke(
        datahub,
        args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    if result.exit_code != 0:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"STDOUT: {result.stdout}")
        logger.error(f"STDERR: {result.stderr}")
        raise Exception(f"Command failed with return code {result.exit_code}")

    return result


def test_dataset_sync_to_datahub(
    setup_teardown_dataset, graph_client: DataHubGraph, auth_session
):
    """Test syncing dataset from YAML to DataHub"""
    with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as tmp:
        temp_file_path = Path(tmp.name)
        try:
            # Create a dataset YAML file
            create_dataset_yaml(temp_file_path)

            # Run the CLI command to sync to DataHub
            cmd = f"dataset sync -f {temp_file_path} --to-datahub"
            result = run_cli_command(cmd, auth_session)

            # Verify success message in output
            assert f"Update succeeded for urn {dataset_urn}" in result.stdout

            # Wait for changes to propagate
            wait_for_writes_to_sync()

            # Verify the dataset exists in DataHub
            assert graph_client.exists(dataset_urn)

            # Retrieve dataset and verify properties
            dataset = Dataset.from_datahub(graph=graph_client, urn=dataset_urn)
            assert dataset.id == dataset_id
            assert dataset.platform == "snowflake"
            assert dataset.description == "Test dataset for CLI sync smoke test"
            assert dataset.tags == ["test", "smoke_test"]
            assert dataset.properties is not None
            assert dataset.properties["origin"] == "cli_test"

        finally:
            # Clean up temporary file
            if temp_file_path.exists():
                os.unlink(temp_file_path)


def test_dataset_sync_from_datahub(
    setup_teardown_dataset, graph_client: DataHubGraph, auth_session
):
    """Test syncing dataset from DataHub to YAML"""
    with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as tmp:
        temp_file_path = Path(tmp.name)
        try:
            # First, create a dataset in DataHub
            dataset = Dataset(
                id=dataset_id,
                platform="snowflake",
                description="Test dataset created directly in DataHub",
                tags=["from_datahub", "cli_test"],
                properties={"origin": "direct_creation"},
                schema=None,
            )

            # Emit the dataset to DataHub
            for mcp in dataset.generate_mcp():
                graph_client.emit(mcp)

            wait_for_writes_to_sync()

            # Create a minimal dataset YAML as a starting point
            create_dataset_yaml(
                temp_file_path, {"description": "This will be overwritten"}
            )

            # Run the CLI command to sync from DataHub
            cmd = f"dataset sync -f {temp_file_path} --from-datahub"
            result = run_cli_command(cmd, auth_session)
            assert result.exit_code == 0

            # Wait to ensure file is updated
            wait_for_writes_to_sync()
            # Verify the YAML file was updated
            with open(temp_file_path, "r") as f:
                content = f.read()
                # Check for the comment preservation
                assert "# Dataset sync test" in content
                # Check for content from DataHub
                assert "Test dataset created directly in DataHub" in content
                assert "from_datahub" in content
                assert "direct_creation" in content

        finally:
            # Clean up temporary file
            if temp_file_path.exists():
                os.unlink(temp_file_path)


def test_dataset_sync_bidirectional(
    setup_teardown_dataset, graph_client: DataHubGraph, auth_session
):
    """Test bidirectional sync with modifications on both sides"""
    with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as tmp:
        temp_file_path = Path(tmp.name)
        try:
            # 1. Create initial dataset in YAML
            create_dataset_yaml(temp_file_path)

            # 2. Sync to DataHub
            run_cli_command(
                f"dataset sync -f {temp_file_path} --to-datahub", auth_session
            )
            wait_for_writes_to_sync()

            # 3. Modify directly in DataHub
            dataset_patcher = Dataset.get_patch_builder(dataset_urn)
            dataset_patcher.set_description("Modified directly in DataHub")
            for mcp in dataset_patcher.build():
                graph_client.emit(mcp)
            wait_for_writes_to_sync()

            # 4. Sync from DataHub to update YAML
            run_cli_command(
                f"dataset sync -f {temp_file_path} --from-datahub", auth_session
            )

            # 5. Modify the YAML file directly
            with open(temp_file_path, "r") as f:
                import yaml

                data = yaml.safe_load(f)
            data["properties"]["modified_by"] = "cli_test"
            data["tags"].append("modified_yaml")

            with open(temp_file_path, "w") as f:
                f.write("# Modified comment\n")
                json.dump(data, f, indent=2)

            # 6. Sync back to DataHub
            run_cli_command(
                f"dataset sync -f {temp_file_path} --to-datahub", auth_session
            )
            wait_for_writes_to_sync()

            # 7. Verify both modifications are present in DataHub
            final_dataset = Dataset.from_datahub(graph=graph_client, urn=dataset_urn)
            assert final_dataset.description == "Modified directly in DataHub"
            assert final_dataset.properties is not None
            assert final_dataset.properties["modified_by"] == "cli_test"
            assert final_dataset.tags is not None
            assert "modified_yaml" in final_dataset.tags

            # 8. Sync one more time from DataHub and verify YAML is intact
            run_cli_command(
                f"dataset sync -f {temp_file_path} --from-datahub", auth_session
            )

            with open(temp_file_path, "r") as f:
                content = f.read()
                assert "# Modified comment" in content

        finally:
            # Clean up temporary file
            if temp_file_path.exists():
                os.unlink(temp_file_path)


def test_dataset_sync_validation(
    setup_teardown_dataset, graph_client: DataHubGraph, auth_session
):
    """Test validation during sync to DataHub with invalid references"""
    with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as tmp:
        temp_file_path = Path(tmp.name)
        try:
            # Create dataset with invalid structured property reference
            create_dataset_yaml(
                temp_file_path,
                {
                    "structured_properties": {"non_existent_property": "value"},
                    "schema": {
                        "fields": [
                            {
                                "id": "field1",
                                "type": "string",
                                "structured_properties": {
                                    "non_existent_field_property": "value"
                                },
                            }
                        ]
                    },
                },
            )

            # Attempt to sync to DataHub - should fail due to validation
            cmd = f"dataset sync -f {temp_file_path} --to-datahub"
            try:
                run_cli_command(cmd, auth_session)
                # If we get here, the command didn't fail as expected
                raise AssertionError("Command should have failed due to validation")
            except Exception as e:
                if not isinstance(e, AssertionError):
                    # Command failed as expected
                    pass
                else:
                    raise e

        finally:
            # Clean up temporary file
            if temp_file_path.exists():
                os.unlink(temp_file_path)
